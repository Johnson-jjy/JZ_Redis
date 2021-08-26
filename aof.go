package JZ_Redis

import (
	"JZ_Redis/config"
	"JZ_Redis/datastruct/dict"
	"JZ_Redis/lib/logger"
	"JZ_Redis/redis/parser"
	"JZ_Redis/redis/reply"
	"godis/datastruct/lock"

	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

var pExpireAtBytes = []byte("PEXPIREAT")

func makeExpireCmd(key string, expireAt time.Time) *reply.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtBytes
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return reply.MakeMultiBulkReply(args)
}

// AddAof send command to aof goroutine through channel
func (db *DB) AddAof(args *reply.MultiBulkReply) {
	// aofChan == nil when loadAof
	if config.Properties.AppendOnly && db.aofChan != nil {
		db.aofChan <- args
	}
}

// handleAof listen aof channel and write into file
func (db *DB) handleAof() {
	for cmd := range db.aofChan {
		// todo: use switch and channels instead of mutex
		// 异步协程在持久化之前会尝试获取锁,若其他协程持有锁则会暂停持久化操作
		// 锁也保证了每次写入完整的一条指令不会格式错误
		db.pausingAof.RLock() // prevent other goroutines from pausing aof
		if db.aofRewriteBuffer != nil {
			// replica during rewrite 数据写入重写缓冲区
			// 在重写过程中，持久化协程进行双写 -> 正常写是不受影响的
			db.aofRewriteBuffer <- cmd
		}
		_, err := db.aofFile.Write(cmd.ToBytes())
		if err != nil {
			logger.Warn(err)
		}
		db.pausingAof.RUnlock()
	}
	db.aofFinished <- struct{}{}
}

// loadAof read aof file
func (db *DB) loadAof(maxBytes int) {
	// delete aofChan to prevent write again
	aofChan := db.aofChan
	db.aofChan = nil
	// 最后做一个替换
	defer func(aofChan chan *reply.MultiBulkReply) {
		db.aofChan = aofChan
	}(aofChan)

	// 打开文件开始写入
	file, err := os.Open(db.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}
	ch := parser.ParseStream(reader)
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		cmd := strings.ToLower(string(r.Args[0]))
		command, ok := cmdTable[cmd]
		if ok {
			handler := command.executor
			handler(db, r.Args[1:])
		}
	}
}

/*-- aof rewrite --*/
func (db *DB) aofRewrite() {
	file, fileSize, err := db.startRewrite()
	if err != nil {
		logger.Warn(err)
		return
	}

	// load aof file
	tmpDB := &DB{
		data:   dict.MakeSimple(),
		ttlMap: dict.MakeSimple(),
		locker: lock.Make(lockerSize),

		aofFilename: db.aofFilename,
	}
	// 只读取开始重写前aof文件的内容
	tmpDB.loadAof(int(fileSize))

	// rewrite aof file
	tmpDB.data.ForEach(func(key string, raw interface{}) bool {
		entity, _ := raw.(*DataEntity)
		cmd := EntityToCmd(key, entity)
		if cmd != nil {
			_, _ = file.Write(cmd.ToBytes())
		}
		return true
	})
	tmpDB.ttlMap.ForEach(func(key string, raw interface{}) bool {
		expireTime, _ := raw.(time.Time)
		cmd := makeExpireCmd(key, expireTime)
		if cmd != nil {
			_, _ = file.Write(cmd.ToBytes())
		}
		return true
	})

	db.finishRewrite(file)
}

func (db *DB) startRewrite() (*os.File, int64, error) {
	// 暂停AOF写入， 数据会在 db.aofChan 中暂时堆积
	db.pausingAof.Lock() // pausing aof
	defer db.pausingAof.Unlock()

	err := db.aofFile.Sync() // ?
	if err != nil {
		logger.Warn("fsync failed")
		return nil, 0, err
	}

	// create rewrite channel
	// 创建重写缓冲区
	db.aofRewriteBuffer = make(chan *reply.MultiBulkReply, aofQueueSize)

	// get current aof file size
	// 读取当前 aof 文件大小, 不读取重写过程中新写入的内容
	fileInfo, _ := os.Stat(db.aofFilename)
	filesize := fileInfo.Size()

	// create tmp file 创建临时文件
	file, err := ioutil.TempFile("", "aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, 0, err
	}
	return file, filesize, nil
}

// 重写完毕后写入缓冲区中的数据并替换正式文件
func (db *DB) finishRewrite(tmpFile *os.File) {
	db.pausingAof.Lock() // pausing aof 暂停AOF写入
	defer db.pausingAof.Unlock()

	// write commands created during rewriting to tmp file
	// 将重写缓冲区内的数据写入临时文件
	// 因为handleAof已被暂停，在遍历期间aofRewriteChan中不会有新数据
loop:
	for {
		// aof is pausing, there won't be any new commands in aofRewriteBuffer
		select {
		case cmd := <-db.aofRewriteBuffer:
			_, err := tmpFile.Write(cmd.ToBytes())
			if err != nil {
				logger.Warn(err)
			}
		default:
			// channel is empty, break loop 只有 channel 为空时才会进入此分支 -> 跳出
			break loop
		}
	}
	// 释放重写缓冲区
	close(db.aofRewriteBuffer)
	db.aofRewriteBuffer = nil

	// replace current aof file by tmp file
	// 使用临时文件代替aof文件
	_ = db.aofFile.Close()
	_ = os.Rename(tmpFile.Name(), db.aofFilename)

	// reopen aof file for further write
	// 重新打开文件描述符以保证正常写入
	aofFile, err := os.OpenFile(db.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	db.aofFile = aofFile
}