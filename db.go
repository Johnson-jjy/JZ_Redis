package JZ_Redis

import (
	"JZ_Redis/config"
	"JZ_Redis/datastruct/dict"
	"JZ_Redis/datastruct/lock"
	"JZ_Redis/interface/redis"
	"JZ_Redis/lib/logger"
	"JZ_Redis/pubsub"
	"JZ_Redis/redis/reply"
	"os"
	"sync"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize = 1 << 10
	lockerSize = 1024
	aofQueueSize = 1 << 16
)

// DB stores data and execute user's commands
type DB struct {
	// key -> DataEntity
	data dict.Dict
	// key -> expireTime (time.Time)
	ttlMap dict.Dict
	// key -> version(uint32)
	versionMap dict.Dict

	// dict.Dict will ensure concurrent-safety of ite method
	// use this mutex for complicated command only, eg. rpush, incr ...
	locker *lock.Locks
	// stop all data access for execFlushDB
	stopWorld sync.WaitGroup
	// handle publish/subscribe
	hub *pubsub.Hub

	// main goroutine send commands to aof goroutine through aofChan
	// 主线程使用此channel将要持久化的命令发送到异步协程
	aofChan     chan *reply.MultiBulkReply
	// append file 文件描述符
	aofFile     *os.File
	// append file路径
	aofFilename string
	// aof goroutine will send msg to main goroutine through this channel when aof tasks finished and ready to shutdown
	aofFinished chan struct{}

	// buffer commands received during aof rewrite progress
	// aof重写需要的缓冲区
	aofRewriteBuffer chan *reply.MultiBulkReply

	// pause aof for start/finish aof rewrite progress
	// 在必要的时候使用此字段停止持久化操作
	pausingAof sync.RWMutex
}

// DataEntity stores data bound to a key, including a string, list, hash, set and so on
type DataEntity struct {
	Data interface{}
}

// ExecFunc is interface for command executor
// args don't include cmd line
type ExecFunc func(db *DB, args [][]byte) redis.Reply

// PreFunc analyses command line when queued command to `multi`
// returns related write keys and read keys
type PreFunc func(args [][]byte) ([]string, []string)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// UndoFunc returns undo logs for the given command line
// execute from head to tail when undo
type UndoFunc func(db *DB, args [][]byte) []CmdLine

// MakeDB create DB instance and start it
func MakeDB() *DB {
	db := &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		locker:     lock.Make(lockerSize),
		hub:        pubsub.MakeHub(),
	}

	// aof
	if config.Properties.AppendOnly {
		db.aofFilename = config.Properties.AppendFilename
		db.loadAof(0)
		aofFile, err := os.OpenFile(db.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			logger.Warn(err)
		} else {
			db.aofFile = aofFile
			db.aofChan = make(chan *reply.MultiBulkReply, aofQueueSize)
		}
		db.aofFinished = make(chan struct{})
		go func() {
			db.handleAof()
		}()
	}
	return db
}