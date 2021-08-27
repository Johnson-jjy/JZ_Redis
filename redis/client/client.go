package client

import (
	"JZ_Redis/interface/redis"
	"JZ_Redis/lib/logger"
	"JZ_Redis/lib/sync/wait"
	"JZ_Redis/redis/parser"
	"JZ_Redis/redis/reply"
	"net"
	"runtime/debug"
	"sync"
	"time"
)

// Client is a pipeline mode redis client
type Client struct {
	conn        net.Conn		// 与服务端的 tcp 连接
	pendingReqs chan *request // wait to send 等待发送的请求
	waitingReqs chan *request // waiting response 等待服务器响应的请求
	ticker      *time.Ticker // 用于触发心跳包的计时器
	addr        string

	// 有请求正在处理不能立即停止，用于实现 graceful shutdown
	working *sync.WaitGroup // its counter presents unfinished requests(pending and waiting)
}

// request is a message sends to redis server
type request struct {
	id        uint64 // 请求id
	args      [][]byte // 上行参数
	reply     redis.Reply // 收到的返回值
	heartbeat bool // 标记是否是心跳请求
	waiting   *wait.Wait // 调用协程发送请求后通过 waitgroup 等待请求异步处理完成
	err       error
}

const (
	chanSize = 256
	maxWait  = 3 * time.Second
)

// MakeClient creates a new client  Client构造器
func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		addr:        addr,
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		working:     &sync.WaitGroup{},
	}, nil
}

// Start starts asynchronous goroutines  启动异步协程的代码
func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second)
	go client.handleWrite()
	go func() {
		err := client.handleRead()
		if err != nil {
			logger.Error(err)
		}
	}()
	go client.heartbeat()
}

// Close stops asynchronous goroutines and close connection
// 关闭 client 的时候记得等待请求完成
func (client *Client) Close() {
	client.ticker.Stop()
	// stop new request 先阻止新请求进入队列
	close(client.pendingReqs)

	// wait stop process 等待处理中的请求完成
	client.working.Wait()

	// clean 释放资源
	_ = client.conn.Close() // 关闭与服务端的连接，连接关闭后读协程会退出
	close(client.waitingReqs) // 关闭队列
}

func (client *Client) handleConnectionError(err error) error {
	err1 := client.conn.Close()
	if err1 != nil {
		if opErr, ok := err1.(*net.OpError); ok {
			if opErr.Err.Error() != "use of closed network connection" {
				return err1
			}
		} else {
			return err1
		}
	}
	conn, err1 := net.Dial("tcp", client.addr)
	if err1 != nil {
		logger.Error(err1)
		return err1
	}
	client.conn = conn
	go func() {
		_ = client.handleRead()
	}()
	return nil
}

func (client *Client) heartbeat() {
	for range client.ticker.C {
		client.doHeartbeat()
	}
}

// 写协程入口
func (client *Client) handleWrite() {
	for req := range client.pendingReqs {
		client.doRequest(req)
	}
}

// Send sends a request to redis server
// 调用者将请求发送给后台协程，并通过 wait group 等待异步处理完成
func (client *Client) Send(args [][]byte) redis.Reply {
	request := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingReqs <- request // 请求入队
	timeout := request.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return reply.MakeErrReply("server time out")
	}
	if request.err != nil {
		return reply.MakeErrReply("request failed")
	}
	return request.reply
}

func (client *Client) doHeartbeat() {
	request := &request{
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingReqs <- request
	request.waiting.WaitWithTimeout(maxWait)
}

// 发送请求
func (client *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}
	// 序列化请求
	re := reply.MakeMultiBulkReply(req.args)
	bytes := re.ToBytes()
	_, err := client.conn.Write(bytes)
	i := 0
	// 失败重试
	for err != nil && i < 3 {
		err = client.handleConnectionError(err)
		if err == nil {
			_, err = client.conn.Write(bytes)
		}
		i++
	}
	if err == nil {
		// 发送成功等待服务器响应
		client.waitingReqs <- req
	} else {
		req.err = err
		req.waiting.Done()
	}
}

// 收到服务端的响应
func (client *Client) finishRequest(reply redis.Reply) {
	defer func() {
		if err := recover(); err != nil { // ? 这里的recover是针对谁处理?
			debug.PrintStack()
			logger.Error(err)
		}
	}()
	request := <-client.waitingReqs
	if request == nil {
		return
	}
	request.reply = reply
	if request.waiting != nil { // 为什么会有nil的情况?
		request.waiting.Done()
	}
}

// 读协程是个 RESP 协议解析器
func (client *Client) handleRead() error {
	ch := parser.ParseStream(client.conn)
	for payload := range ch {
		if payload.Err != nil {
			client.finishRequest(reply.MakeErrReply(payload.Err.Error()))
			continue
		}
		client.finishRequest(payload.Data)
	}
	return nil
}
