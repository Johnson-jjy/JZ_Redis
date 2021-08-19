package tcp

import (
	"JZ_Redis/lib/logger"
	"JZ_Redis/lib/sync/atomic"
	"JZ_Redis/lib/sync/wait"
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

/**
 * A echo server to test whether the server is functioning normally
 * 接受客户端连接并将客户端发送的内容原样传回客户端
 */

// EchoHandler echos received line to client, using for test
type EchoHandler struct {
	// 保存所有工作状态client的集合(把map当set用)
	// 需使用并发安全的容器
	activeConn sync.Map
	// 关闭状态标识位
	closing    atomic.Boolean
}

// MakeEchoHandler creates EchoHandler
func MakeEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

// EchoClient is client for EchoHandler, using for test
type EchoClient struct {
	// tcp 连接
	Conn    net.Conn
	// 当服务端开始发送数据时进入waiting, 阻止其它goroutine关闭连接
	// wait.Wait是作者编写的带有最大等待时间的封装:
	Waiting wait.Wait
}

// Close close connection
func (c *EchoClient) Close() error {
	c.Waiting.WaitWithTimeout(10 * time.Second)
	c.Conn.Close()
	return nil
}

// Handle echos received line to client
func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// closing handler refuse new connection
		// 关闭中的 handler 不会处理新连接
		_ = conn.Close()
	}

	client := &EchoClient{
		Conn: conn,
	}
	h.activeConn.Store(client, struct{}{}) // 记住仍然存活的连接

	reader := bufio.NewReader(conn)
	for {
		// may occurs: client EOF, client timeout, server early close
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connection close")
				h.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}
		// 发送数据前先置为waiting状态，阻止连接被关闭
		client.Waiting.Add(1)

		// 模拟关闭时未完成发送的情况
		//logger.Info("sleeping")
		//time.Sleep(10 * time.Second)
		fmt.Printf("Server get it: %s\n", msg)
		b := []byte(msg)
		_, _ = conn.Write(b)
		// 发送完毕, 结束waiting
		client.Waiting.Done()
	}
}

// Close stops echo handler 关闭服务器
func (h *EchoHandler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	// 逐个关闭连接
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*EchoClient)
		_ = client.Close()
		return true
	})
	return nil
}