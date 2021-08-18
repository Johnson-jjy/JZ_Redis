package tcp

import (
	"context"
	"net"
)

// HandleFunc represents application handler function
type HandleFunc func(ctx context.Context, conn net.Conn)

// Handler represents application server over tcp (是应用层服务器的抽象)
type Handler interface {
	Handle(ctx context.Context, conn net.Conn)  // 应用层服务都提供一个"处理方法", 传入context用于关闭, 传入conn作为处理对象
	Close() error // 对可能发生的错误进行相关处理
}


