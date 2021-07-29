package tcp

import (
	"context"
	"net"
)

// HandleFunc represents application handler function
type HandleFunc func(ctx context.Context, conn net.Conn)

// Handler represents application server over tcp (是应用层服务器的抽象)
type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}


