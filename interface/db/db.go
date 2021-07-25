package db

import "JZ_Redis/interface/redis"

// DB is the interface for redis style storage engine
type DB interface {
	Exec(clent redis.Connection, args [][]byte) redis.Reply
	AfterClientClose(c redis.Connection)
	Close()
}