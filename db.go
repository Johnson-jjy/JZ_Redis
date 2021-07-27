package JZ_Redis

import (
	"JZ_Redis/DataStruct/dict"
	"JZ_Redis/DataStruct/lock"
	"JZ_Redis/pubsub"
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
//	aofChan chan *reply.MultiBulkReply

}
