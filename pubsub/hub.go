package pubsub

import (
	"JZ_Redis/datastruct/dict"
	"JZ_Redis/datastruct/lock"
)

// Hub stores all subscribe relations
type Hub struct {
	// channel -> list(*Client)
	subs dict.Dict
	// lock channel
	subsLocker *lock.Locks
}

// MakeHub creates new hub
func MakeHub() *Hub {
	return &Hub{
		subs: dict.MakeConcurrent(4),
		subsLocker: lock.Make(16),
	}
}
