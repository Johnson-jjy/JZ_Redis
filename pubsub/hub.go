package pubsub

import (
	"JZ_Redis/DataStruct/dict"
	"JZ_Redis/DataStruct/lock"
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
	return &Hun{
		subs: dict.MakeConcurrent(4),
		subsLocker: lock.Make(16),
	}
}
