package lock

import "sync"

const (
	prime32 = uint32(16777619)
)

// Locks provides rw locks for key
type Locks struct {
	table []*sync.RWMutex
}
