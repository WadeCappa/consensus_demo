package db

import (
	"sync"
)

type RemoteClocks struct {
	clocks map[string]map[uint64]*Clock
	lock   sync.Mutex
}

func NewRemoteClocks() *RemoteClocks {
	return &RemoteClocks{
		clocks: map[string]map[uint64]*Clock{},
		lock:   sync.Mutex{},
	}
}

func (r *RemoteClocks) Get(nodeId uint64, key string) *Clock {
	r.lock.Lock()
	defer r.lock.Unlock()

	record, exists := r.clocks[key]
	if !exists {
		return nil
	}

	clock, exists := record[nodeId]
	if !exists {
		return nil
	}

	return clock
}

func (r *RemoteClocks) Accept(nodeId uint64, key string, newClock *Clock) {
	r.lock.Lock()
	defer r.lock.Unlock()

	record, exists := r.clocks[key]
	if !exists {
		r.clocks[key] = map[uint64]*Clock{
			nodeId: newClock,
		}
	}

	if record == nil {
		r.clocks[key] = map[uint64]*Clock{
			nodeId: newClock,
		}
		return
	}

	oldClock, exists := record[nodeId]
	if !exists {
		record[nodeId] = newClock
		return
	}
	record[nodeId] = oldClock.Merge(newClock)
}
