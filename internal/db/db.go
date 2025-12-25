package db

import (
	"fmt"
	"sync"
	"time"
)

type Database struct {
	lock    sync.Mutex
	data    map[string]*Record
	localId uint64
}

func NewDatabase(localId uint64) *Database {
	return &Database{
		data:    map[string]*Record{},
		lock:    sync.Mutex{},
		localId: localId,
	}
}

func (d *Database) Get(key string) (*Record, bool) {
	d.lock.Lock()
	defer d.lock.Unlock()
	result, exists := d.data[key]
	if !exists {
		return nil, false
	}
	return result, true
}

func (d *Database) Put(key string, update []byte, updateTime time.Time) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	prev, exists := d.data[key]
	if !exists {
		clock := newClock(d.localId, 1)
		chunk := NewChunk(d.localId, 1, updateTime, update)
		d.data[key] = FromChunk(clock, chunk)
		return nil
	}

	prev.Update(d.localId, prev.GetVersion(d.localId)+1, updateTime, update)
	return nil
}

func (d *Database) Range(consumer func(key string, record *Record) error) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	for key, val := range d.data {
		if err := consumer(key, val); err != nil {
			return fmt.Errorf("consuming db rows: %w", err)
		}
	}
	return nil
}

func (d *Database) Merge(key string, remoteClock *Clock, chunks []*Chunk) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	record, exists := d.data[key]
	if !exists {
		d.data[key] = NewRecord(remoteClock, chunks)
		return nil
	}

	if err := record.Merge(remoteClock, chunks); err != nil {
		return fmt.Errorf("merging remote data with local data: %w", err)
	}
	return nil
}
