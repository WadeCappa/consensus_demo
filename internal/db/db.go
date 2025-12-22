package db

import (
	"fmt"
	"sync"
)

type Database struct {
	lock sync.Mutex
	data map[string]*Record
}

func NewDatabase() *Database {
	return &Database{
		data: map[string]*Record{},
		lock: sync.Mutex{},
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

func (d *Database) Put(key string, data *Record) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	prev, exists := d.data[key]
	if !exists {
		d.data[key] = data.Bump()
		return nil
	}

	if prev.Version != data.Version {
		return fmt.Errorf("version of %d did not match expected version of %d", data.Version, prev.Version)
	}

	d.data[key] = data.Bump()
	return nil
}
