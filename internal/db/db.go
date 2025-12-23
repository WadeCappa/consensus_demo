package db

import (
	"fmt"
	"sync"
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

func (d *Database) Get(key string) (*DataVersion, bool) {
	d.lock.Lock()
	defer d.lock.Unlock()
	result, exists := d.data[key]
	if !exists {
		return nil, false
	}
	return result.toDataVersion(), true
}

func (d *Database) Put(key string, data *DataVersion) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	prev, exists := d.data[key]
	if !exists {
		d.data[key] = newRecord(data.Data, newClock(d.localId, data.Version+1))
		return nil
	}

	currentVersion := prev.Clock.getVersion()
	if currentVersion != data.Version {
		return fmt.Errorf("version of %d did not match expected version of %d", data.Version, currentVersion)
	}

	prev.update(d.localId, currentVersion+1, data.Data)
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

func (d *Database) Merge(key string, data []byte, remoteClock *Clock) error {
	d.lock.Lock()
	defer d.lock.Unlock()
	record, exists := d.data[key]
	if !exists {
		d.data[key] = newRecord(data, remoteClock)
		return nil
	}

	if err := record.Merge(data, remoteClock); err != nil {
		return fmt.Errorf("merging remote data with local data: %w", err)
	}
	return nil
}
