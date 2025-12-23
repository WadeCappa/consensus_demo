package db

import "fmt"

type Record struct {
	Clock *Clock
	Data  []byte
}

func newRecord(data []byte, clock *Clock) *Record {
	return &Record{
		Clock: clock,
		Data:  data,
	}
}

func (r *Record) Merge(data []byte, remoteClock *Clock) error {
	orderVal := Order(r.Clock, remoteClock)
	switch orderVal {
	case Before:
		r.Data = data
		r.Clock = remoteClock
		return nil
	case After, Equal:
		// do nothing
		return nil
	case Concurrent:
		return fmt.Errorf("cannot merge concurrent clocks: a) %s, b) %s", r.Clock.toString(), remoteClock.toString())
	default:
		return fmt.Errorf("unrecognized order value of %d", orderVal)
	}
}

func (r *Record) toDataVersion() *DataVersion {
	return NewDataVersion(r.Clock.getVersion(), r.Data)
}

func (r *Record) update(localId, newVersion uint64, update []byte) {
	r.Clock.set(localId, newVersion)
	r.Data = update
}
