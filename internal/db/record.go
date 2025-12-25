package db

import (
	"fmt"
	"slices"
	"time"

	clockspb "github.com/WadeCappa/consensus/gen/go/clocks/v1"
)

type Record struct {
	Clock  *Clock
	Chunks []*Chunk
}

type Chunk struct {
	writeTime time.Time
	nodeId    uint64
	version   uint64
	data      []byte
}

func Concat(chunks []*Chunk) []byte {
	var result []byte
	for _, c := range chunks {
		result = append(result, c.data...)
	}
	return result
}

func NewRecord(clock *Clock, chunks []*Chunk) *Record {
	return &Record{
		Clock:  clock,
		Chunks: chunks,
	}
}

func NewChunk(nodeId, version uint64, writeTime time.Time, data []byte) *Chunk {
	return &Chunk{
		writeTime: writeTime,
		nodeId:    nodeId,
		version:   version,
		data:      data,
	}
}

func FromChunk(clock *Clock, update *Chunk) *Record {
	return &Record{
		Clock:  clock,
		Chunks: []*Chunk{update},
	}
}

func ChunksFromWireType(chunks []*clockspb.Chunk) []*Chunk {
	result := make([]*Chunk, len(chunks))
	for i, c := range chunks {
		result[i] = NewChunk(c.GetNodeId(), c.GetVersion(), asTime(c.GetWriteTimeUnixMillis()), c.GetData())
	}
	return result
}

func ChunksToWireType(chunks []*Chunk) []*clockspb.Chunk {
	result := make([]*clockspb.Chunk, len(chunks))
	for i, c := range chunks {
		result[i] = &clockspb.Chunk{
			NodeId:              c.nodeId,
			Version:             c.version,
			WriteTimeUnixMillis: uint64(c.writeTime.UnixMilli()),
			Data:                c.data,
		}
	}
	return result
}

func (r *Record) GetVersion(nodeId uint64) uint64 {
	return r.Clock.getVersion(nodeId)
}

func (r *Record) Merge(remoteClock *Clock, chunks []*Chunk) error {
	orderVal := Order(r.Clock, remoteClock)
	switch orderVal {
	case Before:
		r.Clock = remoteClock
		r.Chunks = append(r.Chunks, chunks[len(r.Chunks):]...)
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

func (r *Record) Update(nodeId, version uint64, updateTime time.Time, data []byte) {
	i := len(r.Chunks)
	for j, c := range r.Chunks {
		if updateTime.Before(c.writeTime) {
			i = j
			break
		}
	}

	r.Chunks = slices.Insert(r.Chunks, i, NewChunk(nodeId, version, updateTime, data))
	r.Clock.set(nodeId, version)
}

func (c *Chunk) Visit(f func(writeTime time.Time, nodeId uint64, version uint64, data []byte)) {
	f(c.writeTime, c.nodeId, c.version, c.data)
}

func asTime(millis uint64) time.Time {
	return time.UnixMilli(int64(millis))
}
