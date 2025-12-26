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
		for _, c := range chunks {
			alreadySeen := r.Clock.getVersion(c.nodeId)
			if c.version <= alreadySeen {
				fmt.Printf("encountered old chunk of version %d for node %d, but we have already seen %d\n", c.version, c.nodeId, alreadySeen)
				continue
			}
			r.Chunks = append(r.Chunks, c)
		}
		r.Clock = remoteClock
		return nil
	case After, Equal:
		fmt.Printf("encountered outdated clock of %s, where our clock is %s\n", remoteClock.toString(), r.Clock.toString())
		return nil
	case Concurrent:
		fmt.Printf("encountered concurrent clock of %s, where our clock is %s\n", remoteClock.toString(), r.Clock.toString())
		r.Chunks = mergeChunks(r.Clock, r.Chunks, chunks)
		r.Clock = r.Clock.Merge(remoteClock)
		return nil
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

func (r *Record) GetChunksSince(alreadySeenData *Clock) []*Chunk {
	var result []*Chunk
	for _, c := range r.Chunks {
		if c.version > alreadySeenData.getVersion(c.nodeId) {
			result = append(result, c)
		}
	}
	return result
}

func asTime(millis uint64) time.Time {
	return time.UnixMilli(int64(millis))
}

func mergeChunks(clock *Clock, a, b []*Chunk) []*Chunk {
	var result []*Chunk
	pa := 0
	pb := 0
	for {
		if pa == len(a) || pb == len(b) {
			break
		}
		if a[pa].writeTime.Before(b[pb].writeTime) {
			result = append(result, a[pa])
			pa += 1
		} else {
			c := b[pb]
			if c.version > clock.getVersion(c.nodeId) {
				result = append(result, c)
			}
			pb += 1
		}
	}
	if pa == len(a) {
		result = append(result, b[pb:]...)
	} else if pb == len(b) {
		for _, c := range a[pa:] {
			if c.version > clock.getVersion(c.nodeId) {
				result = append(result, c)
			}
		}
	}
	return result
}
