package db

import (
	"encoding/json"
	"fmt"
	"log"

	clockspb "github.com/WadeCappa/consensus/gen/go/clocks/v1"
)

type Ordering int

const (
	Before Ordering = iota
	After
	Concurrent
	Equal
)

type Clock struct {
	clock map[uint64]uint64
}

func From(clock map[uint64]uint64) *Clock {
	return &Clock{
		clock: clock,
	}
}

func newClock(localId, startingVersion uint64) *Clock {
	return &Clock{
		clock: map[uint64]uint64{
			localId: startingVersion,
		},
	}
}

func FromWireType(clock *clockspb.VectorClock) *Clock {
	return &Clock{
		clock: clock.Clock,
	}
}

func (c *Clock) ToWireType() *clockspb.VectorClock {
	return &clockspb.VectorClock{
		Clock: c.clock,
	}
}

func Order(a, b *Clock) Ordering {
	var aFirst, bFirst bool
	for key, aValue := range a.clock {
		bValue := b.clock[key]
		if aValue < bValue {
			aFirst = true
		} else if bValue < aValue {
			bFirst = true
		}
	}

	for key, bValue := range b.clock {
		if _, ok := a.clock[key]; ok {
			continue
		}
		if bValue > 0 {
			aFirst = true
		}
	}

	if aFirst && !bFirst {
		return Before
	}
	if bFirst && !aFirst {
		return After
	}
	if bFirst && aFirst {
		return Concurrent
	}

	return Equal
}

func (c *Clock) getVersion(nodeId uint64) uint64 {
	return c.clock[nodeId]
}

func (c *Clock) set(id, newVersion uint64) {
	c.clock[id] = newVersion
}

func (c *Clock) toString() string {
	jsonData, err := json.Marshal(c.clock)
	if err != nil {
		log.Fatal(fmt.Errorf("serializing clock: %w", err))
	}

	return string(jsonData)
}
