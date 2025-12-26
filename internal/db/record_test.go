package db_test

import (
	"testing"
	"time"

	"github.com/WadeCappa/consensus/internal/db"
	"github.com/stretchr/testify/require"
)

var (
	testNodeId = uint64(101)
)

func TestAcceptNewChunksIntoEmptyRecord(t *testing.T) {
	old := db.NewRecord(db.EmptyClock(), []*db.Chunk{})
	newChunks := []*db.Chunk{
		db.NewChunk(testNodeId, 1, time.Now(), []byte("test-data")),
	}
	newClock := db.From(map[uint64]uint64{
		testNodeId: 1,
	})
	require.NoError(t, old.Merge(newClock, newChunks))
	require.Equal(t, newChunks, old.Chunks)
	require.Equal(t, db.Equal, db.Order(old.Clock, newClock))
	require.Equal(t, newClock.ToWireType(), old.Clock.ToWireType())
}

func TestMergeEqualEntries(t *testing.T) {
	c := db.From(map[uint64]uint64{
		testNodeId: 1,
	})
	chunks := []*db.Chunk{
		db.NewChunk(testNodeId, 1, time.Now(), []byte("test-data")),
	}

	current := db.NewRecord(c, chunks)

	// Don't merge the same chunk. Since the vector clocks are equal we should drop this new
	// data without affecting the current record.
	require.NoError(t, current.Merge(c, []*db.Chunk{
		db.NewChunk(testNodeId, 1, time.Now(), []byte("different-data")),
	}))

	require.Equal(t, chunks, current.Chunks)
	require.Equal(t, db.Equal, db.Order(current.Clock, c))
	require.Equal(t, c.ToWireType(), current.Clock.ToWireType())
}

func TestRejectOutdataData(t *testing.T) {
	c := db.From(map[uint64]uint64{
		testNodeId: 1,
	})
	chunks := []*db.Chunk{
		db.NewChunk(testNodeId, 1, time.Now(), []byte("test-data")),
	}

	current := db.NewRecord(c, chunks)

	// Don't merge the same chunk. Since this new data is behind, we shouldn't merge.
	require.NoError(t, current.Merge(db.EmptyClock(), []*db.Chunk{
		db.NewChunk(testNodeId, 1, time.Now(), []byte("different-data")),
	}))

	require.Equal(t, chunks, current.Chunks)
	require.Equal(t, db.Equal, db.Order(current.Clock, c))
	require.Equal(t, c.ToWireType(), current.Clock.ToWireType())
}

func TestMergingNewDataFromSameNode(t *testing.T) {
	startingClock := db.From(map[uint64]uint64{
		testNodeId: 1,
	})
	chunks := []*db.Chunk{
		db.NewChunk(testNodeId, 1, time.Now(), []byte("test-data")),
	}

	current := db.NewRecord(startingClock, chunks)

	newChunks := []*db.Chunk{
		db.NewChunk(testNodeId, 2, time.Now(), []byte("more-data")),
	}

	newClock := db.From(map[uint64]uint64{
		testNodeId: 2,
	})
	require.NoError(t, current.Merge(newClock, newChunks))

	require.Equal(t, current.Chunks, []*db.Chunk{chunks[0], newChunks[0]})
	require.Equal(t, db.Equal, db.Order(current.Clock, newClock))
	require.Equal(t, newClock.ToWireType(), current.Clock.ToWireType())
}

func TestMergingConcurrentClocks(t *testing.T) {
	currentTime := time.Now()
	startingClock := db.From(map[uint64]uint64{
		testNodeId: 3,
	})
	chunks := []*db.Chunk{
		db.NewChunk(testNodeId, 1, currentTime.Add(time.Second), []byte("test-data")),
		db.NewChunk(testNodeId, 2, currentTime.Add(time.Second*3), []byte("test-data-2")),
		db.NewChunk(testNodeId, 3, currentTime.Add(time.Second*5), []byte("test-data-3")),
	}

	current := db.NewRecord(startingClock, chunks)

	newChunks := []*db.Chunk{
		db.NewChunk(testNodeId+1, 1, currentTime.Add(time.Second*2), []byte("more-data")),
		db.NewChunk(testNodeId+1, 2, currentTime.Add(time.Second*4), []byte("more-data-2")),
	}
	newClock := db.From(map[uint64]uint64{
		testNodeId + 1: 2,
	})
	require.NoError(t, current.Merge(newClock, newChunks))

	expectedClock := db.From(map[uint64]uint64{
		testNodeId:     3,
		testNodeId + 1: 2,
	})
	require.Equal(t, db.Equal, db.Order(expectedClock, current.Clock))
	require.Equal(t, []*db.Chunk{chunks[0], newChunks[0], chunks[1], newChunks[1]}, current.Chunks)
}

func TestMergingRepeatedData(t *testing.T) {
	currentTime := time.Now()
	startingClock := db.From(map[uint64]uint64{
		testNodeId: 1,
	})
	chunks := []*db.Chunk{
		db.NewChunk(testNodeId, 1, currentTime.Add(time.Second), []byte("test-data")),
	}

	current := db.NewRecord(startingClock, chunks)

	newChunks := []*db.Chunk{
		chunks[0],
		db.NewChunk(testNodeId, 2, currentTime.Add(time.Second*2), []byte("more-data")),
	}
	newClock := db.From(map[uint64]uint64{
		testNodeId: 2,
	})
	require.NoError(t, current.Merge(newClock, newChunks))

	require.Equal(t, db.Equal, db.Order(newClock, current.Clock))
	require.Equal(t, newChunks, current.Chunks)
}
