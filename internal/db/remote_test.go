package db_test

import (
	"testing"

	"github.com/WadeCappa/consensus/internal/db"
	"github.com/stretchr/testify/require"
)

func TestAcceptNew(t *testing.T) {
	clocks := db.NewRemoteClocks()
	testNodeId := uint64(1)
	testKey := "test-key"
	testClock := db.From(map[uint64]uint64{
		32: 43,
		12: 3,
	})
	clocks.Accept(testNodeId, testKey, testClock)

	result := clocks.Get(testNodeId, testKey)
	order := db.Order(testClock, result)
	require.Equal(t, db.Equal, order)
	require.Equal(t, testClock.ToWireType(), result.ToWireType())
}
