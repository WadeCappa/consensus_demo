package db_test

import (
	"testing"

	"github.com/WadeCappa/consensus/internal/db"
	"github.com/stretchr/testify/require"
)

func TestOrdering(t *testing.T) {
	tests := []struct {
		name     string
		first    map[uint64]uint64
		second   map[uint64]uint64
		expected db.Ordering
	}{
		{
			name: "first_clock_before",
			first: map[uint64]uint64{
				0: 4,
				1: 4,
				2: 4,
			},
			second: map[uint64]uint64{
				0: 4,
				1: 5,
				2: 4,
			},
			expected: db.Before,
		},
		{
			name: "second_clock_before",
			first: map[uint64]uint64{
				0: 4,
				1: 4,
				2: 4,
			},
			second: map[uint64]uint64{
				0: 4,
				1: 3,
				2: 4,
			},
			expected: db.After,
		},
		{
			name: "clocks_equal",
			first: map[uint64]uint64{
				0: 4,
				1: 4,
				2: 4,
			},
			second: map[uint64]uint64{
				0: 4,
				1: 4,
				2: 4,
			},
			expected: db.Equal,
		},
		{
			name: "clocks_concurrent",
			first: map[uint64]uint64{
				0: 4,
				1: 2,
				2: 4,
			},
			second: map[uint64]uint64{
				0: 3,
				1: 4,
				2: 4,
			},
			expected: db.Concurrent,
		},
		{
			name: "first_has_new_data",
			first: map[uint64]uint64{
				0: 4,
				1: 4,
			},
			second: map[uint64]uint64{
				0: 4,
			},
			expected: db.After,
		},
		{
			name: "second_has_new_data",
			first: map[uint64]uint64{
				0: 4,
			},
			second: map[uint64]uint64{
				0: 4,
				1: 4,
			},
			expected: db.Before,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := db.Order(db.From(test.first), db.From(test.second))
			require.Equal(t, test.expected, result)
		})
	}
}
