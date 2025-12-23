package db_test

import (
	"testing"

	"github.com/WadeCappa/consensus/internal/db"
	"github.com/stretchr/testify/require"
)

func TestMerging(t *testing.T) {
	tests := []struct {
		name     string
		old      *db.Record
		new      *db.Record
		expected *db.Record
	}{
		{
			name: "merge_value_into_empty",
			old: &db.Record{
				Data: []byte("old-value"),
				Clock: db.From(
					map[uint64]uint64{},
				),
			},
			new: &db.Record{
				Data: []byte("new-value"),
				Clock: db.From(
					map[uint64]uint64{
						0: 1,
					},
				),
			},
			expected: &db.Record{
				Data: []byte("new-value"),
				Clock: db.From(
					map[uint64]uint64{
						0: 1,
					},
				),
			},
		},
		{
			name: "merge_equal_records",
			old: &db.Record{
				Data: []byte("old-value"),
				Clock: db.From(
					map[uint64]uint64{
						0: 1,
					},
				),
			},
			new: &db.Record{
				Data: []byte("new-value"),
				Clock: db.From(
					map[uint64]uint64{
						0: 1,
					},
				),
			},
			expected: &db.Record{
				Data: []byte("old-value"),
				Clock: db.From(
					map[uint64]uint64{
						0: 1,
					},
				),
			},
		},
		{
			name: "reject_merge_of_outdated_data",
			old: &db.Record{
				Data: []byte("old-value"),
				Clock: db.From(
					map[uint64]uint64{
						0: 1,
						1: 1,
					},
				),
			},
			new: &db.Record{
				Data: []byte("new-value"),
				Clock: db.From(
					map[uint64]uint64{
						0: 1,
					},
				),
			},
			expected: &db.Record{
				Data: []byte("old-value"),
				Clock: db.From(
					map[uint64]uint64{
						0: 1,
						1: 1,
					},
				),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, test.old.Merge(test.new.Data, test.new.Clock))
			require.Equal(t, test.expected.Data, test.old.Data)
			require.Equal(t, db.Equal, db.Order(test.old.Clock, test.expected.Clock))
		})
	}
}
