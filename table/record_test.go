package table

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecordBytes(t *testing.T) {
	tt := []struct {
		Name     string
		Record   Record
		Expected []byte
	}{
		{
			Name: "basic record",
			Record: Record{
				Metadata: Metadata{
					127,
					false,
				},
				Value: []byte{1, 2, 3, 4},
			},
			Expected: []byte{0, 0, 0, 0, 0, 0, 0, 127, 0, 1, 2, 3, 4},
		},
		{
			Name: "deleted record",
			Record: Record{
				Metadata: Metadata{
					128,
					true,
				},
				Value: []byte{5, 6, 7, 8},
			},
			Expected: []byte{0, 0, 0, 0, 0, 0, 0, 128, 1, 5, 6, 7, 8},
		},
	}
	for _, tc := range tt {
		t.Run(tc.Name, func(t *testing.T) {
			b := tc.Record.Bytes()
			assert.Equal(t, tc.Expected, b)
			r := Record{}
			assert.Nil(t, r.FromBytes(tc.Expected))
			assert.Equal(t, tc.Record, r)
		})
	}
	// Test for invalid bytes
	t.Run("invalid bytes", func(t *testing.T) {
		r := Record{}
		b := []byte{1, 2, 3, 4}
		assert.NotNil(t, r.FromBytes(b))
	})
	// Make sure that value comparison works fine
	t.Run("record comparison", func(t *testing.T) {
		r1 := Record{Metadata: Metadata{Version: 256}, Value: []byte{'x'}}
		r2 := Record{Metadata: Metadata{Version: 255}, Value: []byte{'y'}}
		assert.Equal(t, 1, bytes.Compare(r1.Bytes(), r2.Bytes()))
	})
}
