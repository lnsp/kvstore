package table

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/juju/ratelimit"
)

type WritableAutoflushTable struct {
	Basename string
	MaxSize  int64
	Written  []string

	ratelimit *ratelimit.Bucket
	active    *WritableTable
}

func OpenWritableWithAutoflush(name string, size int64, bucket *ratelimit.Bucket) *WritableAutoflushTable {
	return &WritableAutoflushTable{
		Basename: name,
		MaxSize:  size,

		active:    nil,
		ratelimit: bucket,
	}
}

func (table *WritableAutoflushTable) Append(key, value []byte) error {
	// Check if table needs to be initialized
	if table.active == nil {
		name := table.Basename + uuid.New().String()
		table.Written = append(table.Written, name)
		new, err := OpenWritableWithRateLimit(name, table.ratelimit)
		if err != nil {
			return fmt.Errorf("failed to setup new autoflush table: %v", err)
		}
		table.active = new
	}
	// Append record
	if err := table.active.Append(key, value); err != nil {
		return fmt.Errorf("failed to append to autoflush table: %v", err)
	}
	// Check if table is too big, flush and replace if required
	if table.active.Size >= table.MaxSize {
		if err := table.active.Close(); err != nil {
			return err
		}
		table.active = nil
	}
	return nil
}

func (table *WritableAutoflushTable) Close() error {
	if table.active != nil {
		err := table.active.Close()
		table.active = nil
		return err
	}
	return nil
}
