package table

import (
	"fmt"
	"sync"

	"github.com/juju/ratelimit"
)

// AFTable is a appendable key-value list that automatically flushes itself to the file system.
type AFTable struct {
	Path    string
	MaxSize int64

	RateLimit *ratelimit.Bucket

	// mu protects the fields below.
	mu     sync.Mutex
	active *WTable
	index  int
}

// NewAFTable creates a new AFTable.
func NewAFTable(path string, size int64, limit *ratelimit.Bucket) *AFTable {
	return &AFTable{
		Path:      path,
		MaxSize:   size,
		RateLimit: limit,

		active: nil,
		index:  0,
	}
}

// Append appends a key-value pair to the AFTable.
func (table *AFTable) Append(key, value []byte) error {
	table.mu.Lock()
	defer table.mu.Unlock()
	// Check if table needs to be initialized
	if table.active == nil {
		name := fmt.Sprintf("%s-%d", table.index)
		new, err := NewWTable(name, table.RateLimit)
		if err != nil {
			return fmt.Errorf("setup new autoflush table: %v", err)
		}
		table.active = new
		table.index++
	}
	// Append record
	if err := table.active.Append(key, value); err != nil {
		return fmt.Errorf("append to autoflush table: %v", err)
	}
	// Check if table is too big, flush and replace if required
	if table.active.Size() >= table.MaxSize {
		if err := table.active.Close(); err != nil {
			return err
		}
		table.active = nil
	}
	return nil
}

// Close closes the AFTable.
func (table *AFTable) Close() error {
	if table.active != nil {
		err := table.active.Close()
		table.active = nil
		return err
	}
	return nil
}
