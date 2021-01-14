package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"github.com/lnsp/kvstore/table"

	"github.com/sirupsen/logrus"
)

var logger = logrus.New()

// This is about 64MiB.
const maxMemtableSize = 1 << 26

func tableName(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, uuid.New())
}

type mutex struct {
	lock chan bool
}

func (m mutex) Lock() {
	<-m.lock
}

func (m mutex) TryLock() bool {
	select {
	case <-m.lock:
		return true
	default:
		return false
	}
}

func (m mutex) Unlock() {
	m.lock <- true
}

func makeMutex() mutex {
	m := mutex{make(chan bool, 1)}
	m.Unlock()
	return m
}

type Store struct {
	Path string

	// access synchronizes all actions related to the active memtable.
	memory sync.RWMutex
	// flush synchronizes all write-actions related to the flushed memtable.
	flush mutex

	active, flushed *table.Memtable
	compaction      *table.Leveled
}

// New creates a new store.
func New(path string) (*Store, error) {
	// Ensure that directory exists
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("ensure path exists: %w", err)
	}
	store := &Store{
		Path:       path,
		flush:      makeMutex(),
		compaction: table.NewLeveledCompaction(filepath.Join(path, "leveled")),
	}
	if err := store.Restore(); err != nil {
		return nil, err
	}
	return store, nil
}

// Restore restores the old store's state.
func (store *Store) Restore() error {
	// Remove intermediate tables
	err := table.RemovePartialTables(store.Path)
	if err != nil {
		return err
	}
	// Load new memtable
	memtablePath := filepath.Join(store.Path, "mem")
	store.active, err = table.OpenMemtable(memtablePath, tableName("mem"))
	if err != nil {
		return err
	}
	if store.active.Size >= maxMemtableSize {
		if err := store.Flush(); err != nil {
			return err
		}
	}
	tables, err := table.OpenTables(store.compaction.Name)
	if err != nil {
		return err
	}
	if err := store.compaction.Restore(tables); err != nil {
		return fmt.Errorf("failed to restore tables: %w", err)
	}
	return nil
}

// mergeFlushed merges active and flushed memtables back into one
// and restores it as the active memtable.
func (store *Store) mergeFlushed() {
	store.memory.Lock()
	store.flushed.Merge(store.active)
	store.active = store.flushed
	store.flushed = nil
	store.memory.Unlock()
}

// Flush replaces the active memtable with a new one
// and compacts the old one to disk. This can be done
// while serving entries from the flushed memtable
// as well as the active memtable and all other disk tables.
func (store *Store) Flush() error {
	// store.flush must be locked on call.
	store.flush.TryLock()
	// Lock any "flush" related activities
	defer store.flush.Unlock()
	replace, err := table.NewMemtable(tableName("mem"))
	if err != nil {
		return err
	}
	// Lock active table for swapping
	store.memory.Lock()
	store.flushed = store.active
	store.active = replace
	store.memory.Unlock()

	logger.WithFields(logrus.Fields{
		"name": store.flushed.Name,
		"size": store.flushed.Size,
	}).Debug("Flushing memtable")
	// Unlock active table again
	// Compact flushed table
	if err := store.flushed.Compact(); err != nil {
		store.mergeFlushed()
		return err
	}
	logger.WithFields(logrus.Fields{
		"name": store.flushed.Name,
	}).Debug("Compacted flushed memtable")
	// Open new table
	flushed, err := table.Open(store.flushed.Name)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"name": store.flushed.Name,
		}).WithError(err).Error("Failed to open flushed table, merging back into main memory")
		store.mergeFlushed()
		return err
	}
	if err := store.compaction.Add(flushed); err != nil {
		logger.WithFields(logrus.Fields{
			"name": store.flushed.Name,
		}).WithError(err).Error("Failed to push flushed to leveled compaction, merging back")
		store.mergeFlushed()
		return err
	}
	store.flushed.Close()
	store.flushed.Cleanup()
	logger.WithFields(logrus.Fields{
		"name": store.flushed.Name,
	}).Debug("Moved flushed to disk")
	// Add flushed table to tables
	store.memory.Lock()
	store.flushed = nil
	store.memory.Unlock()
	return nil
}

func (store *Store) Close() error {
	store.memory.Lock()
	defer store.memory.Unlock()
	if err := store.active.Compact(); err != nil {
		return err
	}
	if err := store.active.Close(); err != nil {
		return err
	}
	if err := store.active.Cleanup(); err != nil {
		return err
	}
	if err := store.compaction.Close(); err != nil {
		return err
	}
	return nil
}

// Put stores a key-value pair.
func (store *Store) Put(key []byte, record *table.Record) error {
	// Lock in-memory table write access.
	store.memory.Lock()
	defer store.memory.Unlock()
	if err := store.active.Put(key, record.Bytes()); err != nil {
		return err
	}
	if store.active.Size >= maxMemtableSize && store.flush.TryLock() {
		go store.Flush()
	}
	return nil
}

func (store *Store) collect(key []byte) [][]byte {
	store.memory.RLock()
	values := store.active.Get(key)
	if store.flushed != nil {
		values = append(values, store.flushed.Get(key)...)
	}
	store.memory.RUnlock()
	return append(values, store.compaction.Get(key)...)
}

func (store *Store) Get(key []byte) []table.Record {
	values := store.collect(key)
	records := make([]table.Record, len(values))
	// Only return record with highest version
	for i, v := range values {
		rec := table.Record{}
		rec.FromBytes(v)
		records[i] = rec
	}
	return records
}

func (store *Store) MemSize() int64 {
	var size int64
	store.memory.Lock()
	size += store.active.Size
	if store.flushed != nil {
		size += store.flushed.Size
	}
	store.memory.Unlock()
	return size
}
