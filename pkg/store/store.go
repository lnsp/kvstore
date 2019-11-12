package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
	"valar/kv/pkg/store/table"

	"github.com/sirupsen/logrus"
)

var logger = logrus.New()

func init() {
	logger.SetLevel(logrus.DebugLevel)
}

const MaxMemSize = 2 << 26

func tableName(name string) string {
	return fmt.Sprintf("%s-%s", name, time.Now().UTC().Format("2006-02-01-15-04-05"))
}

type Record struct {
	Time  int64
	Value []byte
}

func (record Record) String() string {
	return fmt.Sprintf("%s [%d]", string(record.Value), record.Time)
}

func (record *Record) FromBytes(data []byte) {
	buffer := bytes.NewBuffer(data)
	binary.Read(buffer, binary.BigEndian, &record.Time)
	record.Value = buffer.Bytes()
}

func (record *Record) Bytes() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, record.Time)
	buffer.Write(record.Value)
	return buffer.Bytes()
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
	Name string

	// access synchronizes all actions related to the active memtable.
	memory, disk sync.RWMutex
	// flush synchronizes all write-actions related to the flushed memtable.
	flush mutex

	active, flushed *table.Memtable
	compaction      *table.Leveled
}

func New(name string) (*Store, error) {
	store := &Store{
		Name:  name,
		flush: makeMutex(),
	}
	if err := store.Restore(); err != nil {
		return nil, err
	}
	return store, nil
}

func (store *Store) Restore() error {
	// Remove intermediate tables
	err := table.RemoveIntermediateTables(store.Name)
	if err != nil {
		return err
	}
	// Load new memtable
	store.active, err = table.OpenMemtable(store.Name, tableName(store.Name))
	if err != nil {
		return err
	}
	if store.active.Size >= MaxMemSize {
		if err := store.Flush(); err != nil {
			return err
		}
	}
	// Remove all tables with matching logs
	store.tables, err = table.OpenTables(store.Name)
	if err != nil {
		return err
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
	replace, err := table.NewMemtable(tableName(store.Name))
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
	store.flushed.Close()
	store.flushed.Cleanup()
	logger.WithFields(logrus.Fields{
		"name": store.flushed.Name,
	}).Debug("Moved flushed to disk")
	// Add flushed table to tables
	store.memory.Lock()
	store.flushed = nil
	store.disk.Lock()
	store.tables = append(store.tables, flushed)
	store.disk.Unlock()
	store.memory.Unlock()
	return nil
}

func (store *Store) Close() error {
	store.memory.Lock()
	if err := store.active.Compact(); err != nil {
		return err
	}
	if err := store.active.Close(); err != nil {
		return err
	}
	if err := store.active.Cleanup(); err != nil {
		return err
	}
	store.disk.Lock()
	for _, table := range store.tables {
		if err := table.Close(); err != nil {
			return err
		}
	}
	// After this, all PUT and GET operations on this
	// table will lead to a deadlock.
	return nil
}

func (store *Store) Put(key []byte, record *Record) error {
	// Lock in-memory table write access.
	store.memory.Lock()
	defer store.memory.Unlock()
	if err := store.active.Put(key, record.Bytes()); err != nil {
		return err
	}
	if store.active.Size >= MaxMemSize && store.flush.TryLock() {
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
	store.disk.RLock()
	for _, t := range store.tables {
		local := t.Get(key)
		values = append(values, local...)
	}
	store.disk.RUnlock()
	return values
}

func (store *Store) Get(key []byte) []*Record {
	values := store.collect(key)
	records := make([]*Record, len(values))
	for i, v := range values {
		rec := &Record{}
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
