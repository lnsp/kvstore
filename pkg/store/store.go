package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
	"valar/godat/pkg/store/table"
)

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

type Store struct {
	Name string

	memtable *table.Memtable
	tables   []*table.Table
}

func New(name string) (*Store, error) {
	memtable, err := table.NewMemtable(tableName(name))
	if err != nil {
		return nil, err
	}
	store := &Store{
		Name:     name,
		memtable: memtable,
	}
	if err := store.Restore(); err != nil {
		return nil, err
	}
	return store, nil
}

func (store *Store) Restore() error {
	tables, err := table.OpenGlob(store.Name)
	if err != nil {
		return err
	}
	store.tables = tables
	return nil
}

func (store *Store) Flush() error {
	tmp := store.memtable
	memtable, err := table.NewMemtable(tableName(store.Name))
	if err != nil {
		return err
	}
	store.memtable = memtable
	if err := tmp.Compact(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := tmp.Cleanup(); err != nil {
		return err
	}
	flushed, err := table.Open(tmp.Name)
	if err != nil {
		return err
	}
	store.tables = append(store.tables, flushed)
	return nil
}

func (store *Store) Close() error {
	if err := store.memtable.Compact(); err != nil {
		return err
	}
	if err := store.memtable.Close(); err != nil {
		return err
	}
	if err := store.memtable.Cleanup(); err != nil {
		return err
	}
	for _, table := range store.tables {
		if err := table.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) Put(key []byte, record *Record) error {
	if err := store.memtable.Put(key, record.Bytes()); err != nil {
		return err
	}
	if store.memtable.Size >= MaxMemSize {
		return store.Flush()
	}
	return nil
}

func (store *Store) collect(key []byte) [][]byte {
	values := store.memtable.Get(key)
	for _, t := range store.tables {
		local := t.Get(key)
		values = append(values, local...)
	}
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
