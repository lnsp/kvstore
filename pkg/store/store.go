package store

import (
	"bytes"

	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/emirpasic/gods/utils"
)

type Record struct {
	Value     []byte
	Timestamp int64
}

type Store struct {
	mem *redblacktree.Tree
}

func New() (*Store, error) {
	store := &Store{
		mem: redblacktree.NewWith(utils.Comparator(func(a, b interface{}) int {
			return bytes.Compare(a.([]byte), b.([]byte))
		})),
	}
	return store, nil
}

func (store *Store) Put(key []byte, record *Record) {
	store.mem.Put(key, record)
}

func (store *Store) Get(key []byte) (*Record, bool) {
	record, ok := store.mem.Get(key)
	if !ok {
		return nil, false
	}
	return record.(*Record), true
}
