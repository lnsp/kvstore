// Package index contains data structures to help cope with caches, indices and key filters.
package index

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	"github.com/emirpasic/gods/sets/treeset"
	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/emirpasic/gods/utils"
	lru "github.com/hashicorp/golang-lru"
	"github.com/willf/bloom"
)

const (
	filterBits    = 20000
	filterKFamily = 5
)

// Cache implements a simple ARC cache.
type Cache struct {
	*lru.ARCCache
}

// NewCache initializes a new cache with the given size.
func NewCache(size int) *Cache {
	arc, _ := lru.NewARC(size)
	return &Cache{arc}
}

// Filter implements a simple key filter based on a bloom filter.
type Filter struct {
	*bloom.BloomFilter
}

// NewFilter initializes a new bloom filter.
func NewFilter() *Filter {
	return &Filter{
		bloom.New(filterBits, filterKFamily),
	}
}

// Memory is an in-memory key-valueset store.
type Memory struct {
	*redblacktree.Tree
	mu sync.RWMutex
}

type ValueSet struct {
	*treeset.Set
	mu sync.RWMutex
}

func NewValueSet() *ValueSet {
	return &ValueSet{
		Set: treeset.NewWith(byteComparator),
	}
}

func (set *ValueSet) Add(value []byte) {
	set.mu.Lock()
	set.Set.Add(value)
	set.mu.Unlock()
}

func (set *ValueSet) Values() [][]byte {
	set.mu.RLock()
	values := make([][]byte, set.Set.Size())
	it := set.Set.Iterator()
	for i := 0; it.Next(); i++ {
		values[i] = it.Value().([]byte)
	}
	set.mu.RUnlock()
	return values
}

// NewMemory initializes a new in-memory key-valueset store.
func NewMemory() *Memory {
	tree := redblacktree.NewWith(byteComparator)
	return &Memory{
		Tree: tree,
	}
}

// Put stores the given key-value pair in the in-memory tree.
func (memory *Memory) Put(key []byte, value []byte) {
	memory.mu.Lock()
	node, ok := memory.Tree.Get(key)
	if !ok {
		node = NewValueSet()
		memory.Tree.Put(key, node)
	}
	memory.mu.Unlock()
	set := node.(*ValueSet)
	set.Add(value)
}

// Get returns the valueset for the given key.
func (memory *Memory) Get(key []byte) [][]byte {
	memory.mu.RLock()
	node, ok := memory.Tree.Get(key)
	if !ok {
		memory.mu.RUnlock()
		return nil
	}
	memory.mu.RUnlock()
	set := node.(*ValueSet)
	return set.Values()
}

// Iterator returns an abstraction to iterate through all key-value pairs in the tree.
func (memory *Memory) Iterator() MemoryIterator {
	return MemoryIterator{
		tree: memory.Tree.Iterator(),
	}
}

// SetIterator returns an abstraction to iterate through all key-valueset pairs in the tree.
func (memory *Memory) SetIterator() MemorySetIterator {
	return MemorySetIterator{
		tree: memory.Tree.Iterator(),
	}
}

// MemorySetIterator implements an iterator for scanning through key-valueset pairs.
type MemorySetIterator struct {
	tree redblacktree.Iterator
}

// Next fetches the next pair and returns true on success.
func (iterator *MemorySetIterator) Next() bool {
	return iterator.tree.Next()
}

// Key returns the pair's key.
func (iterator *MemorySetIterator) Key() []byte {
	return iterator.tree.Key().([]byte)
}

// Values returns the pair's valueset.
func (iterator *MemorySetIterator) Values() [][]byte {
	return iterator.tree.Value().(*ValueSet).Values()
}

// MemoryIterator implements an iterator for scanning through key-value pairs.
type MemoryIterator struct {
	tree       redblacktree.Iterator
	set        treeset.Iterator
	key, value []byte
	init       bool
}

// Next fetches the next pair and returns true on success
func (iterator *MemoryIterator) Next() bool {
	if !iterator.init {
		if !iterator.tree.Next() {
			return false
		}
		iterator.set = iterator.tree.Value().(*ValueSet).Iterator()
		iterator.key = iterator.tree.Key().([]byte)
		iterator.init = true
	}
	next := iterator.set.Next()
	for !next && iterator.tree.Next() {
		iterator.set = iterator.tree.Value().(*ValueSet).Iterator()
		iterator.key = iterator.tree.Key().([]byte)
		next = iterator.set.Next()
	}
	if !next {
		return false
	}
	iterator.value = iterator.set.Value().([]byte)
	return true
}

// Key returns the key of the current pair pointer.
func (iterator *MemoryIterator) Key() []byte {
	return iterator.key
}

// Value returns the value of the current pair pointer.
func (iterator *MemoryIterator) Value() []byte {
	return iterator.value
}

// Index implements an in-memory key-offset map.
type Index struct {
	*redblacktree.Tree
}

// Get returns the offset associated with the given key.
func (index *Index) Get(key []byte) (int64, bool) {
	floor, ok := index.Tree.Floor(key)
	if !ok {
		return 0, false
	}
	return floor.Value.(int64), true
}

// WriteTo serializes the index and writes it to the given stream in a binary format.
func (index *Index) WriteTo(file io.Writer) (int64, error) {
	var n int64
	iterator := index.Iterator()
	for iterator.Next() {
		key := iterator.Key().([]byte)
		offset := iterator.Value().(int64)
		keyLen := int64(len(key))
		if err := binary.Write(file, binary.LittleEndian, keyLen); err != nil {
			return n, err
		}
		if _, err := file.Write(key); err != nil {
			return n, err
		}
		if err := binary.Write(file, binary.LittleEndian, offset); err != nil {
			return n, err
		}
		n += 16 + keyLen
	}
	return n, nil
}

// ReadFrom deserializes the given binary stream and stores all read key-offset pairs in the index.
func (index *Index) ReadFrom(file io.Reader) (int64, error) {
	var n, keyLen int64
	for binary.Read(file, binary.LittleEndian, &keyLen) == nil {
		key := make([]byte, keyLen)
		if _, err := file.Read(key); err != nil {
			return n, err
		}
		var offset int64
		if err := binary.Read(file, binary.LittleEndian, &offset); err != nil {
			return n, err
		}
		index.Put(key, offset)
		n += 16 + keyLen
	}
	return n, nil
}

// NewIndex constructs a new key-offset index.
func NewIndex() *Index {
	tree := redblacktree.NewWith(byteComparator)
	return &Index{tree}
}

// byteComparator implements a comparator for byte slices.
var byteComparator = utils.Comparator(func(a, b interface{}) int {
	return bytes.Compare(a.([]byte), b.([]byte))
})
