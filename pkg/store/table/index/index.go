package index

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/emirpasic/gods/sets/treeset"
	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/emirpasic/gods/utils"
	lru "github.com/hashicorp/golang-lru"
	"github.com/willf/bloom"
)

const (
	indexSuffix = ".index"
)

type Cache struct {
	*lru.ARCCache
}

func NewCache(size int) *Cache {
	arc, _ := lru.NewARC(size)
	return &Cache{arc}
}

type Filter struct {
	*bloom.BloomFilter
}

func NewFilter() *Filter {
	return &Filter{
		bloom.New(20000, 5),
	}
}

type Memory struct {
	*redblacktree.Tree
}

func NewMemory() *Memory {
	tree := redblacktree.NewWith(byteComparator)
	return &Memory{tree}
}

func (memory *Memory) Put(key []byte, value []byte) {
	node, ok := memory.Tree.Get(key)
	if !ok {
		node = treeset.NewWith(byteComparator)
		memory.Tree.Put(key, node)
	}
	set := node.(*treeset.Set)
	set.Add(value)
}

func (memory *Memory) Get(key []byte) [][]byte {
	node, ok := memory.Tree.Get(key)
	if !ok {
		return nil
	}
	set := node.(*treeset.Set)
	values := make([][]byte, set.Size())
	for i, v := range set.Values() {
		values[i] = v.([]byte)
	}
	return values
}

func (memory *Memory) Iterator() MemoryIterator {
	return MemoryIterator{
		tree: memory.Tree.Iterator(),
	}
}

type MemoryIterator struct {
	tree       redblacktree.Iterator
	set        treeset.Iterator
	key, value []byte
	init       bool
}

func (iterator *MemoryIterator) Next() bool {
	if !iterator.init {
		if !iterator.tree.Next() {
			return false
		}
		iterator.set = iterator.tree.Value().(*treeset.Set).Iterator()
		iterator.key = iterator.tree.Key().([]byte)
		iterator.init = true
	}
	next := iterator.set.Next()
	for !next && iterator.tree.Next() {
		iterator.set = iterator.tree.Value().(*treeset.Set).Iterator()
		iterator.key = iterator.tree.Key().([]byte)
		next = iterator.set.Next()
	}
	if !next {
		return false
	}
	iterator.value = iterator.set.Value().([]byte)
	return true
}

func (iterator *MemoryIterator) Key() []byte {
	return iterator.key
}

func (iterator *MemoryIterator) Value() []byte {
	return iterator.value
}

type Index struct {
	*redblacktree.Tree
}

func (index *Index) Get(key []byte) (int64, bool) {
	floor, ok := index.Tree.Floor(key)
	if !ok {
		return 0, false
	}
	return floor.Value.(int64), true
}

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

func NewIndex() *Index {
	tree := redblacktree.NewWith(byteComparator)
	return &Index{tree}
}

var byteComparator = utils.Comparator(func(a, b interface{}) int {
	return bytes.Compare(a.([]byte), b.([]byte))
})
