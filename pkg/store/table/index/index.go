package index

import (
	"bytes"
	"encoding/binary"
	"io"

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
	tree := redblacktree.NewWith(utils.Comparator(func(a, b interface{}) int {
		return bytes.Compare(a.([]byte), b.([]byte))
	}))
	return &Index{tree}
}
