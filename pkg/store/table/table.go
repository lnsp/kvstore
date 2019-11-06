package table

import (
	"bytes"
	"encoding/binary"
	"os"

	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/emirpasic/gods/utils"
	"github.com/golang/snappy"
)

const (
	tableSuffix  = ".table"
	indexSuffix  = ".index"
	MaxBlockSize = 2 << 16
)

type Table struct {
	Name  string
	File  *os.File
	Index *redblacktree.Tree

	// mfile is reserved for merge operations.
	mfile *os.File
}

func writeIndex(name string, index *redblacktree.Tree) error {
	file, err := os.Create(name + indexSuffix)
	if err != nil {
		return err
	}
	defer file.Close()
	iterator := index.Iterator()
	for iterator.Next() {
		key := iterator.Key().([]byte)
		offset := iterator.Value().(int64)
		keyLen := uint64(len(key))
		if err := binary.Write(file, binary.LittleEndian, keyLen); err != nil {
			return err
		}
		if _, err := file.Write(key); err != nil {
			return err
		}
		if err := binary.Write(file, binary.LittleEndian, offset); err != nil {
			return err
		}
	}
	return nil
}

func readIndex(name string) (*redblacktree.Tree, error) {
	index := newIndex()
	file, err := os.Open(name + indexSuffix)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var keyLen uint64
	for binary.Read(file, binary.LittleEndian, &keyLen) == nil {
		key := make([]byte, keyLen)
		if _, err := file.Read(key); err != nil {
			return nil, err
		}
		var offset int64
		if err := binary.Read(file, binary.LittleEndian, &offset); err != nil {
			return nil, err
		}
		index.Put(key, offset)
	}
	return index, nil
}

func newIndex() *redblacktree.Tree {
	return redblacktree.NewWith(utils.Comparator(func(a, b interface{}) int {
		return bytes.Compare(a.([]byte), b.([]byte))
	}))
}

func Open(name string) (*Table, error) {
	tableFile, err := os.Open(name + tableSuffix)
	if err != nil {
		return nil, err
	}
	index, err := readIndex(name)
	if err != nil {
		return nil, err
	}
	return &Table{
		Name:  name,
		File:  tableFile,
		Index: index,
	}, nil
}

func (table *Table) Close() error {
	return table.File.Close()
}

func (table *Table) find(key []byte) ([]byte, bool) {
	// Find block in index
	floor, ok := table.Index.Floor(key)
	if !ok {
		return nil, false
	}
	offset := floor.Value.(int64)
	return Seek(table.File, offset)
}

func (table *Table) Iterate() func() ([]byte, []byte, bool) {
	var (
		indexIterator     = table.Index.Iterator()
		blockIterator     func() ([]byte, []byte, bool)
		key, value, block []byte
		ok                bool
	)
	if indexIterator.Next() {
		offset := indexIterator.Value().(int64)
		block, _ = Seek(table.File, offset)
		blockIterator = Scan(block)
		key, value, ok = blockIterator()
	}
	return func() ([]byte, []byte, bool) {
		if !ok {
			return nil, nil, false
		}
		tmpKey, tmpValue := key, value
		key, value, ok = blockIterator()
		if !ok && indexIterator.Next() {
			offset := indexIterator.Value().(int64)
			block, _ = Seek(table.File, offset)
			blockIterator = Scan(block)
			key, value, ok = blockIterator()
		}
		return tmpKey, tmpValue, true
	}
}

func (table *Table) mergeIterator() func() ([]byte, bool) {
	indexIterator := table.Index.Iterator()
	return func() ([]byte, bool) {
		if !indexIterator.Next() {
			return nil, false
		}
		return Seek(table.mfile, indexIterator.Value().(int64))
	}
}

func (table *Table) openMerge() error {
	file, err := os.Open(table.Name + tableSuffix)
	if err != nil {
		return err
	}
	table.mfile = file
	return nil
}

func (table *Table) closeMerge() error {
	return table.mfile.Close()
}

func Find(block, key []byte) ([]byte, bool) {
	var offset, size uint64 = 0, uint64(len(block))
	for offset < size {
		var (
			keyLen     = binary.LittleEndian.Uint16(block[offset : offset+2])
			valueLen   = binary.LittleEndian.Uint16(block[offset+2 : offset+4])
			keyStart   = offset + 4
			valueStart = keyStart + uint64(keyLen)
			valueEnd   = valueStart + uint64(valueLen)
		)
		switch bytes.Compare(key, block[keyStart:valueStart]) {
		case 0:
			return block[valueStart:valueEnd], true
		case 1:
			return nil, false
		case -1:
			offset = valueEnd
		}
	}
	return nil, false
}

func Scan(block []byte) func() ([]byte, []byte, bool) {
	var offset, size uint64 = 0, uint64(len(block))
	return func() ([]byte, []byte, bool) {
		if offset >= size {
			return nil, nil, false
		}
		var (
			keyLen     = binary.LittleEndian.Uint16(block[offset : offset+2])
			valueLen   = binary.LittleEndian.Uint16(block[offset+2 : offset+4])
			keyStart   = offset + 4
			valueStart = keyStart + uint64(keyLen)
			valueEnd   = valueStart + uint64(valueLen)
		)
		offset = valueEnd
		return block[keyStart:valueStart], block[valueStart:valueEnd], true
	}
}

func (table *Table) Get(key []byte) ([]byte, bool) {
	block, ok := table.find(key)
	if !ok {
		return nil, false
	}
	return Find(block, key)
}

// WritableTable is an unfinished table in write mode.
// Values can only be written by increasing key order.
type WritableTable struct {
	Index *redblacktree.Tree
	File  *os.File
	Name  string

	buffer *bytes.Buffer
	size   int64
}

func (table *WritableTable) Append(key, value []byte) error {
	// Append record to table
	keyLen := len(key)
	valueLen := len(value)
	totalLen := 4 + keyLen + valueLen
	// Check for flush
	if table.buffer.Len()+totalLen > MaxBlockSize {
		if err := table.Flush(); err != nil {
			return err
		}
	}
	// Check for index
	if table.buffer.Len() == 0 {
		table.Index.Put(key, table.size)
	}
	// Write to buffer
	if err := binary.Write(table.buffer, binary.LittleEndian, uint16(keyLen)); err != nil {
		return err
	}
	if err := binary.Write(table.buffer, binary.LittleEndian, uint16(valueLen)); err != nil {
		return err
	}
	table.buffer.Write(key)
	table.buffer.Write(value)
	return nil
}

func (table *WritableTable) Flush() error {
	compressedBlock := snappy.Encode(nil, table.buffer.Bytes())
	compressedBlockSize := uint64(len(compressedBlock))
	if err := binary.Write(table.File, binary.LittleEndian, compressedBlockSize); err != nil {
		return err
	}
	if _, err := table.File.Write(compressedBlock); err != nil {
		return err
	}
	table.size += 8 + int64(compressedBlockSize)
	table.buffer.Reset()
	return nil
}

func (table *WritableTable) FlushIndex() error {
	// write current index to disk
	return writeIndex(table.Name, table.Index)
}

func (table *WritableTable) Close() error {
	if err := table.Flush(); err != nil {
		return err
	}
	if err := table.FlushIndex(); err != nil {
		return err
	}
	return table.File.Close()
}

func OpenWritable(name string) (*WritableTable, error) {
	file, err := os.Create(name + tableSuffix)
	if err != nil {
		return nil, err
	}
	return &WritableTable{
		Index:  newIndex(),
		File:   file,
		Name:   name,
		buffer: bytes.NewBuffer(make([]byte, 0, MaxBlockSize)),
	}, nil
}

func Max(a, b []byte) []byte {
	switch bytes.Compare(a, b) {
	case -1:
		return b
	default:
		return a
	}
}

// Merge merges two existing tables into one structure.
// We assume that 'left' and 'right' can both be opened in RO-mode.
func Merge(name string, left *Table, right *Table) error {
	table, err := OpenWritable(name)
	if err != nil {
		return err
	}
	defer table.Close()
	// Open merge iterators
	if err := left.openMerge(); err != nil {
		return err
	}
	defer left.closeMerge()
	if err := right.openMerge(); err != nil {
		return err
	}
	defer right.closeMerge()
	// Extract first block
	var (
		leftMerge                          = left.mergeIterator()
		rightMerge                         = right.mergeIterator()
		leftBlock, leftOK                  = leftMerge()
		rightBlock, rightOK                = rightMerge()
		leftScan                           = Scan(leftBlock)
		rightScan                          = Scan(rightBlock)
		leftKey, leftValue, leftValueOK    = leftScan()
		rightKey, rightValue, rightValueOK = rightScan()
	)
	for leftOK && rightOK {
		for leftValueOK && rightValueOK {
			switch bytes.Compare(leftKey, rightKey) {
			case 0:
				if err := table.Append(leftKey, Max(leftValue, rightValue)); err != nil {
					return err
				}
				// Since both keys equal, both next
				leftKey, leftValue, leftValueOK = leftScan()
				rightKey, rightValue, rightValueOK = rightScan()
			case -1:
				if err := table.Append(leftKey, leftValue); err != nil {
					return err
				}
				leftKey, leftValue, leftValueOK = leftScan()
			case 1:
				if err := table.Append(rightKey, rightValue); err != nil {
					return err
				}
				rightKey, rightValue, rightValueOK = rightScan()
			}
		}
		if !leftValueOK {
			leftBlock, leftOK = leftMerge()
			if leftOK {
				leftScan = Scan(leftBlock)
				leftKey, leftValue, leftValueOK = leftScan()
			}
		}
		if !rightValueOK {
			rightBlock, rightOK = rightMerge()
			if rightOK {
				rightScan = Scan(rightBlock)
				rightKey, rightValue, rightValueOK = rightScan()
			}
		}
	}
	for leftOK {
		for leftValueOK {
			if err := table.Append(leftKey, leftValue); err != nil {
				return err
			}
			leftKey, leftValue, leftValueOK = leftScan()
		}
		// Iterator to next left
		leftBlock, leftOK = leftMerge()
		if leftOK {
			leftScan = Scan(leftBlock)
			leftKey, leftValue, leftValueOK = leftScan()
		}
	}
	for rightOK {
		for rightValueOK {
			if err := table.Append(rightKey, rightValue); err != nil {
				return err
			}
			rightKey, rightValue, rightValueOK = rightScan()
		}
		// Iterate to next right
		rightBlock, rightOK = rightMerge()
		if rightOK {
			rightScan = Scan(rightBlock)
			rightKey, rightValue, rightValueOK = rightScan()
		}
	}
	return nil
}

func Seek(file *os.File, offset int64) ([]byte, bool) {
	// Seek to offset
	if _, err := file.Seek(offset, os.SEEK_SET); err != nil {
		return nil, false
	}
	var compressedBlockSize uint64
	if err := binary.Read(file, binary.LittleEndian, &compressedBlockSize); err != nil {
		return nil, false
	}
	// Read block and decompress it
	compressedBlock := make([]byte, compressedBlockSize)
	if _, err := file.Read(compressedBlock); err != nil {
		return nil, false
	}
	block, err := snappy.Decode(nil, compressedBlock)
	if err != nil {
		return nil, false
	}
	return block, true
}
