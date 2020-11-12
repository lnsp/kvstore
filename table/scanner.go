package table

import (
	"bytes"

	"github.com/lnsp/kvstore/table/block"
)

// ScanRows returns a row scanner for the given block.
func ScanRows(block []byte) RowScanner {
	return RowScanner{
		block: block,
	}
}

// ScanBlocks returns a block scanner for the given file.
func ScanBlocks(file block.ReadSeekLocker) BlockScanner {
	return BlockScanner{
		input:  file,
		offset: 0,
		block:  nil,
	}
}

// BlockScanner implements a basic block scanner that iterates over a table block.
type BlockScanner struct {
	input          block.ReadSeekLocker
	block          []byte
	peeked, offset int64
}

func (scanner *BlockScanner) Next() bool {
	scanner.Skip()
	block, offset, ok := block.Seek(scanner.input, scanner.offset)
	if !ok {
		return false
	}
	scanner.block = block
	scanner.offset += offset
	return true
}

func (scanner *BlockScanner) Peek() bool {
	block, offset, ok := block.Seek(scanner.input, scanner.offset)
	if !ok {
		return false
	}
	scanner.block = block
	scanner.peeked = offset
	return true
}

func (scanner *BlockScanner) Skip() {
	scanner.offset += scanner.peeked
	scanner.peeked = 0
}

func (scanner *BlockScanner) Block() []byte {
	return scanner.block
}

type RowScanner struct {
	block, key, value []byte
	peeked, offset    int64
}

func (scanner *RowScanner) Next() bool {
	scanner.Skip()
	key, value, offset, ok := block.NextRow(scanner.block, scanner.offset)
	if !ok {
		return false
	}
	scanner.key = key
	scanner.value = value
	scanner.offset += offset
	return true
}

func (scanner *RowScanner) Peek() bool {
	key, value, offset, ok := block.NextRow(scanner.block, scanner.offset)
	if !ok {
		return false
	}
	scanner.key = key
	scanner.value = value
	scanner.peeked = offset
	return true
}

func (scanner *RowScanner) Skip() {
	scanner.offset += scanner.peeked
	scanner.peeked = 0
}

func (scanner *RowScanner) Key() []byte {
	return scanner.key
}

func (scanner *RowScanner) Value() []byte {
	return scanner.value
}

type Scanner interface {
	Next() bool
	Key() []byte
	Value() []byte
}

type TableScanner struct {
	block BlockScanner
	row   RowScanner

	key, value []byte
}

func (scanner *TableScanner) Next() bool {
	next := scanner.row.Next()
	for !next && scanner.block.Next() {
		scanner.row = ScanRows(scanner.block.Block())
		next = scanner.row.Next()
	}
	scanner.key = scanner.row.Key()
	scanner.value = scanner.row.Value()
	return next
}

func (scanner *TableScanner) Peek() bool {
	next := scanner.row.Peek()
	for !next && scanner.block.Next() {
		scanner.row = ScanRows(scanner.block.Block())
		next = scanner.row.Peek()
	}
	scanner.key = scanner.row.Key()
	scanner.value = scanner.row.Value()
	return next
}

func (scanner *TableScanner) Skip() {
	scanner.row.Skip()
}

func (scanner *TableScanner) Key() []byte {
	return scanner.key
}

func (scanner *TableScanner) Value() []byte {
	return scanner.value
}

func (scanner *TableScanner) Compare(other *TableScanner) (int, []byte, []byte) {
	switch bytes.Compare(scanner.Key(), other.Key()) {
	case 1:
		return 1, other.Key(), other.Value()
	case -1:
		return -1, scanner.Key(), scanner.Value()
	default:
		_, max := minmax(scanner.Value(), other.Value())
		return 0, scanner.Key(), max
	}
}
