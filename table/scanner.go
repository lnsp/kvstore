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
// The block scanner is NOT thread-safe.
type BlockScanner struct {
	input          block.ReadSeekLocker
	block          []byte
	peeked, offset int64
}

// Next returns true if there is a next block and moves the cursor forward.
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

// Peek returns true if there is a next block, but does not move the cursor forward.
func (scanner *BlockScanner) Peek() bool {
	block, offset, ok := block.Seek(scanner.input, scanner.offset)
	if !ok {
		return false
	}
	scanner.block = block
	scanner.peeked = offset
	return true
}

// Skip skips the current block.
func (scanner *BlockScanner) Skip() {
	scanner.offset += scanner.peeked
	scanner.peeked = 0
}

// Block returns the last-scanned block.
func (scanner *BlockScanner) Block() []byte {
	return scanner.block
}

// RowScanner scans through rows of a block.
// The scanner is NOT thread-safe.
type RowScanner struct {
	block, key, value []byte
	peeked, offset    int64
}

// Next moves the cursor forward and returns true if there is a next row.
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

// Peek returns true if there is a next row.
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

// Skip skips the current row.
func (scanner *RowScanner) Skip() {
	scanner.offset += scanner.peeked
	scanner.peeked = 0
}

// Key returns the last-scanned key.
func (scanner *RowScanner) Key() []byte {
	return scanner.key
}

// Value returns the last-scanned value.
func (scanner *RowScanner) Value() []byte {
	return scanner.value
}

// Scanner can scan through a table row-by-row while automatically fetching new blocks.
type Scanner struct {
	block BlockScanner
	row   RowScanner

	key, value []byte
}

// Next loads the next row (if possible) and returns true if a new key-value pair has been fetched.
// It moves the cursor one step forward.
func (scanner *Scanner) Next() bool {
	next := scanner.row.Next()
	for !next && scanner.block.Next() {
		scanner.row = ScanRows(scanner.block.Block())
		next = scanner.row.Next()
	}
	scanner.key = scanner.row.Key()
	scanner.value = scanner.row.Value()
	return next
}

// Peek loads the next row (if possible) and returns true if a new key-value pair has been fetched.
// It does not actually move the current cursor forward.
func (scanner *Scanner) Peek() bool {
	next := scanner.row.Peek()
	for !next && scanner.block.Peek() {
		row := ScanRows(scanner.block.Block())
		next = row.Peek()
	}
	scanner.key = scanner.row.Key()
	scanner.value = scanner.row.Value()
	return next
}

// Skip skips the current cursor position.
func (scanner *Scanner) Skip() {
	scanner.row.Skip()
}

// Key returns the key of the current cursor position.
func (scanner *Scanner) Key() []byte {
	return scanner.key
}

// Value returns the value of the current cursor position.
func (scanner *Scanner) Value() []byte {
	return scanner.value
}

// Compare compares the current table scanner position with the other table scanner.
// The smaller key-value pair ordered by (key, value) will be returned.
func (scanner *Scanner) Compare(other *Scanner) (int, []byte, []byte) {
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
