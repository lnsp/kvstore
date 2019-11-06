package table

import (
	"bytes"
	"encoding/binary"
	"os"

	"valar/godat/pkg/store/table/index"

	"github.com/golang/snappy"
)

const (
	tableSuffix  = ".table"
	indexSuffix  = ".index"
	filterSuffix = ".filter"
	MaxBlockSize = 2 << 16
	MaxCacheSize = 128
)

type Table struct {
	Name string
	File *os.File

	// Performance optimizations
	Index  *index.Index
	Filter *index.Filter
	Cache  *index.Cache

	// mfile is reserved for merge operations.
	mfile *os.File
}

// Open a table-triple and initializes the table file and loads index and filter into memory.
func Open(name string) (*Table, error) {
	tableFile, err := os.Open(name + tableSuffix)
	if err != nil {
		return nil, err
	}
	table := &Table{
		Name:   name,
		File:   tableFile,
		Index:  index.NewIndex(),
		Filter: index.NewFilter(),
		Cache:  index.NewCache(MaxCacheSize),
	}
	indexFile, err := os.Open(name + indexSuffix)
	if err != nil {
		return table, err
	}
	defer indexFile.Close()
	filterFile, err := os.Open(name + filterSuffix)
	if err != nil {
		return table, err
	}
	defer filterFile.Close()
	return table, nil
}

// Close closes a table file.
func (table *Table) Close() error {
	return table.File.Close()
}

func (table *Table) find(key []byte) ([]byte, bool) {
	// Check filter
	if !table.Filter.Test(key) {
		return nil, false
	}
	// Find block in index
	offset, ok := table.Index.Get(key)
	if !ok {
		return nil, false
	}
	// Check if block in cache
	block, ok := table.Cache.Get(offset)
	if !ok {
		block, _, ok = Seek(table.File, offset)
		if ok {
			table.Cache.Add(offset, block)
		}
	}
	return block.([]byte), ok
}

func (table *Table) openMerge() (Scanner, error) {
	file, err := os.Open(table.Name + tableSuffix)
	if err != nil {
		return Scanner{}, err
	}
	table.mfile = file
	return Scanner{
		block: BlockScanner{
			file: table.mfile,
		},
	}, nil
}

func (table *Table) closeMerge() error {
	return table.mfile.Close()
}

func (table *Table) Get(key []byte) ([]byte, bool) {
	block, ok := table.find(key)
	if !ok {
		return nil, false
	}
	return Find(block, key)
}

func (table *Table) Scan() Scanner {
	return Scanner{
		block: BlockScanner{
			file: table.File,
		},
	}
}

// WritableTable is an unfinished table in write mode.
// Values can only be written by increasing key order.
type WritableTable struct {
	Index  *index.Index
	Filter *index.Filter
	File   *os.File
	Name   string

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
	// Add to filter
	table.Filter.Add(key)
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

func (table *WritableTable) FlushFilter() error {
	filterFile, err := os.Create(table.Name + filterSuffix)
	if err != nil {
		return err
	}
	defer filterFile.Close()
	_, err = table.Filter.WriteTo(filterFile)
	return err
}

func (table *WritableTable) FlushIndex() error {
	indexFile, err := os.Create(table.Name + indexSuffix)
	if err != nil {
		return err
	}
	defer indexFile.Close()
	_, err = table.Index.WriteTo(indexFile)
	return err
}

func (table *WritableTable) Close() error {
	if err := table.Flush(); err != nil {
		return err
	}
	if err := table.FlushFilter(); err != nil {
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
		Filter: index.NewFilter(),
		Index:  index.NewIndex(),
		File:   file,
		Name:   name,
		buffer: bytes.NewBuffer(make([]byte, 0, MaxBlockSize)),
	}, nil
}

func max(a, b []byte) []byte {
	switch bytes.Compare(a, b) {
	case -1:
		return b
	default:
		return a
	}
}

// Merge combines two tables into one.
func Merge(name string, left *Table, right *Table) error {
	table, err := OpenWritable(name)
	if err != nil {
		return err
	}
	defer table.Close()
	ls, err := left.openMerge()
	if err != nil {
		return err
	}
	rs, err := right.openMerge()
	if err != nil {
		return err
	}
	for ls.Peek() && rs.Peek() {
		switch bytes.Compare(ls.Key(), rs.Key()) {
		case 0:
			if err := table.Append(ls.Key(), max(ls.Value(), rs.Value())); err != nil {
				return err
			}
			ls.Skip()
			rs.Skip()
		case 1:
			if err := table.Append(rs.Key(), rs.Value()); err != nil {
				return err
			}
			rs.Skip()
		case -1:
			if err := table.Append(ls.Key(), ls.Value()); err != nil {
				return err
			}
			ls.Skip()
		}
	}
	for ls.Peek() {
		if err := table.Append(ls.Key(), ls.Value()); err != nil {
			return err
		}
		ls.Skip()
	}
	for rs.Peek() {
		if err := table.Append(rs.Key(), rs.Value()); err != nil {
			return err
		}
		rs.Skip()
	}
	return nil
}

// Seek searches the file for a block at the given offset.
func Seek(file *os.File, offset int64) ([]byte, int64, bool) {
	// Seek to offset
	if _, err := file.Seek(offset, os.SEEK_SET); err != nil {
		return nil, 0, false
	}
	var compressedBlockSize int64
	if err := binary.Read(file, binary.LittleEndian, &compressedBlockSize); err != nil {
		return nil, 0, false
	}
	// Read block and decompress it
	compressedBlock := make([]byte, compressedBlockSize)
	if _, err := file.Read(compressedBlock); err != nil {
		return nil, 0, false
	}
	block, err := snappy.Decode(nil, compressedBlock)
	if err != nil {
		return nil, 0, false
	}
	return block, 8 + compressedBlockSize, true
}

// Find looks for a row with the given key in the block.
func Find(block, key []byte) ([]byte, bool) {
	var offset, size int64 = 0, int64(len(block))
	for offset < size {
		var (
			keyLen     = binary.LittleEndian.Uint16(block[offset : offset+2])
			valueLen   = binary.LittleEndian.Uint16(block[offset+2 : offset+4])
			keyStart   = offset + 4
			valueStart = keyStart + int64(keyLen)
			valueEnd   = valueStart + int64(valueLen)
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

// Next reads the next row from the block and returns the key, value and offset.
func Next(block []byte, offset int64) ([]byte, []byte, int64, bool) {
	if offset >= int64(len(block)) {
		return nil, nil, offset, false
	}
	var (
		keyLen     = binary.LittleEndian.Uint16(block[offset : offset+2])
		valueLen   = binary.LittleEndian.Uint16(block[offset+2 : offset+4])
		keyStart   = offset + 4
		valueStart = keyStart + int64(keyLen)
		valueEnd   = valueStart + int64(valueLen)
	)
	return block[keyStart:valueStart], block[valueStart:valueEnd], valueEnd - offset, true
}

// ScanRows returns a row scanner for the given block.
func ScanRows(block []byte) RowScanner {
	return RowScanner{
		block: block,
	}
}

// ScanBlocks returns a block scanner for the given file.
func ScanBlocks(file *os.File) BlockScanner {
	return BlockScanner{
		file:   file,
		offset: 0,
		block:  nil,
	}
}

type BlockScanner struct {
	file           *os.File
	block          []byte
	peeked, offset int64
}

func (scanner *BlockScanner) Next() bool {
	scanner.Skip()
	block, offset, ok := Seek(scanner.file, scanner.offset)
	if !ok {
		return false
	}
	scanner.block = block
	scanner.offset += offset
	return true
}

func (scanner *BlockScanner) Peek() bool {
	block, offset, ok := Seek(scanner.file, scanner.offset)
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
	key, value, offset, ok := Next(scanner.block, scanner.offset)
	if !ok {
		return false
	}
	scanner.key = key
	scanner.value = value
	scanner.offset += offset
	return true
}

func (scanner *RowScanner) Peek() bool {
	key, value, offset, ok := Next(scanner.block, scanner.offset)
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

type Scanner struct {
	block BlockScanner
	row   RowScanner

	key, value []byte
}

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

func (scanner *Scanner) Peek() bool {
	next := scanner.row.Peek()
	for !next && scanner.block.Next() {
		scanner.row = ScanRows(scanner.block.Block())
		next = scanner.row.Peek()
	}
	scanner.key = scanner.row.Key()
	scanner.value = scanner.row.Value()
	return next
}

func (scanner *Scanner) Skip() {
	scanner.row.Skip()
}

func (it *Scanner) Key() []byte {
	return it.key
}

func (it *Scanner) Value() []byte {
	return it.value
}
