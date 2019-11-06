package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"valar/godat/pkg/store/table/index"

	"github.com/golang/snappy"
	"github.com/ncw/directio"
)

const (
	logSuffix    = ".log"
	tableSuffix  = ".table"
	indexSuffix  = ".index"
	filterSuffix = ".filter"

	// MaxBlockSize defines the maximum size of a table block. By default 64 KiB.
	MaxBlockSize = 2 << 16
	// MaxCacheSize defines the maximum number of blocks cached in memory. By default 8 MiB.
	MaxCacheSize = 128
)

type Memtable struct {
	Name string
	Size int64

	log *os.File
	mem *index.Memory
}

func NewMemtable(name string) (*Memtable, error) {
	log, err := directio.OpenFile(name+logSuffix, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	Memtable := &Memtable{
		Name: name,
		log:  log,
		mem:  index.NewMemory(),
	}
	// Read until nothing available
	var keyLen, valueLen int64
	for binary.Read(log, binary.LittleEndian, &keyLen) == nil {
		// Read value len
		if err := binary.Read(log, binary.LittleEndian, &valueLen); err != nil {
			return nil, err
		}
		// Read key
		key := make([]byte, keyLen)
		if _, err := log.Read(key); err != nil {
			return nil, err
		}
		// Read value
		value := make([]byte, valueLen)
		if _, err := log.Read(value); err != nil {
			return nil, err
		}
		Memtable.mem.Put(key, value)
		Memtable.Size += 16 + keyLen + valueLen
	}
	return Memtable, nil
}

func (table *Memtable) Get(key []byte) [][]byte {
	return table.mem.Get(key)
}

func (table *Memtable) commit(key, value []byte) error {
	keyLen, valueLen := int64(len(key)), int64(len(value))
	if err := binary.Write(table.log, binary.LittleEndian, keyLen); err != nil {
		return err
	}
	if err := binary.Write(table.log, binary.LittleEndian, valueLen); err != nil {
		return err
	}
	if _, err := table.log.Write(key); err != nil {
		return err
	}
	if _, err := table.log.Write(value); err != nil {
		return err
	}
	table.Size += 16 + keyLen + valueLen
	return nil
}

func (table *Memtable) Put(key, value []byte) error {
	if err := table.commit(key, value); err != nil {
		return err
	}
	table.mem.Put(key, value)
	return nil
}

func (table *Memtable) Close() error {
	return table.log.Close()
}

func (table *Memtable) Compact() error {
	compacted, err := OpenWritable(table.Name)
	if err != nil {
		return err
	}
	defer compacted.Close()
	iterator := table.mem.Iterator()
	for iterator.Next() {
		if err := compacted.Append(iterator.Key(), iterator.Value()); err != nil {
			return err
		}
	}
	return nil
}

func (table *Memtable) Cleanup() error {
	return os.Remove(table.Name + logSuffix)
}

// Table is a key-sorted list of key-value pairs stored on disk.
// It is backed by multiple performance and size optimizations, such as
// block-based compression, key filtering using bloom filters,
// ARC cache for block accesses and RB tree based key indexing.
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
	if _, err := table.Index.ReadFrom(indexFile); err != nil {
		return table, err
	}
	filterFile, err := os.Open(name + filterSuffix)
	if err != nil {
		return table, err
	}
	defer filterFile.Close()
	if _, err := table.Filter.ReadFrom(filterFile); err != nil {
		return table, err
	}
	return table, nil
}

// OpenGlob opens a slice of tables identified by a common prefix.
func OpenGlob(glob string) ([]*Table, error) {
	matches, err := filepath.Glob(fmt.Sprintf("%s*%s", glob, tableSuffix))
	if err != nil {
		return nil, err
	}
	tables := make([]*Table, 0, len(matches))
	for _, name := range matches {
		table, err := Open(strings.TrimSuffix(name, tableSuffix))
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

// Close closes a table file.
func (table *Table) Close() error {
	return table.File.Close()
}

func (table *Table) seek(key []byte) ([]byte, bool) {
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

// Get returns the matching value to a key.
func (table *Table) Get(key []byte) [][]byte {
	block, ok := table.seek(key)
	if !ok {
		return nil
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

func sort(a, b []byte) ([]byte, []byte) {
	switch bytes.Compare(a, b) {
	case -1:
		return a, b
	default:
		return b, a
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
			min, max := sort(ls.Value(), rs.Value())
			if err := table.Append(ls.Key(), min); err != nil {
				return err
			}
			if err := table.Append(ls.Key(), max); err != nil {
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
func Find(block, key []byte) [][]byte {
	var (
		offset = int64(0)
		size   = int64(len(block))
		scan   = true
		values = make([][]byte, 0, 1)
	)
	for offset < size && scan {
		var (
			keyLen     = binary.LittleEndian.Uint16(block[offset : offset+2])
			valueLen   = binary.LittleEndian.Uint16(block[offset+2 : offset+4])
			keyStart   = offset + 4
			valueStart = keyStart + int64(keyLen)
			valueEnd   = valueStart + int64(valueLen)
		)
		switch bytes.Compare(key, block[keyStart:valueStart]) {
		case 0:
			values = append(values, block[valueStart:valueEnd])
		case 1:
			scan = false
		}
		offset = valueEnd
	}
	return values
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
