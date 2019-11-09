package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"valar/godat/pkg/store/table/index"

	"github.com/golang/snappy"
	"github.com/juju/ratelimit"
	"github.com/sirupsen/logrus"
)

var logger = logrus.New()

func init() {
	logger.SetLevel(logrus.DebugLevel)
}

const (
	logSuffix    = ".log"
	tableSuffix  = ".table"
	indexSuffix  = ".index"
	filterSuffix = ".filter"

	// MaxBlockSize defines the maximum size of a table block. By default 64 KiB.
	MaxBlockSize = 2 << 16
	// MaxCacheSize defines the maximum number of blocks cached in memory. By default 8 MiB.
	MaxCacheSize = 128

	defaultCompactRate = float64(2 << 21)
	defaultCompactCap  = int64(2 << 22)
)

type Memtable struct {
	Name string
	Size int64

	log *File
	mem *index.Memory
}

type File struct {
	*os.File
	sync.Mutex
}

func (memtable *Memtable) ReadFrom(cached io.Reader) (int64, error) {
	// Read until nothing available
	var keyLen, valueLen, bytesRead int64
	for binary.Read(cached, binary.LittleEndian, &keyLen) == nil {
		if bytesRead%1_000_000 == 0 {
			logger.WithFields(logrus.Fields{
				"name":      memtable.Name,
				"bytesRead": bytesRead,
			}).Debug("load memtable from disk")
		}
		bytesRead += 8
		// Read value len
		if err := binary.Read(cached, binary.LittleEndian, &valueLen); err != nil && err != io.EOF {
			return bytesRead, fmt.Errorf("failed to read value len: %v", err)
		} else if err == io.EOF {
			return bytesRead, nil
		}
		bytesRead += 8
		// Read key
		key := make([]byte, keyLen)
		if _, err := cached.Read(key); err != nil && err != io.EOF {
			return bytesRead, fmt.Errorf("failed to read key, expected len %d: %v", keyLen, err)
		} else if err == io.EOF {
			return bytesRead, nil
		}
		bytesRead += keyLen
		// Read value
		value := make([]byte, valueLen)
		if _, err := cached.Read(value); err != nil && err != io.EOF {
			return bytesRead, fmt.Errorf("failed to read value: %v", err)
		} else if err == io.EOF {
			return bytesRead, nil
		}
		bytesRead += valueLen
		memtable.mem.Put(key, value)
	}
	return bytesRead, nil
}

func NewMemtable(name string) (*Memtable, error) {
	cached, err := os.OpenFile(name+logSuffix, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	Memtable := &Memtable{
		Name: name,
		mem:  index.NewMemory(),
	}
	Memtable.Size, err = Memtable.ReadFrom(cached)
	if err != nil {
		return nil, fmt.Errorf("failed to load file %s: %v", name, err)
	}
	Memtable.log = &File{File: cached}
	return Memtable, nil
}

func (table *Memtable) Get(key []byte) [][]byte {
	values := table.mem.Get(key)
	return values
}

func (table *Memtable) commit(key, value []byte) error {
	table.log.Lock()
	defer table.log.Unlock()
	buffer := new(bytes.Buffer)
	keyLen, valueLen := int64(len(key)), int64(len(value))
	if err := binary.Write(buffer, binary.LittleEndian, keyLen); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.LittleEndian, valueLen); err != nil {
		return err
	}
	if _, err := buffer.Write(key); err != nil {
		return err
	}
	if _, err := buffer.Write(value); err != nil {
		return err
	}
	// Commit
	if _, err := table.log.Write(buffer.Bytes()); err != nil {
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

func (table *Memtable) Merge(newer *Memtable) error {
	// Copy all log entries to this table
	newer.log.Lock()
	defer newer.log.Unlock()
	// Remove old log file
	// Copy memory conents
	size := newer.mem.Size()
	iterator := newer.mem.Iterator()
	for index := 0; iterator.Next(); index++ {
		if index%10000 == 0 {
			logger.WithFields(logrus.Fields{
				"from":     newer.Name,
				"into":     table.Name,
				"progress": float64(index) / float64(size),
			}).Debug("merge memtables")
		}
		table.Put(iterator.Key(), iterator.Value())
	}
	if err := newer.Close(); err != nil {
		return err
	}
	if err := newer.Cleanup(); err != nil {
		return err
	}
	return nil
}

func (table *Memtable) Close() error {
	return table.log.Close()
}

func (table *Memtable) Compact() error {
	compacted, err := OpenWritableWithRateLimit(table.Name, defaultCompactRate, defaultCompactCap)
	if err != nil {
		return fmt.Errorf("failed to open writable table: %v", err)
	}
	defer compacted.Close()
	iterator := table.mem.SetIterator()
	for index := 0; iterator.Next(); index++ {
		values := iterator.Values()
		n := len(values)
		if err := compacted.Append(iterator.Key(), values[n-1]); err != nil {
			return fmt.Errorf("failed to compact key %v: %v", iterator.Key(), err)
		}
	}
	return nil
}

func (table *Memtable) Cleanup() error {
	return os.Remove(table.Name + logSuffix)
}

func (table *Memtable) All() []MemtableRecord {
	records := make([]MemtableRecord, table.mem.Size())
	iterator := table.mem.SetIterator()
	for index := 0; iterator.Next(); index++ {
		records[index] = MemtableRecord{iterator.Key(), iterator.Values()}
	}
	return records
}

type MemtableRecord struct {
	Key    []byte
	Values [][]byte
}

// Table is a key-sorted list of key-value pairs stored on disk.
// It is backed by multiple performance and size optimizations, such as
// block-based compression, key filtering using bloom filters,
// ARC cache for block accesses and RB tree based key indexing.
type Table struct {
	Name string
	File *File

	// Table key range
	begin, end []byte

	// Performance optimizations
	Index  *index.Index
	Filter *index.Filter
	Cache  *index.Cache

	// mfile is reserved for merge operations.
	mfile *File
}

// Open a table-triple and initializes the table file and loads index and filter into memory.
func Open(name string) (*Table, error) {
	tableFile, err := os.Open(name + tableSuffix)
	if err != nil {
		return nil, err
	}
	table := &Table{
		Name:   name,
		File:   &File{File: tableFile},
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
	if err := table.determineKeyRange(); err != nil {
		return table, fmt.Errorf("failed to determine key range: %v", err)
	}
	return table, nil
}

func OpenMemtable(prefix, name string) (*Memtable, error) {
	matches, err := filepath.Glob(fmt.Sprintf("%s*%s", prefix, logSuffix))
	if err != nil {
		return nil, err
	}
	memtable, err := NewMemtable(name)
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	for _, table := range matches {
		logger.WithFields(logrus.Fields{
			"from": table,
			"into": name,
		}).Debug("merge memtables")
		old, err := NewMemtable(strings.TrimSuffix(table, logSuffix))
		if err != nil {
			return nil, err
		}
		if err := memtable.Merge(old); err != nil {
			return nil, err
		}
	}
	return memtable, nil
}

func RemoveIntermediateTables(prefix string) error {
	matches, err := filepath.Glob(fmt.Sprintf("%s*%s", prefix, tableSuffix))
	if err != nil {
		return err
	}
	for _, table := range matches {
		log := strings.TrimSuffix(table, tableSuffix) + logSuffix
		if _, err := os.Stat(log); os.IsNotExist(err) {
			continue
		}
		if err := os.Remove(table); err != nil {
			return err
		}
	}
	return nil
}

// OpenTables opens a slice of tables identified by a common prefix.
func OpenTables(glob string) ([]*Table, error) {
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

func (table *Table) determineKeyRange() error {
	// Get first key
	iterator := table.Index.Iterator()
	if !iterator.First() {
		return fmt.Errorf("failed to find first block")
	}
	table.begin = iterator.Key().([]byte)
	// Seek last block, last entry
	if !iterator.Last() {
		return fmt.Errorf("failed to find last block")
	}
	endOffset := iterator.Value().(int64)
	block, _, ok := Seek(table.File, endOffset)
	if !ok {
		return fmt.Errorf("failed to load last block")
	}
	scanner := RowScanner{
		block: block,
	}
	for scanner.Next() {
		table.end = scanner.Key()
	}
	return nil
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

func (table *Table) openMerge() (*TableScanner, error) {
	file, err := os.Open(table.Name + tableSuffix)
	if err != nil {
		return nil, err
	}
	table.mfile = &File{File: file}
	return &TableScanner{
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

func (table *Table) Scan() *TableScanner {
	return &TableScanner{
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
	File   io.Writer
	Name   string

	disk   *os.File
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
	return table.disk.Close()
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
		disk:   file,
		buffer: bytes.NewBuffer(make([]byte, 0, MaxBlockSize)),
	}, nil
}

func OpenWritableWithRateLimit(name string, refill float64, cap int64) (*WritableTable, error) {
	file, err := os.Create(name + tableSuffix)
	if err != nil {
		return nil, err
	}
	writer := ratelimit.Writer(file, ratelimit.NewBucketWithRate(refill, cap))
	return &WritableTable{
		Filter: index.NewFilter(),
		Index:  index.NewIndex(),
		File:   writer,
		Name:   name,
		disk:   file,
		buffer: bytes.NewBuffer(make([]byte, 0, MaxBlockSize)),
	}, nil
}

func minmax(a, b []byte) ([]byte, []byte) {
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
			min, max := minmax(ls.Value(), rs.Value())
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

func seekCompressedBlock(file *File, offset int64) ([]byte, int64, bool) {
	file.Lock()
	defer file.Unlock()
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
	return compressedBlock, 8 + compressedBlockSize, true
}

// Seek searches the file for a block at the given offset.
func Seek(file *File, offset int64) ([]byte, int64, bool) {
	compressedBlock, size, ok := seekCompressedBlock(file, offset)
	if !ok {
		return nil, 0, false
	}
	block, err := snappy.Decode(nil, compressedBlock)
	if err != nil {
		return nil, 0, false
	}
	return block, size, true
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
func ScanBlocks(file *File) BlockScanner {
	return BlockScanner{
		file:   file,
		offset: 0,
		block:  nil,
	}
}

type BlockScanner struct {
	file           *File
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

type Leveled struct {
	BaseSize    int
	LevelSize   int
	LevelFactor int
	Base        []*Table
	Levels      []Run
}

func (compaction *Leveled) Add(table *Table) error {
	compaction.Base = append(compaction.Base, table)
	if len(compaction.Base) > compaction.BaseSize {
		return compaction.Push()
	}
	return nil
}

func (compaction *Leveled) Push() error {
	return nil
}

type Run struct {
	*index.RunIndex
	MaxSize int
}

func NewRun(max int) *Run {
	return &Run{
		RunIndex: index.NewRunIndex(),
		MaxSize:  max,
	}
}
