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

	"github.com/lnsp/kvstore/table/index"

	"github.com/golang/snappy"
	"github.com/juju/ratelimit"
	"github.com/sirupsen/logrus"
)

var logger = logrus.New()

var DefaultBucket = ratelimit.NewBucketWithRate(2<<21, 2<<22)

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

// Table is a key-sorted list of key-value pairs stored on disk.
// It is backed by multiple performance and size optimizations, such as
// block-based compression, key filtering using bloom filters,
// ARC cache for block accesses and RB tree based key indexing.
type Table struct {
	Name string
	File *File

	// Table key range
	Begin, End []byte

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

// Recover recovers a set of memtables matched by the given prefix and stores a merged version inside a single memtable.
func Recover(prefix, name string) (*MTable, error) {
	matches, err := filepath.Glob(fmt.Sprintf("%s*%s", prefix, logSuffix))
	if err != nil {
		return nil, err
	}
	memtable, err := NewMTableFromFile(name)
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	for _, table := range matches {
		logger.WithFields(logrus.Fields{
			"from": table,
			"into": name,
		}).Debug("merge memtables")
		old, err := NewMTableFromFile(strings.TrimSuffix(table, logSuffix))
		if err != nil {
			return nil, err
		}
		if err := memtable.Merge(old); err != nil {
			return nil, err
		}
	}
	return memtable, nil
}

// RemoveTableLogs drops all tables logs associated with tables with the given prefix.
func RemoveTableLogs(prefix string) error {
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

// Delete closes a table file and removes it from disk.
func (table *Table) Delete() error {
	if err := table.File.Close(); err != nil {
		return err
	}
	// Delete table, index and filter
	if err := os.Remove(table.Name + tableSuffix); err != nil {
		return err
	}
	if err := os.Remove(table.Name + filterSuffix); err != nil {
		return err
	}
	if err := os.Remove(table.Name + indexSuffix); err != nil {
		return err
	}
	return nil
}

func (table *Table) determineKeyRange() error {
	// Get first key
	iterator := table.Index.Iterator()
	if !iterator.First() {
		return fmt.Errorf("failed to find first block")
	}
	table.Begin = iterator.Key().([]byte)
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
		table.End = scanner.Key()
	}
	return nil
}

func (table *Table) Range(key []byte) bool {
	return bytes.Compare(key, table.Begin) >= 0 && bytes.Compare(key, table.End) <= 0
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
	table, err := OpenWTableFromFile(name, nil)
	if err != nil {
		return err
	}
	defer table.Close()
	ls, err := left.openMerge()
	if err != nil {
		return err
	}
	defer left.closeMerge()
	rs, err := right.openMerge()
	if err != nil {
		return err
	}
	defer right.closeMerge()
	for ls.Peek() && rs.Peek() {
		switch bytes.Compare(ls.Key(), rs.Key()) {
		case 0:
			_, max := minmax(ls.Value(), rs.Value())
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
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
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
