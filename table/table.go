package table

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/lnsp/kvstore/table/block"
	"github.com/lnsp/kvstore/table/index"

	"github.com/juju/ratelimit"
	"github.com/sirupsen/logrus"
)

var logger = logrus.New()

func init() {
	logger.SetLevel(logrus.DebugLevel)
}

var DefaultBucket = ratelimit.NewBucketWithRate(2<<21, 2<<22)

const (
	logSuffix    = ".log"
	tableSuffix  = ".table"
	indexSuffix  = ".index"
	filterSuffix = ".filter"

	// MaxCacheSize defines the maximum number of blocks cached in memory. By default 8 MiB.
	MaxCacheSize = 128
)

type LockedFile struct {
	*os.File
	sync.Mutex
}

// Table is a key-sorted list of key-value pairs stored on disk.
// It is backed by multiple performance and size optimizations, such as
// block-based compression, key filtering using bloom filters,
// ARC cache for block accesses and RB tree based key indexing.
type Table struct {
	Name string
	File *LockedFile

	// Table key range
	Begin, End []byte

	// Performance optimizations
	Index  *index.Index
	Filter *index.Filter
	Cache  *index.Cache

	// mfile is reserved for merge operations.
	mfile *LockedFile
}

// Open a table-triple and initializes the table file and loads index and filter into memory.
func Open(name string) (*Table, error) {
	tableFile, err := os.Open(name + tableSuffix)
	if err != nil {
		return nil, err
	}
	table := &Table{
		Name:   name,
		File:   &LockedFile{File: tableFile},
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

// determineKeyRange calculates the key range this table covers.
func (table *Table) determineKeyRange() error {
	// Get first key
	iterator := table.Index.Iterator()
	if !iterator.First() {
		table.Begin = []byte{}
		table.End = []byte{}
		return nil
	}
	table.Begin = iterator.Key().([]byte)
	// Seek last block, last entry
	if !iterator.Last() {
		return fmt.Errorf("failed to find last block")
	}
	endOffset := iterator.Value().(int64)
	b, _, ok := block.Seek(table.File, endOffset)
	if !ok {
		return fmt.Errorf("failed to load last block")
	}
	scanner := RowScanner{
		block: b,
	}
	for scanner.Next() {
		table.End = scanner.Key()
	}
	return nil
}

// InRange returns if the table may contain the given key.
func (table *Table) InRange(key []byte) bool {
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
	b, ok := table.Cache.Get(offset)
	if !ok {
		b, _, ok = block.Seek(table.File, offset)
		if ok {
			table.Cache.Add(offset, b)
		}
	}
	return b.([]byte), ok
}

func (table *Table) openMerge() (*TableScanner, error) {
	file, err := os.Open(table.Name + tableSuffix)
	if err != nil {
		return nil, err
	}
	table.mfile = &LockedFile{File: file}
	return &TableScanner{
		block: BlockScanner{
			input: table.mfile,
		},
	}, nil
}

func (table *Table) closeMerge() error {
	return table.mfile.Close()
}

// Get returns the matching value to a key.
func (table *Table) Get(key []byte) [][]byte {
	b, ok := table.seek(key)
	if !ok {
		return nil
	}
	return block.FindRow(b, key)
}

// Scan returns a scanner that scans through the table.
func (table *Table) Scan() *TableScanner {
	return &TableScanner{
		block: BlockScanner{
			input: table.File,
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
func Merge(path string, left *Table, right *Table, bucket *ratelimit.Bucket) error {
	table, err := NewWTable(path, bucket)
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
