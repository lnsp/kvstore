package table

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/lnsp/kvstore/table/block"
	"github.com/lnsp/kvstore/table/index"

	"github.com/golang/snappy"
	"github.com/juju/ratelimit"
)

// WTable is an unfinished table in write mode.
// Values can only be written by increasing key order.
type WTable struct {
	Name string

	// mu protects the fields below.
	mu     sync.Mutex
	output io.Writer
	disk   *os.File
	buffer *bytes.Buffer
	index  *index.Index
	filter *index.Filter

	size int64
}

// Size returns the current size of the appentable.
func (wt *WTable) Size() int64 {
	wt.mu.Lock()
	size := wt.size
	wt.mu.Unlock()
	return size
}

// SizeOf returns the written size of the key-value pair.
func (wt *WTable) SizeOf(key, value []byte) int {
	return 4 + len(key) + len(value)
}

// Append appends the key to the table.
func (wt *WTable) Append(key, value []byte) error {
	wt.mu.Lock()
	defer wt.mu.Unlock()
	// Check if this is our first entry, create index entry in case
	if wt.buffer.Len() == 0 {
		wt.index.Put(key, wt.size)
	}
	wt.filter.Add(key)
	// Write key-value pair to buffer
	if err := block.WriteRow(wt.buffer, key, value); err != nil {
		return err
	}
	// Check if a flush is required
	if wt.buffer.Len() < block.FlushSize {
		return nil
	}
	return wt.flushBlock()
}

// Flush writes the size of the compressed block + the compressed block data to disk.
func (wt *WTable) flushBlock() error {
	compressedBlock := snappy.Encode(nil, wt.buffer.Bytes())
	compressedBlockSize := uint64(len(compressedBlock))
	if err := binary.Write(wt.output, binary.LittleEndian, compressedBlockSize); err != nil {
		return err
	}
	if _, err := wt.output.Write(compressedBlock); err != nil {
		return err
	}
	wt.size += 8 + int64(compressedBlockSize)
	wt.buffer.Reset()
	return nil
}

// FlushFilter writes the table filter to disk.
func (wt *WTable) flushFilter() error {
	filterFile, err := os.Create(wt.Name + filterSuffix)
	if err != nil {
		return err
	}
	defer filterFile.Close()
	_, err = wt.filter.WriteTo(filterFile)
	return err
}

// FlushIndex writes the table index to disk.
func (wt *WTable) flushIndex() error {
	indexFile, err := os.Create(wt.Name + indexSuffix)
	if err != nil {
		return err
	}
	defer indexFile.Close()
	_, err = wt.index.WriteTo(indexFile)
	return err
}

// Close closes the appentable.
func (wt *WTable) Close() error {
	if err := wt.flushBlock(); err != nil {
		return err
	}
	if err := wt.flushFilter(); err != nil {
		return err
	}
	if err := wt.flushIndex(); err != nil {
		return err
	}
	return wt.disk.Close()
}

// NewWTable opens a new appentable.
func NewWTable(path string, bucket *ratelimit.Bucket) (*WTable, error) {
	file, err := os.Create(path + tableSuffix)
	if err != nil {
		return nil, err
	}
	writer := ratelimit.Writer(file, bucket)
	return &WTable{
		Name: path,

		disk:   file,
		output: writer,
		filter: index.NewFilter(),
		index:  index.NewIndex(),
		buffer: bytes.NewBuffer(make([]byte, 0, block.FlushSize)),
	}, nil
}
