package table

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/lnsp/kvstore/table/index"

	"github.com/golang/snappy"
	"github.com/juju/ratelimit"
)

// WTable is an unfinished table in write mode.
// Values can only be written by increasing key order.
// WTable is not safe to use with multiple goroutines concurrently.
type WTable struct {
	Name string

	index  *index.Index
	filter *index.Filter
	closer io.Closer
	writer io.Writer
	size   int64
	buffer *bytes.Buffer
}

func (table *WTable) Size() int64 {
	return table.size
}

func (table *WTable) Append(key, value []byte) error {
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
		table.index.Put(key, table.Size)
	}
	// Add to filter
	table.filter.Add(key)
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

func (table *WTable) Flush() error {
	compressedBlock := snappy.Encode(nil, table.buffer.Bytes())
	compressedBlockSize := uint64(len(compressedBlock))
	if err := binary.Write(table.writer, binary.LittleEndian, compressedBlockSize); err != nil {
		return err
	}
	if _, err := table.writer.Write(compressedBlock); err != nil {
		return err
	}
	table.size += 8 + int64(compressedBlockSize)
	table.buffer.Reset()
	return nil
}

func (table *WTable) FlushFilter() error {
	filterFile, err := os.Create(table.Name + filterSuffix)
	if err != nil {
		return err
	}
	defer filterFile.Close()
	_, err = table.filter.WriteTo(filterFile)
	return err
}

func (table *WTable) FlushIndex() error {
	indexFile, err := os.Create(table.Name + indexSuffix)
	if err != nil {
		return err
	}
	defer indexFile.Close()
	_, err = table.index.WriteTo(indexFile)
	return err
}

func (table *WTable) Close() error {
	if err := table.Flush(); err != nil {
		return err
	}
	if err := table.FlushFilter(); err != nil {
		return err
	}
	if err := table.FlushIndex(); err != nil {
		return err
	}
	return table.closer.Close()
}

// OpenWTable opens a new write-only table.
func OpenWTable(name string, readWriteCloser io.ReadWriteCloser, bucket *ratelimit.Bucket) (*WTable, error) {
	writer := io.Writer(readWriteCloser)
	if bucket != nil {
		writer = ratelimit.Writer(writer, bucket)
	}

	return &WTable{
		Name: name,

		filter: index.NewFilter(),
		index:  index.NewIndex(),
		writer: writer,
		closer: readWriteCloser,
		buffer: bytes.NewBuffer(make([]byte, 0, MaxBlockSize)),
	}, nil
}

func OpenWTableFromFile(name string, bucket *ratelimit.Bucket) (*WTable, error) {
	file, err := os.Create(name + tableSuffix)
	if err != nil {
		return nil, err
	}
	return OpenWTable(name, file, bucket)
}
