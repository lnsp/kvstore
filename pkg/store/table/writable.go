package table

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"valar/godat/pkg/store/table/index"

	"github.com/golang/snappy"
	"github.com/juju/ratelimit"
)

// WritableTable is an unfinished table in write mode.
// Values can only be written by increasing key order.
type WritableTable struct {
	Index  *index.Index
	Filter *index.Filter
	File   io.Writer
	Name   string
	Size   int64

	disk   *os.File
	buffer *bytes.Buffer
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
		table.Index.Put(key, table.Size)
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
	table.Size += 8 + int64(compressedBlockSize)
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

func OpenWritableWithRateLimit(name string, bucket *ratelimit.Bucket) (*WritableTable, error) {
	file, err := os.Create(name + tableSuffix)
	if err != nil {
		return nil, err
	}
	writer := ratelimit.Writer(file, bucket)
	return &WritableTable{
		Filter: index.NewFilter(),
		Index:  index.NewIndex(),
		File:   writer,
		Name:   name,
		disk:   file,
		buffer: bytes.NewBuffer(make([]byte, 0, MaxBlockSize)),
	}, nil
}
