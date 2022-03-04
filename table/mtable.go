package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/lnsp/kvstore/table/index"

	"github.com/sirupsen/logrus"
)

// MTable represents an in-memory table.
type MTable struct {
	Name string

	mem  *index.Memory
	path string

	// mu protects the fields below.
	mu   sync.Mutex
	log  io.ReadWriteCloser
	size int64
}

// ReadFrom reads from an existing mtable log.
func (mtable *MTable) ReadFrom(cached io.Reader) (int64, error) {
	mtable.mu.Lock()
	defer mtable.mu.Unlock()
	// Read until nothing available
	var keyLen, valueLen int64
	i64size := int64(binary.Size(int64(0)))
	for {
		if err := binary.Read(cached, binary.LittleEndian, &keyLen); err != nil && err != io.EOF {
			return mtable.size, fmt.Errorf("failed to read key len: %w", err)
		} else if err == io.EOF {
			return mtable.size, nil
		}
		// Read value len
		if err := binary.Read(cached, binary.LittleEndian, &valueLen); err != nil && err != io.EOF {
			return mtable.size, fmt.Errorf("failed to read value len: %w", err)
		} else if err == io.EOF {
			return mtable.size, nil
		}
		// Read key
		key := make([]byte, keyLen)
		if _, err := cached.Read(key); err != nil && err != io.EOF {
			return mtable.size, fmt.Errorf("failed to read key, expected len %d: %w", keyLen, err)
		} else if err == io.EOF {
			return mtable.size, nil
		}
		// Read value
		value := make([]byte, valueLen)
		if _, err := cached.Read(value); err != nil && err != io.EOF {
			return mtable.size, fmt.Errorf("failed to read value: %w", err)
		} else if err == io.EOF {
			return mtable.size, nil
		}
		mtable.mem.Put(key, value)
		mtable.size += 2*i64size + keyLen + valueLen
	}
}

// NewMTableFromFile initializes a new MTable backed by disk.
func NewMTableFromFile(name string) (*MTable, error) {
	f, err := os.OpenFile(name+logSuffix, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return NewMTable(f, name)
}

// NewMTable initializes a new in-memory table.
func NewMTable(log io.ReadWriteCloser, name string) (*MTable, error) {
	path := ""
	if f, ok := log.(*os.File); ok {
		path = f.Name()
	}
	mtable := &MTable{
		Name: name,

		path: path,
		mem:  index.NewMemory(),
		log:  log,
		size: 0,
	}
	if _, err := mtable.ReadFrom(log); err != nil {
		return nil, fmt.Errorf("read mtable from %s: %w", name, err)
	}
	return mtable, nil
}

func (table *MTable) Size() int64 {
	table.mu.Lock()
	defer table.mu.Unlock()
	return table.size
}

func (table *MTable) Get(key []byte) [][]byte {
	values := table.mem.Get(key)
	return values
}

func (table *MTable) commit(key, value []byte) error {
	// Build item buffer
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
	table.mu.Lock()
	_, err := table.log.Write(buffer.Bytes())
	if err != nil {
		table.mu.Unlock()
		return err
	}
	table.size += 16 + keyLen + valueLen
	table.mu.Unlock()
	return nil
}

// Put adds a new key value pair to the table.
func (table *MTable) Put(key, value []byte) error {
	if err := table.commit(key, value); err != nil {
		return err
	}
	table.mem.Put(key, value)
	return nil
}

// Merge combines the newer memtable into this one.
func (table *MTable) Merge(newer *MTable) error {
	newer.mu.Lock()
	defer newer.mu.Unlock()
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
		if err := table.Put(iterator.Key(), iterator.Value()); err != nil {
			return err
		}
	}
	if err := newer.Close(); err != nil {
		return err
	}
	if err := newer.Cleanup(); err != nil {
		return err
	}
	return nil
}

// Close closes the mtable log.
func (table *MTable) Close() error {
	return table.log.Close()
}

// Compact compacts the memtable to a disk-backed table.
// This does not remove the memtable log file.
func (table *MTable) Compact() error {
	compacted, err := OpenWTableFromFile(table.Name, nil)
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

// Cleanup removes the table log from disk.
func (table *MTable) Cleanup() error {
	if table.path != "" {
		return os.Remove(table.path)
	}
	return nil
}

// All returns all stored MTable records.
func (table *MTable) All() []MTableRecord {
	records := make([]MTableRecord, table.mem.Size())
	iterator := table.mem.SetIterator()
	for index := 0; iterator.Next(); index++ {
		records[index] = MTableRecord{iterator.Key(), iterator.Values()}
	}
	return records
}

// MTableRecord represents a single record in the MTable.
type MTableRecord struct {
	Key    []byte
	Values [][]byte
}
