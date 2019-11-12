package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"valar/kv/pkg/store/table/index"

	"github.com/sirupsen/logrus"
)

type Memtable struct {
	Name string
	Size int64

	log *File
	mem *index.Memory
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
	compacted, err := OpenWritableWithRateLimit(table.Name, DefaultBucket)
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
