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

	"github.com/lnsp/kvstore/table/index"
	"github.com/sirupsen/logrus"
)

// OpenMemtable opens a new memtable.
// The prefix given is used for glob searching of memtable logs of the form "prefix(*).log".
// All matched logs are then merged back together into a in-memory table.
func OpenMemtable(prefix, path string) (*Memtable, error) {
	matches, err := filepath.Glob(fmt.Sprintf("%s*%s", prefix, logSuffix))
	if err != nil {
		return nil, err
	}
	memtable, err := NewMemtable(path)
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	for _, table := range matches {
		logger.WithFields(logrus.Fields{
			"from": table,
			"into": path,
		}).Debug("Merging memtables")
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

// RemovePartialTables deletes all tables which have a log file attached to them.
func RemovePartialTables(prefix string) error {
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

type WriteCloseSeeker interface {
	io.Writer
	io.Seeker
	io.Closer
}

// Memtable is an in-memory key-value table.
type Memtable struct {
	Name string
	Size int64

	mem *index.Memory

	// mu protects the fields below.
	mu       sync.Mutex
	log      WriteCloseSeeker
	logshift int64
}

// ReadFrom converts the byte stream item by item into a memtable.
// The memory layout is the same used for log items.
// Each entry consists of [keyLen : int64][valueLen : int64][key : bytes][value : bytes].
func (table *Memtable) ReadFrom(cached io.Reader) (int64, error) {
	// Read until nothing available
	var keyLen, valueLen, totalBytesRead int64
	for {
		// Read key len
		if err := binary.Read(cached, binary.LittleEndian, &keyLen); err == io.EOF || err == io.ErrUnexpectedEOF {
			return totalBytesRead, nil
		} else if err != nil {
			return totalBytesRead, fmt.Errorf("failed to read key len: %v", err)
		}
		// Read value len
		if err := binary.Read(cached, binary.LittleEndian, &valueLen); err == io.EOF || err == io.ErrUnexpectedEOF {
			return totalBytesRead, nil
		} else if err != nil {
			return totalBytesRead, fmt.Errorf("failed to read value len: %v", err)
		}
		// Read key
		key := make([]byte, keyLen)
		if n, err := cached.Read(key); int64(n) != keyLen || err == io.EOF || err == io.ErrUnexpectedEOF {
			return totalBytesRead, nil
		} else if err != nil {
			return totalBytesRead, fmt.Errorf("failed to read key, expected len %d: %v", keyLen, err)
		}
		// Read value
		value := make([]byte, valueLen)
		if n, err := cached.Read(value); int64(n) != valueLen || err == io.EOF || err == io.ErrUnexpectedEOF {
			return totalBytesRead, nil
		} else if err != nil {
			return totalBytesRead, fmt.Errorf("failed to read value: %v", err)
		}
		table.mem.Put(key, value)
		totalBytesRead += 16 + keyLen + valueLen
	}
}

// NewMemtable initializes a new memtable.
func NewMemtable(name string) (*Memtable, error) {
	cached, err := os.OpenFile(name+logSuffix, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	t := &Memtable{
		Name: name,
		mem:  index.NewMemory(),
		log:  cached,
	}
	t.Size, err = t.ReadFrom(cached)
	if err != nil {
		return nil, fmt.Errorf("load memtable %s: %v", name, err)
	}
	return t, nil
}

// Get returns the value associated with the key.
func (table *Memtable) Get(key []byte) [][]byte {
	values := table.mem.Get(key)
	return values
}

// commit commits the entry to the memtable.
// mu must be held.
func (table *Memtable) commit(key, value []byte) error {
	buffer := new(bytes.Buffer)
	keyLen, valueLen := int64(len(key)), int64(len(value))
	// In case the last write has failed, we may need to shift our current cursor back
	if table.logshift != 0 {
		if _, err := table.log.Seek(io.SeekCurrent, int(table.logshift)); err != nil {
			return err
		}
		table.logshift = 0
	}
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
	if n, err := table.log.Write(buffer.Bytes()); err != nil {
		table.logshift = -int64(n)
		return err
	}
	table.Size += 16 + keyLen + valueLen
	return nil
}

// Put stores a key-value pair in the memtable.
func (table *Memtable) Put(key, value []byte) error {
	table.mu.Lock()
	defer table.mu.Unlock()
	if err := table.commit(key, value); err != nil {
		return err
	}
	table.mem.Put(key, value)
	return nil
}

// Merge merges this memtable with a newer one.
func (table *Memtable) Merge(newer *Memtable) error {
	// Copy all log entries to this table
	newer.mu.Lock()
	defer newer.mu.Unlock()
	// Copy memory conents
	iterator := newer.mem.Iterator()
	for index := 0; iterator.Next(); index++ {
		key, value := iterator.Key(), iterator.Value()
		if err := table.commit(key, value); err != nil {
			return err
		}
		table.mem.Put(key, value)
	}
	if err := newer.Close(); err != nil {
		return err
	}
	// Remove old log file
	if err := newer.Cleanup(); err != nil {
		return err
	}
	return nil
}

// Close closes the memtable.
func (table *Memtable) Close() error {
	return table.log.Close()
}

// Compact compresses the memtable log to disk.
func (table *Memtable) Compact() error {
	table.mu.Lock()
	defer table.mu.Unlock()
	compacted, err := NewWTable(table.Name, DefaultBucket)
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
