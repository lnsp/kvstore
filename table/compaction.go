package table

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

const (
	MaxTableCacheTime    = time.Minute
	DefaultLeveledBase   = 5
	DefaultLeveledCount  = 10
	DefaultLeveledSize   = 2 << 24
	DefaultLeveledFactor = 10
)

func NewLeveledCompaction() *Leveled {
	return &Leveled{
		BaseSize:         DefaultLeveledSize,
		BaseCount:        DefaultLeveledBase,
		LevelSize:        DefaultLeveledSize,
		LevelCount:       DefaultLeveledCount,
		LevelSizeFactor:  DefaultLeveledFactor,
		LevelCountFactor: DefaultLeveledFactor,
	}
}

type Leveled struct {
	BaseSize, LevelSize, LevelSizeFactor    int64
	BaseCount, LevelCount, LevelCountFactor int

	Base   []*Table
	Levels []*Run

	mu sync.RWMutex
}

func (compaction *Leveled) Get(key []byte) [][]byte {
	compaction.mu.RLock()
	values := make([][]byte, 0, 1)
	for _, t := range compaction.Base {
		values = append(values, t.Get(key)...)
	}
	for _, level := range compaction.Levels {
		values = append(values, level.Get(key)...)
	}
	compaction.mu.RUnlock()
	return values
}

func (compaction *Leveled) Add(table *Table) error {
	compaction.mu.Lock()
	defer compaction.mu.Unlock()
	compaction.Base = append(compaction.Base, table)
	if len(compaction.Base) > compaction.BaseCount {
		if err := compaction.rebalance(); err != nil {
			return fmt.Errorf("leveled compaction rebalance: %v", err)
		}
	}
	return nil
}

// rebalance assumes that compaction has already been locked.
func (compaction *Leveled) rebalance() error {
	// Pick the first table
	level := 0
	victim := compaction.Base[0]
	compaction.Base = compaction.Base[1:]
	restore := func() {
		if level == 0 {
			compaction.Base = append(compaction.Base, victim)
			return
		}
		compaction.Levels[level-1].Push(victim)
	}
	for level < len(compaction.Levels) && victim != nil {
		// Merge victim with level
		if err := compaction.Levels[level].Merge(victim); err != nil {
			restore()
			return fmt.Errorf("failed victim merge: %v", err)
		}
		victim = nil
		// Check for extradiction
		if compaction.Levels[level].Overflow() {
			victim = compaction.Levels[level].Tables[0]
			compaction.Levels[level].Tables = compaction.Levels[level].Tables[1:]
		}
		level++
	}
	// victim not nil => we are missing levels
	if victim != nil {
		next := &Run{
			Name:     fmt.Sprintf("level%d", level),
			MaxCount: compaction.LevelCount,
			MaxSize:  compaction.LevelSize,
		}
		if err := next.Merge(victim); err != nil {
			restore()
			return fmt.Errorf("failed next level merge: %v", err)
		}
		compaction.Levels = append(compaction.Levels)
		compaction.LevelCount *= compaction.LevelCountFactor
		compaction.LevelSize *= compaction.LevelSizeFactor
	}
	return nil
}

type Run struct {
	Name     string
	Tables   []*Table
	MaxSize  int64
	MaxCount int

	// RW lock for tables
	access sync.RWMutex
	// Lock for garbage collector
	collector sync.Mutex
	garbage   []*Table
}

func (run *Run) Overflow() bool {
	return len(run.Tables) > run.MaxCount
}

// Get retrieves the table associated with the given key.
// Since a table may cease to exist after the max table cache time
// the table has to be fetched each time it is looked up.
func (run *Run) Get(key []byte) [][]byte {
	run.access.RLock()
	defer run.access.RUnlock()
	n := sort.Search(len(run.Tables), func(i int) bool {
		return bytes.Compare(run.Tables[i].Begin, key) >= 0
	})
	if n >= len(run.Tables) {
		return nil
	}
	return run.Tables[n].Get(key)
}

func (run *Run) Close() error {
	run.collector.Lock()
	for _, t := range run.garbage {
		if err := t.Delete(); err != nil {
			logger.WithError(err).Warn("failed to close garbage table")
		}
	}
	run.collector.Unlock()
	run.access.Lock()
	for _, t := range run.Tables {
		if err := t.Close(); err != nil {
			logger.WithError(err).Warn("failed to close active table")
		}
	}
	run.access.Unlock()
	return nil
}

func (run *Run) cleanup(tables []*Table) {
	run.collector.Lock()
	run.garbage = append(run.garbage, tables...)
	run.collector.Unlock()

	time.AfterFunc(MaxTableCacheTime, func() {
		run.collector.Lock()
		for _, t := range run.garbage {
			if err := t.Delete(); err != nil {
				logger.WithError(err).Warn("failed to close garbage table")
			}
			logger.WithFields(logrus.Fields{
				"table": t.Name,
				"run":   run.Name,
			}).Debug("Collected garbage table")
		}
		run.garbage = nil
		run.collector.Unlock()
	})
}

// Push inserts a single table back into the run.
// The table should have been evicted from the run beforehand.
func (run *Run) Push(table *Table) {
	run.access.Lock()
	index := sort.Search(len(run.Tables), func(i int) bool {
		return bytes.Compare(run.Tables[i].Begin, table.Begin) >= 0
	})
	run.Tables = append(run.Tables, nil)
	copy(run.Tables[index+1:], run.Tables[index:])
	run.Tables[index] = table
	run.access.Unlock()
}

// Join inserts a table vector replacing the tables at the given range.
// The tables are not range checked before insertion.
func (run *Run) Join(tables []*Table, begin, end int) error {
	run.access.Lock()
	defer run.access.Unlock()
	// Invoke tables to be cleaned up
	run.cleanup(run.Tables[begin:end])
	run.Tables = append(run.Tables[:begin], append(tables, run.Tables[end:]...)...)
	return nil
}

func (run *Run) generateAutoflushName() string {
	return fmt.Sprintf("%s-%s-%s", run.Name, time.Now().UTC().Format("2006-02-01-15-04-05"), uuid.New().String())
}

func (run *Run) Merge(table *Table) error {
	// [0 - 4] [5 - 10] [11 - 19] [20 - 24] [25 - 30]
	// [13 - 22]
	begin := sort.Search(len(run.Tables), func(i int) bool {
		return bytes.Compare(run.Tables[i].End, table.Begin) >= 0
	})
	// Begin should point to [11 - 19]
	end := sort.Search(len(run.Tables), func(i int) bool {
		return bytes.Compare(run.Tables[i].Begin, table.End) > 0
	})
	logger.WithFields(logrus.Fields{
		"begin": begin,
		"end":   end,
		"run":   run.Name,
		"table": table.Name,
	}).Debug("Merging into run")
	// End should point to [25 - 30]
	merger := table.Scan()
	iterators := make([]*TableScanner, end-begin)
	for i := begin; i < end; i++ {
		iterators[i-begin] = run.Tables[i].Scan()
	}
	autoflush := OpenWritableWithAutoflush(run.generateAutoflushName(), run.MaxSize, DefaultBucket)
	defer autoflush.Close()
	// Perform merge
	for i := 0; i < len(iterators); i++ {
		// If merger.Peek() false, we just go through all iterators and append to
		// the autoflush table. If the iterators go empty, we will just run through the
		// outer loop and push all merger items onto the autoflush table.
		for iterators[i].Peek() && merger.Peek() {
			p, k, v := merger.Compare(iterators[i])
			if err := autoflush.Append(k, v); err != nil {
				return err
			}
			if p >= 0 {
				iterators[i].Skip()
			}
			if p <= 0 {
				merger.Skip()
			}
		}
		for iterators[i].Peek() {
			if err := autoflush.Append(iterators[i].Key(), iterators[i].Value()); err != nil {
				return err
			}
		}
	}
	for merger.Next() {
		if err := autoflush.Append(merger.Key(), merger.Value()); err != nil {
			return err
		}
	}
	// Should not fail since atomic
	if err := autoflush.Close(); err != nil {
		return err
	}
	// Load all tables into run
	tables, err := OpenTables(autoflush.Basename)
	if err != nil {
		return fmt.Errorf("failed to load merged tables: %v", err)
	}
	if err := run.Join(tables, begin, end); err != nil {
		return fmt.Errorf("failed to finalize merge: %v", err)
	}
	return nil
}
