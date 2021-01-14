package table

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

const (
	RunPrefix            = "run"
	MaxTableCacheTime    = time.Minute
	DefaultLeveledBase   = 5
	DefaultLeveledCount  = 10
	DefaultLeveledSize   = 2 << 26
	DefaultLeveledFactor = 10
)

func NewLeveledCompaction(name string) *Leveled {
	return &Leveled{
		Name:             name,
		BaseSize:         DefaultLeveledSize,
		BaseCount:        DefaultLeveledBase,
		LevelSize:        DefaultLeveledSize,
		LevelCount:       DefaultLeveledCount,
		LevelSizeFactor:  DefaultLeveledFactor,
		LevelCountFactor: DefaultLeveledFactor,
	}
}

type Leveled struct {
	Name                                    string
	BaseSize, LevelSize, LevelSizeFactor    int64
	BaseCount, LevelCount, LevelCountFactor int

	Base   []*Table
	Levels []*Run

	mu sync.RWMutex
}

func (compaction *Leveled) Restore(tables []*Table) error {
	// Check if tables is prefixed
	for _, table := range tables {
		if strings.HasPrefix(table.Name, fmt.Sprintf("%s-%s", compaction.Name, RunPrefix)) {
			// Find out which level
			var level int
			fmt.Sscanf(table.Name, compaction.Name+"-%s%04d", &level)
			for level >= len(compaction.Levels) {
				compaction.Levels = append(compaction.Levels, &Run{
					Name:     fmt.Sprintf("%s-%s%04d", compaction.Name, RunPrefix, len(compaction.Levels)),
					MaxSize:  compaction.LevelSize,
					MaxCount: compaction.LevelCount,
				})
				compaction.LevelSize *= compaction.LevelSizeFactor
				compaction.LevelCount *= compaction.LevelCountFactor
			}
			if !compaction.Levels[level].Push(table) {
				if err := compaction.Levels[level].Merge(table); err != nil {
					return fmt.Errorf("add to level: %w", err)
				}
			}
			logrus.WithFields(logrus.Fields{
				"level": level,
				"table": table.Name,
			}).Debug("Add to leveled compaction")
		} else {
			if err := compaction.Add(table); err != nil {
				return fmt.Errorf("add to compaction: %w", err)
			}
			logrus.WithFields(logrus.Fields{
				"table": table.Name,
			}).Debug("Add to base compaction")
		}
	}
	return nil
}

func (compaction *Leveled) Close() error {
	compaction.mu.Lock()
	defer compaction.mu.Unlock()
	for _, t := range compaction.Base {
		if err := t.Close(); err != nil {
			return err
		}
	}
	for _, r := range compaction.Levels {
		if err := r.Close(); err != nil {
			return err
		}
	}
	return nil
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
	if err := compaction.rebalance(); err != nil {
		return fmt.Errorf("leveled compaction rebalance: %v", err)
	}
	return nil
}

func (compaction *Leveled) nextLevel(table *Table) error {
	level := len(compaction.Levels)
	next := &Run{
		Name:     fmt.Sprintf("%s-%s%04d", compaction.Name, RunPrefix, level),
		MaxCount: compaction.LevelCount,
		MaxSize:  compaction.LevelSize,
	}
	if err := next.Merge(table); err != nil {
		return fmt.Errorf("merge into next level: %w", err)
	}
	compaction.Levels = append(compaction.Levels)
	compaction.LevelCount *= compaction.LevelCountFactor
	compaction.LevelSize *= compaction.LevelSizeFactor
	return nil
}

// rebalance assumes that compaction has already been locked.
// We first pick the merge victim from our Base slice.
// Then try to merge it up the tree (as long as there are existing levels).
func (compaction *Leveled) rebalance() error {
	// Ignore if no rebalance needed
	if len(compaction.Base) < 1 || len(compaction.Base) < compaction.BaseCount {
		return nil
	}
	// Pick the first table
	level := 0
	victim := compaction.Base[0]
	compaction.Base = compaction.Base[1:]
	// Restore makes sure to append the victim back to the
	// level it originally came from.
	restore := func() {
		if level == 0 {
			compaction.Base = append(compaction.Base, victim)
			return
		}
		compaction.Levels[level-1].Push(victim)
	}
	for level < len(compaction.Levels) {
		// Merge victim with level
		if err := compaction.Levels[level].Merge(victim); err != nil {
			restore()
			return fmt.Errorf("failed victim merge: %w", err)
		}
		// Remove old victim
		if err := victim.Delete(); err != nil {
			return fmt.Errorf("failed victim delete: %w", err)
		}
		// Check for extradiction, else we can return early
		if compaction.Levels[level].Overflow() {
			victim = compaction.Levels[level].Tables[0]
			compaction.Levels[level].Tables = compaction.Levels[level].Tables[1:]
		} else {
			return nil
		}
		level++
	}
	// victim not nil, we need to append a new level
	if err := compaction.nextLevel(victim); err != nil {
		restore()
		return fmt.Errorf("failed victim next level: %w", err)
	}
	if err := victim.Delete(); err != nil {
		return fmt.Errorf("failed victim delete: %w", err)
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

// Overflow returns true if there are more tables in the run than allowed.
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

// Close closes a run.
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
				continue
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
// If the table can not be inserted without violating the non-intersecting interval property
// the function returns false.
func (run *Run) Push(table *Table) bool {
	run.access.Lock()
	defer run.access.Unlock()
	index := sort.Search(len(run.Tables), func(i int) bool {
		return bytes.Compare(run.Tables[i].Begin, table.Begin) >= 0
	})
	if index < len(run.Tables)-1 {
		if bytes.Compare(run.Tables[index+1].Begin, table.End) <= 0 {
			return false
		}
	}
	run.Tables = append(run.Tables, nil)
	copy(run.Tables[index+1:], run.Tables[index:])
	run.Tables[index] = table
	return true
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

// Merge merges the given table into the run.
func (run *Run) Merge(table *Table) error {
	// Find overlapping tables
	begin := sort.Search(len(run.Tables), func(i int) bool {
		return bytes.Compare(run.Tables[i].End, table.Begin) >= 0
	})
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
	iterators := make([]*Scanner, end-begin)
	for i := begin; i < end; i++ {
		iterators[i-begin] = run.Tables[i].Scan()
	}
	autoflush := NewAFTable(run.generateAutoflushName(), run.MaxSize, DefaultBucket)
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
	tables, err := OpenTables(autoflush.Path)
	if err != nil {
		return fmt.Errorf("failed to load merged tables: %v", err)
	}
	if err := run.Join(tables, begin, end); err != nil {
		return fmt.Errorf("failed to finalize merge: %v", err)
	}
	return nil
}
