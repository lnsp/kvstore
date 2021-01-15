package table

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func MakeTmpMemtable() (*Memtable, func(), error) {
	tmpdir, err := ioutil.TempDir("", "memtable")
	if err != nil {
		return nil, func() {}, err
	}
	memtable, err := NewMemtable(filepath.Join(tmpdir, "memtable_test"))
	if err != nil {
		return nil, func() {}, err
	}
	return memtable, func() {
		memtable.Close()
		os.RemoveAll(tmpdir)
	}, nil
}

func TestMemtableMerge(t *testing.T) {
	memtable1, close1, err := MakeTmpMemtable()
	assert.Nil(t, err)
	defer close1()

	memtable2, close2, err := MakeTmpMemtable()
	assert.Nil(t, err)
	defer close2()

	assert.Nil(t, memtable1.Put([]byte("hello"), []byte("world")))
	assert.Nil(t, memtable1.Put([]byte("another"), []byte("value")))
	assert.Nil(t, memtable2.Put([]byte("hello"), []byte("brother")))
	assert.Nil(t, memtable2.Put([]byte("zzz"), []byte("sleepy")))

	assert.Nil(t, memtable1.Merge(memtable2))
	assert.Equal(t, [][]byte{
		[]byte("brother"),
		[]byte("world")},
		memtable1.Get([]byte("hello")))
	assert.Equal(t, [][]byte{[]byte("value")}, memtable1.Get([]byte("another")))
	assert.Equal(t, [][]byte{[]byte("sleepy")}, memtable1.Get([]byte("zzz")))
	assert.Len(t, memtable1.Get([]byte("notfound")), 0)
}

func TestMemtablePutAndGet(t *testing.T) {
	memtable, close, err := MakeTmpMemtable()
	assert.Nil(t, err)
	defer close()

	assert.Nil(t, memtable.Put([]byte("nothing"), []byte("important")))
	assert.Nil(t, memtable.Put([]byte("just"), []byte("memtable things")))
	assert.Nil(t, memtable.Put([]byte("what"), []byte("omega")))
	assert.Nil(t, memtable.Put([]byte("what"), []byte("alpha")))
	assert.Equal(t, [][]byte{[]byte("important")}, memtable.Get([]byte("nothing")))
	assert.Equal(t, [][]byte{[]byte("memtable things")}, memtable.Get([]byte("just")))
	assert.Equal(t, [][]byte{[]byte("alpha"), []byte("omega")}, memtable.Get([]byte("what")))
}

func TestMemtableWrite(t *testing.T) {
	memtable, close, err := MakeTmpMemtable()
	assert.Nil(t, err)
	defer close()

	assert.Nil(t, memtable.Put([]byte("nothingi"), []byte("mportant")))
	assert.Nil(t, memtable.Put([]byte("just"), []byte("memorythings")))

	// Check file
	bb, err := ioutil.ReadFile(memtable.Name + logSuffix)
	assert.Nil(t, err)
	assert.Equal(t, []byte{
		8, 0, 0, 0, 0, 0, 0, 0,
		8, 0, 0, 0, 0, 0, 0, 0,
		'n', 'o', 't', 'h', 'i', 'n', 'g', 'i',
		'm', 'p', 'o', 'r', 't', 'a', 'n', 't',
		4, 0, 0, 0, 0, 0, 0, 0,
		12, 0, 0, 0, 0, 0, 0, 0,
		'j', 'u', 's', 't',
		'm', 'e', 'm', 'o', 'r', 'y', 't', 'h', 'i', 'n', 'g', 's',
	}, bb)
}

func TestMemtableReadFrom(t *testing.T) {
	memtable, close, err := MakeTmpMemtable()
	assert.Nil(t, err)
	defer close()

	// Create tmp directory
	t.Run("decode memtable", func(t *testing.T) {
		bb := bytes.NewBuffer([]byte{
			8, 0, 0, 0, 0, 0, 0, 0,
			8, 0, 0, 0, 0, 0, 0, 0,
			'j', 'u', 's', 't', 'm', 'e', 'm', 'o',
			'r', 'y', 't', 'h', 'i', 'n', 'g', 's',
		})
		br, err := memtable.ReadFrom(bb)
		assert.Nil(t, err)
		assert.Equal(t, br, int64(32))
		// Check that value is in there
		value := memtable.Get([]byte("justmemo"))
		assert.Equal(t, len(value), 1)
		assert.Equal(t, value[0], []byte("rythings"))
	})
	t.Run("decode multi-value memtable", func(t *testing.T) {
		bb := bytes.NewBuffer([]byte{
			8, 0, 0, 0, 0, 0, 0, 0,
			8, 0, 0, 0, 0, 0, 0, 0,
			'n', 'o', 't', 'h', 'i', 'n', 'g', 'i',
			'm', 'p', 'o', 'r', 't', 'a', 'n', 't',
			4, 0, 0, 0, 0, 0, 0, 0,
			12, 0, 0, 0, 0, 0, 0, 0,
			'j', 'u', 's', 't',
			'm', 'e', 'm', 'o', 'r', 'y', 't', 'h', 'i', 'n', 'g', 's',
		})
		br, err := memtable.ReadFrom(bb)
		assert.Nil(t, err)
		assert.Equal(t, br, int64(64))
		// Check that value is in there
		value := memtable.Get([]byte("nothingi"))
		assert.Equal(t, len(value), 1)
		assert.Equal(t, value[0], []byte("mportant"))
		value = memtable.Get([]byte("just"))
		assert.Equal(t, len(value), 1)
		assert.Equal(t, value[0], []byte("memorythings"))
	})
	t.Run("decode bad memtable", func(t *testing.T) {
		tt := []struct {
			Name     string
			Data     []byte
			Expected int64
		}{
			{
				Name: "key len too short",
				Data: []byte{
					8, 0, 0, 0,
				},
				Expected: 0,
			},
			{
				Name: "value len too short",
				Data: []byte{
					8, 0, 0, 0, 0, 0, 0, 0,
					8, 0, 0, 0,
				},
				Expected: 0,
			},
			{
				Name: "key too short",
				Data: []byte{
					8, 0, 0, 0, 0, 0, 0, 0,
					8, 0, 0, 0, 0, 0, 0, 0,
					'n', 'o', 't',
				},
				Expected: 0,
			},
			{
				Name: "value too short",
				Data: []byte{
					8, 0, 0, 0, 0, 0, 0, 0,
					8, 0, 0, 0, 0, 0, 0, 0,
					'n', 'o', 't', 'h', 'i', 'n', 'g', 'i',
					'm', 'p', 'o', 'r', 't',
				},
				Expected: 0,
			},
			{
				Name: "one valid, one invalid",
				Data: []byte{
					8, 0, 0, 0, 0, 0, 0, 0,
					8, 0, 0, 0, 0, 0, 0, 0,
					'n', 'o', 't', 'h', 'i', 'n', 'g', 'i',
					'm', 'p', 'o', 'r', 't', 'a', 'n', 't',
					12, 0, 3, 4, 0,
				},
				Expected: 32,
			},
		}
		for _, tc := range tt {
			t.Run(tc.Name, func(t *testing.T) {
				n, err := memtable.ReadFrom(bytes.NewBuffer(tc.Data))
				assert.Nil(t, err)
				assert.Equal(t, tc.Expected, int64(n))
			})
		}
	})
}
