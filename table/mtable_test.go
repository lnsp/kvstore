package table_test

import (
	"bytes"
	"testing"

	"github.com/lnsp/kvstore/table"
)

type mockReadWriteCloser struct {
	in  *bytes.Buffer
	out *bytes.Buffer
}

func (mockReadWriteCloser) Close() error { return nil }

func (mock *mockReadWriteCloser) Write(b []byte) (int, error) { return mock.out.Write(b) }

func (mock *mockReadWriteCloser) Read(b []byte) (int, error) { return mock.in.Read(b) }

func (mock *mockReadWriteCloser) Reopen() {
	mock.in = bytes.NewBuffer(mock.out.Bytes())
	mock.out = bytes.NewBuffer(nil)
}

func newMockReadWriteCloser() *mockReadWriteCloser {
	return &mockReadWriteCloser{
		in:  &bytes.Buffer{},
		out: &bytes.Buffer{},
	}
}

func TestMTablePutAndGet(t *testing.T) {
	// create buffer
	mockRWC := newMockReadWriteCloser()
	mt, err := table.NewMTable(mockRWC, "mtable")
	if err != nil {
		t.Fatal("expected no err while setup mtable, got", err)
	}
	// generate sample data
	keys := []string{
		"hello",
		"world",
		"this",
		"is",
		"unique",
	}
	// insert things into mtable
	for i := 0; i < len(keys); i++ {
		if err := mt.Put([]byte(keys[i]), []byte(keys[i])); err != nil {
			t.Fatal("expected no put error, got", err)
		}
	}
	// check mtable size
	expectedSize := 44 + 5*16 // number of bytes * 2 + number of entries * 16
	if size := mt.Size(); size != int64(expectedSize) {
		t.Fatal("expected table size", expectedSize, "got", size)
	}
	// check inserted things
	for i := 0; i < len(keys); i++ {
		values := mt.Get([]byte(keys[i]))
		// expect that one value is assigned
		if n := len(values); n != 1 {
			t.Fatal("expected single assigned value, got", n)
		}
		if !bytes.Equal(values[0], []byte(keys[i])) {
			t.Fatal("expected", []byte(keys[i]), "got", values[0])
		}
	}
	// close mtable
	if err := mt.Close(); err != nil {
		t.Fatal("expected no close error, got", err)
	}
}

func TestMTableRestore(t *testing.T) {
	mockRWC := newMockReadWriteCloser()
	mt, err := table.NewMTable(mockRWC, "mtable")
	if err != nil {
		t.Fatal("expected no err on setup mtable, got", err)
	}
	// generate sample data
	keys := []string{
		"hello",
		"world",
		"this",
		"is",
		"unique",
	}
	// insert things into mtable
	for i := 0; i < len(keys); i++ {
		if err := mt.Put([]byte(keys[i]), []byte(keys[i])); err != nil {
			t.Fatal("expected no put error, got", err)
		}
	}
	// close mtable
	if err := mt.Close(); err != nil {
		t.Fatal("expected no error on close mtable, got", err)
	}
	mockRWC.Reopen()
	// open mtable again
	mt, err = table.NewMTable(mockRWC, mt.Name)
	if err != nil {
		t.Fatal("expected no err on open mtable, got", err)
	}
	// check if strings still exist
	for i := 0; i < len(keys); i++ {
		values := mt.Get([]byte(keys[i]))
		// expect that one value is assigned
		if n := len(values); n != 1 {
			t.Fatal("expected single assigned value, got", n)
		}
		if !bytes.Equal(values[0], []byte(keys[i])) {
			t.Fatal("expected", []byte(keys[i]), "got", values[0])
		}
	}
	// close mtable again
	if err := mt.Close(); err != nil {
		t.Fatal("expected no error on close mtable, got", err)
	}
}

func TestMTableMerge(t *testing.T) {
	mt1, err := table.NewMTable(newMockReadWriteCloser(), "mtable1")
	if err != nil {
		t.Fatal("expected no err on setup mtable, got", err)
	}
	mt2, err := table.NewMTable(newMockReadWriteCloser(), "mtable2")
	if err != nil {
		t.Fatal("expected no err on setup mtable, got", err)
	}
	// generate sample data
	keys1 := []string{
		"hello",
		"world",
		"this",
	}
	keys2 := []string{
		"is",
		"unique",
	}
	// insert things into mtable 1
	for _, k := range keys1 {
		if err := mt1.Put([]byte(k), []byte(k)); err != nil {
			t.Fatal("expected no put error, got", err)
		}
	}
	// insert things into mtable 2
	for _, k := range keys2 {
		if err := mt2.Put([]byte(k), []byte(k)); err != nil {
			t.Fatal("expected no put error, got", err)
		}
	}
	// merge into mt1
	if err := mt1.Merge(mt2); err != nil {
		t.Fatal("expected no merge error, got", err)
	}
	// check keys
	keysMerged := []string{
		"hello",
		"world",
		"this",
		"is",
		"unique",
	}
	for _, k := range keysMerged {
		values := mt1.Get([]byte(k))
		// expect that one value is assigned
		if n := len(values); n != 1 {
			t.Fatal("expected single assigned value, got", n)
		}
		if !bytes.Equal(values[0], []byte(k)) {
			t.Fatal("expected", []byte(k), "got", values[0])
		}
	}
}
