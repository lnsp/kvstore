package store

import "testing"

func TestStorePutGet(t *testing.T) {
	store, err := New()
	if err != nil {
		t.Fatal("failed to init store: ", err)
	}
	store.Put([]byte("hello"), &Record{
		Value: []byte("darkness"),
	})
	store.Put([]byte("my"), &Record{
		Value: []byte("old"),
	})
	store.Put([]byte("my"), &Record{
		Value: []byte("old"),
	})
}
