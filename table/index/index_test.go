package index_test

import (
	"math/rand"
	"testing"

	"github.com/lnsp/kvstore/table/index"
)

func BenchmarkMemoryPut(b *testing.B) {
	mem := index.NewMemory()

	key := make([]byte, b.N*8)
	rand.Read(key)
	value := make([]byte, b.N*8)
	rand.Read(value)

	// fill key, value
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mem.Put(key[i*8:(i+1)*8], value[i*8:(i+1)*8])
	}
}

func BenchmarkMemoryGet(b *testing.B) {
	mem := index.NewMemory()

	key := make([]byte, b.N*8)
	rand.Read(key)
	value := make([]byte, b.N*8)
	rand.Read(value)

	// fill key, value
	for i := 0; i < b.N; i++ {
		mem.Put(key[i*8:(i+1)*8], value[i*8:(i+1)*8])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mem.Get(key)
	}
}
