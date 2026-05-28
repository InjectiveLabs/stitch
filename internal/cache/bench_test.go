package cache

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkHashIndexGetHit(b *testing.B) {
	idx := New(10000)
	for i := 0; i < 1000; i++ {
		idx.Set("k"+strconv.Itoa(i), int64(i+1))
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			idx.Get("k" + strconv.Itoa(i%1000))
			i++
		}
	})
}

func BenchmarkHashIndexSet(b *testing.B) {
	idx := New(100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Set("k"+strconv.Itoa(i), int64(i+1))
	}
}

func BenchmarkResponseCacheGetHit(b *testing.B) {
	c := NewResponseCache(ResponseCacheOpts{Capacity: 10000})
	body := make([]byte, 512) // typical small JSON-RPC response
	for i := 0; i < 1000; i++ {
		c.Set("k"+strconv.Itoa(i), body, 5*time.Minute)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = c.Get("k" + strconv.Itoa(i%1000))
			i++
		}
	})
}

func BenchmarkResponseCacheSet(b *testing.B) {
	c := NewResponseCache(ResponseCacheOpts{Capacity: 100000})
	body := make([]byte, 512)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set("k"+strconv.Itoa(i), body, 5*time.Minute)
	}
}

func BenchmarkBuildKey(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = BuildKey("eth_rpc", "eth_getBalance", int64(i), 0xdeadbeef)
	}
}

func BenchmarkHashParams(b *testing.B) {
	body := []byte(`["0xabcd","0x1234"]`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = HashParams(body)
	}
}
