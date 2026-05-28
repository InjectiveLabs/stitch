package eth_rpc

import (
	"testing"
	"time"
)

func TestFilterStoreBindLookup(t *testing.T) {
	s := NewFilterStore(0)
	s.Bind("0xabc", "shard1")
	if got := s.Lookup("0xabc"); got != "shard1" {
		t.Errorf("got %q", got)
	}
	if got := s.Lookup("0xnope"); got != "" {
		t.Errorf("absent: got %q", got)
	}
	if s.Size() != 1 {
		t.Errorf("size: %d", s.Size())
	}
}

func TestFilterStoreForget(t *testing.T) {
	s := NewFilterStore(0)
	s.Bind("0xabc", "shard1")
	s.Forget("0xabc")
	if got := s.Lookup("0xabc"); got != "" {
		t.Errorf("forgot but still: %q", got)
	}
}

func TestFilterStoreTTL(t *testing.T) {
	s := NewFilterStore(20 * time.Millisecond)
	s.Bind("0xabc", "shard1")
	time.Sleep(40 * time.Millisecond)
	if got := s.Lookup("0xabc"); got != "" {
		t.Errorf("expired but still: %q", got)
	}
	s.Sweep()
	if s.Size() != 0 {
		t.Errorf("post-sweep size: %d", s.Size())
	}
}
