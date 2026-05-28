package config

import "sync/atomic"

// Holder publishes a Config snapshot via an atomic pointer so request handlers
// can read without locks. Swap on hot reload.
type Holder struct {
	current atomic.Pointer[Config]
}

func NewHolder(initial *Config) *Holder {
	h := &Holder{}
	h.current.Store(initial)
	return h
}

func (h *Holder) Get() *Config { return h.current.Load() }

func (h *Holder) Swap(next *Config) *Config { return h.current.Swap(next) }
