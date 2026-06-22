package types

// RouteKey is the routing decision the decoder produces. Selector consumes
// it; Forwarder uses Idempotent/Cacheable/Hedge to drive retry, cache,
// and dispatch policy.
type RouteKey struct {
	Protocol   Protocol
	Method     string // e.g. "block", "eth_getBalance" — for metrics labels
	Class      MethodClass
	Height     *int64 // nil = latest / unknown
	Range      *HeightRange
	Hash       []byte // empty unless Class == ClassByHash
	Idempotent bool
	Cacheable  bool
	Hedge      bool // forwarder may dispatch a delayed second attempt
}

// HeightRange is an inclusive block-height span. Nil Lower means genesis;
// nil Upper means current head.
type HeightRange struct {
	Lower *int64
	Upper *int64
}

// HeightOrZero returns Height dereferenced or 0 (interpreted as "latest").
func (k RouteKey) HeightOrZero() int64 {
	if k.Height == nil {
		return 0
	}
	return *k.Height
}

// MaxRequestedHeightOrZero returns the highest concrete height in the
// routing key, if one is known. It is used as a floor for stale head probes.
func (k RouteKey) MaxRequestedHeightOrZero() int64 {
	if k.Height != nil {
		return *k.Height
	}
	if k.Range != nil && k.Range.Upper != nil {
		return *k.Range.Upper
	}
	if k.Range != nil && k.Range.Lower != nil {
		return *k.Range.Lower
	}
	return 0
}
