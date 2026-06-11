package wsurl

import "testing"

// TestNormalize covers the union of the cases the four historical copies
// handled (subscription session, health prober, eth_ws listener shim).
func TestNormalize(t *testing.T) {
	cases := []struct{ in, want string }{
		// Already-ws URLs pass through.
		{"ws://example:8546", "ws://example:8546"},
		{"wss://example:8546", "wss://example:8546"},
		{"ws://example:8546/path", "ws://example:8546/path"},
		// http(s) → ws(s).
		{"http://example:8546", "ws://example:8546"},
		{"https://example:8546", "wss://example:8546"},
		{"https://example:8546/rpc", "wss://example:8546/rpc"},
		// Bare host:port and unknown schemes pass through.
		{"example:8546", "example:8546"},
		{"grpc://example:9900", "grpc://example:9900"},
		{"", ""},
		// Scheme matching is case-sensitive and requires the full
		// "scheme://" form — near-misses pass through untouched.
		{"WS://example", "WS://example"},
		{"http:/example", "http:/example"},
	}
	for _, c := range cases {
		if got := Normalize(c.in); got != c.want {
			t.Errorf("Normalize(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

// TestInjStreamURL covers the inj_session/hub variant: scheme mapping
// plus /injstream-ws path handling and the bare host:port default.
func TestInjStreamURL(t *testing.T) {
	cases := []struct{ in, want string }{
		// Full ws(s) URLs are trusted as-is — including the path.
		{"ws://example:1996/injstream-ws", "ws://example:1996/injstream-ws"},
		{"wss://example:1996/injstream-ws", "wss://example:1996/injstream-ws"},
		{"ws://example:1996", "ws://example:1996"},
		{"wss://example:1996", "wss://example:1996"},
		// http(s) → ws(s) with the canonical path appended.
		{"http://example:1996", "ws://example:1996/injstream-ws"},
		{"https://example:1996", "wss://example:1996/injstream-ws"},
		// Path-carrying http URLs keep their path; /injstream-ws is
		// appended after it (the documented behavior).
		{"http://x/foo", "ws://x/foo/injstream-ws"},
		// Bare host:port (the chainstream endpoint slot's native form).
		{"example:9999", "ws://example:9999/injstream-ws"},
		{"127.0.0.1:1996", "ws://127.0.0.1:1996/injstream-ws"},
		// Malformed near-schemes fall to the bare-host default. The
		// historical sliced-prefix copies mangled these (e.g.
		// "https:/host" became "wss://ost/…"); recognizing only complete
		// schemes is the resolved, predictable behavior.
		{"wss:/example", "ws://wss:/example/injstream-ws"},
		{"https:/example", "ws://https:/example/injstream-ws"},
	}
	for _, c := range cases {
		if got := InjStreamURL(c.in); got != c.want {
			t.Errorf("InjStreamURL(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}
