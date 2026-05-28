package health

import "testing"

func TestNormalizeWS(t *testing.T) {
	cases := []struct{ in, want string }{
		{"ws://example:8546", "ws://example:8546"},
		{"wss://example:8546", "wss://example:8546"},
		{"http://example:8546", "ws://example:8546"},
		{"https://example:8546", "wss://example:8546"},
		{"example:8546", "example:8546"}, // pass-through for unknown schemes
	}
	for _, c := range cases {
		if got := normalizeWS(c.in); got != c.want {
			t.Errorf("normalizeWS(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
