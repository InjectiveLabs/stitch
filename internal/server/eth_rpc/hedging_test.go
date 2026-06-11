package eth_rpc

import (
	"testing"
	"time"
)

// eth_call carries hedge: true + idempotent: true in the manifest, so it is
// the canonical probe for config-level gating.
const hedgedCallBody = `{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":"0xabcd"},"0x1000"]}`

func TestHedgeGatingMutatesKey(t *testing.T) {
	cases := []struct {
		name    string
		enabled bool
		methods []string
		want    bool
	}{
		{"disabled never hedges", false, nil, false},
		{"disabled ignores allowlist", false, []string{"eth_call"}, false},
		{"enabled empty list follows manifest", true, nil, true},
		{"enabled and method on list", true, []string{"eth_call", "eth_getLogs"}, true},
		{"enabled but method off list", true, []string{"eth_getLogs"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := New("ignored", nil)
			s.SetHedging(tc.enabled, tc.methods)
			d, _, err := decodeOne(s.manifest, []byte(hedgedCallBody))
			if err != nil {
				t.Fatal(err)
			}
			if !d.key.Hedge {
				t.Fatal("manifest should flag eth_call hedge=true before gating")
			}
			s.applyHedgePolicy(&d)
			if d.key.Hedge != tc.want {
				t.Errorf("hedge=%v; want %v", d.key.Hedge, tc.want)
			}
		})
	}
}

func TestHedgeGatingDefaultsToDisabled(t *testing.T) {
	s := New("ignored", nil)
	d, _, err := decodeOne(s.manifest, []byte(hedgedCallBody))
	if err != nil {
		t.Fatal(err)
	}
	s.applyHedgePolicy(&d)
	if d.key.Hedge {
		t.Error("hedging must be off until SetHedging enables it")
	}
}

func TestHedgeGatingNeverAddsUnflaggedMethods(t *testing.T) {
	// eth_getBalance has no hedge flag in the manifest; enabling hedging
	// (even naming it explicitly) must not add it.
	s := New("ignored", nil)
	s.SetHedging(true, []string{"eth_getBalance"})
	d, _, err := decodeOne(s.manifest, []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xabcd","0x10"]}`))
	if err != nil {
		t.Fatal(err)
	}
	s.applyHedgePolicy(&d)
	if d.key.Hedge {
		t.Error("config must not hedge methods the manifest doesn't flag")
	}
}

// TestEthHedgeDisabledNeverFiresSecondRequest exercises the wiring through
// handleSingle: with hedging disabled (the default) a slow primary must NOT
// trigger a hedged request to the second candidate, even though eth_call is
// hedge-flagged in the manifest and the forwarder's HedgeAfter (200ms)
// elapses well before the primary responds.
func TestEthHedgeDisabledNeverFiresSecondRequest(t *testing.T) {
	r := setupEth(t)
	defer r.close()
	r.shard.delay.Store(int64(600 * time.Millisecond))

	// Height 0x1000 routes to shard1 (bounded 1..50000) ahead of archive.
	resp := post(t, r.frontT.URL, hedgedCallBody)
	resp.Body.Close()

	if got := r.archive.hits.Load(); got != 0 {
		t.Errorf("hedging disabled: archive must never be hit; got %d", got)
	}
	if got := r.shard.hits.Load(); got != 1 {
		t.Errorf("shard1 hits=%d; want 1", got)
	}
}
