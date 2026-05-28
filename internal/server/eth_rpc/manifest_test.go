package eth_rpc

import (
	"strings"
	"testing"
)

func TestManifestLoaded(t *testing.T) {
	if DefaultManifest == nil || len(DefaultManifest.Names()) == 0 {
		t.Fatal("manifest not loaded")
	}
}

func TestManifestCoversInjectiveSurface(t *testing.T) {
	// Spot-check that every namespace from the Injective query reference §3
	// has at least one method, and that the high-traffic methods are
	// classified.
	required := []string{
		// eth
		"eth_blockNumber", "eth_chainId", "eth_getBalance", "eth_getCode",
		"eth_getStorageAt", "eth_getProof", "eth_getTransactionCount",
		"eth_getBlockByNumber", "eth_getBlockByHash", "eth_getBlockReceipts",
		"eth_getTransactionByHash", "eth_getTransactionReceipt",
		"eth_getTransactionLogs", "eth_call", "eth_estimateGas",
		"eth_sendRawTransaction", "eth_getLogs",
		"eth_newFilter", "eth_getFilterChanges", "eth_uninstallFilter",
		"eth_subscribe",
		// web3 / net / txpool
		"web3_clientVersion", "web3_sha3",
		"net_version", "net_listening", "net_peerCount",
		"txpool_content", "txpool_inspect", "txpool_status",
		// hidden namespaces
		"personal_listAccounts", "personal_sign",
		"debug_traceTransaction", "debug_traceCall", "debug_traceBlockByNumber",
		"miner_setEtherbase",
		// inj
		"inj_getTxHashByEthHash",
	}
	for _, m := range required {
		if !DefaultManifest.Has(m) {
			t.Errorf("missing manifest entry: %s", m)
		}
	}
}

func TestManifestSensibleClassifications(t *testing.T) {
	cases := []struct {
		method     string
		predicate  func(Spec) bool
		desc       string
	}{
		{"eth_chainId", func(s Spec) bool { return s.Stateless }, "stateless"},
		{"eth_sendRawTransaction", func(s Spec) bool { return s.Broadcast && !s.IsIdempotent() }, "broadcast non-idempotent"},
		{"eth_subscribe", func(s Spec) bool { return s.Subscription }, "subscription"},
		{"eth_call", func(s Spec) bool { return s.Hedge && s.HeightParam != nil && *s.HeightParam == 1 && s.StateOverrideParam != nil }, "call hedge+height+override"},
		{"eth_getBalance", func(s Spec) bool { return s.Cacheable && s.HeightParam != nil && *s.HeightParam == 1 }, "balance cacheable+height_param=1"},
		{"eth_getBlockByHash", func(s Spec) bool { return s.HashParam != nil && *s.HashParam == 0 && s.Kind == "block_hash" }, "block_by_hash"},
		{"eth_getTransactionByHash", func(s Spec) bool { return s.HashParam != nil && s.Kind == "tx_hash" }, "tx_by_hash"},
		{"eth_newFilter", func(s Spec) bool { return s.StickyFilter && s.FollowID == nil }, "filter mint"},
		{"eth_getFilterChanges", func(s Spec) bool { return s.StickyFilter && s.FollowID != nil && *s.FollowID == 0 }, "filter follow"},
		{"personal_listAccounts", func(s Spec) bool { return s.Hidden }, "personal hidden"},
		{"debug_traceTransaction", func(s Spec) bool { return s.Hidden && s.HashParam != nil && s.Kind == "tx_hash" }, "debug hidden+tx_hash"},
		{"inj_getTxHashByEthHash", func(s Spec) bool { return s.HashParam != nil && s.Kind == "tx_hash" && s.Cacheable }, "inj cacheable+tx_hash"},
	}
	for _, c := range cases {
		t.Run(c.method, func(t *testing.T) {
			s := DefaultManifest.Lookup(c.method)
			if !c.predicate(s) {
				t.Errorf("manifest entry %q failed predicate %s: %+v", c.method, c.desc, s)
			}
		})
	}
}

func TestManifestUnknownMethodIsPermissive(t *testing.T) {
	s := DefaultManifest.Lookup("eth_makesomethingup_blarg")
	if s.Height != "latest" {
		t.Errorf("unknown method should default to latest; got %+v", s)
	}
	if s.IsIdempotent() {
		t.Error("unknown method must not default to idempotent (would be retried)")
	}
}

func TestManifestRejectsBadSpec(t *testing.T) {
	_, err := loadManifestFromBytes([]byte(`my_method: { kind: bogus, height_param: 0 }`))
	if err == nil || !strings.Contains(err.Error(), "kind") {
		t.Fatalf("expected kind error, got %v", err)
	}
}

// Helper exposed only for tests — wraps loadManifest with a custom source.
func loadManifestFromBytes(data []byte) (*Manifest, error) {
	saved := manifestYAML
	manifestYAML = data
	defer func() { manifestYAML = saved }()
	return loadManifest()
}
