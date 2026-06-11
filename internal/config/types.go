package config

import "time"

// Config is the top-level stitch configuration. Loaded from YAML and
// hot-reloaded via SIGHUP or the admin API.
type Config struct {
	Listen   ListenConfig    `yaml:"listen"`
	Log      LogConfig       `yaml:"log"`
	Policies PoliciesConfig  `yaml:"policies"`
	Backends []BackendConfig `yaml:"backends"`
	Auth     AuthConfig      `yaml:"auth,omitempty"`
}

// ListenConfig groups the addresses for every protocol listener.
// An empty Addr means the listener is disabled.
type ListenConfig struct {
	RPC         AddrConfig `yaml:"rpc"`
	GRPC        AddrConfig `yaml:"grpc"`
	API         AddrConfig `yaml:"api"`
	EthRPC      AddrConfig `yaml:"eth_rpc"`
	EthWS       AddrConfig `yaml:"eth_ws"`
	ChainStream AddrConfig `yaml:"chainstream"`
	InjWS       AddrConfig `yaml:"inj_ws"`
	Admin       AddrConfig `yaml:"admin"`
}

// AddrConfig is a listen address with optional TLS. An empty Addr disables
// the listener it belongs to.
type AddrConfig struct {
	Addr    string    `yaml:"addr"`
	TLS     TLSConfig `yaml:"tls,omitempty"`
	Comment string    `yaml:"comment,omitempty"`
}

// Enabled reports whether this listener should be brought up.
func (a AddrConfig) Enabled() bool { return a.Addr != "" }

// TLSConfig holds TLS material for a listener.
type TLSConfig struct {
	CertFile string `yaml:"cert_file,omitempty"`
	KeyFile  string `yaml:"key_file,omitempty"`
}

// LogConfig controls structured logging output.
type LogConfig struct {
	Level  string `yaml:"level"`  // debug | info | warn | error
	Format string `yaml:"format"` // json | text
}

// PoliciesConfig groups runtime tunables.
type PoliciesConfig struct {
	Failover         FailoverPolicy         `yaml:"failover"`
	Hedging          HedgingPolicy          `yaml:"hedging"`
	Circuit          CircuitPolicy          `yaml:"circuit"`
	Cache            CachePolicy            `yaml:"cache"`
	Health           HealthPolicy           `yaml:"health"`
	Subscriptions    SubscriptionsPolicy    `yaml:"subscriptions"`
	DangerousMethods DangerousMethodsPolicy `yaml:"dangerous_methods,omitempty"`
}

// DangerousMethodsPolicy controls the allowlist of methods that are
// default-denied because they leak key material, mutate node state, or
// dump full storage tries (eth's debug_*, personal_*, miner_*).
//
// Operators opt-in specific methods by name; everything else stays
// default-denied. There are no tiers — open-source stitch is meant to be
// run privately, with operators fronting it for their own auth needs.
type DangerousMethodsPolicy struct {
	Allow []string `yaml:"allow,omitempty"`
}

type FailoverPolicy struct {
	MaxAttempts       int           `yaml:"max_attempts"`
	PerAttemptTimeout time.Duration `yaml:"per_attempt_timeout"`
}

// HedgingPolicy gates hedged dispatch. Hedging only fires for methods the
// manifest flags hedge-safe; Enabled turns the feature on, Methods (when
// non-empty) further restricts it to the listed method names, and
// HedgeAfter is the delay before the second request fires.
type HedgingPolicy struct {
	Enabled    bool          `yaml:"enabled"`
	Methods    []string      `yaml:"methods,omitempty"`
	HedgeAfter time.Duration `yaml:"hedge_after,omitempty"`
	MaxHedge   time.Duration `yaml:"max_hedge,omitempty"`
}

type CircuitPolicy struct {
	ErrorThreshold float64       `yaml:"error_threshold"`
	MinRequests    int           `yaml:"min_requests"`
	OpenDuration   time.Duration `yaml:"open_duration"`
}

// CachePolicy tunes the response cache and the hash→height index. TTL is
// the lifetime of response-cache entries; HashIndexEntries and
// ResponseEntries cap the two caches' entry counts.
type CachePolicy struct {
	Enabled           bool          `yaml:"enabled"`
	ConfirmationDepth int64         `yaml:"confirmation_depth"`
	TTL               time.Duration `yaml:"ttl,omitempty"`
	HashIndexEntries  int           `yaml:"hash_index_entries,omitempty"`
	ResponseEntries   int           `yaml:"response_entries,omitempty"`
	L1SizeMB          int           `yaml:"l1_size_mb,omitempty"`
	L2Kind            string        `yaml:"l2_kind,omitempty"` // none | redis
	L2Addr            string        `yaml:"l2_addr,omitempty"`
}

type HealthPolicy struct {
	ProbeInterval time.Duration `yaml:"probe_interval"`
	MaxLagBlocks  int64         `yaml:"max_lag_blocks"`
}

// SubscriptionsPolicy tunes the WS subscription listeners.
//
// Multicast (default false) coalesces /injstream-ws clients with the same
// canonical filter onto one shared upstream connection. SlowConsumer and
// SendBuffer apply to the multicast fan-out: the policy when a client's
// send buffer fills, and that buffer's capacity (default 64).
// ReplayTimeout is the max time to wait for a dialable upstream during a
// subscription resume before dropping the subscriber/session; 0 disables
// the retry window (a resume gets a single dial pass).
type SubscriptionsPolicy struct {
	Multicast     bool          `yaml:"multicast"`
	SlowConsumer  string        `yaml:"slow_consumer"` // drop | disconnect | backpressure
	SendBuffer    int           `yaml:"send_buffer,omitempty"`
	ReplayTimeout time.Duration `yaml:"replay_timeout"`
}

// BackendConfig declares one upstream node.
type BackendConfig struct {
	Name      string            `yaml:"name"`
	Coverage  Coverage          `yaml:"coverage"`
	Weight    int               `yaml:"weight,omitempty"`
	Tags      []string          `yaml:"tags,omitempty"`
	Endpoints map[string]string `yaml:"endpoints"`
}

// Coverage describes which heights this backend serves.
//
//	kind=archive          → genesis..head
//	kind=bounded lower..upper (closed interval)
//	kind=open   lower..head
//	kind=pruned keep      → head-keep+1..head (sliding window)
type Coverage struct {
	Kind  string `yaml:"kind"`
	Lower int64  `yaml:"lower,omitempty"`
	Upper int64  `yaml:"upper,omitempty"`
	Keep  int64  `yaml:"keep,omitempty"`
}

const (
	CoverageArchive = "archive"
	CoverageBounded = "bounded"
	CoverageOpen    = "open"
	CoveragePruned  = "pruned"
)

// AuthConfig is reserved for phase 8.
type AuthConfig struct {
	Enabled bool       `yaml:"enabled"`
	Tiers   []TierConf `yaml:"tiers,omitempty"`
}

type TierConf struct {
	Name           string   `yaml:"name"`
	AllowedMethods []string `yaml:"allowed_methods,omitempty"`
	BlockedMethods []string `yaml:"blocked_methods,omitempty"`
	RPS            int      `yaml:"rps,omitempty"`
}

// EndpointKey lists the endpoint slot names recognized in BackendConfig.Endpoints.
const (
	EndpointRPC         = "rpc"
	EndpointGRPC        = "grpc"
	EndpointAPI         = "api"
	EndpointEthRPC      = "eth_rpc"
	EndpointEthWS       = "eth_ws"
	EndpointChainStream = "chainstream"
)

// KnownEndpointKeys is the canonical set of endpoint slot names; used by
// validation to flag typos.
var KnownEndpointKeys = map[string]struct{}{
	EndpointRPC:         {},
	EndpointGRPC:        {},
	EndpointAPI:         {},
	EndpointEthRPC:      {},
	EndpointEthWS:       {},
	EndpointChainStream: {},
}
