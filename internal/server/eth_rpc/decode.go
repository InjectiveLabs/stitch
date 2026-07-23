package eth_rpc

import (
	"bytes"
	"encoding/json"

	"github.com/InjectiveLabs/stitch/internal/types"
)

// jsonRPCRequest is the wire shape of one JSON-RPC v2 request.
type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      json.RawMessage `json:"id"`
}

// decoded captures the routing decision plus the per-call data the
// handler needs to emit a JSON-RPC error if it has to short-circuit.
type decoded struct {
	method string
	id     json.RawMessage
	spec   Spec
	key    types.RouteKey
	// followFilterID is the filter id this call wants to follow (sticky),
	// or "" if the call is not a sticky-follow call. The handler reads it
	// to consult the FilterStore.
	followFilterID string
	// expectFilterMint is true for eth_newFilter / eth_newBlockFilter /
	// eth_newPendingTransactionFilter — the handler captures the response
	// id and binds it.
	expectFilterMint bool
	// fatal is set if the request is structurally rejected (e.g. subscribe
	// over HTTP). The handler returns the error without forwarding.
	fatal *fatalError
}

type fatalError struct {
	code    int
	message string
}

// decodeOne parses one JSON-RPC request envelope and produces a routing
// decision. The error return is reserved for envelope-level parse errors
// (unparseable JSON); method-level rejections come back via decoded.fatal.
func decodeOne(m *Manifest, raw []byte) (decoded, *jsonRPCRequest, error) {
	var req jsonRPCRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return decoded{}, nil, err
	}
	d := decoded{
		method: req.Method,
		id:     req.ID,
	}
	if req.Method == "" {
		d.fatal = &fatalError{code: -32600, message: "missing method"}
		return d, &req, nil
	}
	d.spec = m.Lookup(req.Method)
	d.key = buildRouteKey(req.Method, d.spec, req.Params)

	switch {
	case d.spec.Subscription:
		d.fatal = &fatalError{
			code:    -32601,
			message: req.Method + " requires a WebSocket connection (HTTP listener does not support subscriptions)",
		}
	case d.spec.StickyFilter && d.spec.FollowID != nil:
		if id, ok := extractFilterID(param(req.Params, *d.spec.FollowID)); ok {
			d.followFilterID = id
		}
	case d.spec.StickyFilter && d.spec.FollowID == nil:
		d.expectFilterMint = true
	}
	return d, &req, nil
}

// buildRouteKey reads a manifest spec + raw params and produces a RouteKey.
func buildRouteKey(method string, s Spec, params json.RawMessage) types.RouteKey {
	idemp := s.IsIdempotent()
	key := types.RouteKey{
		Protocol:   types.ProtoEthRPC,
		Method:     method,
		Class:      types.ClassLatest,
		Idempotent: idemp,
		Cacheable:  s.Cacheable,
		Hedge:      s.Hedge,
	}

	switch {
	case s.Subscription:
		key.Class = types.ClassSubscribe
	case s.Broadcast:
		key.Class = types.ClassBroadcast
		key.Idempotent = false
	case s.Stateless:
		key.Class = types.ClassStateless
	case s.HashParam != nil:
		key.Class = types.ClassByHash
		switch s.Kind {
		case "block_hash":
			if h, ok := extractBlockHash(param(params, *s.HashParam)); ok {
				key.Hash = h
			}
		case "tx_hash":
			if h, ok := extractTxHash(param(params, *s.HashParam)); ok {
				key.Hash = h
			}
		}
	case s.HeightParam != nil:
		raw := param(params, *s.HeightParam)
		switch s.Kind {
		case "block_number":
			if h, ok := extractBlockNumber(raw); ok {
				key.Height = &h
				key.Class = types.ClassByHeight
			}
		case "block_number_or_hash":
			if h, ok := extractBlockNumber(raw); ok {
				key.Height = &h
				key.Class = types.ClassByHeight
			} else if hh, ok := extractBlockHash(raw); ok {
				key.Hash = hh
				key.Class = types.ClassByHash
			}
		}
	case s.Height == "latest":
		key.Class = types.ClassLatest
	}
	applyFilterObjectRoute(&key, method, params)

	// State-override on eth_call disables caching even when finalized.
	if s.StateOverrideParam != nil {
		if so := param(params, *s.StateOverrideParam); len(bytes.TrimSpace(so)) > 0 && !bytes.Equal(bytes.TrimSpace(so), []byte("null")) {
			key.Cacheable = false
		}
	}
	return key
}

func applyFilterObjectRoute(key *types.RouteKey, method string, params json.RawMessage) {
	switch method {
	case "eth_getLogs", "eth_newFilter":
	default:
		return
	}
	raw := bytes.TrimSpace(param(params, 0))
	if len(raw) == 0 || raw[0] != '{' {
		return
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return
	}
	if h, ok := extractBlockHash(obj["blockHash"]); ok {
		key.Hash = h
		key.Height = nil
		key.Range = nil
		key.Class = types.ClassByHash
		return
	}

	lower, lowerOK := filterBlockNumber(obj["fromBlock"])
	upper, upperOK := filterBlockNumber(obj["toBlock"])
	switch {
	case lowerOK && upperOK && lower == upper:
		key.Height = &lower
		key.Range = nil
		key.Class = types.ClassByHeight
	case lowerOK && upperOK && lower < upper:
		key.Height = nil
		key.Range = &types.HeightRange{Lower: &lower, Upper: &upper}
		key.Class = types.ClassByHeightRange
	case lowerOK && !upperOK:
		key.Height = nil
		key.Range = &types.HeightRange{Lower: &lower}
		key.Class = types.ClassByHeightRange
	}
}

func filterBlockNumber(raw json.RawMessage) (int64, bool) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return 0, false
	}
	return extractBlockNumber(raw)
}
