package cmt_rpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/decentrio/stitch/internal/types"
)

// decoded captures everything we learn from inspecting a CometBFT RPC
// request: the routing key + the (possibly buffered) body for replay on
// retry by the forwarder.
type decoded struct {
	key  types.RouteKey
	body []byte // pre-read POST body; nil for GET
}

// JSON-RPC v2 envelope (incoming). params can be an object or an array.
type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      json.RawMessage `json:"id"`
}

// decode inspects r and produces a routing decision. Two paths:
//
//   - GET /<method>?param=value      — URI over HTTP
//   - POST /                          — JSON-RPC envelope in body
//
// (POST /<method>?param=value also occurs in some Cosmos clients; treat as
// URI mode based on path prefix.)
func decode(r *http.Request) (decoded, error) {
	if r.Method == http.MethodGet {
		return decodeURI(r), nil
	}
	if r.Method != http.MethodPost {
		return decoded{}, errors.New("method not allowed")
	}
	if path := strings.TrimPrefix(r.URL.Path, "/"); path != "" && path != "websocket" {
		// POST with a method-named path: still URI mode.
		return decodeURI(r), nil
	}
	return decodeJSONRPC(r)
}

func decodeURI(r *http.Request) decoded {
	method := strings.TrimPrefix(r.URL.Path, "/")
	method = strings.TrimSuffix(method, "/")
	spec := Lookup(method)
	q := r.URL.Query()

	d := decoded{
		key: types.RouteKey{
			Protocol:   types.ProtoRPC,
			Method:     method,
			Class:      spec.Class,
			Idempotent: spec.Idempotent,
			Cacheable:  spec.Cacheable,
		},
	}
	if spec.HeightParam != "" {
		if hs := q.Get(spec.HeightParam); hs != "" {
			if h, ok := parseHeight(hs); ok {
				d.key.Height = &h
				d.key.Class = types.ClassByHeight
			}
		}
	}
	applyURIHeightHints(&d.key, method, q, r.URL.RawQuery)
	if spec.HashParam != "" {
		if hs := q.Get(spec.HashParam); hs != "" {
			d.key.Hash = []byte(hs)
		}
	}
	return d
}

func decodeJSONRPC(r *http.Request) (decoded, error) {
	body, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		return decoded{}, err
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	// Batch requests: route each to latest for now; phase 6 may parallelize.
	if len(body) > 0 && body[0] == '[' {
		return decoded{
			body: body,
			key: types.RouteKey{
				Protocol: types.ProtoRPC,
				Method:   "_batch",
				Class:    types.ClassLatest,
			},
		}, nil
	}

	var req jsonRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return decoded{
			body: body,
			key: types.RouteKey{
				Protocol: types.ProtoRPC,
				Method:   "_invalid",
				Class:    types.ClassLatest,
			},
		}, nil
	}
	spec := Lookup(req.Method)
	d := decoded{
		body: body,
		key: types.RouteKey{
			Protocol:   types.ProtoRPC,
			Method:     req.Method,
			Class:      spec.Class,
			Idempotent: spec.Idempotent,
			Cacheable:  spec.Cacheable,
		},
	}
	if spec.HeightParam != "" {
		if hs := paramFromJSON(req.Params, spec.HeightParam, 0); hs != "" {
			if h, ok := parseHeight(hs); ok {
				d.key.Height = &h
				d.key.Class = types.ClassByHeight
			}
		}
	}
	applyJSONHeightHints(&d.key, req.Method, req.Params)
	if spec.HashParam != "" {
		if hs := paramFromJSON(req.Params, spec.HashParam, 0); hs != "" {
			d.key.Hash = []byte(hs)
		}
	}
	return d, nil
}

// paramFromJSON extracts a named parameter from a JSON-RPC params payload.
// Object form returns params[name]; array form returns params[arrayIdx].
func paramFromJSON(raw json.RawMessage, name string, arrayIdx int) string {
	return paramFromJSONAny(raw, []string{name}, arrayIdx)
}

func paramFromJSONAny(raw json.RawMessage, names []string, arrayIdx int) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return ""
	}
	if raw[0] == '[' {
		if arrayIdx < 0 {
			return ""
		}
		var arr []json.RawMessage
		if err := json.Unmarshal(raw, &arr); err != nil {
			return ""
		}
		if arrayIdx >= len(arr) {
			return ""
		}
		return unquoteRaw(arr[arrayIdx])
	}
	if raw[0] == '{' {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			return ""
		}
		for _, name := range names {
			if v, ok := obj[name]; ok {
				return unquoteRaw(v)
			}
		}
	}
	return ""
}

func unquoteRaw(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return ""
	}
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			return s
		}
	}
	return string(raw)
}

func parseHeight(s string) (int64, bool) {
	if s == "" || s == "0" {
		return 0, false
	}
	s = strings.TrimSpace(strings.Trim(s, `"'`))
	if s == "" || s == "0" {
		return 0, false
	}
	// CometBFT accepts both decimal and 0x… in some clients.
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		h, err := strconv.ParseInt(s[2:], 16, 64)
		return h, err == nil && h > 0
	}
	h, err := strconv.ParseInt(s, 10, 64)
	return h, err == nil && h > 0
}

var (
	heightParamNames      = []string{"height", "block_height", "blockHeight"}
	rangeLowerParamNames  = []string{"minHeight", "min_height", "fromHeight", "from_height"}
	rangeUpperParamNames  = []string{"maxHeight", "max_height", "toHeight", "to_height"}
	eventHeightConstraint = regexp.MustCompile(`(?i)(?:tx|block)\.height\s*(<=|>=|=|<|>)\s*['"]?(0x[0-9a-f]+|[0-9]+)['"]?`)
)

const maxInt64 = 1<<63 - 1

type heightBounds struct {
	lower   *int64
	upper   *int64
	invalid bool
}

func applyURIHeightHints(key *types.RouteKey, method string, q url.Values, rawQuery string) {
	if key.Height != nil || key.Range != nil {
		return
	}
	if v := firstQueryValue(q, heightParamNames); v != "" {
		if h, ok := parseHeight(v); ok {
			setPointHeight(key, h)
			return
		}
	}
	if applyRangeParamHints(key, firstQueryValue(q, rangeLowerParamNames), firstQueryValue(q, rangeUpperParamNames)) {
		return
	}
	if (method == "tx_search" || method == "block_search") && applyEventQueryHeightHints(key, q.Get("query")) {
		return
	}
	// Some clients put the event expression into the raw query string
	// directly or double-encode the `query` value. The decoded values above
	// are preferred, but scanning RawQuery keeps routing resilient.
	if decoded, err := url.QueryUnescape(rawQuery); err == nil {
		_ = applyEventQueryHeightHints(key, decoded)
	}
}

func applyJSONHeightHints(key *types.RouteKey, method string, params json.RawMessage) {
	if key.Height != nil || key.Range != nil {
		return
	}
	if v := paramFromJSONAny(params, heightParamNames, -1); v != "" {
		if h, ok := parseHeight(v); ok {
			setPointHeight(key, h)
			return
		}
	}
	lowerIdx, upperIdx := -1, -1
	if method == "blockchain" {
		lowerIdx, upperIdx = 0, 1
	}
	if applyRangeParamHints(
		key,
		paramFromJSONAny(params, rangeLowerParamNames, lowerIdx),
		paramFromJSONAny(params, rangeUpperParamNames, upperIdx),
	) {
		return
	}
	if method == "tx_search" || method == "block_search" {
		_ = applyEventQueryHeightHints(key, paramFromJSONAny(params, []string{"query"}, 0))
	}
}

func firstQueryValue(q url.Values, names []string) string {
	for _, name := range names {
		if v := q.Get(name); v != "" {
			return v
		}
	}
	return ""
}

func applyRangeParamHints(key *types.RouteKey, lower, upper string) bool {
	var b heightBounds
	if h, ok := parseHeight(lower); ok {
		b.add(">=", h)
	}
	if h, ok := parseHeight(upper); ok {
		b.add("<=", h)
	}
	return b.apply(key)
}

func applyEventQueryHeightHints(key *types.RouteKey, query string) bool {
	if query == "" {
		return false
	}
	var b heightBounds
	for _, m := range eventHeightConstraint.FindAllStringSubmatch(query, -1) {
		if len(m) != 3 {
			continue
		}
		h, ok := parseHeight(m[2])
		if !ok {
			continue
		}
		b.add(m[1], h)
	}
	return b.apply(key)
}

func (b *heightBounds) add(op string, h int64) {
	switch op {
	case "=":
		b.maxLower(h)
		b.minUpper(h)
	case ">=":
		b.maxLower(h)
	case ">":
		if h == maxInt64 {
			b.invalid = true
			return
		}
		b.maxLower(h + 1)
	case "<=":
		b.minUpper(h)
	case "<":
		if h <= 1 {
			b.invalid = true
			return
		}
		b.minUpper(h - 1)
	}
}

func (b *heightBounds) maxLower(h int64) {
	if b.lower == nil || h > *b.lower {
		v := h
		b.lower = &v
	}
}

func (b *heightBounds) minUpper(h int64) {
	if b.upper == nil || h < *b.upper {
		v := h
		b.upper = &v
	}
}

func (b heightBounds) apply(key *types.RouteKey) bool {
	if b.invalid || (b.lower == nil && b.upper == nil) {
		return false
	}
	if b.lower != nil && b.upper != nil {
		if *b.lower > *b.upper {
			return false
		}
		if *b.lower == *b.upper {
			setPointHeight(key, *b.lower)
			return true
		}
	}
	key.Height = nil
	key.Range = &types.HeightRange{
		Lower: cloneInt64Ptr(b.lower),
		Upper: cloneInt64Ptr(b.upper),
	}
	key.Class = types.ClassByHeightRange
	return true
}

func setPointHeight(key *types.RouteKey, h int64) {
	key.Height = &h
	key.Range = nil
	key.Class = types.ClassByHeight
}

func cloneInt64Ptr(in *int64) *int64 {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}
