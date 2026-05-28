package cmt_rpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
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
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return ""
	}
	if raw[0] == '[' {
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
		v, ok := obj[name]
		if !ok {
			return ""
		}
		return unquoteRaw(v)
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
	// CometBFT accepts both decimal and 0x… in some clients.
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		h, err := strconv.ParseInt(s[2:], 16, 64)
		return h, err == nil
	}
	h, err := strconv.ParseInt(s, 10, 64)
	return h, err == nil
}
