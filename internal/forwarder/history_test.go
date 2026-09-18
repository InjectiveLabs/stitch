package forwarder

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	"github.com/InjectiveLabs/stitch/internal/types"
)

const missingState = `{"code":2,"message":"failed to load state at height 105504992; version mismatch on immutable IAVL tree; version does not exist"}`

func TestHistoricalHTTPFallback(t *testing.T) {
	for _, tc := range []struct {
		name       string
		protocol   types.Protocol
		method     string
		status     int
		body       string
		class      types.MethodClass
		idempotent bool
		wantRetry  bool
	}{
		{"rest", types.ProtoAPI, "/cosmos/bank/v1beta1/params", 500, missingState, types.ClassByHeight, true, true},
		{"abci", types.ProtoRPC, "abci_query", 200, `{"jsonrpc":"2.0","id":7,"result":{"response":{"code":18,"log":"failed to load state at height 105504992; version does not exist"}}}`, types.ClassByHeight, true, true},
		{"rpc_error", types.ProtoRPC, "abci_query", 200, `{"jsonrpc":"2.0","id":7,"error":{"message":"internal error","data":"failed to load state at height 105504992; version does not exist"}}`, types.ClassByHeight, true, true},
		{"unrelated_error", types.ProtoAPI, "query", 500, `{"code":2,"message":"permission denied"}`, types.ClassByHeight, true, false},
		{"latest", types.ProtoAPI, "query", 500, missingState, types.ClassLatest, true, false},
		{"write", types.ProtoAPI, "write", 500, missingState, types.ClassByHeight, false, false},
		{"evm", types.ProtoEthRPC, "eth_call", 500, missingState, types.ClassByHeight, true, false},
		{"unstructured", types.ProtoAPI, "query", 500, "failed to load state; version does not exist", types.ClassByHeight, true, false},
		{"oversize", types.ProtoAPI, "query", 500, missingState + strings.Repeat(" ", maxHistoricalErrorBytes), types.ClassByHeight, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer bad.Close()
			var hits atomic.Int32
			good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				if r.Header.Get("x-cosmos-block-height") != "105504992" {
					t.Error("height header lost")
				}
				body, _ := io.ReadAll(r.Body)
				if string(body) != "request payload" {
					t.Errorf("request changed: %q", body)
				}
				w.Header().Set("x-cosmos-block-height", "105504992")
				_, _ = io.WriteString(w, `{"ok":true}`)
			}))
			defer good.Close()
			bs := []*backend.Backend{mkBackend("missing", bad.URL), mkBackend("overlap", good.URL)}
			for _, b := range bs {
				b.Endpoints[tc.protocol] = b.Endpoints[types.ProtoRPC]
			}
			cm := circuit.NewManager(circuit.Policy{MinRequests: 1, ErrorThreshold: .5, OpenDuration: time.Minute})
			f := newForwarderWithCircuit(stubSelector{bs}, cm, 3)
			h := int64(105504992)
			req := httptest.NewRequest(http.MethodGet, "/query", strings.NewReader("request payload"))
			req.Header.Set("x-cosmos-block-height", "105504992")
			rec := httptest.NewRecorder()
			f.Forward(rec, req, types.RouteKey{Protocol: tc.protocol, Method: tc.method, Class: tc.class, Height: &h, Idempotent: tc.idempotent})
			if (hits.Load() == 1) != tc.wantRetry {
				t.Fatalf("retry hits=%d wantRetry=%v", hits.Load(), tc.wantRetry)
			}
			if tc.wantRetry {
				if rec.Code != 200 || rec.Body.String() != `{"ok":true}` {
					t.Fatalf("response %d %s", rec.Code, rec.Body)
				}
				if cm.State("missing", tc.protocol) != circuit.StateClosed {
					t.Error("retention gap tripped circuit")
				}
			} else if rec.Code != tc.status || rec.Body.String() != tc.body {
				t.Fatal("non-retryable response changed")
			}
		})
	}
}

func TestHistoricalHTTPExhaustionPreservesError(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Upstream", "retained")
		w.WriteHeader(500)
		_, _ = io.WriteString(w, missingState)
	}))
	defer upstream.Close()
	b := mkBackend("only", upstream.URL)
	b.Endpoints[types.ProtoAPI] = upstream.URL
	f := newForwarder(stubSelector{[]*backend.Backend{b}})
	h := int64(105504992)
	rec := httptest.NewRecorder()
	f.Forward(rec, httptest.NewRequest("GET", "/query", nil), types.RouteKey{Protocol: types.ProtoAPI, Method: "query", Class: types.ClassByHeight, Height: &h, Idempotent: true})
	if rec.Code != 500 || rec.Body.String() != missingState || rec.Header().Get("X-Upstream") != "retained" {
		t.Fatalf("changed error: %d %s", rec.Code, rec.Body)
	}
}

func TestHistoricalPeekPreservesReadFailure(t *testing.T) {
	want := errors.New("upstream truncated")
	resp := &http.Response{StatusCode: 500, Body: io.NopCloser(io.MultiReader(bytes.NewBufferString("partial"), failedRead{want}))}
	h := int64(1)
	if historicalError(resp, types.RouteKey{Protocol: types.ProtoAPI, Class: types.ClassByHeight, Height: &h, Idempotent: true}) != nil {
		t.Fatal("partial body classified")
	}
	body, err := io.ReadAll(resp.Body)
	if string(body) != "partial" || !errors.Is(err, want) {
		t.Fatalf("body=%q error=%v", body, err)
	}
}
