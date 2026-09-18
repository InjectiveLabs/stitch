package cmt_rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/cache"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	"github.com/InjectiveLabs/stitch/internal/forwarder"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/types"
)

type fixedCMTSelector []*backend.Backend

func (s fixedCMTSelector) Candidates(types.RouteKey) []*backend.Backend { return s }

func TestABCIErrorsAreNotCached(t *testing.T) {
	for _, code := range []string{`18`, `"18"`, `"invalid"`} {
		t.Run(code, func(t *testing.T) {
			var calls atomic.Int64
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
					return
				}
				applicationCode := code
				if calls.Add(1) > 1 {
					applicationCode = "0"
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"response":{"code":%s,"value":"retained-state"}}}`, req.ID, applicationCode)
			}))
			defer upstream.Close()
			sel := fixedCMTSelector{{Name: "archive", Endpoints: map[types.Protocol]string{types.ProtoRPC: upstream.URL}}}
			cm := circuit.NewManager(circuit.Policy{MinRequests: 10})
			fwd := forwarder.NewHTTP(sel, pool.NewHTTPPool(), cm, forwarder.Policy{MaxAttempts: 1})
			srv := New("ignored", fwd)
			srv.SetResponseCache(cache.NewResponseCache(cache.ResponseCacheOpts{Capacity: 10}), func() int64 { return 200 }, 0, time.Minute)
			for i := 1; i <= 3; i++ {
				request := fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"abci_query","params":{"path":"/store/bank/key","height":"75"}}`, i)
				r := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(request))
				w := httptest.NewRecorder()
				srv.ServeHTTP(w, r)
				wantCache := "miss"
				if i == 3 {
					wantCache = "hit"
				}
				if got := w.Header().Get("x-stitch-cache"); got != wantCache {
					t.Fatalf("call %d cache = %q, want %q; %s", i, got, wantCache, w.Body.String())
				}
				var response map[string]json.RawMessage
				if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				assertCMTID(t, response, fmt.Sprint(i))
			}
			if calls.Load() != 2 {
				t.Fatalf("upstream calls = %d, want 2", calls.Load())
			}
		})
	}
}

func TestABCICacheSuccessCodes(t *testing.T) {
	for _, fields := range []string{`"code":0`, `"code":"0"`, `"value":"empty-code"`} {
		body := []byte(`{"jsonrpc":"2.0","id":1,"result":{"response":{` + fields + `}}}`)
		if !cmtCacheableResponse("abci_query", body) {
			t.Fatalf("successful response rejected: %s", body)
		}
	}
	for _, result := range []string{`{}`, `{"response":null}`, `{"response":{"code":null}}`, `{"response":{"code":-1}}`} {
		body := []byte(`{"jsonrpc":"2.0","id":1,"result":` + result + `}`)
		if cmtCacheableResponse("abci_query", body) {
			t.Fatalf("invalid ABCI response cached: %s", body)
		}
	}
}
