package eth_rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
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

type truncationSelector []*backend.Backend

func (s truncationSelector) Candidates(types.RouteKey) []*backend.Backend { return s }

func newTruncationEVM(t *testing.T, endpoints ...string) *Server {
	t.Helper()
	var sel truncationSelector
	for i, endpoint := range endpoints {
		sel = append(sel, &backend.Backend{Name: fmt.Sprintf("evm-truncation-%d", i), Endpoints: map[types.Protocol]string{types.ProtoEthRPC: endpoint}})
	}
	p := pool.NewHTTPPool()
	t.Cleanup(p.CloseIdle)
	cm := circuit.NewManager(circuit.Policy{MinRequests: 100})
	return New("ignored", forwarder.NewHTTP(sel, p, cm, forwarder.Policy{MaxAttempts: 3, PerAttemptTimeout: time.Second}))
}

func TestTruncatedEVMBatchItemPreservesIDAndOtherResults(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		switch string(req.ID) {
		case `"failed:string"`, "9007199254740993":
			w.Header().Set("Content-Length", "1000")
			_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":"poison"}`, req.ID)
		case "4":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":4,"error":{"code":-32042,"message":"upstream error","data":{"detail":true}}}`))
		default:
			_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":"0x59f"}`, req.ID)
		}
	}))
	defer upstream.Close()
	s := newTruncationEVM(t, upstream.URL)
	front := httptest.NewServer(s.Handler())
	defer front.Close()
	resp := post(t, front.URL, `[
		{"jsonrpc":"2.0","id":"failed:string","method":"eth_chainId","params":[]},
		{"jsonrpc":"2.0","id":9007199254740993,"method":"eth_chainId","params":[]},
		{"jsonrpc":"2.0","id":3,"method":"eth_chainId","params":[]},
		{"jsonrpc":"2.0","id":4,"method":"eth_chainId","params":[]}
	]`)
	defer resp.Body.Close()
	var items []struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   *struct {
			Code    int             `json:"code"`
			Message string          `json:"message"`
			Data    json.RawMessage `json:"data"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || len(items) != 4 {
		t.Fatalf("batch status=%d items=%d", resp.StatusCode, len(items))
	}
	for i, wantID := range []string{`"failed:string"`, "9007199254740993", "3", "4"} {
		if items[i].JSONRPC != "2.0" || string(items[i].ID) != wantID {
			t.Fatalf("item %d lost framing/ID: %+v", i, items[i])
		}
	}
	for i := range 2 {
		if items[i].Error == nil || items[i].Error.Code != -32000 || len(items[i].Result) != 0 {
			t.Fatalf("truncated item %d became success: %+v", i, items[i])
		}
	}
	if items[2].Error != nil || string(items[2].Result) != `"0x59f"` {
		t.Fatalf("unrelated successful item changed: %+v", items[2])
	}
	if items[3].Error == nil || items[3].Error.Code != -32042 || string(items[3].Error.Data) != `{"detail":true}` {
		t.Fatalf("existing JSON-RPC error changed: %+v", items[3])
	}
}

func TestTruncatedFilterMintDoesNotBindOrRetry(t *testing.T) {
	var badCalls, nextCalls atomic.Int32
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		badCalls.Add(1)
		w.Header().Set("Content-Length", "1000")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0xdead"}`))
	}))
	defer bad.Close()
	next := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		nextCalls.Add(1)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0xbeef"}`))
	}))
	defer next.Close()
	s := newTruncationEVM(t, bad.URL, next.URL)
	front := httptest.NewServer(s.Handler())
	defer front.Close()
	resp := post(t, front.URL, `{"jsonrpc":"2.0","id":1,"method":"eth_newFilter","params":[{}]}`)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusBadGateway || !json.Valid(body) || bytes.Contains(body, []byte(`"result"`)) {
		t.Fatalf("filter response: status=%d body=%q err=%v", resp.StatusCode, body, err)
	}
	if s.FilterStore().Size() != 0 || badCalls.Load() != 1 || nextCalls.Load() != 0 {
		t.Fatalf("failed mint bound/retried: bindings=%d bad=%d next=%d", s.FilterStore().Size(), badCalls.Load(), nextCalls.Load())
	}
}

func TestTruncatedEVMResponseDoesNotPoisonResponseOrHashCache(t *testing.T) {
	for _, responseCacheEnabled := range []bool{false, true} {
		t.Run(fmt.Sprint(responseCacheEnabled), func(t *testing.T) {
			badHash, goodHash := "0x"+strings.Repeat("ab", 32), "0x"+strings.Repeat("cd", 32)
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req jsonRPCRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
					return
				}
				hash := goodHash
				first := calls.Add(1) == 1
				if first {
					hash = badHash
				}
				body := fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":{"number":"0x4b","hash":%q}}`, req.ID, hash)
				if first {
					// The JSON alone looks complete and cacheable. Transport
					// completion must be checked before either cache sees it.
					w.Header().Set("Content-Length", strconv.Itoa(len(body)+100))
				}
				_, _ = w.Write([]byte(body))
			}))
			defer upstream.Close()
			s := newTruncationEVM(t, upstream.URL)
			idx := cache.New(10)
			s.SetHashCache(idx)
			rc := cache.NewResponseCache(cache.ResponseCacheOpts{Capacity: 10})
			if responseCacheEnabled {
				s.SetResponseCache(rc, func() int64 { return 200 }, 0, time.Minute)
			}
			front := httptest.NewServer(s.Handler())
			defer front.Close()
			for id := 1; id <= 3; id++ {
				resp := post(t, front.URL, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getBlockByNumber","params":["0x4b",false]}`, id))
				body, err := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if err != nil || !json.Valid(body) {
					t.Fatalf("invalid response: body=%q err=%v", body, err)
				}
				if _, found := idx.Get(cache.EthBlockKey(badHash)); found {
					t.Fatal("failed response poisoned hash index")
				}
				if id == 1 {
					if resp.StatusCode != http.StatusBadGateway || rc.Size() != 0 {
						t.Fatalf("failed response returned/cached success: status=%d entries=%d", resp.StatusCode, rc.Size())
					}
					continue
				}
				var envelope struct {
					ID json.RawMessage `json:"id"`
				}
				if json.Unmarshal(body, &envelope) != nil || string(envelope.ID) != strconv.Itoa(id) || !bytes.Contains(body, []byte(goodHash)) || resp.StatusCode != http.StatusOK {
					t.Fatalf("valid response/cache replay failed: status=%d body=%q", resp.StatusCode, body)
				}
				if height, found := idx.Get(cache.EthBlockKey(goodHash)); !found || height != 75 {
					t.Fatal("valid response failed to populate hash index")
				}
			}
			wantCalls := int32(3)
			if responseCacheEnabled {
				wantCalls = 2
			}
			if calls.Load() != wantCalls {
				t.Fatalf("upstream calls=%d want=%d", calls.Load(), wantCalls)
			}
		})
	}
}
