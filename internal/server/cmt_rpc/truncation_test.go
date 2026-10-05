package cmt_rpc

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/cache"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	"github.com/InjectiveLabs/stitch/internal/forwarder"
	"github.com/InjectiveLabs/stitch/internal/health"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/types"
)

const blockResultsPath = "/block_results?height=75"

func completeBlockResults() []byte {
	return []byte(`{"jsonrpc":"2.0","id":-1,"result":{"height":"75","txs_results":[],"data":"` + strings.Repeat("x", 2<<20) + `"}}`)
}

func truncationBackend(name, endpoint string, lower int64, weight int) *backend.Backend {
	return &backend.Backend{
		Name: name, Weight: weight,
		Coverage:  backend.Coverage{Kind: backend.CovOpen, Lower: lower},
		Endpoints: map[types.Protocol]string{types.ProtoRPC: endpoint},
	}
}

func newTruncationServer(t *testing.T, bs []*backend.Backend, attempts int, timeout time.Duration) *httptest.Server {
	t.Helper()
	cm := circuit.NewManager(circuit.Policy{MinRequests: 100})
	reg := backend.NewRegistry(bs)
	sel := selector.NewRangeSelector(reg, health.NewRegistry(), cm, 0)
	p := pool.NewHTTPPool()
	t.Cleanup(p.CloseIdle)
	s := New("ignored", forwarder.NewHTTP(sel, p, cm, forwarder.Policy{MaxAttempts: attempts, PerAttemptTimeout: timeout}))
	s.SetResponseCache(cache.NewResponseCache(cache.ResponseCacheOpts{Capacity: 10, MaxBytes: 16 << 20}), func() int64 { return 200 }, 0, time.Minute)
	front := httptest.NewServer(s.Handler())
	t.Cleanup(front.Close)
	return front
}

func getTruncationResponse(t *testing.T, front *httptest.Server) (*http.Response, []byte) {
	t.Helper()
	resp, err := front.Client().Get(front.URL + blockResultsPath)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("buffered response was not complete: %v", err)
	}
	if resp.Header.Get("x-request-id") == "" {
		t.Fatal("caller request ID was lost")
	}
	return resp, body
}

func TestBufferedBodyFailureReturns502WithoutCaching(t *testing.T) {
	for _, mode := range []string{"content-length", "chunked", "deadline", "valid-json-prefix"} {
		t.Run(mode, func(t *testing.T) {
			good := completeBlockResults()
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) > 1 {
					_, _ = w.Write(good)
					return
				}
				w.Header().Set("ETag", "failed-attempt")
				w.Header().Set("Cache-Control", "public,max-age=86400")
				prefix := good[:128<<10] // Exercise the historical-error replay boundary.
				switch mode {
				case "content-length":
					w.Header().Set("Content-Length", strconv.Itoa(len(good)))
				case "valid-json-prefix":
					// Even syntactically valid JSON must not be cached when the
					// HTTP body failed before its promised end.
					prefix = []byte(`{"jsonrpc":"2.0","id":-1,"result":{"height":"75","poison":true}}`)
					w.Header().Set("Content-Length", strconv.Itoa(len(prefix)+100))
				}
				_, _ = w.Write(prefix)
				w.(http.Flusher).Flush()
				switch mode {
				case "chunked":
					panic(http.ErrAbortHandler) // Omit the final HTTP chunk.
				case "deadline":
					<-r.Context().Done()
				}
			}))
			defer upstream.Close()
			front := newTruncationServer(t, []*backend.Backend{truncationBackend("bad-"+mode, upstream.URL, 1, 100)}, 1, 300*time.Millisecond)
			resp, body := getTruncationResponse(t, front)
			if resp.StatusCode != http.StatusBadGateway || !bytes.Contains(body, []byte(`"error":`)) || bytes.Contains(body, []byte(`"result":`)) {
				t.Fatalf("failed attempt leaked success: status=%d body=%q", resp.StatusCode, body)
			}
			if resp.Header.Get("ETag") != "" || resp.Header.Get("Cache-Control") != "" {
				t.Fatalf("failed attempt headers survived: %v", resp.Header)
			}
			if resp.Header.Get("x-stitch-cache") != "miss" {
				t.Fatal("cache miss header lost during reset")
			}
			for _, wantCache := range []string{"miss", "hit"} {
				resp, body = getTruncationResponse(t, front)
				if resp.StatusCode != http.StatusOK || !bytes.Equal(body, good) || resp.Header.Get("x-stitch-cache") != wantCache {
					t.Fatalf("recovery/cache: status=%d bytes=%d cache=%q, want %q", resp.StatusCode, len(body), resp.Header.Get("x-stitch-cache"), wantCache)
				}
			}
			if calls.Load() != 2 {
				t.Fatalf("upstream calls=%d, want one failure and one successful fill", calls.Load())
			}
		})
	}
}

func TestBufferedBodyFailureRetriesOnlyEligibleCandidatesWithinBudget(t *testing.T) {
	for _, maxAttempts := range []int{1, 2} {
		t.Run(fmt.Sprint(maxAttempts), func(t *testing.T) {
			goodBody := completeBlockResults()
			var badCalls, goodCalls, ineligibleCalls atomic.Int32
			bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				badCalls.Add(1)
				w.Header().Set("Content-Length", "9000000")
				w.Header().Set("ETag", "bad")
				_, _ = w.Write(goodBody[:128<<10])
			}))
			defer bad.Close()
			good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				goodCalls.Add(1)
				w.Header().Set("ETag", "good")
				_, _ = w.Write(goodBody)
			}))
			defer good.Close()
			ineligible := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				ineligibleCalls.Add(1)
				_, _ = w.Write(goodBody)
			}))
			defer ineligible.Close()
			front := newTruncationServer(t, []*backend.Backend{
				truncationBackend("ineligible", ineligible.URL, 100, 1000),
				truncationBackend("bad", bad.URL, 1, 200),
				truncationBackend("good", good.URL, 1, 100),
			}, maxAttempts, time.Second)
			resp, body := getTruncationResponse(t, front)
			if maxAttempts == 1 {
				if resp.StatusCode != http.StatusBadGateway || goodCalls.Load() != 0 {
					t.Fatalf("attempt budget exceeded: status=%d good calls=%d", resp.StatusCode, goodCalls.Load())
				}
			} else if resp.StatusCode != http.StatusOK || !bytes.Equal(body, goodBody) || resp.Header.Get("ETag") != "good" || goodCalls.Load() != 1 {
				t.Fatalf("eligible retry failed: status=%d bytes=%d headers=%v good calls=%d", resp.StatusCode, len(body), resp.Header, goodCalls.Load())
			}
			if maxAttempts == 2 {
				resp, body = getTruncationResponse(t, front)
				if !bytes.Equal(body, goodBody) || resp.Header.Get("x-stitch-cache") != "hit" || goodCalls.Load() != 1 {
					t.Fatal("successful retry was not cached intact")
				}
			}
			if badCalls.Load() != 1 || ineligibleCalls.Load() != 0 {
				t.Fatalf("bad calls=%d ineligible calls=%d", badCalls.Load(), ineligibleCalls.Load())
			}
		})
	}
}

func TestLargeSlowBufferedResponseCompletesAndCaches(t *testing.T) {
	want := completeBlockResults()
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		for offset := 0; offset < len(want); offset += 128 << 10 {
			end := min(offset+128<<10, len(want))
			if _, err := w.Write(want[offset:end]); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			time.Sleep(5 * time.Millisecond)
		}
	}))
	defer upstream.Close()
	front := newTruncationServer(t, []*backend.Backend{truncationBackend("slow", upstream.URL, 1, 100)}, 1, 2*time.Second)
	for _, wantCache := range []string{"miss", "hit"} {
		resp, body := getTruncationResponse(t, front)
		if resp.StatusCode != http.StatusOK || !bytes.Equal(body, want) || resp.Header.Get("x-stitch-cache") != wantCache {
			t.Fatalf("large response: status=%d bytes=%d cache=%q", resp.StatusCode, len(body), resp.Header.Get("x-stitch-cache"))
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("upstream calls=%d, want one complete cache fill", calls.Load())
	}
}

func TestWebSocketQueryTruncationReturnsErrorAndStaysUsable(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if bytes.Contains(body, []byte(`"id":1`)) {
			w.Header().Set("Content-Length", "1000")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":`))
			return
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"result":{}}`))
	}))
	defer upstream.Close()
	front := newTruncationServer(t, []*backend.Backend{truncationBackend("ws-http", upstream.URL, 1, 100)}, 1, time.Second)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(front.URL, "http")+"/websocket", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for id := 1; id <= 2; id++ {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"status","params":{}}`, id))); err != nil {
			t.Fatal(err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		_, body, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		want := `"error":`
		if id == 2 {
			want = `"result":`
		}
		if !bytes.Contains(body, []byte(want)) {
			t.Fatalf("query %d: %s", id, body)
		}
	}
}
