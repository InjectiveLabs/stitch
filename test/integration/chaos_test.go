// Package integration: chaos test — runs a sustained workload against
// stitch while a chaos goroutine repeatedly kills + revives upstreams.
// Phase 9 acceptance: client request success rate stays high under
// rolling backend failures.
package integration

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/circuit"
	"github.com/decentrio/stitch/internal/forwarder"
	"github.com/decentrio/stitch/internal/health"
	"github.com/decentrio/stitch/internal/pool"
	"github.com/decentrio/stitch/internal/selector"
	"github.com/decentrio/stitch/internal/server/cmt_rpc"
	"github.com/decentrio/stitch/internal/types"
)

// flickerMock is like upstream but adds a Revive method so chaos can
// bring a "killed" backend back online.
type flickerMock struct {
	name   string
	addr   string
	srv    *http.Server
	dead   atomic.Bool
	hits   atomic.Int64
	height int64
}

func newFlickerMock(t *testing.T, name string, height int64) *flickerMock {
	t.Helper()
	m := &flickerMock{name: name, height: height}
	mux := http.NewServeMux()
	mux.HandleFunc("/", m.handle)
	srv := httptest.NewServer(mux)
	m.srv = srv.Config
	m.addr = srv.URL
	t.Cleanup(srv.Close)
	return m
}

func (m *flickerMock) handle(w http.ResponseWriter, r *http.Request) {
	m.hits.Add(1)
	if m.dead.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("content-type", "application/json")
	switch r.URL.Path {
	case "/status":
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":-1,"result":{"sync_info":{"latest_block_height":"` + strconv.FormatInt(m.height, 10) + `","catching_up":false}}}`))
	case "/block":
		h := r.URL.Query().Get("height")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":-1,"result":{"block":{"header":{"height":"` + h + `"}},"served_by":"` + m.name + `"}}`))
	default:
		_, _ = w.Write([]byte(`{"served_by":"` + m.name + `"}`))
	}
}

func (m *flickerMock) Kill()       { m.dead.Store(true) }
func (m *flickerMock) Revive()     { m.dead.Store(false) }
func (m *flickerMock) URL() string { return m.addr }

func TestChaosRollingBackendKillsKeepsClientsHappy(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos: skipping under -short")
	}

	// Spin up 5 archive-class backends so any one being dead leaves
	// 4 live for the selector + forwarder to fall back to.
	const numBackends = 5
	mocks := make([]*flickerMock, numBackends)
	bs := make([]*backend.Backend, numBackends)
	for i := 0; i < numBackends; i++ {
		mocks[i] = newFlickerMock(t, fmt.Sprintf("b%d", i), 100000)
		bs[i] = &backend.Backend{
			Name:      mocks[i].name,
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoRPC: mocks[i].URL()},
		}
	}
	reg := backend.NewRegistry(bs)
	h := health.NewRegistry()
	for _, bb := range bs {
		h.Update(health.Snapshot{Backend: bb.Name, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100000})
	}
	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5, MinRequests: 4, OpenDuration: 200 * time.Millisecond,
	})
	sel := selector.NewRangeSelector(reg, h, cm, 0)
	fwd := forwarder.NewHTTP(sel, pool.NewHTTPPool(), cm, forwarder.Policy{
		MaxAttempts:       4,
		PerAttemptTimeout: 1 * time.Second,
	})
	rpcSrv := cmt_rpc.New("ignored", fwd)
	front := httptest.NewServer(rpcSrv.Handler())
	defer front.Close()

	// Chaos goroutine: every ~50ms, kill or revive a random backend.
	chaosDone := make(chan struct{})
	go func() {
		t := time.NewTicker(50 * time.Millisecond)
		defer t.Stop()
		seq := uint64(0)
		for {
			select {
			case <-chaosDone:
				// Revive everything before we exit so any final
				// in-flight request can succeed.
				for _, m := range mocks {
					m.Revive()
				}
				return
			case <-t.C:
				idx := int(seq % numBackends)
				seq++
				if seq%3 == 0 {
					mocks[idx].Revive()
				} else {
					mocks[idx].Kill()
				}
				// Always keep at least 2 alive.
				alive := 0
				for _, m := range mocks {
					if !m.dead.Load() {
						alive++
					}
				}
				if alive < 2 {
					mocks[(idx+1)%numBackends].Revive()
				}
			}
		}
	}()

	// Driver: spin N concurrent workers that each fire M requests.
	const workers = 10
	const reqsPerWorker = 50
	var success, failure atomic.Int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < reqsPerWorker; i++ {
				resp, err := http.Get(front.URL + "/status")
				if err != nil {
					failure.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode == 200 {
					success.Add(1)
				} else {
					failure.Add(1)
				}
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	close(chaosDone)
	time.Sleep(100 * time.Millisecond) // let chaos goroutine settle

	total := success.Load() + failure.Load()
	if total != int64(workers*reqsPerWorker) {
		t.Errorf("expected %d total requests; got %d", workers*reqsPerWorker, total)
	}
	rate := float64(success.Load()) / float64(total)
	t.Logf("chaos test: %d success / %d total (%.1f%% success rate)", success.Load(), total, rate*100)

	// Acceptance: with at least 2 backends always alive and 4 retry
	// attempts, success rate should comfortably exceed 95%.
	if rate < 0.95 {
		t.Errorf("success rate %.1f%% below threshold; got %d/%d",
			rate*100, success.Load(), total)
	}
}
