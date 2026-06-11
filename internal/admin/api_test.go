package admin

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/cache"
	"github.com/decentrio/stitch/internal/circuit"
	"github.com/decentrio/stitch/internal/health"
	"github.com/decentrio/stitch/internal/types"
)

func makeRig(t *testing.T) (*Server, *httptest.Server, *backend.Registry, *health.Registry, *circuit.Manager, *cache.HashIndex, *cache.ResponseCache) {
	t.Helper()
	bs := []*backend.Backend{
		{
			Name:      "primary",
			Coverage:  backend.Coverage{Kind: backend.CovArchive},
			Weight:    100,
			Tags:      []string{"primary"},
			Endpoints: map[types.Protocol]string{types.ProtoRPC: "http://primary:26657"},
		},
		{
			Name:      "shard1",
			Coverage:  backend.Coverage{Kind: backend.CovBounded, Lower: 1, Upper: 50000},
			Weight:    100,
			Endpoints: map[types.Protocol]string{types.ProtoRPC: "http://shard1:26657"},
		},
	}
	reg := backend.NewRegistry(bs)
	h := health.NewRegistry()
	for _, bb := range bs {
		h.Update(health.Snapshot{Backend: bb.Name, Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 100000})
	}
	cm := circuit.NewManager(circuit.Policy{
		ErrorThreshold: 0.5, MinRequests: 2, OpenDuration: time.Second,
	})
	hi := cache.New(100)
	rc := cache.NewResponseCache(cache.ResponseCacheOpts{Capacity: 100})

	s := New("ignored")
	reloads := 0
	s.SetDeps(Deps{
		Registry:  reg,
		Health:    h,
		Circuit:   cm,
		HashCache: hi,
		RespCache: rc,
		OnReload:  func() error { reloads++; return nil },
	})
	front := httptest.NewServer(s.Handler())
	t.Cleanup(func() { front.Close() })
	return s, front, reg, h, cm, hi, rc
}

func TestAdminBackendsList(t *testing.T) {
	_, front, _, _, _, _, _ := makeRig(t)

	resp, err := http.Get(front.URL + "/admin/backends")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	var got []backendStatus
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 backends; got %d", len(got))
	}
	if got[0].Name == "" || got[1].Name == "" {
		t.Errorf("missing names: %+v", got)
	}
}

func TestAdminBackendDetail(t *testing.T) {
	_, front, _, _, _, _, _ := makeRig(t)

	resp, err := http.Get(front.URL + "/admin/backends/shard1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	var got backendStatus
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "shard1" {
		t.Errorf("name: %s", got.Name)
	}
	if got.Coverage.Kind != "bounded" || got.Coverage.Lower != 1 || got.Coverage.Upper != 50000 {
		t.Errorf("coverage: %+v", got.Coverage)
	}
}

func TestAdminBackendNotFound(t *testing.T) {
	_, front, _, _, _, _, _ := makeRig(t)

	resp, err := http.Get(front.URL + "/admin/backends/nope")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404; got %d", resp.StatusCode)
	}
}

func TestAdminBackendDrainEnable(t *testing.T) {
	_, front, reg, _, _, _, _ := makeRig(t)

	resp, err := http.Post(front.URL+"/admin/backends/shard1/drain", "", strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("drain status: %d", resp.StatusCode)
	}
	if !reg.IsDrained("shard1") {
		t.Error("registry should report shard1 drained")
	}

	resp, err = http.Post(front.URL+"/admin/backends/shard1/enable", "", strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if reg.IsDrained("shard1") {
		t.Error("registry should report shard1 NOT drained after enable")
	}
}

func TestAdminDrainRequiresPOST(t *testing.T) {
	_, front, _, _, _, _, _ := makeRig(t)

	resp, err := http.Get(front.URL + "/admin/backends/shard1/drain")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405; got %d", resp.StatusCode)
	}
}

func TestAdminCacheStats(t *testing.T) {
	_, front, _, _, _, hi, rc := makeRig(t)

	hi.Set("0xa", 1)
	hi.Set("0xb", 2)
	rc.Set("k", []byte("v"), 0)

	resp, err := http.Get(front.URL + "/admin/cache/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got cacheStats
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.HashIndex.Size != 2 {
		t.Errorf("hash size: %d", got.HashIndex.Size)
	}
	if got.Response.Size != 1 {
		t.Errorf("response size: %d", got.Response.Size)
	}
}

func TestAdminCachePurge(t *testing.T) {
	_, front, _, _, _, hi, rc := makeRig(t)

	hi.Set("0xa", 1)
	hi.Set("0xb", 2)
	hi.Set("0xc", 3)
	rc.Set("k1", []byte("v1"), 0)
	rc.Set("k2", []byte("v2"), 0)

	resp, err := http.Post(front.URL+"/admin/cache/purge", "", strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	var got struct {
		HashIndexPurged int `json:"hash_index_purged"`
		ResponsePurged  int `json:"response_purged"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.HashIndexPurged != 3 {
		t.Errorf("hash_index_purged=%d; want 3", got.HashIndexPurged)
	}
	if got.ResponsePurged != 2 {
		t.Errorf("response_purged=%d; want 2", got.ResponsePurged)
	}
	if hi.Size() != 0 || rc.Size() != 0 {
		t.Errorf("caches not emptied: hash=%d resp=%d", hi.Size(), rc.Size())
	}
}

func TestAdminCachePurgeRequiresPOST(t *testing.T) {
	_, front, _, _, _, _, _ := makeRig(t)

	resp, err := http.Get(front.URL + "/admin/cache/purge")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405; got %d", resp.StatusCode)
	}
}

func TestAdminCachePurgeWithoutCachesWired(t *testing.T) {
	s := New("ignored")
	s.SetDeps(Deps{})
	front := httptest.NewServer(s.Handler())
	defer front.Close()

	resp, err := http.Post(front.URL+"/admin/cache/purge", "", strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("nil caches should purge zero, not fail; status=%d", resp.StatusCode)
	}
	var got struct {
		HashIndexPurged int `json:"hash_index_purged"`
		ResponsePurged  int `json:"response_purged"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.HashIndexPurged != 0 || got.ResponsePurged != 0 {
		t.Errorf("expected zeros; got %+v", got)
	}
}

func TestAdminReload(t *testing.T) {
	s := New("ignored")
	reloads := 0
	s.SetDeps(Deps{
		OnReload: func() error { reloads++; return nil },
	})
	front := httptest.NewServer(s.Handler())
	defer front.Close()

	resp, err := http.Post(front.URL+"/admin/reload", "", strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	if reloads != 1 {
		t.Errorf("expected 1 reload; got %d", reloads)
	}
}

func TestAdminReloadSurfacesError(t *testing.T) {
	s := New("ignored")
	s.SetDeps(Deps{
		OnReload: func() error { return errReloadFailed },
	})
	front := httptest.NewServer(s.Handler())
	defer front.Close()

	resp, err := http.Post(front.URL+"/admin/reload", "", strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500; got %d", resp.StatusCode)
	}
}

var errReloadFailed = stringError("reload pretend to fail")

type stringError string

func (s stringError) Error() string { return string(s) }
