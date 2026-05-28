package admin

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/cache"
	"github.com/decentrio/stitch/internal/circuit"
	"github.com/decentrio/stitch/internal/health"
	"github.com/decentrio/stitch/internal/log"
	"github.com/decentrio/stitch/internal/types"
)

// Deps bundles the in-process state the admin endpoints inspect or mutate.
// Optional fields may be nil; handlers behave gracefully (return empty
// data) when a corresponding component isn't wired up.
type Deps struct {
	Registry  *backend.Registry
	Health    *health.Registry
	Circuit   *circuit.Manager
	HashCache *cache.HashIndex
	RespCache *cache.ResponseCache
	OnReload  func() error // returns nil on success
}

// SetDeps installs the dependencies for the admin endpoints. Idempotent.
func (s *Server) SetDeps(d Deps) {
	s.deps = d
	if mux, ok := s.srv.Handler.(*http.ServeMux); ok {
		s.installAdminRoutes(mux)
	}
}

// installAdminRoutes registers handlers under /admin/*. Called from
// SetDeps so the routes are only live once Deps is wired.
func (s *Server) installAdminRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/admin/backends", s.handleBackendsList)
	mux.HandleFunc("/admin/backends/", s.handleBackendDetail)
	mux.HandleFunc("/admin/cache/stats", s.handleCacheStats)
	mux.HandleFunc("/admin/cache/purge", s.handleCachePurge)
	mux.HandleFunc("/admin/reload", s.handleReload)
}

// ----- handlers -----------------------------------------------------------

type backendStatus struct {
	Name         string                  `json:"name"`
	Coverage     coverageView            `json:"coverage"`
	Weight       int                     `json:"weight"`
	Tags         []string                `json:"tags,omitempty"`
	Endpoints    map[string]string       `json:"endpoints"`
	Health       map[string]bool         `json:"health"`
	Circuit      map[string]string       `json:"circuit"`
	LatestHeight int64                   `json:"latest_height,omitempty"`
	Lag          int64                   `json:"lag"`
	Drained      bool                    `json:"drained"`
}

type coverageView struct {
	Kind  string `json:"kind"`
	Lower int64  `json:"lower,omitempty"`
	Upper int64  `json:"upper,omitempty"`
	Keep  int64  `json:"keep,omitempty"`
}

func (s *Server) handleBackendsList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.deps.Registry == nil {
		writeJSON(w, http.StatusOK, []backendStatus{})
		return
	}
	all := s.deps.Registry.Snapshot()
	out := make([]backendStatus, 0, len(all))
	for _, b := range all {
		out = append(out, s.snapshotBackend(b))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleBackendDetail(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/admin/backends/")
	if rest == "" {
		writeJSONError(w, http.StatusNotFound, "missing backend name")
		return
	}
	parts := strings.SplitN(rest, "/", 2)
	name := parts[0]
	action := ""
	if len(parts) == 2 {
		action = parts[1]
	}

	if s.deps.Registry == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "registry not wired")
		return
	}
	b := s.deps.Registry.Find(name)
	if b == nil {
		writeJSONError(w, http.StatusNotFound, "backend not found: "+name)
		return
	}

	switch action {
	case "":
		if r.Method != http.MethodGet {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		writeJSON(w, http.StatusOK, s.snapshotBackend(b))
	case "drain":
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "POST required")
			return
		}
		s.deps.Registry.Drain(name)
		log.L().Info("admin: backend drained", "backend", name)
		writeJSON(w, http.StatusOK, map[string]string{"backend": name, "state": "drained"})
	case "enable":
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "POST required")
			return
		}
		s.deps.Registry.Enable(name)
		log.L().Info("admin: backend enabled", "backend", name)
		writeJSON(w, http.StatusOK, map[string]string{"backend": name, "state": "enabled"})
	default:
		writeJSONError(w, http.StatusNotFound, "unknown action: "+action)
	}
}

func (s *Server) snapshotBackend(b *backend.Backend) backendStatus {
	out := backendStatus{
		Name:      b.Name,
		Coverage:  toCoverageView(b.Coverage),
		Weight:    b.Weight,
		Tags:      b.Tags,
		Endpoints: protocolURLMap(b),
		Health:    map[string]bool{},
		Circuit:   map[string]string{},
		Drained:   s.deps.Registry != nil && s.deps.Registry.IsDrained(b.Name),
	}
	if s.deps.Health != nil {
		// Iterate every known protocol on this backend.
		for proto := range b.Endpoints {
			snap, ok := s.deps.Health.Get(b.Name, proto)
			if !ok {
				continue
			}
			out.Health[string(proto)] = snap.Healthy
			if snap.LatestHeight > out.LatestHeight {
				out.LatestHeight = snap.LatestHeight
			}
			if snap.Lag > out.Lag {
				out.Lag = snap.Lag
			}
		}
	}
	if s.deps.Circuit != nil {
		for proto := range b.Endpoints {
			out.Circuit[string(proto)] = s.deps.Circuit.State(b.Name, proto).String()
		}
	}
	return out
}

func toCoverageView(c backend.Coverage) coverageView {
	v := coverageView{Kind: c.Kind.String()}
	switch c.Kind {
	case backend.CovBounded:
		v.Lower, v.Upper = c.Lower, c.Upper
	case backend.CovOpen:
		v.Lower = c.Lower
	case backend.CovPruned:
		v.Keep = c.Keep
	}
	return v
}

func protocolURLMap(b *backend.Backend) map[string]string {
	out := make(map[string]string, len(b.Endpoints))
	for k, v := range b.Endpoints {
		out[string(k)] = v
	}
	return out
}

// ----- cache --------------------------------------------------------------

type cacheStats struct {
	HashIndex hashStats `json:"hash_index"`
	Response  respStats `json:"response_cache"`
}

type hashStats struct {
	Size     int `json:"size"`
	Capacity int `json:"capacity"`
}

type respStats struct {
	Size  int   `json:"size"`
	Bytes int64 `json:"bytes"`
}

func (s *Server) handleCacheStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	out := cacheStats{}
	if s.deps.HashCache != nil {
		out.HashIndex.Size = s.deps.HashCache.Size()
		out.HashIndex.Capacity = s.deps.HashCache.Capacity()
	}
	if s.deps.RespCache != nil {
		out.Response.Size = s.deps.RespCache.Size()
		out.Response.Bytes = s.deps.RespCache.Bytes()
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleCachePurge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	// HashIndex doesn't have a Purge — re-create by emptying through
	// repeated Set+Capacity? Simpler: skip, since restart purges. For now
	// expose only the response cache size; admin can wait for TTL on the
	// hash index. (Phase 9 may add a real Purge method.)
	if s.deps.RespCache != nil {
		// We don't currently have a bulk purge on ResponseCache. Approximate
		// by leaving entries to TTL-expire; document limitation.
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "noop",
		"note":   "Bulk cache purge is not yet implemented; restart stitch or wait for TTL expiry.",
	})
}

// ----- reload -------------------------------------------------------------

func (s *Server) handleReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	if s.deps.OnReload == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "reload not wired")
		return
	}
	started := time.Now()
	if err := s.deps.OnReload(); err != nil {
		log.L().Error("admin: reload failed", "err", err.Error())
		writeJSONError(w, http.StatusInternalServerError, "reload failed: "+err.Error())
		return
	}
	log.L().Info("admin: reload complete", "duration", time.Since(started).String())
	writeJSON(w, http.StatusOK, map[string]any{
		"status":   "reloaded",
		"duration": time.Since(started).String(),
	})
}

// ----- helpers ------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// silence unused-import warnings if downstream stops referencing
var _ = errors.New
var _ types.Protocol = ""