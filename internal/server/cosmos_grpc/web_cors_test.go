package cosmos_grpc

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	"github.com/InjectiveLabs/stitch/internal/forwarder"
	healthreg "github.com/InjectiveLabs/stitch/internal/health"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/server/cosmos_rest"
	"github.com/InjectiveLabs/stitch/internal/types"
)

func conflictingCORSHeaders() http.Header {
	return http.Header{
		"Access-Control-Allow-Origin":          {"*", "https://upstream.example"},
		"Access-Control-Allow-Credentials":     {"true"},
		"Access-Control-Allow-Methods":         {"DELETE"},
		"Access-Control-Allow-Headers":         {"*"},
		"Access-Control-Allow-Private-Network": {"true"},
		"Access-Control-Expose-Headers":        {"*"},
		"Access-Control-Max-Age":               {"86400"},
		"Vary":                                 {"Accept-Encoding"},
		"X-Upstream":                           {"kept"},
	}
}

func assertCORSHeaders(t *testing.T, headers http.Header, origin string) {
	t.Helper()
	want := []string{origin}
	if origin == "" {
		want = nil
	}
	if got := headers.Values("Access-Control-Allow-Origin"); !slices.Equal(got, want) {
		t.Fatalf("Allow-Origin = %v; want %v", got, want)
	}
	for _, name := range []string{"Allow-Credentials", "Allow-Methods", "Allow-Headers", "Allow-Private-Network", "Max-Age"} {
		if got := headers.Values("Access-Control-" + name); len(got) != 0 {
			t.Fatalf("upstream %s escaped policy: %v", name, got)
		}
	}
	exposed := strings.ToLower(strings.Join(headers.Values("Access-Control-Expose-Headers"), ","))
	if origin == "" {
		if exposed != "" {
			t.Fatalf("unexpected exposed headers without Origin: %s", exposed)
		}
	} else if exposed != "grpc-status, grpc-message, grpc-status-details-bin, x-cosmos-block-height, x-request-id" {
		t.Fatalf("exposed headers changed: %s", exposed)
	}
	vary := strings.ToLower(strings.Join(headers.Values("Vary"), ","))
	if !strings.Contains(vary, "origin") || !strings.Contains(vary, "accept-encoding") {
		t.Fatalf("cache dimensions lost: %v", headers.Values("Vary"))
	}
	if headers.Get("X-Upstream") != "kept" {
		t.Fatalf("non-CORS upstream header lost: %v", headers)
	}
}

func TestWebCORSPolicyOverridesRESTUpstream(t *testing.T) {
	var hits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get(HeightHeader) != "112637000" || r.URL.RawQuery != "height=112637000" {
			t.Errorf("upstream request changed: %s %v", r.URL, r.Header)
		}
		for name, values := range conflictingCORSHeaders() {
			w.Header()[name] = values
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(HeightHeader, "112637000")
		_, _ = io.WriteString(w, `{"height":"112637000"}`)
	}))
	defer upstream.Close()
	reg := backend.NewRegistry([]*backend.Backend{{Name: "archive", Weight: 100,
		Coverage:  backend.Coverage{Kind: backend.CovArchive},
		Endpoints: map[types.Protocol]string{types.ProtoAPI: upstream.URL},
	}})
	health := healthreg.NewRegistry()
	health.Update(healthreg.Snapshot{Backend: "archive", Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 200000000})
	cm := circuit.NewManager(circuit.Policy{ErrorThreshold: 0.5, MinRequests: 10, OpenDuration: time.Minute})
	fwd := forwarder.NewHTTP(selector.NewRangeSelector(reg, health, cm, 0), pool.NewHTTPPool(), cm, forwarder.Policy{MaxAttempts: 1})
	rest := cosmos_rest.New("", fwd).Handler()
	for _, tc := range []struct {
		name    string
		origins []string
		origin  string
		denied  bool
	}{
		{"allowed", []string{"https://client.example"}, "https://client.example", false},
		{"wildcard", []string{"*"}, "https://client.example", false},
		{"no origin", nil, "", false},
		{"disallowed", []string{"https://client.example"}, "https://other.example", true},
		{"default deny", nil, "https://client.example", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newWebBackend(t, WebOptions{AllowedOrigins: tc.origins}, rest, func(grpc.ServerStream) error {
				t.Error("REST reached gRPC backend")
				return nil
			})
			before := hits.Load()
			req, _ := http.NewRequest(http.MethodGet, srv.URL+"/cosmos/bank/v1beta1/supply?height=112637000", nil)
			req.Header.Set(HeightHeader, "112637000")
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if tc.denied {
				if resp.StatusCode != http.StatusForbidden || hits.Load() != before || len(resp.Header.Values("Access-Control-Allow-Origin")) != 0 {
					t.Fatalf("denied origin reached upstream or got CORS: %d %v", resp.StatusCode, resp.Header)
				}
				return
			}
			if resp.StatusCode != http.StatusOK || string(body) != `{"height":"112637000"}` || resp.Header.Get(HeightHeader) != "112637000" || hits.Load() != before+1 {
				t.Fatalf("REST forwarding changed: %d %s %v", resp.StatusCode, body, resp.Header)
			}
			assertCORSHeaders(t, resp.Header, tc.origin)
		})
	}
}

func TestWebCORSPolicyOverridesGRPCUpstream(t *testing.T) {
	for _, format := range []string{"native", "application/grpc-web+proto", "application/grpc-web-text+proto"} {
		for _, origin := range []string{"", "https://client.example"} {
			t.Run(format+"/origin="+origin, func(t *testing.T) {
				srv := newWebBackend(t, WebOptions{AllowedOrigins: []string{"https://client.example"}}, nil, func(stream grpc.ServerStream) error {
					md := metadata.MD{}
					for name, values := range conflictingCORSHeaders() {
						md.Set(name, values...)
					}
					if err := stream.SendHeader(md); err != nil {
						return err
					}
					stream.SetTrailer(metadata.Pairs("x-test-trailer", "kept"))
					return stream.SendMsg(&emptypb.Empty{})
				})
				if format == "native" {
					conn, err := grpc.NewClient(strings.TrimPrefix(srv.URL, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()
					if origin != "" {
						ctx = metadata.AppendToOutgoingContext(ctx, "origin", origin)
					}
					var headers, trailers metadata.MD
					if err := conn.Invoke(ctx, webBlockMethod, &emptypb.Empty{}, &emptypb.Empty{}, grpc.Header(&headers), grpc.Trailer(&trailers)); err != nil {
						t.Fatal(err)
					}
					canonical := http.Header{}
					for name, values := range headers {
						canonical[http.CanonicalHeaderKey(name)] = values
					}
					assertCORSHeaders(t, canonical, origin)
					if got := trailers.Get("x-test-trailer"); len(got) != 1 || got[0] != "kept" {
						t.Fatalf("native trailer lost: %v", trailers)
					}
					return
				}
				req := webRequest(t, srv.URL+webBlockMethod, format, nil)
				if origin != "" {
					req.Header.Set("Origin", origin)
				}
				resp, err := srv.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				messages, trailers := readWebResponse(t, resp)
				if len(messages) != 1 || trailers.Get("Grpc-Status") != "0" || trailers.Get("X-Test-Trailer") != "kept" {
					t.Fatalf("gRPC-Web framing or trailers changed: messages=%d trailers=%v", len(messages), trailers)
				}
				assertCORSHeaders(t, resp.Header, origin)
			})
		}
	}
}

func TestWebCORSPolicySurvivesLateHeaderWrites(t *testing.T) {
	for _, mode := range []string{"set", "add", "flush", "empty", "error"} {
		t.Run(mode, func(t *testing.T) {
			fallback := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				for name, values := range conflictingCORSHeaders() {
					if mode == "add" {
						for _, value := range values {
							w.Header().Add(name, value)
						}
					} else {
						w.Header()[name] = values
					}
				}
				// Direct map writes must not bypass case-insensitive filtering.
				w.Header()["access-control-allow-origin"] = []string{"https://bypass.example"}
				if mode == "flush" {
					w.(http.Flusher).Flush()
				}
				if mode == "error" {
					w.WriteHeader(http.StatusBadGateway)
				}
				if mode != "empty" {
					_, _ = io.WriteString(w, "body")
				}
			})
			srv := newWebBackend(t, WebOptions{AllowedOrigins: []string{"https://client.example"}}, fallback, func(grpc.ServerStream) error { return nil })
			req, _ := http.NewRequest(http.MethodGet, srv.URL+"/rest", nil)
			req.Header.Set("Origin", "https://client.example")
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if mode != "empty" && string(body) != "body" {
				t.Fatalf("body changed: %q", body)
			}
			wantStatus := http.StatusOK
			if mode == "error" {
				wantStatus = http.StatusBadGateway
			}
			if resp.StatusCode != wantStatus {
				t.Fatalf("status changed: %d; want %d", resp.StatusCode, wantStatus)
			}
			assertCORSHeaders(t, resp.Header, "https://client.example")
		})
	}
}

func TestWebCORSPolicyPreservesRESTStreaming(t *testing.T) {
	release := make(chan struct{})
	fallback := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for name, values := range conflictingCORSHeaders() {
			w.Header()[name] = values
		}
		w.Header().Set("Trailer", "X-Test-Trailer")
		_, _ = io.WriteString(w, "first")
		w.(http.Flusher).Flush()
		<-release
		_, _ = io.WriteString(w, "last")
		w.Header().Set("X-Test-Trailer", "kept")
	})
	srv := newWebBackend(t, WebOptions{AllowedOrigins: []string{"https://client.example"}}, fallback, func(grpc.ServerStream) error { return nil })
	// Always release the server, including when an assertion fails.
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/rest", nil)
	req.Header.Set("Origin", "https://client.example")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	first := make([]byte, len("first"))
	if _, err := io.ReadFull(resp.Body, first); err != nil || string(first) != "first" {
		t.Fatalf("first chunk was not flushed: %q %v", first, err)
	}
	assertCORSHeaders(t, resp.Header, "https://client.example")
	close(release)
	last, err := io.ReadAll(resp.Body)
	if err != nil || string(last) != "last" || resp.Trailer.Get("X-Test-Trailer") != "kept" {
		t.Fatalf("stream or trailers changed: %q %v %v", last, resp.Trailer, err)
	}
}
