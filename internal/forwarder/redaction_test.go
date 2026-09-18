package forwarder

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/log"
	"github.com/InjectiveLabs/stitch/internal/types"
)

type forwardCall func(*HTTP, http.ResponseWriter, *http.Request, types.RouteKey)

var redactionForwardCalls = map[string]forwardCall{
	"ordinary":  (*HTTP).Forward,
	"hedge":     (*HTTP).Hedge,
	"broadcast": (*HTTP).Broadcast,
}

func TestForwardErrorsDoNotExposeURLCredentials(t *testing.T) {
	const query = "height=75&api_key=QuerySecret&other=OtherSecret"
	const endpoint = "http://PrivateUser:PrivatePassword@archive.example" //nolint:gosec // Synthetic credentials used to test redaction.
	for name, call := range redactionForwardCalls {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			if err := log.Init("debug", "json", &logs); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Init("info", "text", os.Stderr) })
			candidates := []*backend.Backend{mkBackend("first", endpoint), mkBackend("second", endpoint)}
			fwd := newHedgeForwarder(stubSelector{cands: candidates}, time.Millisecond)
			for _, candidate := range candidates {
				transport := fwd.pool.Transport(candidate.Name)
				transport.Proxy = nil
				transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
					// The inner diagnostic repeats the URL, in addition to the
					// url.Error added by http.Client.Do.
					return nil, fmt.Errorf("redirect %s/status?%s: %w", endpoint, query, context.DeadlineExceeded)
				}
			}
			r := httptest.NewRequest(http.MethodGet, "/status?"+query, nil)
			w := httptest.NewRecorder()
			call(fwd, w, r, types.RouteKey{Protocol: types.ProtoRPC, Method: "status", Idempotent: true})
			if w.Code != http.StatusBadGateway {
				t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
			}
			for source, text := range map[string]string{"response": w.Body.String(), "log": logs.String()} {
				for _, secret := range []string{"PrivateUser", "PrivatePassword", "QuerySecret", "OtherSecret", "api_key", "height="} {
					if strings.Contains(text, secret) {
						t.Fatalf("%s leaked %q: %s", source, secret, text)
					}
				}
				for _, diagnostic := range []string{"archive.example/status", "context deadline exceeded"} {
					if !strings.Contains(text, diagnostic) {
						t.Errorf("%s lost diagnostic %q: %s", source, diagnostic, text)
					}
				}
			}
			if r.URL.RawQuery != query {
				t.Fatal("request query changed")
			}
		})
	}
}

func TestForwardingPreservesQueryAndUpstreamCredentials(t *testing.T) {
	const query = "height=75&api_key=QuerySecret&filter=a%2Bb&duplicate=1&duplicate=2"
	for name, call := range redactionForwardCalls {
		t.Run(name, func(t *testing.T) {
			seen := make(chan string, 2)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				user, password, ok := r.BasicAuth()
				if !ok || user != "UpstreamUser" || password != "UpstreamPassword" {
					t.Error("upstream credentials were changed")
				}
				seen <- r.URL.RawQuery
				_, _ = w.Write([]byte("ok"))
			}))
			defer upstream.Close()
			endpoint := strings.Replace(upstream.URL, "://", "://UpstreamUser:UpstreamPassword@", 1)
			fwd := newHedgeForwarder(stubSelector{cands: []*backend.Backend{
				mkBackend("first", endpoint), mkBackend("second", endpoint),
			}}, time.Millisecond)
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/status?"+query, nil)
			call(fwd, w, r, types.RouteKey{Protocol: types.ProtoRPC, Method: "status", Idempotent: true})
			if w.Code != http.StatusOK || w.Body.String() != "ok" {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if got := <-seen; got != query {
				t.Fatalf("forwarded query %q, want %q", got, query)
			}
		})
	}
}

func TestRedactionPreservesErrorClassification(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, io.ErrUnexpectedEOF} {
		err := &url.Error{Op: "Get", URL: "http://archive.example/?api_key=Secret", Err: cause}
		wrapped := &ErrAllAttemptsFailed{Attempts: 2, Last: err}
		before := classifyErr(wrapped)
		if strings.Contains(wrapped.Error(), "Secret") {
			t.Fatal("aggregate error leaked a query")
		}
		_ = log.ErrorMessage(wrapped)
		var original *url.Error
		if classifyErr(wrapped) != before || !errors.Is(wrapped, cause) || !errors.As(wrapped, &original) || original != err {
			t.Fatal("diagnostic formatting changed error classification or identity")
		}
	}
}

func TestMalformedRedirectErrorsDoNotExposeQuery(t *testing.T) {
	for name, call := range redactionForwardCalls {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			if err := log.Init("debug", "json", &logs); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Init("info", "text", os.Stderr) })
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "/%zz?"+r.URL.RawQuery)
				w.WriteHeader(http.StatusFound)
			}))
			defer upstream.Close()
			fwd := newHedgeForwarder(stubSelector{cands: []*backend.Backend{
				mkBackend("first", upstream.URL), mkBackend("second", upstream.URL),
			}}, time.Millisecond)
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/status?api_key=QuerySecret", nil)
			call(fwd, w, r, types.RouteKey{Protocol: types.ProtoRPC, Method: "status", Idempotent: true})
			if w.Code != http.StatusBadGateway {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			for source, text := range map[string]string{"response": w.Body.String(), "log": logs.String()} {
				if strings.Contains(text, "QuerySecret") || strings.Contains(text, "api_key") {
					t.Fatalf("%s leaked redirect query: %s", source, text)
				}
				if !strings.Contains(text, "failed to parse Location") {
					t.Fatalf("%s lost parse diagnosis: %s", source, text)
				}
			}
		})
	}
}
