package cosmos_grpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"html"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/InjectiveLabs/stitch/internal/backend"
	"github.com/InjectiveLabs/stitch/internal/circuit"
	healthreg "github.com/InjectiveLabs/stitch/internal/health"
	"github.com/InjectiveLabs/stitch/internal/pool"
	"github.com/InjectiveLabs/stitch/internal/selector"
	"github.com/InjectiveLabs/stitch/internal/types"
)

const webBlockMethod = "/cosmos.base.tendermint.v1beta1.Service/GetBlockByHeight"

func webFrame(payload []byte) []byte {
	size := uint64(len(payload))
	if size > math.MaxUint32 {
		panic("test payload exceeds gRPC frame size")
	}
	out := make([]byte, 5, 5+len(payload))
	binary.BigEndian.PutUint32(out[1:5], uint32(size))
	return append(out, payload...)
}

func webRequest(t *testing.T, url, format string, payload []byte) *http.Request {
	t.Helper()
	body := webFrame(payload)
	if strings.Contains(format, "text") {
		body = []byte(base64.StdEncoding.EncodeToString(body))
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", format)
	req.Header.Set("X-Grpc-Web", "1")
	return req
}

// Text gRPC-Web can flush independently padded base64 chunks. Decode in four-byte
// units to inspect both unary responses and flushed streams without assuming the
// whole body is a single base64 entity.
type webTextReader struct {
	in      io.Reader
	pending []byte
}

func (r *webTextReader) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if len(r.pending) == 0 {
		var encoded [4]byte
		if _, err := io.ReadFull(r.in, encoded[:]); err != nil {
			return 0, err
		}
		decoded, err := base64.StdEncoding.DecodeString(string(encoded[:]))
		if err != nil {
			return 0, err
		}
		r.pending = decoded
	}
	n := copy(dst, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

func webReader(resp *http.Response) io.Reader {
	if strings.Contains(resp.Header.Get("Content-Type"), "text") {
		return &webTextReader{in: resp.Body}
	}
	return resp.Body
}

func readWebFrame(t *testing.T, in io.Reader) (byte, []byte) {
	t.Helper()
	var header [5]byte
	if _, err := io.ReadFull(in, header[:]); err != nil {
		t.Fatal(err)
	}
	size := binary.BigEndian.Uint32(header[1:5])
	if size > 1<<20 {
		t.Fatalf("unexpected response frame size %d", size)
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(in, payload); err != nil {
		t.Fatal(err)
	}
	return header[0], payload
}

func readWebResponse(t *testing.T, resp *http.Response) ([][]byte, http.Header) {
	t.Helper()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("HTTP %d: %s", resp.StatusCode, body)
	}
	in := webReader(resp)
	var messages [][]byte
	trailers := resp.Header.Clone() // Trailers-only errors can live in headers.
	for {
		var header [5]byte
		if _, err := io.ReadFull(in, header[:]); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		size := binary.BigEndian.Uint32(header[1:5])
		if size > 1<<20 {
			t.Fatalf("unexpected response frame size %d", size)
		}
		body := make([]byte, size)
		if _, err := io.ReadFull(in, body); err != nil {
			t.Fatal(err)
		}
		if header[0] == 0 {
			messages = append(messages, body)
			continue
		}
		if header[0] != 0x80 {
			t.Fatalf("unexpected frame flag %x", header[0])
		}
		for _, line := range strings.Split(string(body), "\r\n") {
			key, _, ok := strings.Cut(line, ":")
			if ok && strings.ToLower(key) != key {
				t.Fatalf("trailer name must be lowercase: %q", key)
			}
		}
		md, err := textproto.NewReader(bufio.NewReader(strings.NewReader(string(body) + "\r\n"))).ReadMIMEHeader()
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range md {
			trailers[k] = v
		}
		rest, err := io.ReadAll(in)
		if err != nil || len(rest) != 0 {
			t.Fatalf("trailers must be final frame: rest=%x err=%v", rest, err)
		}
		break
	}
	return messages, trailers
}

func TestWebHistoricalRouting(t *testing.T) {
	for _, format := range []string{"application/grpc-web+proto", "application/grpc-web-text+proto"} {
		for _, tc := range []struct {
			name, height, wantHeight string
			payload                  []byte
			wantShard                bool
		}{
			{"metadata", "1234", "1234", nil, true},
			{"body", "", "1234", heightPayload(1, 1234), true},
			{"metadata wins", "90000", "90000", heightPayload(1, 1234), false},
		} {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				rig := setupGRPC(t)
				defer rig.close()
				httpServer := httptest.NewServer(rig.front.WebHandler(WebOptions{}, nil))
				defer httpServer.Close()
				req := webRequest(t, httpServer.URL+webBlockMethod, format, tc.payload)
				req.Header.Set("Connection", "close, X-Hop-Only")
				req.Header.Set("X-Hop-Only", "must-not-forward")
				if tc.height != "" {
					req.Header.Set(HeightHeader, tc.height)
				}
				resp, err := httpServer.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				messages, trailers := readWebResponse(t, resp)
				if len(messages) != 1 || trailers.Get("Grpc-Status") != "0" {
					t.Fatalf("messages=%d trailers=%v", len(messages), trailers)
				}
				chosen, other := rig.archive, rig.shard
				if tc.wantShard {
					chosen, other = rig.shard, rig.archive
				}
				if chosen.hits.Load() != 1 || other.hits.Load() != 0 {
					t.Fatalf("routing: chosen=%d other=%d", chosen.hits.Load(), other.hits.Load())
				}
				if chosen.heightHeader.Load() != tc.wantHeight || !bytes.Equal(chosen.body.Load().([]byte), tc.payload) {
					t.Fatalf("request changed: height=%v body=%x", chosen.heightHeader.Load(), chosen.body.Load())
				}
				// The same grpc.Server must keep its native transport usable.
				if err := callHealth(t, rig.frontConn, "1234"); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func newWebBackend(t *testing.T, opts WebOptions, fallback http.Handler, handler func(grpc.ServerStream) error) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	upstream := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		var request emptypb.Empty
		if err := stream.RecvMsg(&request); err != nil {
			return err
		}
		return handler(stream)
	}))
	go func() { _ = upstream.Serve(listener) }()
	t.Cleanup(upstream.Stop)
	reg := backend.NewRegistry([]*backend.Backend{{Name: "archive", Weight: 100,
		Coverage:  backend.Coverage{Kind: backend.CovArchive},
		Endpoints: map[types.Protocol]string{types.ProtoGRPC: listener.Addr().String()},
	}})
	h := healthreg.NewRegistry()
	h.Update(healthreg.Snapshot{Backend: "archive", Protocol: types.ProtoRPC, Healthy: true, LatestHeight: 200000000})
	cm := circuit.NewManager(circuit.Policy{ErrorThreshold: 0.5, MinRequests: 10, OpenDuration: time.Minute})
	connections := pool.NewGRPCPool(time.Minute)
	t.Cleanup(connections.CloseAll)
	front, err := New("127.0.0.1:0", NewDirector(selector.NewRangeSelector(reg, h, cm, 0), cm, connections))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = front.lis.Close(); front.srv.Stop() })
	httpServer := httptest.NewServer(front.WebHandler(opts, fallback))
	httpServer.Client().Timeout = 4 * time.Second
	t.Cleanup(httpServer.Close)
	return httpServer
}

func TestWebMetadataTrailersAndErrors(t *testing.T) {
	for _, format := range []string{"application/grpc-web", "application/grpc-web-text"} {
		t.Run(format, func(t *testing.T) {
			srv := newWebBackend(t, WebOptions{AllowedOrigins: []string{"https://client.example"}}, nil, func(stream grpc.ServerStream) error {
				md, _ := metadata.FromIncomingContext(stream.Context())
				for _, name := range []string{"connection", "x-hop-only", "keep-alive", "proxy-connection", "upgrade"} {
					if len(md.Get(name)) != 0 {
						return status.Error(codes.InvalidArgument, "HTTP hop metadata leaked upstream")
					}
				}
				if md.Get("x-api-key")[0] != "test-key" || md.Get(HeightHeader)[0] != "1234" {
					return status.Error(codes.InvalidArgument, "metadata changed")
				}
				if err := stream.SendHeader(metadata.Pairs(HeightHeader, "1234")); err != nil {
					return err
				}
				if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
					return err
				}
				stream.SetTrailer(metadata.Pairs("x-test-trailer", "kept"))
				return status.Error(codes.NotFound, "missing test state")
			})
			req := webRequest(t, srv.URL+webBlockMethod, format, nil)
			req.Header.Set("Origin", "https://client.example")
			req.Header.Set("X-API-Key", "test-key")
			req.Header.Set("Connection", "close, X-Hop-Only")
			req.Header.Set("X-Hop-Only", "must-not-forward")
			req.Header.Set("Keep-Alive", "timeout=5")
			req.Header.Set("Proxy-Connection", "keep-alive")
			req.Header.Set("Upgrade", "unwanted")
			req.Header.Set(HeightHeader, "1234")
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			messages, trailers := readWebResponse(t, resp)
			if len(messages) != 1 || trailers.Get("Grpc-Status") != "5" || trailers.Get("Grpc-Message") != "missing test state" || trailers.Get("X-Test-Trailer") != "kept" {
				t.Fatalf("messages=%d trailers=%v", len(messages), trailers)
			}
			if resp.Header.Get(HeightHeader) != "1234" || resp.Header.Get("Access-Control-Allow-Origin") != "https://client.example" {
				t.Fatalf("response headers=%v", resp.Header)
			}
		})
	}
}

func TestWebCORSAndRESTFallback(t *testing.T) {
	var hits atomic.Int64
	fallback := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set(HeightHeader, r.Header.Get(HeightHeader))
		_, _ = io.WriteString(w, html.EscapeString(r.Method+" "+r.URL.RequestURI()))
	})
	srv := newWebBackend(t, WebOptions{AllowedOrigins: []string{"https://client.example"}}, fallback, func(stream grpc.ServerStream) error {
		return stream.SendMsg(&emptypb.Empty{})
	})
	for _, requested := range []string{"content-type,x-api-key,x-cosmos-block-height,x-grpc-web", "content-type,x-cosmos-block-height"} {
		req, _ := http.NewRequest(http.MethodOptions, srv.URL+webBlockMethod, nil)
		req.Header.Set("Origin", "https://client.example")
		req.Header.Set("Access-Control-Request-Method", "POST")
		req.Header.Set("Access-Control-Request-Headers", requested)
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent || resp.Header.Get("Access-Control-Allow-Origin") != "https://client.example" || resp.Header.Get("Access-Control-Allow-Headers") == "" {
			t.Fatalf("preflight: %d %v", resp.StatusCode, resp.Header)
		}
	}
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/cosmos/bank/v1beta1/supply?height=1234", nil)
	req.Header.Set("Origin", "https://client.example")
	req.Header.Set(HeightHeader, "1234")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "GET /cosmos/bank/v1beta1/supply?height=1234" || resp.Header.Get(HeightHeader) != "1234" || resp.Header.Get("Access-Control-Allow-Origin") != "https://client.example" {
		t.Fatalf("REST fallback changed: %s %v", body, resp.Header)
	}
	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodOptions} {
		req, _ := http.NewRequest(method, srv.URL+webBlockMethod, nil)
		req.Header.Set("Origin", "https://other.example")
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden || resp.Header.Get("Access-Control-Allow-Origin") != "" {
			t.Fatalf("denied origin: %d %v", resp.StatusCode, resp.Header)
		}
	}
	if hits.Load() != 1 {
		t.Fatalf("preflight or rejected origin reached fallback: %d", hits.Load())
	}
}

func TestWebStreamsAndCancellation(t *testing.T) {
	for _, format := range []string{"application/grpc-web+proto", "application/grpc-web-text+proto"} {
		t.Run(format, func(t *testing.T) {
			canceled := make(chan struct{})
			srv := newWebBackend(t, WebOptions{}, nil, func(stream grpc.ServerStream) error {
				if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
					return err
				}
				<-stream.Context().Done()
				close(canceled)
				return stream.Context().Err()
			})
			req := webRequest(t, srv.URL+"/test.Stream/Watch", format, nil)
			ctx, cancel := context.WithCancel(req.Context())
			defer cancel()
			resp, err := srv.Client().Do(req.WithContext(ctx))
			if err != nil {
				t.Fatal(err)
			}
			// This must arrive while the upstream is still open, not after it ends.
			flag, payload := readWebFrame(t, webReader(resp))
			if flag != 0 || len(payload) != 0 {
				t.Fatalf("first frame: %x %x", flag, payload)
			}
			cancel()
			resp.Body.Close()
			select {
			case <-canceled:
			case <-time.After(3 * time.Second):
				t.Fatal("browser cancellation did not reach upstream")
			}
		})
	}
}

func TestWebLimitsAndContentTypes(t *testing.T) {
	var hits atomic.Int64
	srv := newWebBackend(t, WebOptions{MaxRequestBytes: 16}, nil, func(stream grpc.ServerStream) error {
		hits.Add(1)
		return stream.SendMsg(&emptypb.Empty{})
	})
	for _, tc := range []struct {
		name, format, method string
		payload              []byte
		want                 int
	}{
		{"body limit", "application/grpc-web+proto", http.MethodPost, make([]byte, 20), http.StatusRequestEntityTooLarge},
		{"text body limit", "application/grpc-web-text+proto", http.MethodPost, make([]byte, 10), http.StatusRequestEntityTooLarge},
		{"unsupported codec", "application/grpc-web+json", http.MethodPost, nil, http.StatusUnsupportedMediaType},
		{"invalid suffix", "application/grpc-web-invalid", http.MethodPost, nil, http.StatusUnsupportedMediaType},
		{"wrong method", "application/grpc-web+proto", http.MethodGet, nil, http.StatusMethodNotAllowed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := webRequest(t, srv.URL+webBlockMethod, tc.format, tc.payload)
			req.Method = tc.method
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Fatalf("status %d; want %d", resp.StatusCode, tc.want)
			}
		})
	}
	if hits.Load() != 0 {
		t.Fatalf("rejected request reached upstream: %d", hits.Load())
	}
}

func TestWebRejectsMalformedAndChunkedOversizeBodies(t *testing.T) {
	var hits atomic.Int64
	srv := newWebBackend(t, WebOptions{MaxRequestBytes: 16}, nil, func(stream grpc.ServerStream) error {
		hits.Add(1)
		return stream.SendMsg(&emptypb.Empty{})
	})
	for _, tc := range []struct {
		name, format string
		body         []byte
	}{
		{"chunked binary limit", "application/grpc-web+proto", webFrame(make([]byte, 20))},
		{"chunked text limit", "application/grpc-web-text+proto", []byte(base64.StdEncoding.EncodeToString(webFrame(make([]byte, 20))))},
		{"invalid base64", "application/grpc-web-text+proto", []byte("$$$$")},
		{"truncated frame", "application/grpc-web+proto", []byte{0, 0, 0}},
		{"declared message too large", "application/grpc-web+proto", []byte{0, 4, 0, 0, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodPost, srv.URL+webBlockMethod, bytes.NewReader(tc.body))
			req.ContentLength = -1 // Force HTTP/1 chunked input: no length to trust.
			req.Header.Set("Content-Type", tc.format)
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			messages, trailers := readWebResponse(t, resp)
			if len(messages) != 0 || trailers.Get("Grpc-Status") == "" || trailers.Get("Grpc-Status") == "0" {
				t.Fatalf("malformed request accepted: messages=%d trailers=%v", len(messages), trailers)
			}
		})
	}
	if hits.Load() != 0 {
		t.Fatalf("malformed request reached upstream: %d", hits.Load())
	}
}

func TestWebDeadlineAndTrailersOnly(t *testing.T) {
	for _, tc := range []struct{ name, timeout, wantStatus string }{
		{"deadline", "25m", "4"},
		{"trailers only", "", "7"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			finished := make(chan struct{})
			srv := newWebBackend(t, WebOptions{}, nil, func(stream grpc.ServerStream) error {
				defer close(finished)
				if tc.timeout == "" {
					return status.Error(codes.PermissionDenied, "denied test request")
				}
				<-stream.Context().Done()
				return stream.Context().Err()
			})
			req := webRequest(t, srv.URL+webBlockMethod, "application/grpc-web-text+proto", nil)
			if tc.timeout != "" {
				req.Header.Set("Grpc-Timeout", tc.timeout)
			}
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			messages, trailers := readWebResponse(t, resp)
			if len(messages) != 0 || trailers.Get("Grpc-Status") != tc.wantStatus {
				t.Fatalf("messages=%d trailers=%v", len(messages), trailers)
			}
			select {
			case <-finished:
			case <-time.After(3 * time.Second):
				t.Fatal("upstream still running after request ended")
			}
		})
	}
}

func TestWebOriginDefaultsAndCustomHeaders(t *testing.T) {
	for _, tc := range []struct {
		name       string
		origins    []string
		wantStatus int
	}{
		{"default deny", nil, http.StatusForbidden},
		{"explicit wildcard", []string{"*"}, http.StatusNoContent},
		{"explicit origin", []string{"https://client.example"}, http.StatusNoContent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newWebBackend(t, WebOptions{AllowedOrigins: tc.origins, AllowedRequestHeaders: []string{"X-Custom-Client"}}, nil, func(_ grpc.ServerStream) error {
				t.Error("preflight reached upstream")
				return nil
			})
			for _, requestHeaders := range []string{"content-type,x-custom-client,x-grpc-web", "content-type,x-disallowed"} {
				req, _ := http.NewRequest(http.MethodOptions, srv.URL+webBlockMethod, nil)
				req.Header.Set("Origin", "https://client.example")
				req.Header.Set("Access-Control-Request-Method", "POST")
				req.Header.Set("Access-Control-Request-Headers", requestHeaders)
				resp, err := srv.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
				if resp.StatusCode != tc.wantStatus {
					t.Fatalf("status %d; want %d", resp.StatusCode, tc.wantStatus)
				}
				wantCORS := tc.wantStatus == http.StatusNoContent && !strings.Contains(requestHeaders, "disallowed")
				if got := resp.Header.Get("Access-Control-Allow-Origin") != ""; got != wantCORS {
					t.Fatalf("preflight headers=%v; permitted=%v", resp.Header, wantCORS)
				}
			}
		})
	}
}

func TestWebListenerAlsoServesNativeGRPC(t *testing.T) {
	for _, subtype := range []string{"", "proto"} {
		for _, byBody := range []bool{false, true} {
			t.Run("subtype="+subtype+"/body="+strconv.FormatBool(byBody), func(t *testing.T) {
				rig := setupGRPC(t)
				defer rig.close()
				var fallbackHits atomic.Int64
				fallback := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					fallbackHits.Add(1)
					_, _ = io.WriteString(w, "rest fallback")
				})
				handler := rig.front.WebHandler(WebOptions{AllowedOrigins: []string{"https://client.example"}}, fallback)
				srv := httptest.NewServer(h2c.NewHandler(handler, &http2.Server{}))
				defer srv.Close()
				conn, err := grpc.NewClient(strings.TrimPrefix(srv.URL, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				request := &emptypb.Empty{}
				var payload []byte
				if byBody {
					payload = heightPayload(1, 1234)
					request.ProtoReflect().SetUnknown(payload)
				} else {
					ctx = metadata.AppendToOutgoingContext(ctx, HeightHeader, "1234")
				}
				var headers metadata.MD
				opts := []grpc.CallOption{grpc.Header(&headers)}
				if subtype != "" {
					opts = append(opts, grpc.CallContentSubtype(subtype))
				}
				if err := conn.Invoke(ctx, webBlockMethod, request, &emptypb.Empty{}, opts...); err != nil {
					t.Fatal(err)
				}
				wantType := "application/grpc"
				if subtype != "" {
					wantType += "+" + subtype
				}
				if got := headers.Get("content-type"); len(got) != 1 || got[0] != wantType {
					t.Fatalf("native content type changed: %v; want %q", got, wantType)
				}
				if rig.shard.hits.Load() != 1 || rig.archive.hits.Load() != 0 || rig.shard.heightHeader.Load() != "1234" || !bytes.Equal(rig.shard.body.Load().([]byte), payload) {
					t.Fatalf("native historical route changed: shard=%d archive=%d height=%v body=%x", rig.shard.hits.Load(), rig.archive.hits.Load(), rig.shard.heightHeader.Load(), rig.shard.body.Load())
				}
				// The browser formats and REST remain available on this listener.
				for _, format := range []string{"application/grpc-web+proto", "application/grpc-web-text+proto"} {
					req := webRequest(t, srv.URL+webBlockMethod, format, heightPayload(1, 1234))
					resp, err := srv.Client().Do(req)
					if err != nil {
						t.Fatal(err)
					}
					messages, trailers := readWebResponse(t, resp)
					if len(messages) != 1 || trailers.Get("Grpc-Status") != "0" {
						t.Fatalf("web failed alongside native: %v", trailers)
					}
				}
				resp, err := srv.Client().Get(srv.URL + "/cosmos/bank/v1beta1/supply")
				if err != nil {
					t.Fatal(err)
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if string(body) != "rest fallback" || fallbackHits.Load() != 1 {
					t.Fatalf("unexpected REST fallback: %q hits=%d", body, fallbackHits.Load())
				}
				// Native dispatch must not bypass the shared origin restriction.
				deniedCtx := metadata.AppendToOutgoingContext(ctx, "origin", "https://other.example")
				err = conn.Invoke(deniedCtx, webBlockMethod, request, &emptypb.Empty{})
				if status.Code(err) != codes.PermissionDenied {
					t.Fatalf("native disallowed origin: %v", err)
				}
				if rig.shard.hits.Load() != 3 {
					t.Fatalf("denied native origin reached upstream: %d", rig.shard.hits.Load())
				}
			})
		}
	}
}

func TestWebListenerRejectsNativeGRPCOverHTTP1(t *testing.T) {
	srv := newWebBackend(t, WebOptions{}, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("native request reached REST fallback")
	}), func(grpc.ServerStream) error {
		t.Error("HTTP/1 native request reached backend")
		return nil
	})
	for _, contentType := range []string{"application/grpc", "application/grpc+proto"} {
		req := webRequest(t, srv.URL+webBlockMethod, contentType, nil)
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusHTTPVersionNotSupported || !strings.Contains(string(body), "requires HTTP/2") {
			t.Fatalf("native HTTP/1 response: status=%d body=%q", resp.StatusCode, body)
		}
	}
}
