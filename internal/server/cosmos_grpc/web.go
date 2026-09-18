package cosmos_grpc

import (
	"mime"
	"net/http"
	"strings"

	"github.com/rs/cors"
	"github.com/traefik/grpc-web/go/grpcweb"
)

// WebOptions applies only to the optional HTTP gRPC-Web handler. It does not
// change the native gRPC listener or the REST listener.
type WebOptions struct {
	// AllowedOrigins contains exact browser origins. An explicit "*" permits
	// any origin; an empty list denies requests carrying an Origin header.
	// Requests without Origin remain usable by non-browser clients.
	AllowedOrigins []string
	// AllowedRequestHeaders extends the standard gRPC-Web/Cosmos/auth headers.
	AllowedRequestHeaders []string
	// MaxRequestBytes bounds the encoded HTTP body, including base64 overhead.
	// Zero uses 90 MiB; the native gRPC server separately caps messages at 64 MiB.
	MaxRequestBytes int64
}

const defaultWebMaxRequestBytes int64 = 90 << 20

// WebHandler exposes the same routing, metadata and protobuf handling as the
// native listener. The upstream library owns binary/text framing and status
// trailers; native gRPC over HTTP/2 is served directly, and ordinary HTTP
// requests are sent to fallback (normally Cosmos REST).
//
// Mount this on a separate optional HTTP listener with HTTP/2 (TLS or h2c)
// enabled for native clients. Only unary and server-streaming browser RPCs are
// supported; the nonstandard gRPC-Web WebSocket transport is deliberately
// disabled. Closing a request cancels its native proxy stream.
func (s *Server) WebHandler(opts WebOptions, fallback http.Handler) http.Handler {
	if fallback == nil {
		fallback = http.NotFoundHandler()
	}
	allowed := make(map[string]bool, len(opts.AllowedOrigins))
	for _, origin := range opts.AllowedOrigins {
		allowed[origin] = true
	}
	allowedOrigin := func(origin string) bool { return allowed["*"] || allowed[origin] }
	headers := []string{
		"Content-Type", "X-Grpc-Web", "X-User-Agent", "Grpc-Timeout",
		"Grpc-Encoding", "Grpc-Accept-Encoding", "Authorization", "X-API-Key",
		"X-Cosmos-Block-Height", "Accept", "Accept-Language",
	}
	headers = append(headers, opts.AllowedRequestHeaders...)
	corsHandler := cors.New(cors.Options{
		AllowOriginFunc: allowedOrigin,
		AllowedMethods:  []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodOptions},
		AllowedHeaders:  headers,
		ExposedHeaders:  []string{"Grpc-Status", "Grpc-Message", "Grpc-Status-Details-Bin", HeightHeader, "X-Request-ID"},
		MaxAge:          600,
	})
	wrapped := grpcweb.WrapServer(s.srv, grpcweb.WithWebsockets(false))
	maxBytes := opts.MaxRequestBytes
	if maxBytes <= 0 {
		maxBytes = defaultWebMaxRequestBytes
	}
	route := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if mediaType == "application/grpc" || mediaType == "application/grpc+proto" {
			if err != nil {
				http.Error(w, "invalid gRPC content type", http.StatusUnsupportedMediaType)
				return
			}
			if r.ProtoMajor != 2 {
				http.Error(w, "native gRPC requires HTTP/2", http.StatusHTTPVersionNotSupported)
				return
			}
			// Preserve the native transport's content type, metadata, trailers,
			// message limits and streaming behavior without web conversion.
			s.srv.ServeHTTP(w, r)
			return
		}
		if !strings.HasPrefix(mediaType, "application/grpc-web") {
			fallback.ServeHTTP(w, r)
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "gRPC-Web requires POST", http.StatusMethodNotAllowed)
			return
		}
		switch mediaType {
		case "application/grpc-web", "application/grpc-web+proto", "application/grpc-web-text", "application/grpc-web-text+proto":
			// The transparent backend uses protobuf; do not pass other codecs
			// or prefix matches such as application/grpc-web-invalid to it.
		default:
			http.Error(w, "unsupported gRPC-Web content type", http.StatusUnsupportedMediaType)
			return
		}
		if err != nil {
			http.Error(w, "invalid gRPC-Web content type", http.StatusUnsupportedMediaType)
			return
		}
		if r.ContentLength > maxBytes {
			http.Error(w, "gRPC-Web request too large", http.StatusRequestEntityTooLarge)
			return
		}
		// The protocol adapter rewrites request headers for grpc.Server. Clone
		// first so middleware and the caller retain the original HTTP request.
		r = r.Clone(r.Context())
		// HTTP/1 connection headers are invalid HTTP/2 metadata. Without
		// removing them, a transparent upstream rejects even a normal
		// Connection: close request with RST_STREAM(PROTOCOL_ERROR).
		for _, value := range r.Header.Values("Connection") {
			for _, name := range strings.Split(value, ",") {
				r.Header.Del(strings.TrimSpace(name))
			}
		}
		for _, name := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Transfer-Encoding", "Upgrade", "TE", "Trailer"} {
			r.Header.Del(name)
		}
		r.Header.Set("Content-Type", mediaType)
		r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
		// CORS is handled once outside the adapter so REST fallback and
		// preflights without x-grpc-web use the same explicit origin policy.
		wrapped.HandleGrpcWebRequest(w, r)
	})
	withCORS := corsHandler.Handler(authoritativeWebCORS(route))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clearWebCORSHeaders(w.Header())
		if origin := r.Header.Get("Origin"); origin != "" && !allowedOrigin(origin) {
			w.Header().Add("Vary", "Origin")
			http.Error(w, "origin is not allowed", http.StatusForbidden)
			return
		}
		withCORS.ServeHTTP(w, r)
	})
}
