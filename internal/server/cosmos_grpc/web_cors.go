package cosmos_grpc

import (
	"net/http"
	"strings"
)

// Run inside the CORS middleware: snapshot its decision before the REST
// upstream or gRPC adapter adds headers, then restore it before headers leave
// the server. In particular, upstream wildcard/credential policies must not
// widen this listener's policy or produce multiple Allow-Origin values.
func authoritativeWebCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		policy := make(http.Header)
		for name, values := range w.Header() {
			if isWebCORSHeader(name) {
				policy[name] = append([]string(nil), values...)
			}
		}
		protected := &webCORSResponseWriter{ResponseWriter: w, policy: policy}
		var writer http.ResponseWriter = protected
		if flusher, ok := w.(http.Flusher); ok {
			writer = &flushingWebCORSResponseWriter{webCORSResponseWriter: protected, flusher: flusher}
		}
		next.ServeHTTP(writer, r)
		// A handler may return without explicitly writing a status or body.
		protected.restoreHeaders()
	})
}

func isWebCORSHeader(name string) bool {
	return strings.HasPrefix(strings.ToLower(name), "access-control-")
}

func clearWebCORSHeaders(headers http.Header) {
	for name := range headers {
		if isWebCORSHeader(name) {
			delete(headers, name)
		}
	}
}

type webCORSResponseWriter struct {
	http.ResponseWriter
	policy      http.Header
	wroteHeader bool
}

func (w *webCORSResponseWriter) restoreHeaders() {
	headers := w.Header()
	clearWebCORSHeaders(headers)
	for name, values := range w.policy {
		headers[name] = append([]string(nil), values...)
	}
	// Keep upstream cache dimensions while preventing it from dropping Origin.
	for _, value := range headers.Values("Vary") {
		for _, token := range strings.Split(value, ",") {
			if token = strings.TrimSpace(token); token == "*" || strings.EqualFold(token, "Origin") {
				return
			}
		}
	}
	headers.Add("Vary", "Origin")
}

func (w *webCORSResponseWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.restoreHeaders()
	if code >= 200 || code == http.StatusSwitchingProtocols {
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *webCORSResponseWriter) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func (w *webCORSResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

type flushingWebCORSResponseWriter struct {
	*webCORSResponseWriter
	flusher http.Flusher
}

func (w *flushingWebCORSResponseWriter) Flush() {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	w.flusher.Flush()
}
