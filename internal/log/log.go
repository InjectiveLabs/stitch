// Package log centralizes structured logging.
//
// All log lines carry request_id when one is present in the context. Direct
// use of fmt.Print* and the stdlib log package is forbidden by golangci-lint
// (forbidigo) outside cmd/.
package log

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
)

type ctxKey int

const (
	requestIDKey ctxKey = iota
	backendKey
	protocolKey
	methodKey
)

var defaultLogger atomic.Pointer[slog.Logger]

func init() {
	defaultLogger.Store(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
}

// Init configures the package-level logger from level/format/writer. It is
// safe to call multiple times (e.g. on hot reload).
func Init(level, format string, w io.Writer) error {
	if w == nil {
		w = os.Stderr
	}
	lvl, err := parseLevel(level)
	if err != nil {
		return err
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	switch format {
	case "json", "":
		h = slog.NewJSONHandler(w, opts)
	case "text":
		h = slog.NewTextHandler(w, opts)
	default:
		return &invalidFormatError{format: format}
	}
	defaultLogger.Store(slog.New(h))
	return nil
}

// L returns the package-level logger. Cheap; safe to call per request.
func L() *slog.Logger { return defaultLogger.Load() }

// FromCtx returns a logger pre-populated with request-scoped fields.
func FromCtx(ctx context.Context) *slog.Logger {
	l := L()
	if id, ok := ctx.Value(requestIDKey).(string); ok && id != "" {
		l = l.With(slog.String("request_id", id))
	}
	if b, ok := ctx.Value(backendKey).(string); ok && b != "" {
		l = l.With(slog.String("backend", b))
	}
	if p, ok := ctx.Value(protocolKey).(string); ok && p != "" {
		l = l.With(slog.String("protocol", p))
	}
	if m, ok := ctx.Value(methodKey).(string); ok && m != "" {
		l = l.With(slog.String("method", m))
	}
	return l
}

// WithRequestID stamps a request_id onto ctx. The empty string is a no-op so
// callers can pass through optional ids.
func WithRequestID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIDKey, id)
}

func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// WithBackend / WithProtocol / WithMethod attach scope fields used by the
// forwarder and per-protocol servers.
func WithBackend(ctx context.Context, name string) context.Context {
	if name == "" {
		return ctx
	}
	return context.WithValue(ctx, backendKey, name)
}

func WithProtocol(ctx context.Context, p string) context.Context {
	if p == "" {
		return ctx
	}
	return context.WithValue(ctx, protocolKey, p)
}

func WithMethod(ctx context.Context, m string) context.Context {
	if m == "" {
		return ctx
	}
	return context.WithValue(ctx, methodKey, m)
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, &invalidLevelError{level: s}
	}
}

type invalidLevelError struct{ level string }

func (e *invalidLevelError) Error() string { return "invalid log level: " + e.level }

type invalidFormatError struct{ format string }

func (e *invalidFormatError) Error() string { return "invalid log format: " + e.format }
