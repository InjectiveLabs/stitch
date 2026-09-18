package log

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

func TestErrorMessageRedactsURLCredentials(t *testing.T) {
	const raw = "https://private-user:private-pass@archive.example/status?api_key=secret-key&height=75#private-fragment" //nolint:gosec // Synthetic credentials used to test redaction.
	innerURL := "https://other-user:other-pass@other.example/redirect?token=other-key"                                   //nolint:gosec // Synthetic credentials used to test redaction.
	transport := &url.Error{Op: "Get", URL: raw, Err: context.DeadlineExceeded}
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"transport", transport, `Get "https://archive.example/status": context deadline exceeded`},
		{"wrapped", fmt.Errorf("retry exhausted: %w", transport), `retry exhausted: Get "https://archive.example/status": context deadline exceeded`},
		{"nested", &url.Error{Op: "Post", URL: innerURL, Err: transport}, `Post "https://other.example/redirect": Get "https://archive.example/status": context deadline exceeded`},
		{"joined", errors.Join(errors.New("first candidate failed"), transport), "first candidate failed\nGet \"https://archive.example/status\": context deadline exceeded"},
		{"inner repeats URL", &url.Error{Op: "Get", URL: raw, Err: fmt.Errorf("redirect from %s to %s failed", raw, innerURL)}, `Get "https://archive.example/status": redirect from https://archive.example/status to https://other.example/redirect failed`},
		{"wrapper repeats URL", fmt.Errorf("request %s: %w", innerURL, transport), `request https://other.example/redirect: Get "https://archive.example/status": context deadline exceeded`},
		{"relative", &url.Error{Op: "Get", URL: "/status?api_key=secret-key", Err: errors.New("unsupported protocol")}, `Get "/status": unsupported protocol`},
		{"malformed", &url.Error{Op: "parse", URL: "https://private-user:private-pass@%zz/?api_key=secret-key", Err: errors.New("invalid URL escape")}, `parse "[redacted URL]": invalid URL escape`},
		{"opaque", &url.Error{Op: "parse", URL: "http:private-user:private-pass?api_key=secret-key", Err: errors.New("invalid URL")}, `parse "[redacted URL]": invalid URL`},
		{"ordinary", errors.New("connection reset by peer"), "connection reset by peer"},
		{"nil", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ErrorMessage(tt.err)
			for _, secret := range []string{"private-user", "private-pass", "secret-key", "private-fragment", "other-user", "other-pass", "other-key", "api_key", "height=", "token="} {
				if strings.Contains(got, secret) {
					t.Fatalf("credential or query leaked: %q", got)
				}
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
	if transport.URL != raw || transport.Err != context.DeadlineExceeded {
		t.Fatal("formatting changed the original error")
	}
	if !errors.Is(transport, context.DeadlineExceeded) {
		t.Fatal("timeout classification changed")
	}
}

func TestErrorMessageRedactsCopiedAndQuotedURLs(t *testing.T) {
	for _, raw := range []string{
		"/status?api_key=Secret",
		`https://archive.example/status?api_key=before"Secret`,
		`https://archive.example/status?api_key=before\Secret`,
		"https://archive.example/status?first=before\napi_key=Secret",
	} {
		for _, format := range []string{"request %s: %w", "request %q: %w", "request %+q: %w"} {
			t.Run(fmt.Sprintf("%q/%s", raw, format), func(t *testing.T) {
				inner := &url.Error{Op: "Get", URL: raw, Err: context.DeadlineExceeded}
				got := ErrorMessage(fmt.Errorf(format, raw, inner))
				if strings.Contains(got, "Secret") || strings.Contains(got, "api_key") {
					t.Fatalf("copied URL leaked: %q", got)
				}
			})
		}
	}
}

func TestErrorMessageRedactsURLsRepeatedByUnstructuredCause(t *testing.T) {
	for _, raw := range []string{
		`https://other.example/status?api_key=before"Secret`,
		`https://other.example/status?api_key=before\Secret`,
	} {
		for _, format := range []string{"redirect %s failed", "redirect %q failed"} {
			cause := fmt.Errorf(format, raw)
			err := &url.Error{Op: "Get", URL: "https://archive.example/status?api_key=OuterSecret", Err: cause}
			got := ErrorMessage(err)
			if strings.Contains(got, "Secret") || strings.Contains(got, "api_key") {
				t.Fatalf("underlying URL leaked: %q", got)
			}
		}
	}
}

func TestErrorMessageRedactsOverlappingURLs(t *testing.T) {
	short := "https://archive.example/status?api_key=Secret"
	long := short + "&token=OtherSecret"
	inner := &url.Error{Op: "Get", URL: long, Err: context.DeadlineExceeded}
	outer := &url.Error{Op: "Get", URL: short, Err: fmt.Errorf("redirect %s: %w", long, inner)}
	for _, err := range []error{outer, errors.Join(outer, inner), fmt.Errorf("request %q: %w", long, outer)} {
		got := ErrorMessage(err)
		if strings.Contains(got, "Secret") || strings.Contains(got, "token=") || strings.Contains(got, "api_key") {
			t.Fatalf("overlapping URL leaked: %q", got)
		}
	}
}

func TestErrorMessageRedactsFlattenedRelativeRedirect(t *testing.T) {
	for _, location := range []string{"/%zz?api_key=Secret", "%zz?api_key=Secret", "?api_key=Secret#%zz", "/%zz#Secret"} {
		cause := fmt.Errorf("failed to parse Location header %q: parse %q: invalid URL escape %%zz", location, location)
		err := &url.Error{Op: "Get", URL: "http://archive.example/status", Err: cause}
		got := ErrorMessage(err)
		if strings.Contains(got, "Secret") || strings.Contains(got, "api_key") {
			t.Fatalf("flattened redirect URL leaked: %q", got)
		}
		if !strings.Contains(got, "invalid URL escape %zz") {
			t.Fatalf("parse diagnosis lost: %q", got)
		}
	}
}
