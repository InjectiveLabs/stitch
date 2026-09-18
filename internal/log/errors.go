package log

import (
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// ErrorMessage formats an error for logs or client responses without URL
// credentials, query strings or fragments. It does not change the original
// error, so callers can still classify it with errors.Is and errors.As.
func ErrorMessage(err error) string {
	// Wrappers can copy a relative or malformed URL into their own text.
	// Collect the structured values before rendering so those copies cannot
	// evade redaction, including Go-quoted copies containing escapes.
	var redactions []urlRedaction
	collectErrorURLs(err, &redactions)
	// A short URL can prefix a longer one. Replacing it first could expose
	// the longer URL's remaining query parameters as ordinary error text.
	sort.SliceStable(redactions, func(i, j int) bool { return len(redactions[i].raw) > len(redactions[j].raw) })
	replacements := make([]string, 0, 2*len(redactions))
	for _, r := range redactions {
		replacements = append(replacements, r.raw, r.safe)
	}
	return errorMessage(err, strings.NewReplacer(replacements...))
}

func errorMessage(err error, urls *strings.Replacer) string {
	if err == nil {
		return ""
	}
	var message string
	switch e := err.(type) {
	case *url.Error:
		// Format the structured URL separately: it can be relative, malformed,
		// or repeated inside an underlying transport error.
		message = fmt.Sprintf("%s %q: %s", e.Op, diagnosticURL(e.URL), errorMessage(e.Err, urls))
	case interface{ Unwrap() []error }:
		message = err.Error()
		for _, inner := range e.Unwrap() {
			message = replaceInnerError(message, inner, urls)
		}
	case interface{ Unwrap() error }:
		message = replaceInnerError(err.Error(), e.Unwrap(), urls)
	default:
		message = err.Error()
	}
	// Wrapping errors may copy a URL into their own message, and transports
	// may include a second URL in an unstructured cause. Redact those too.
	message = urls.Replace(message)
	// net/http flattens redirect Location parse errors into ordinary text.
	// Their quoted URL can be relative and absent from the structured tree.
	message = diagnosticQuotedPattern.ReplaceAllStringFunc(message, func(quoted string) string {
		raw, err := strconv.Unquote(quoted)
		if err != nil || (!strings.ContainsAny(raw, "?#") && !strings.Contains(raw, "://") && !strings.HasPrefix(raw, "/")) {
			return quoted
		}
		return strconv.Quote(diagnosticURL(raw))
	})
	return diagnosticURLPattern.ReplaceAllStringFunc(message, diagnosticURLToken)
}

func replaceInnerError(message string, inner error, urls *strings.Replacer) string {
	if inner == nil || inner.Error() == "" {
		return message
	}
	return strings.ReplaceAll(message, inner.Error(), errorMessage(inner, urls))
}

type urlRedaction struct{ raw, safe string }

func collectErrorURLs(err error, redactions *[]urlRedaction) {
	if err == nil {
		return
	}
	if e, ok := err.(*url.Error); ok && e.URL != "" {
		safe := diagnosticURL(e.URL)
		*redactions = append(*redactions,
			urlRedaction{strconv.Quote(e.URL), strconv.Quote(safe)},
			urlRedaction{strconv.QuoteToASCII(e.URL), strconv.Quote(safe)},
			urlRedaction{e.URL, safe})
	}
	switch e := err.(type) {
	case interface{ Unwrap() []error }:
		for _, inner := range e.Unwrap() {
			collectErrorURLs(inner, redactions)
		}
	case interface{ Unwrap() error }:
		collectErrorURLs(e.Unwrap(), redactions)
	}
}

var diagnosticURLPattern = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://[^\s<>]+`)
var diagnosticQuotedPattern = regexp.MustCompile(`"(?:[^"\\]|\\.)*"`)

func diagnosticURLToken(raw string) string {
	// Keep error-message punctuation around an unstructured URL. Quotes and
	// backslashes inside the query remain part of the URL being discarded.
	trimmed := strings.TrimRight(raw, `"'\),.;:!?`)
	return diagnosticURL(trimmed) + raw[len(trimmed):]
}

func diagnosticURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Opaque != "" {
		return "[redacted URL]"
	}
	u.User = nil
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	u.RawFragment = ""
	return u.String()
}
