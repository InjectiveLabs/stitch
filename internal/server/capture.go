package server

import (
	"bytes"
	"net/http"
)

// Capture is an http.ResponseWriter that buffers status, headers, and
// body in memory so a handler can both relay a response to the client
// and inspect it (response-cache population, batch assembly, filter
// bookkeeping). Shared by the eth_rpc and cmt_rpc listeners.
//
// Use NewCapture — the zero value has no header map. Not safe for
// concurrent use; like a real ResponseWriter, it expects one handler
// writing at a time.
type Capture struct {
	header http.Header
	status int
	body   bytes.Buffer
}

// NewCapture returns a Capture whose header map is a clone of parent, so
// headers already set on the real writer (e.g. x-request-id) survive the
// detour through the buffer. Status defaults to 200, matching net/http's
// implicit WriteHeader on first Write.
func NewCapture(parent http.Header) *Capture {
	return &Capture{header: parent.Clone(), status: 200}
}

func (c *Capture) Header() http.Header         { return c.header }
func (c *Capture) WriteHeader(code int)        { c.status = code }
func (c *Capture) Write(b []byte) (int, error) { return c.body.Write(b) }

// Status returns the captured status code (200 if WriteHeader was never
// called).
func (c *Capture) Status() int { return c.status }

// BodyBytes returns the captured body. The slice aliases the internal
// buffer: callers keeping it must not Write to the Capture afterwards.
func (c *Capture) BodyBytes() []byte { return c.body.Bytes() }

// FlushTo replays the captured headers, status, and body onto w.
func (c *Capture) FlushTo(w http.ResponseWriter) {
	for k, vs := range c.header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(c.status)
	_, _ = w.Write(c.body.Bytes())
}
