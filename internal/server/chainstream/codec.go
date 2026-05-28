package chainstream

import (
	"errors"
	"fmt"
)

// rawCodec marshals/unmarshals into a *RawMessage so the chainstream
// server can shuttle proto bytes verbatim between client and upstream
// without touching the cosmos-sdk-heavy injective-core proto types.
//
// We register this with grpc.ForceServerCodec on the chainstream server.
// It does not affect the cosmos_grpc listener (its codec is set via the
// mwitkow init).
type rawCodec struct{}

// RawMessage is the codec's container type. Public so tests in the same
// package can construct it.
type RawMessage struct {
	Bytes []byte
}

func (rawCodec) Marshal(v any) ([]byte, error) {
	if r, ok := v.(*RawMessage); ok {
		return r.Bytes, nil
	}
	return nil, fmt.Errorf("chainstream/rawCodec: cannot marshal %T", v)
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	if r, ok := v.(*RawMessage); ok {
		// Take ownership of data; gRPC reuses buffers.
		r.Bytes = make([]byte, len(data))
		copy(r.Bytes, data)
		return nil
	}
	return fmt.Errorf("chainstream/rawCodec: cannot unmarshal into %T", v)
}

// Name returns "proto" so wire content-type negotiation succeeds with
// real Injective clients that mark requests as application/grpc+proto.
func (rawCodec) Name() string { return "proto" }

// ErrCodecMismatch is exposed for tests asserting graceful failure when
// a non-RawMessage is forwarded.
var ErrCodecMismatch = errors.New("chainstream: only *RawMessage is supported")
