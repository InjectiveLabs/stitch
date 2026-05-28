package subscription

import "google.golang.org/protobuf/encoding/protowire"

// ExtractStreamResponseHeight reads the block_height field (proto field 1,
// varint) from a marshaled injective.stream.v*.StreamResponse without
// allocating a typed message. The field number/type pair is identical in
// both v1beta1 and v2 (we cross-checked the .pb.go) so a single decoder
// covers both.
//
// Returns (0, false) if the field is absent or the bytes are malformed —
// callers treat that as "do not advance cursor", which is the safe
// default during unexpected wire shapes.
func ExtractStreamResponseHeight(data []byte) (uint64, bool) {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return 0, false
		}
		data = data[n:]
		if num == 1 && typ == protowire.VarintType {
			v, m := protowire.ConsumeVarint(data)
			if m < 0 {
				return 0, false
			}
			return v, true
		}
		// Skip the rest of this field.
		m := protowire.ConsumeFieldValue(num, typ, data)
		if m < 0 {
			return 0, false
		}
		data = data[m:]
	}
	return 0, false
}

// EncodeStreamResponseForTest builds a minimal proto-encoded
// StreamResponse with block_height set; remaining payload bytes are
// preserved as field 99 (bytes) so tests can include arbitrary tail
// content. Exposed as a public helper so the chainstream listener test
// (in a sibling package) can construct fixtures.
func EncodeStreamResponseForTest(height uint64, payload []byte) []byte {
	out := protowire.AppendTag(nil, 1, protowire.VarintType)
	out = protowire.AppendVarint(out, height)
	if len(payload) > 0 {
		out = protowire.AppendTag(out, 99, protowire.BytesType)
		out = protowire.AppendBytes(out, payload)
	}
	return out
}
