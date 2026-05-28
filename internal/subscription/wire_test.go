package subscription

import "testing"

func TestExtractStreamResponseHeightRoundTrip(t *testing.T) {
	for _, h := range []uint64{0, 1, 127, 128, 65535, 1 << 20, 1 << 50} {
		bytes := EncodeStreamResponseForTest(h, []byte("ignored"))
		got, ok := ExtractStreamResponseHeight(bytes)
		if !ok {
			t.Errorf("height %d: not extracted", h)
			continue
		}
		if got != h {
			t.Errorf("height %d: got %d", h, got)
		}
	}
}

func TestExtractStreamResponseHeightAbsent(t *testing.T) {
	// A message with only field 99.
	bytes := EncodeStreamResponseForTest(0, []byte{0x01, 0x02, 0x03})
	// Strip the height field by encoding only payload.
	bytes = bytes[2:] // drop tag(1)+varint(0)
	if _, ok := ExtractStreamResponseHeight(bytes); ok {
		t.Error("absent height should return ok=false")
	}
}

func TestExtractStreamResponseHeightMalformed(t *testing.T) {
	if _, ok := ExtractStreamResponseHeight([]byte{0xff, 0xff, 0xff, 0xff}); ok {
		t.Error("malformed input should return ok=false")
	}
}

func TestExtractStreamResponseHeightSkipsOtherFields(t *testing.T) {
	// Field 99 first, then field 1 = 42.
	import_ := []byte{}
	import_ = appendBytesField(import_, 99, []byte("noise"))
	import_ = appendVarintField(import_, 1, 42)
	got, ok := ExtractStreamResponseHeight(import_)
	if !ok || got != 42 {
		t.Errorf("got %d %v; want 42 true", got, ok)
	}
}

func appendVarintField(dst []byte, field, value uint64) []byte {
	dst = append(dst, byte(field<<3)|0x00) // wire type 0 = varint
	for value >= 0x80 {
		dst = append(dst, byte(value)|0x80)
		value >>= 7
	}
	return append(dst, byte(value))
}

func appendBytesField(dst []byte, field uint64, value []byte) []byte {
	dst = append(dst, byte(field<<3)|0x02) // wire type 2 = length-delimited
	// length varint
	l := uint64(len(value))
	for l >= 0x80 {
		dst = append(dst, byte(l)|0x80)
		l >>= 7
	}
	dst = append(dst, byte(l))
	return append(dst, value...)
}
