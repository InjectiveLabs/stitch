package subscription

import (
	"encoding/json"
	"testing"
)

func TestCanonicalKeyEqualsForReorderedFields(t *testing.T) {
	// Object key order should not affect the canonical key.
	a := json.RawMessage(`{"market_ids":["B","A"],"subaccount_ids":["X"]}`)
	b := json.RawMessage(`{"subaccount_ids":["X"],"market_ids":["A","B"]}`)
	ka, err := CanonicalKey(a)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := CanonicalKey(b)
	if err != nil {
		t.Fatal(err)
	}
	if ka != kb {
		t.Errorf("expected same canonical key for reordered+sorted equivalents:\n  a=%s key=%s\n  b=%s key=%s",
			canonString(a), ka, canonString(b), kb)
	}
}

func TestCanonicalKeyDiffersForDifferentValues(t *testing.T) {
	a := json.RawMessage(`{"market_ids":["A","B"]}`)
	b := json.RawMessage(`{"market_ids":["A","C"]}`)
	ka, _ := CanonicalKey(a)
	kb, _ := CanonicalKey(b)
	if ka == kb {
		t.Errorf("filters with different values must hash differently; both got %s", ka)
	}
}

func TestCanonicalKeyStableAcrossMissing(t *testing.T) {
	// Adding a known-empty field changes the hash even if the semantics
	// are equivalent. That's intentional — we don't try to second-guess
	// what counts as "empty" for downstream.
	a := json.RawMessage(`{"market_ids":["A"]}`)
	b := json.RawMessage(`{"market_ids":["A"],"other":null}`)
	ka, _ := CanonicalKey(a)
	kb, _ := CanonicalKey(b)
	if ka == kb {
		t.Errorf("adding fields should change the canonical key (even when semantically equivalent)")
	}
}

func TestCanonicalJSONSortsStringArray(t *testing.T) {
	out, err := CanonicalJSON(json.RawMessage(`{"x":["b","a","c"]}`))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"x":["a","b","c"]}`
	if string(out) != want {
		t.Errorf("got %s; want %s", out, want)
	}
}

func TestCanonicalJSONPreservesMixedArrayOrder(t *testing.T) {
	// Numbers + strings — order has semantic meaning (positional args),
	// so we shouldn't shuffle.
	in := json.RawMessage(`["b",1,"a"]`)
	out, err := CanonicalJSON(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != `["b",1,"a"]` {
		t.Errorf("mixed array reordered: got %s", out)
	}
}

func TestCanonicalKeyHandlesNull(t *testing.T) {
	if _, err := CanonicalKey(json.RawMessage(`null`)); err != nil {
		t.Errorf("null filter should canonicalize: %v", err)
	}
}

func TestCanonicalKeyRejectsMalformed(t *testing.T) {
	if _, err := CanonicalKey(json.RawMessage(`not json`)); err == nil {
		t.Error("malformed input must return error")
	}
}
