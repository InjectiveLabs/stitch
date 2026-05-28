package cosmos_rest

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestExtractHeightFromHeader(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/cosmos/auth/v1beta1/accounts/foo", nil)
	r.Header.Set("x-cosmos-block-height", "42")
	h, ok := extractHeight(r)
	if !ok || h != 42 {
		t.Fatalf("got %d %v", h, ok)
	}
}

func TestExtractHeightFromPath(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/cosmos/base/tendermint/v1beta1/blocks/1234", nil)
	h, ok := extractHeight(r)
	if !ok || h != 1234 {
		t.Fatalf("got %d %v", h, ok)
	}
}

func TestExtractHeightFromQueryHeight(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/cosmos/distribution/v1beta1/validators/X/slashes?starting_height=1&ending_height=2&height=99", nil)
	h, ok := extractHeight(r)
	if !ok || h != 99 {
		t.Fatalf("got %d %v", h, ok)
	}
}

func TestExtractHeightFromTxQuery(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/cosmos/tx/v1beta1/txs?query=tx.height%3D5", nil)
	h, ok := extractHeight(r)
	if !ok || h != 5 {
		t.Fatalf("got %d %v", h, ok)
	}
}

func TestExtractHeightAbsent(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/cosmos/auth/v1beta1/params", nil)
	if _, ok := extractHeight(r); ok {
		t.Fatal("expected absent")
	}
}
