package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadExpandsEnvVars(t *testing.T) {
	t.Setenv("STITCH_TEST_RPC", "http://expanded.example.com:26657")

	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	yaml := `
listen:
  admin: { addr: "127.0.0.1:0" }
backends:
  - name: a
    coverage: { kind: archive }
    endpoints:
      rpc: ${STITCH_TEST_RPC}
`
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := cfg.Backends[0].Endpoints["rpc"]; got != "http://expanded.example.com:26657" {
		t.Errorf("env var not expanded; got %q", got)
	}
}

func TestLoadExpandsUnsetEnvVarToEmpty(t *testing.T) {
	// Just to assert the policy: missing env vars expand to empty.
	// Operators who need a fallback should set the var explicitly.
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	yaml := `
listen:
  admin: { addr: "127.0.0.1:0" }
backends:
  - name: a
    coverage: { kind: archive }
    endpoints:
      rpc: "${STITCH_TEST_DEFINITELY_UNSET_VAR}"
`
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := cfg.Backends[0].Endpoints["rpc"]; got != "" {
		t.Errorf("unset var should expand to empty; got %q", got)
	}
}
