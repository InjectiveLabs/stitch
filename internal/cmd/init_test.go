package cmd

import (
	"path/filepath"
	"testing"

	"github.com/decentrio/stitch/internal/config"
)

// The starter config written by `stitch init` must always pass
// config.Load — it is the first thing a new operator runs, and Load is
// strict (unknown keys, validation). Also pins the conservative
// multicast default: the flag is opt-in, matching the example configs.
func TestInitWritesLoadableConfig(t *testing.T) {
	out := filepath.Join(t.TempDir(), "config.yaml")

	c := initCmd()
	c.SetArgs([]string{"-o", out})
	if err := c.Execute(); err != nil {
		t.Fatalf("stitch init: %v", err)
	}

	cfg, err := config.Load(out)
	if err != nil {
		t.Fatalf("starter config does not load: %v", err)
	}
	if cfg.Policies.Subscriptions.Multicast {
		t.Error("starter config must ship multicast: false (opt-in feature)")
	}
}
