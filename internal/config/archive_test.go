package config

import (
	"strings"
	"testing"
)

func TestArchiveProfileValidation(t *testing.T) {
	for _, tc := range []struct {
		name, archive string
		invalid       bool
	}{
		{"absent", "", false},
		{"valid", "archive: {evm_start_height: 347, cosmos_chain_id: fixture-chain}", false},
		{"zero", "archive: {evm_start_height: 0, cosmos_chain_id: fixture-chain}", true},
		{"negative", "archive: {evm_start_height: -1, cosmos_chain_id: fixture-chain}", true},
		{"missing-chain", "archive: {evm_start_height: 347}", true},
		{"missing-height", "archive: {cosmos_chain_id: fixture-chain}", true},
		{"whitespace-chain", "archive: {evm_start_height: 347, cosmos_chain_id: ' fixture-chain'}", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Parse([]byte(tc.archive + "\nbackends: [{name: a, coverage: {kind: archive}, endpoints: {grpc: 'localhost:9090'}}]\n"))
			if err != nil {
				t.Fatal(err)
			}
			applyDefaults(cfg)
			err = Validate(cfg)
			if tc.invalid && (err == nil || !strings.Contains(err.Error(), "archive.")) {
				t.Fatalf("invalid profile accepted: %v", err)
			}
			if !tc.invalid && err != nil {
				t.Fatal(err)
			}
			if tc.name == "absent" && cfg.Archive != nil {
				t.Fatal("archive start was inferred")
			}
		})
	}
}
