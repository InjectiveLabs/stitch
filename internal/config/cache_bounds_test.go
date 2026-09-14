package config

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestCacheByteBudgetValidation(t *testing.T) {
	type testCase struct {
		name    string
		mib     int64
		wantErr bool
	}
	cases := []testCase{
		{"default", 0, false},
		{"positive", 1, false},
		{"negative", -1, true},
	}
	if strconv.IntSize == 64 {
		cases = append(cases,
			testCase{"largest safe conversion", (1<<63 - 1) / (1024 * 1024), false},
			testCase{"byte conversion overflow", (1<<63-1)/(1024*1024) + 1, true},
		)
	}
	for _, enabled := range []bool{false, true} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("enabled=%t/%s", enabled, tc.name), func(t *testing.T) {
				cfg, err := Parse([]byte(fmt.Sprintf(`
backends:
  - name: archive
    coverage: {kind: archive}
    endpoints: {rpc: http://localhost:26657}
policies:
  cache:
    enabled: %t
    l1_size_mb: %d
`, enabled, tc.mib)))
				if err != nil {
					t.Fatal(err)
				}
				applyDefaults(cfg)
				err = Validate(cfg)
				if tc.wantErr {
					if err == nil || !strings.Contains(err.Error(), "l1_size_mb") {
						t.Fatalf("expected l1_size_mb error, got %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if tc.mib == 0 && cfg.Policies.Cache.L1SizeMB != 1024 {
					t.Fatalf("default byte budget = %d MiB, want 1024", cfg.Policies.Cache.L1SizeMB)
				}
			})
		}
	}
}
