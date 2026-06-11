package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

const starterConfig = `# stitch — starter configuration
# See docs/architecture.md for the full reference.

listen:
  rpc:         { addr: "0.0.0.0:5001" }
  grpc:        { addr: "0.0.0.0:5002" }
  api:         { addr: "0.0.0.0:5003" }
  eth_rpc:     { addr: "0.0.0.0:5005" }
  eth_ws:      { addr: "0.0.0.0:5006" }
  chainstream: { addr: "0.0.0.0:5007" }
  inj_ws:      { addr: "0.0.0.0:5008" }
  admin:       { addr: "127.0.0.1:9091" }   # localhost-only by default

policies:
  failover:
    max_attempts: 3
    per_attempt_timeout: 5s
  hedging:
    enabled: false
  circuit:
    error_threshold: 0.5
    min_requests: 20
    open_duration: 30s
  cache:
    enabled: false
    confirmation_depth: 100
  health:
    probe_interval: 5s
    max_lag_blocks: 50          # gap allowance vs. observed head (see README)
  subscriptions:
    # opt-in: N clients with the same filter share one upstream; non-subscribe
    # frames are rejected in this mode — see README
    multicast: false
    slow_consumer: drop
    replay_timeout: 30s

log:
  level: info       # debug | info | warn | error
  format: json      # json | text

backends:
  - name: local
    weight: 100
    coverage: { kind: archive }
    tags: [local]
    endpoints:
      rpc:         http://127.0.0.1:26657
      grpc:        127.0.0.1:9900
      api:         http://127.0.0.1:10337
      eth_rpc:     http://127.0.0.1:8545
      eth_ws:      ws://127.0.0.1:8546
      chainstream: 127.0.0.1:9999
`

func initCmd() *cobra.Command {
	var out string
	var force bool

	c := &cobra.Command{
		Use:   "init",
		Short: "Write a starter config file",
		RunE: func(_ *cobra.Command, _ []string) error {
			if _, err := os.Stat(out); err == nil && !force {
				return fmt.Errorf("%s already exists (use --force to overwrite)", out)
			} else if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if err := os.WriteFile(out, []byte(starterConfig), 0o644); err != nil {
				return fmt.Errorf("write %s: %w", out, err)
			}
			fmt.Printf("wrote %s\n", out)
			return nil
		},
	}
	c.Flags().StringVarP(&out, "output", "o", "config.yaml", "output path")
	c.Flags().BoolVar(&force, "force", false, "overwrite if exists")
	return c
}
