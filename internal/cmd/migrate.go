package cmd

import (
	"errors"

	"github.com/spf13/cobra"
)

func migrateCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "migrate-config",
		Short: "Translate gateway/subnode config to stitch format (not yet implemented)",
	}
	c.AddCommand(&cobra.Command{
		Use:   "gateway",
		Short: "Translate a decentrio/gateway config.yaml to stitch format",
		RunE: func(_ *cobra.Command, _ []string) error {
			return errors.New("migrate-config gateway: not yet implemented (planned for phase 9)")
		},
	})
	c.AddCommand(&cobra.Command{
		Use:   "subnode",
		Short: "Translate a subnode config.yaml to stitch format",
		RunE: func(_ *cobra.Command, _ []string) error {
			return errors.New("migrate-config subnode: not yet implemented (planned for phase 9)")
		},
	})
	return c
}
