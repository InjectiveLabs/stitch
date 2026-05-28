package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

var rootCmd = &cobra.Command{
	Use:           "stitch",
	Short:         "Height-aware multi-protocol gateway for Cosmos / EVM / Injective nodes",
	Long:          "stitch routes Cosmos and Injective traffic across many upstream nodes by height, with failover, subscription resume, and a unified protocol surface (CometBFT RPC, gRPC, REST, EVM JSON-RPC, ChainStream, /injstream-ws).",
	SilenceUsage:  true,
	SilenceErrors: true,
}

func init() {
	rootCmd.AddCommand(versionCmd())
	rootCmd.AddCommand(initCmd())
	rootCmd.AddCommand(startCmd())
	rootCmd.AddCommand(migrateCmd())
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
