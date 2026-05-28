package cmd

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/spf13/cobra"

	"github.com/decentrio/stitch/internal/admin"
	"github.com/decentrio/stitch/internal/backend"
	"github.com/decentrio/stitch/internal/cache"
	"github.com/decentrio/stitch/internal/circuit"
	"github.com/decentrio/stitch/internal/config"
	"github.com/decentrio/stitch/internal/forwarder"
	"github.com/decentrio/stitch/internal/health"
	"github.com/decentrio/stitch/internal/log"
	"github.com/decentrio/stitch/internal/metrics"
	"github.com/decentrio/stitch/internal/pool"
	"github.com/decentrio/stitch/internal/selector"
	"github.com/decentrio/stitch/internal/server"
	"github.com/decentrio/stitch/internal/server/cmt_rpc"
	"github.com/decentrio/stitch/internal/server/chainstream"
	"github.com/decentrio/stitch/internal/server/cosmos_grpc"
	"github.com/decentrio/stitch/internal/server/cosmos_rest"
	"github.com/decentrio/stitch/internal/server/eth_rpc"
	"github.com/decentrio/stitch/internal/server/eth_ws"
	"github.com/decentrio/stitch/internal/server/inj_ws"
)

func startCmd() *cobra.Command {
	var configPath string
	var shutdownGrace time.Duration

	c := &cobra.Command{
		Use:   "start",
		Short: "Start stitch with the given config",
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := config.Load(configPath)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			if err := log.Init(cfg.Log.Level, cfg.Log.Format, nil); err != nil {
				return fmt.Errorf("init log: %w", err)
			}
			metrics.SetBuildInfo(version, commit, runtime.Version())

			holder := config.NewHolder(cfg)

			// Build runtime backend list.
			backends, err := backend.FromConfig(cfg.Backends)
			if err != nil {
				return fmt.Errorf("backends: %w", err)
			}
			reg := backend.NewRegistry(backends)
			h := health.NewRegistry()
			cmgr := circuit.NewManager(circuit.Policy{
				ErrorThreshold: cfg.Policies.Circuit.ErrorThreshold,
				MinRequests:    cfg.Policies.Circuit.MinRequests,
				OpenDuration:   cfg.Policies.Circuit.OpenDuration,
			})
			selCore := selector.NewRangeSelector(reg, h, cmgr, cfg.Policies.Health.MaxLagBlocks)
			httpPool := pool.NewHTTPPool()
			grpcPool := pool.NewGRPCPool(5 * time.Minute)
			hashIdx := cache.New(100_000)
			respCache := cache.NewResponseCache(cache.ResponseCacheOpts{
				Capacity: 50_000,
				MaxBytes: int64(cfg.Policies.Cache.L1SizeMB) * 1024 * 1024,
			})
			headFn := func() int64 { return h.MaxHead() }
			fwd := forwarder.NewHTTP(selCore, httpPool, cmgr, forwarder.Policy{
				MaxAttempts:       cfg.Policies.Failover.MaxAttempts,
				PerAttemptTimeout: cfg.Policies.Failover.PerAttemptTimeout,
				HedgeAfter:        200 * time.Millisecond,
			})
			grpcDirector := cosmos_grpc.NewDirector(selCore, cmgr, grpcPool)

			log.L().Info("stitch starting",
				"version", version,
				"commit", commit,
				"backends", len(cfg.Backends),
			)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Start health probers as goroutines (not as managed servers; they
			// have no listen socket).
			go health.NewRPCProber(reg, h, cfg.Policies.Health.ProbeInterval).Run(ctx)
			go health.NewRESTProber(reg, h, cfg.Policies.Health.ProbeInterval).Run(ctx)
			go health.NewGRPCProber(reg, grpcPool, h, cfg.Policies.Health.ProbeInterval).Run(ctx)
			go health.NewEthWSProber(reg, h).Run(ctx)
			go grpcPool.RunEvictor(ctx, time.Minute)

			mgr := server.New(shutdownGrace)

			var adm *admin.Server
			if cfg.Listen.Admin.Enabled() {
				adm = admin.New(cfg.Listen.Admin.Addr)
				mgr.Add(adm)
				adm.MarkReady()
			} else {
				log.L().Warn("admin listener disabled (listen.admin.addr is empty)")
			}

			if cfg.Listen.RPC.Enabled() {
				cmtSrv := cmt_rpc.New(cfg.Listen.RPC.Addr, fwd)
				cmtSrv.SetHashCache(hashIdx)
				if cfg.Policies.Cache.Enabled {
					cmtSrv.SetResponseCache(respCache, headFn, cfg.Policies.Cache.ConfirmationDepth)
				}
				mgr.Add(cmtSrv)
			}
			if cfg.Listen.API.Enabled() {
				mgr.Add(cosmos_rest.New(cfg.Listen.API.Addr, fwd))
			}
			if cfg.Listen.GRPC.Enabled() {
				gs, err := cosmos_grpc.New(cfg.Listen.GRPC.Addr, grpcDirector)
				if err != nil {
					return fmt.Errorf("cosmos_grpc: %w", err)
				}
				mgr.Add(gs)
			}
			if cfg.Listen.EthRPC.Enabled() {
				ethSrv := eth_rpc.New(cfg.Listen.EthRPC.Addr, fwd)
				ethSrv.SetHashCache(hashIdx)
				if cfg.Policies.Cache.Enabled {
					ethSrv.SetResponseCache(respCache, headFn, cfg.Policies.Cache.ConfirmationDepth)
				}
				ethSrv.SetDangerousAllowlist(eth_rpc.NewDangerousAllowlist(cfg.Policies.DangerousMethods.Allow))
				mgr.Add(ethSrv)
			}
			if cfg.Listen.EthWS.Enabled() {
				mgr.Add(eth_ws.New(cfg.Listen.EthWS.Addr, selCore))
			}
			if cfg.Listen.ChainStream.Enabled() {
				cs, err := chainstream.New(cfg.Listen.ChainStream.Addr, selCore, cmgr, grpcPool)
				if err != nil {
					return fmt.Errorf("chainstream: %w", err)
				}
				mgr.Add(cs)
			}
			if cfg.Listen.InjWS.Enabled() {
				mgr.Add(inj_ws.New(cfg.Listen.InjWS.Addr, selCore))
			}

			reload := func() error {
				next, err := config.Load(configPath)
				if err != nil {
					return err
				}
				holder.Swap(next)
				if newBackends, err := backend.FromConfig(next.Backends); err == nil {
					reg.Set(newBackends)
				} else {
					return err
				}
				return log.Init(next.Log.Level, next.Log.Format, nil)
			}
			mgr.OnReload(reload)

			if adm != nil {
				adm.SetDeps(admin.Deps{
					Registry:  reg,
					Health:    h,
					Circuit:   cmgr,
					HashCache: hashIdx,
					RespCache: respCache,
					OnReload:  reload,
				})
			}

			err = mgr.Run(context.Background())
			cancel()
			httpPool.CloseIdle()
			grpcPool.CloseAll()
			return err
		},
	}
	c.Flags().StringVarP(&configPath, "config", "c", "config.yaml", "path to config file")
	c.Flags().DurationVar(&shutdownGrace, "shutdown-grace", 15*time.Second, "max time to wait for graceful shutdown")
	return c
}
