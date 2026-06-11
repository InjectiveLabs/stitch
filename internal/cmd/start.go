package cmd

import (
	"context"
	"fmt"
	"runtime"
	"sync"
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
	"github.com/decentrio/stitch/internal/server/chainstream"
	"github.com/decentrio/stitch/internal/server/cmt_rpc"
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
			hashIdx := cache.New(cfg.Policies.Cache.HashIndexEntries)
			respCache := cache.NewResponseCache(cache.ResponseCacheOpts{
				Capacity: cfg.Policies.Cache.ResponseEntries,
				MaxBytes: int64(cfg.Policies.Cache.L1SizeMB) * 1024 * 1024,
			})
			headFn := func() int64 { return h.MaxHead() }
			fwd := forwarder.NewHTTP(selCore, httpPool, cmgr, forwarder.Policy{
				MaxAttempts:       cfg.Policies.Failover.MaxAttempts,
				PerAttemptTimeout: cfg.Policies.Failover.PerAttemptTimeout,
				HedgeAfter:        cfg.Policies.Hedging.HedgeAfter,
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
			// have no listen socket). The eth_ws prober keeps a handle so the
			// reload path can kick an immediate reconcile after pruning.
			ethWSProber := health.NewEthWSProber(reg, h)
			go health.NewRPCProber(reg, h, cfg.Policies.Health.ProbeInterval).Run(ctx)
			go health.NewRESTProber(reg, h, cfg.Policies.Health.ProbeInterval).Run(ctx)
			go health.NewGRPCProber(reg, grpcPool, h, cfg.Policies.Health.ProbeInterval).Run(ctx)
			go ethWSProber.Run(ctx)
			go health.NewBoundedVerifier(reg, h).Run(ctx)
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
					cmtSrv.SetResponseCache(respCache, headFn, cfg.Policies.Cache.ConfirmationDepth, cfg.Policies.Cache.TTL)
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
					ethSrv.SetResponseCache(respCache, headFn, cfg.Policies.Cache.ConfirmationDepth, cfg.Policies.Cache.TTL)
				}
				ethSrv.SetHedging(cfg.Policies.Hedging.Enabled, cfg.Policies.Hedging.Methods)
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

			// Non-reloadable sections keep running with the BOOT config no
			// matter how many reloads happen, so the "ignored until restart"
			// warning must always diff against the config the process started
			// with — not the last-loaded one (which would warn only once and
			// false-warn when a file reverts to boot values).
			bootCfg := cfg
			// reload's multi-step body (load, swap, prune, kick) must not
			// interleave: SIGHUP and POST /admin/reload can fire concurrently.
			var reloadMu sync.Mutex
			reload := func() error {
				reloadMu.Lock()
				defer reloadMu.Unlock()
				next, err := config.Load(configPath)
				if err != nil {
					return err
				}
				newBackends, err := backend.FromConfig(next.Backends)
				if err != nil {
					return err
				}
				holder.Swap(next) // registry of last-loaded config
				reg.Set(newBackends)
				// Drop health snapshots and circuit breakers (and their
				// per-backend metric children) for backends the new config no
				// longer declares. Counter families (RequestsTotal etc.) are
				// deliberately left alone — they're cumulative history, and
				// rates over them decay on their own.
				active := make(map[string]struct{}, len(newBackends))
				for _, b := range newBackends {
					active[b.Name] = struct{}{}
				}
				h.Prune(active)
				cmgr.Prune(active)
				// Reconcile the eth_ws head trackers NOW so a removed
				// backend's newHeads stream stops resurrecting pruned
				// snapshots instead of surviving until the next 30s tick.
				ethWSProber.Kick()
				// Only backends and log apply live; tell the operator what
				// they changed that won't take effect until restart.
				if ignored := config.DiffNonReloadable(bootCfg, next); len(ignored) > 0 {
					log.L().Warn("reload: changes ignored until restart", "sections", ignored)
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
