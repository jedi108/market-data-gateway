package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jedi108/market-data-gateway/internal/apperror"
	"github.com/jedi108/market-data-gateway/internal/auth"
	"github.com/jedi108/market-data-gateway/internal/budget"
	"github.com/jedi108/market-data-gateway/internal/cache"
	"github.com/jedi108/market-data-gateway/internal/cluster"
	"github.com/jedi108/market-data-gateway/internal/config"
	"github.com/jedi108/market-data-gateway/internal/httpapi"
	"github.com/jedi108/market-data-gateway/internal/lease"
	"github.com/jedi108/market-data-gateway/internal/model"
	"github.com/jedi108/market-data-gateway/internal/observability"
	"github.com/jedi108/market-data-gateway/internal/peerapi"
	"github.com/jedi108/market-data-gateway/internal/provider"
	cryptoprovider "github.com/jedi108/market-data-gateway/internal/provider/crypto"
	fakeprovider "github.com/jedi108/market-data-gateway/internal/provider/fake"
	tbankprovider "github.com/jedi108/market-data-gateway/internal/provider/tbank"
	investapi "github.com/jedi108/market-data-gateway/internal/provider/tbank/gen/investapi"
	"github.com/jedi108/market-data-gateway/internal/ratelimit"
	"github.com/jedi108/market-data-gateway/internal/routing"
	"github.com/jedi108/market-data-gateway/internal/service"
	"github.com/jedi108/market-data-gateway/internal/storage"
)

var build = "dev"

func main() {
	configPath := flag.String("config", "", "path to gateway YAML configuration")
	cleanupOnce := flag.Bool("cleanup-once", false, "run one configured SQLite retention pass and exit")
	providerMode := flag.String("provider", "", "upstream provider: fake (local verification only); empty or tbank requires explicit authorization")
	cryptoMode := flag.String("crypto-mode", "", "crypto upstream provider: fake (local verification only); empty disables crypto venues; real mode requires explicit endpoint per venue")
	tbankEndpoint := flag.String("tbank-endpoint", "", "explicit TBank host:port for the real provider transport (required for real traffic; never implicit)")
	logLevel := flag.String("log-level", "INFO", "minimum log level: DEBUG, INFO, WARN, ERROR")
	flag.Parse()
	if !*cleanupOnce && *tbankEndpoint == "" {
		*tbankEndpoint = os.Getenv("TBANK_ENDPOINT")
	}
	if *configPath == "" {
		logFatal("-config is required")
	}
	data, err := os.ReadFile(*configPath)
	if err != nil {
		logFatal(err.Error())
	}
	cfg, err := config.Load(data)
	if err != nil {
		logFatal(err.Error())
	}
	if *cleanupOnce {
		report := runCleanupOnce(cfg)
		encoded, err := json.Marshal(report)
		if err != nil {
			logFatal("cleanup report encoding failed: " + err.Error())
		}
		fmt.Println(string(encoded))
		if report.ExitCode != 0 {
			os.Exit(report.ExitCode)
		}
		return
	}
	logger := newLogger(*logLevel)
	capacityPlan := config.EvaluateCapacityPlan(cfg)
	if capacityPlan.Err != nil {
		if cfg.CapacityPlan.Mode == "enforce" {
			logFatal("capacity plan rejected startup (fail-closed): " + capacityPlan.Err.Error())
		}
		logger.Warn("capacity plan forecast unavailable", "mode", capacityPlan.Mode, "result", "invalid")
	} else if capacityPlan.Mode == "shadow" {
		logger.Info("capacity plan forecast computed", "mode", "shadow", "result", "feasible", "required_gateways", capacityPlan.Forecast.RequiredGateways)
	}
	logger.Info("gateway starting", "node_id", cfg.Cluster.NodeID, "cluster_id", cfg.Cluster.ClusterID, "routing_version", cfg.Cluster.RoutingVersion, "routing_hash", cfg.RoutingHash, "build", build)

	upstream, err := buildProvider(*providerMode, *cryptoMode, cfg, logger, *tbankEndpoint)
	if err != nil {
		logFatal(err.Error())
	}

	// Cluster membership and node identity for the peer boundary.
	routingNodes, peerURLs := clusterTopology(cfg)
	membership, err := cluster.NewMembership(cfg.Cluster.ClusterID, cfg.Cluster.NodeID, cfg.Cluster.RoutingVersion, routingNodes, peerURLs, cfg.RoutingHash)
	if err != nil {
		logFatal("cluster membership: " + err.Error())
	}

	// Node credential: shared-secret domain, read from the environment only.
	nodeToken := os.Getenv(cfg.Auth.NodeTokenEnv)
	if nodeToken == "" {
		logFatal("node token environment " + cfg.Auth.NodeTokenEnv + " is required for the peer boundary")
	}
	nodeAuth := auth.NewNodeAuth(nodeToken)

	store, err := storage.Open(cfg.Limits.StoragePath)
	if err != nil {
		logFatal("persistent store failed to open (fail-closed): " + err.Error())
	}
	// Cluster budget guard: identical on every node from the canonical
	// config; blocks all upstream provider calls while the invariant is
	// violated or a live provider observation has shrunk the budget.
	var budgetGuard *budget.Guard
	if len(cfg.Providers.TBank.NodeHardBudgets) > 0 {
		shares := make([]budget.Share, 0, len(cfg.Cluster.Nodes))
		for _, node := range cfg.Cluster.Nodes {
			shares = append(shares, budget.Share{NodeID: node.ID, HardBudgetPerMinute: cfg.Providers.TBank.NodeHardBudgets[node.ID]})
		}
		budgetGuard, err = budget.New(cfg.Cluster.ClusterID, cfg.Providers.TBank.CredentialGroup, cfg.Providers.TBank.SafeBudgetPerMinute, cfg.Cluster.NodeID, shares)
		if err != nil {
			logFatal("cluster budget guard rejected the configuration (fail-closed): " + err.Error())
		}
	}
	scheduler := ratelimit.NewScheduler(ratelimit.Config{
		NodeBudgetPerMinute:     cfg.Providers.TBank.NodeHardBudgetPerMinute,
		PerClientQuotaPerMinute: cfg.Limits.PerClientQuotaPerMinute,
		MaxTrackedClients:       cfg.Limits.MaxTrackedClients,
		QueueCapacity:           cfg.Limits.QueueCapacity,
		WorkerCount:             cfg.Limits.WorkerCount,
		MaxRetries:              cfg.Limits.MaxRetries,
		RetryBaseDelay:          time.Duration(cfg.Limits.RetryBaseDelayMS) * time.Millisecond,
		IsRetryable:             isRetryableUpstream,
	})
	recorder := observability.NewRecorder()
	capacityResult := "valid"
	if capacityPlan.Err != nil {
		capacityResult = "invalid"
	} else if !capacityPlan.Ready {
		capacityResult = "infeasible"
	}
	recorder.RecordCapacityPlan(cfg.CapacityPlan.CredentialGroup, capacityPlan.Mode, capacityResult, capacityPlan.Forecast)
	// Task 109 client identity allowlist: client_id and priority are derived
	// from the matched credential, never from client-controlled headers.
	// Identities whose credential env is unset on this host are disabled with
	// a warning; with no resolvable identities the legacy loopback behavior
	// is kept.
	clientDirectory := auth.NewClientDirectory(cfg.Auth.Clients, os.Getenv, func(msg string, args ...any) {
		logger.Warn(msg, args...)
	})
	peerClient, err := cluster.NewClient(membership, nodeToken, time.Duration(cfg.Limits.RequestTimeoutMS)*time.Millisecond)
	if err != nil {
		logFatal("peer client: " + err.Error())
	}

	// Phase 6 automatic-failover control plane. In manual failover mode (the
	// fail-closed default and the documented rollback path) no lease gate is
	// wired: ownership is exactly the deterministic primary and the data plane
	// behaves as before. In automatic mode a per-SeriesKey fencing lease is
	// required before any provider call; witnesses (data-plane-quorum nodes)
	// confirm takeovers. A minority partition can never reach quorum, so it can
	// never create a second upstream-owner — split-brain is prevented by fencing.
	var leaseGate *lease.Gate
	var leaseCoordinator *lease.Coordinator
	var witnessStore *lease.WitnessStore
	var localStore *lease.NodeStore
	automatic := cfg.Cluster.FailoverMode == "automatic"
	if automatic {
		witnessStore = lease.NewWitnessStore(nil)
		localStore = lease.NewNodeStore(cfg.Cluster.NodeID, nil)
		members := []lease.Member{localStore}
		totalMembers := 1
		for _, w := range cfg.Cluster.Witnesses {
			members = append(members, cluster.NewRemoteWitness(peerClient, w.ID))
			totalMembers++
		}
		ownerFunc := func(key string) (string, string, error) {
			sk, err := parseSeriesKey(key)
			if err != nil {
				return "", "", err
			}
			assignment, err := membership.Owner(sk)
			if err != nil {
				return "", "", err
			}
			return assignment.Primary, assignment.Standby, nil
		}
		leaseCoordinator, err = lease.NewCoordinator(lease.Options{
			NodeID:       cfg.Cluster.NodeID,
			Version:      cfg.Cluster.RoutingVersion,
			TTLMS:        int64(cfg.Limits.LeaseTTLMS),
			Store:        localStore,
			Members:      members,
			TotalMembers: totalMembers,
			OwnerFunc:    ownerFunc,
			Automatic:    true,
		})
		if err != nil {
			logFatal("lease coordinator: " + err.Error())
		}
		leaseGate, err = lease.NewGate(lease.GateOptions{Store: localStore, Coordinator: leaseCoordinator, Automatic: true, OwnerFunc: ownerFunc})
		if err != nil {
			logFatal("lease gate: " + err.Error())
		}
		logger.Info("automatic failover enabled", "node_id", cfg.Cluster.NodeID, "witnesses", len(cfg.Cluster.Witnesses), "lease_ttl_ms", cfg.Limits.LeaseTTLMS)
	}

	ready := func() bool { return capacityPlan.Ready }
	candleService, err := service.New(cache.New(), store, upstream, scheduler, service.Config{
		RequestTimeout: time.Duration(cfg.Limits.RequestTimeoutMS) * time.Millisecond,
		MaxStaleAge:    time.Duration(cfg.Limits.CacheRetentionMS) * time.Millisecond,
		Router:         membership,
		Peer:           peerClient,
		PeerTimeout:    time.Duration(cfg.Limits.RequestTimeoutMS) * time.Millisecond,
		BudgetGuard:    budgetGuard,
		LeaseGate:      leaseGate,
		BatchFanOut:    cfg.Limits.BatchFanOut,
		// Task 109 cache-first: publication grace shifts the refresh epoch,
		// admission rejections degrade to stale + one bounded background
		// refresh, and the controlled prewarm doses cold start.
		PublicationGrace:           time.Duration(cfg.Limits.PublicationGraceMS) * time.Millisecond,
		MaxRefreshAttemptsPerEpoch: cfg.Limits.MaxRefreshAttemptsPerEpoch,
		BackgroundRefresh:          true,
		Logger:                     logger,
		Recorder:                   recorder,
	})
	if err != nil {
		logFatal(err.Error())
	}
	handlerOpts := []httpapi.HandlerOption{}
	if clientDirectory.Enabled() {
		logger.Info("client identity allowlist active", "identities", len(cfg.Auth.Clients))
		handlerOpts = append(handlerOpts, httpapi.WithIdentityResolver(clientDirectory.Resolve))
	}
	handler, err := httpapi.NewGatewayHandlerWithIdentity(build, ready, candleService, resolver(), cfg.Limits.MaxRequestLimit, cfg.Limits.MaxBatchItems, cfg.Limits.BatchFanOut, handlerOpts...)
	if err != nil {
		logFatal(err.Error())
	}
	peerHandler, err := peerapi.New(nodeAuth, membership, candleService, cluster.NodeInfo{NodeID: cfg.Cluster.NodeID, ClusterID: cfg.Cluster.ClusterID, RoutingVersion: cfg.Cluster.RoutingVersion, RoutingHash: cfg.RoutingHash, Build: build, Ready: true}, logger, witnessStore)
	if err != nil {
		logFatal("peer handler: " + err.Error())
	}

	server := &http.Server{Addr: cfg.Listeners.Client, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	peerServer := &http.Server{Addr: cfg.Listeners.Peer, Handler: peerHandler, ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 2)
	go func() { errCh <- server.ListenAndServe() }()
	go func() { errCh <- peerServer.ListenAndServe() }()
	logger.Info("client listener serving", "addr", cfg.Listeners.Client, "provider_mode", *providerMode)
	logger.Info("peer listener serving", "addr", cfg.Listeners.Peer, "node_id", cfg.Cluster.NodeID, "routing_version", cfg.Cluster.RoutingVersion)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// Task 109 controlled prewarm: after start, the gateway doses warmup-
	// priority refreshes for the local universe (per the configured pacing)
	// so cold Freqtrade starts find a warm cache instead of initiating 80
	// upstream misses at once. Every fetch is subject to scheduler admission
	// and the cluster budget guard; the job stops at shutdown.
	if cfg.Warmup.Enabled {
		go runPrewarm(ctx, candleService, cfg, logger)
	}
	// Phase 6 background lease renewer. When automatic failover is enabled, the
	// node re-confirms each held lease on a strict-majority cadence well before
	// TTL expiry; a renewal that loses quorum drops the held lease (fail-closed)
	// so the gate stops authorizing provider calls. This is the liveness half of
	// the fencing invariant: a node partitioned from the majority loses ownership
	// rather than becoming a split-brain second owner.
	if leaseCoordinator != nil {
		go runLeaseRenewer(ctx, leaseCoordinator, localStore, logger)
	}
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received; draining")
		candleService.Stop()
		drainCtx, drainCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Limits.DrainTimeoutMS)*time.Millisecond)
		defer drainCancel()
		if err := scheduler.Drain(drainCtx); err != nil {
			logger.Warn("scheduler drain incomplete", "reason", err.Error())
		}
		if err := server.Shutdown(drainCtx); err != nil {
			logger.Error("http shutdown failed", "reason", err.Error())
		}
		if err := peerServer.Shutdown(drainCtx); err != nil {
			logger.Error("peer http shutdown failed", "reason", err.Error())
		}
		if err := store.Close(); err != nil {
			logger.Error("store close failed", "reason", err.Error())
		}
		logger.Info("gateway stopped")
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			logFatal(err.Error())
		}
	}
}

type cleanupReport struct {
	Host            string                `json:"host"`
	Database        string                `json:"database"`
	CutoffUTCMS     int64                 `json:"cutoff_utc_ms"`
	RetentionMS     int64                 `json:"retention_ms"`
	CandlesBefore   int64                 `json:"candles_before"`
	CandlesAfter    int64                 `json:"candles_after"`
	CoverageBefore  int64                 `json:"coverage_before"`
	CoverageAfter   int64                 `json:"coverage_after"`
	DeletedCandles  int64                 `json:"deleted_candles"`
	DeletedCoverage int64                 `json:"deleted_coverage"`
	TrimmedCoverage int64                 `json:"trimmed_coverage"`
	RemainingSeries int64                 `json:"remaining_series"`
	Checkpoint      storage.WALCheckpoint `json:"wal_checkpoint"`
	DurationMS      int64                 `json:"duration_ms"`
	Error           string                `json:"error,omitempty"`
	ExitCode        int                   `json:"exit_code"`
}

// runCleanupOnce is deliberately independent from provider, auth, HTTP, and
// scheduler initialization. It reads only the reviewed config and the one
// configured state DB, then reports metadata without credential values.
func runCleanupOnce(cfg config.Config) cleanupReport {
	started := time.Now()
	nowUTCMS := started.UTC().UnixMilli()
	report := cleanupReport{
		Host:        cfg.Cluster.NodeID,
		Database:    cfg.Limits.StoragePath,
		CutoffUTCMS: nowUTCMS - cfg.Limits.CacheRetentionMS,
		RetentionMS: cfg.Limits.CacheRetentionMS,
		ExitCode:    1,
	}
	lock, err := acquireCleanupLock(cfg.Limits.StoragePath + ".cleanup.lock")
	if err != nil {
		report.Error = err.Error()
		report.ExitCode = 75 // EX_TEMPFAIL: another cleanup owns the lock.
		report.DurationMS = time.Since(started).Milliseconds()
		return report
	}
	defer releaseCleanupLock(lock)

	store, err := storage.Open(cfg.Limits.StoragePath)
	if err == nil {
		var stats storage.CleanupStats
		stats, err = store.CleanupBeforeWithStats(report.CutoffUTCMS)
		if closeErr := store.Close(); err == nil {
			err = closeErr
		}
		if err == nil {
			report.CandlesBefore = stats.CandlesBefore
			report.CandlesAfter = stats.CandlesAfter
			report.CoverageBefore = stats.CoverageBefore
			report.CoverageAfter = stats.CoverageAfter
			report.DeletedCandles = stats.DeletedCandles
			report.DeletedCoverage = stats.DeletedCoverage
			report.TrimmedCoverage = stats.TrimmedCoverage
			report.RemainingSeries = stats.RemainingSeries
			report.Checkpoint = stats.Checkpoint
			report.ExitCode = 0
		}
	}
	if err != nil {
		report.Error = err.Error()
	}
	report.DurationMS = time.Since(started).Milliseconds()
	return report
}

func acquireCleanupLock(path string) (*os.File, error) {
	lock, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open cleanup lock: %w", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("cleanup lock busy")
		}
		return nil, fmt.Errorf("acquire cleanup lock: %w", err)
	}
	return lock, nil
}

func releaseCleanupLock(lock *os.File) {
	if lock == nil {
		return
	}
	_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	_ = lock.Close()
}

// clusterTopology maps the validated config membership onto routing nodes
// and per-node peer endpoints.
func clusterTopology(cfg config.Config) ([]routing.Node, map[string]string) {
	nodes := make([]routing.Node, 0, len(cfg.Cluster.Nodes))
	peerURLs := make(map[string]string, len(cfg.Cluster.Nodes))
	for _, node := range cfg.Cluster.Nodes {
		nodes = append(nodes, routing.Node{ID: node.ID, Weight: node.Weight})
		peerURLs[node.ID] = node.PeerURL
	}
	return nodes, peerURLs
}

// buildProvider enforces the phase gate: the real TBank provider requires an
// explicit endpoint plus a token, and must never be constructed incidentally.
// The default remains refusal; only an explicit -tbank-endpoint (or
func buildProvider(mode string, cryptoMode string, cfg config.Config, logger *slog.Logger, endpoint string) (provider.Provider, error) {
	// Crypto venues are served only when -crypto-mode is explicitly set.
	// When empty, the resolver rejects binance/bybit before any provider call
	// (spec: ambiguous/unknown venue completes before provider call).
	mux := provider.NewMux(map[string]provider.Provider{})
	switch cryptoMode {
	case "":
		// crypto disabled; resolver will reject binance/bybit.
	case "fake":
		logger.Warn("FAKE crypto provider active: synthetic candles only, never production")
		reg, err := cryptoprovider.NewRegistry(defaultCryptoInstruments(), defaultCryptoSemantics())
		if err != nil {
			return nil, err
		}
		mux.Register("binance", cryptoprovider.New(cryptoprovider.NewFakeFetcher(), reg))
		mux.Register("bybit", cryptoprovider.New(cryptoprovider.NewFakeFetcher(), reg))
	default:
		// Real crypto provider is intentionally not wired yet: this is a
		// design/spike phase. Refusing keeps the spike isolated from the
		// production TBank pipeline.
		return nil, fmt.Errorf("crypto provider mode %q is not enabled in this build; use -crypto-mode=fake for local verification", cryptoMode)
	}

	switch mode {
	case "fake":
		logger.Warn("FAKE provider active: synthetic candles only, never production")
		mux.Register("tbank", fakeprovider.New())
		return mux, nil
	case "", "tbank":
		token := os.Getenv(cfg.Providers.TBank.TokenEnv)
		if token == "" {
			return nil, fmt.Errorf("real TBank provider requires %s and explicit authorization; use -provider=fake for local verification", cfg.Providers.TBank.TokenEnv)
		}
		if endpoint == "" {
			return nil, fmt.Errorf("real TBank provider requires an explicit -tbank-endpoint (no implicit dial); use -provider=fake for local verification")
		}
		opts := tbankprovider.TransportOptions{
			Endpoint: endpoint,
			Token:    token,
			CAFile:   os.Getenv("TBANK_CA_FILE"),
		}
		dialCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		conn, err := tbankprovider.DialTransport(dialCtx, opts)
		if err != nil {
			return nil, err
		}
		logger.Warn("real TBank provider transport active", "endpoint", endpoint, "credential_group", cfg.Providers.TBank.CredentialGroup)
		mux.Register("tbank", tbankprovider.New(investapi.NewMarketDataServiceClient(conn)))
		return mux, nil
	default:
		return nil, fmt.Errorf("unknown provider mode %q", mode)
	}
}

// defaultCryptoInstruments returns the seed crypto registry (spike scope:
// BTC and ETH on Binance/Bybit, spot trade candles).
func defaultCryptoInstruments() []cryptoprovider.Instrument {
	return []cryptoprovider.Instrument{
		{Venue: "binance", MarketType: "spot", CandleType: "trade", CanonicalSymbol: "BTC/USDT", ProviderInstrumentID: "BTCUSDT"},
		{Venue: "binance", MarketType: "spot", CandleType: "trade", CanonicalSymbol: "ETH/USDT", ProviderInstrumentID: "ETHUSDT"},
		{Venue: "bybit", MarketType: "futures", CandleType: "trade", CanonicalSymbol: "BTC/USDT", ProviderInstrumentID: "BTCUSDT"},
		{Venue: "bybit", MarketType: "futures", CandleType: "trade", CanonicalSymbol: "ETH/USDT", ProviderInstrumentID: "ETHUSDT"},
	}
}

// defaultCryptoSemantics returns per-venue candle semantics (deliberately NOT
// derived from TBank's session-aware policy; crypto is 24x7).
func defaultCryptoSemantics() map[string]cryptoprovider.CandleSemantics {
	return map[string]cryptoprovider.CandleSemantics{
		"binance": {
			AllowedTimeframes:         map[string]bool{"1m": true, "3m": true, "5m": true, "15m": true, "30m": true, "1h": true, "2h": true, "4h": true, "6h": true, "12h": true, "1d": true, "1w": true, "1M": true},
			PublicationLagMS:          2_000,
			IncompleteCandleSupported: true,
			ClosedOnly:                false,
		},
		"bybit": {
			AllowedTimeframes:         map[string]bool{"1m": true, "3m": true, "5m": true, "15m": true, "30m": true, "1h": true, "2h": true, "4h": true, "6h": true, "12h": true, "1d": true, "1w": true},
			PublicationLagMS:          3_000,
			IncompleteCandleSupported: true,
			ClosedOnly:                false,
		},
	}
}

// resolver maps the client-facing venue/symbol/timeframe triple to the
// canonical SeriesKey. Phase 1 wires the canonical TBank sandbox registry;
// Additionally accepts binance/bybit via the crypto registry.
func resolver() httpapi.Resolver {
	tbankReg, err := tbankprovider.NewRegistry(tbankInstruments())
	if err != nil {
		logFatal("instrument registry: " + err.Error())
	}
	cryptoReg, err := cryptoprovider.NewRegistry(defaultCryptoInstruments(), defaultCryptoSemantics())
	if err != nil {
		logFatal("crypto instrument registry: " + err.Error())
	}
	return func(venue, symbol, timeframe string) (model.SeriesKey, error) {
		switch venue {
		case "tbank":
			instrument, err := tbankReg.Resolve(symbol)
			if err != nil {
				return model.SeriesKey{}, err
			}
			switch timeframe {
			case "1m", "5m", "15m", "1h", "1d":
			default:
				return model.SeriesKey{}, apperror.New(apperror.CodeInvalidRequest, "unsupported timeframe", 0)
			}
			return model.SeriesKey{Venue: "tbank", MarketType: instrument.MarketType, ProviderInstrumentID: instrument.ProviderInstrumentID, Timeframe: timeframe, CandleType: "trade"}, nil
		case "binance", "bybit":
			mt := "spot"
			if venue == "bybit" {
				mt = "futures"
			}
			return cryptoReg.ResolveKey(venue, symbol, timeframe, mt, "trade")
		default:
			return model.SeriesKey{}, apperror.New(apperror.CodeInvalidRequest, "unsupported venue", 0)
		}
	}
}

// runPrewarm doses warmup-priority refreshes for the local tbank universe at
// the configured pacing. Each pass walks every configured timeframe's series
// in registry order; already-covered series are skipped by the service
// without provider calls, so the loop converges to a steady, nearly free
// patrol. It exits when ctx is cancelled at shutdown.
func runPrewarm(ctx context.Context, candles *service.Service, cfg config.Config, logger *slog.Logger) {
	interval := time.Minute / time.Duration(cfg.Warmup.SeriesPerMinute)
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	logger.Info("controlled prewarm active", "timeframes", strings.Join(cfg.Warmup.Timeframes, ","), "series_per_minute", cfg.Warmup.SeriesPerMinute, "window_limit", cfg.Warmup.WindowLimit)
	passes := 0
	for {
		for _, timeframe := range cfg.Warmup.Timeframes {
			for _, instrument := range tbankInstruments() {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				key := model.SeriesKey{Venue: "tbank", MarketType: instrument.MarketType, ProviderInstrumentID: instrument.ProviderInstrumentID, Timeframe: timeframe, CandleType: "trade"}
				if err := key.Validate(); err != nil {
					continue
				}
				// Match the client view: window ends at the live edge so a
				// prewarmed series is a complete cache hit for Freqtrade.
				now := time.Now()
				step, ok := warmupStep(timeframe)
				if !ok {
					continue
				}
				to := (now.UnixMilli()/step.Milliseconds())*step.Milliseconds() + step.Milliseconds()
				from := to - int64(cfg.Warmup.WindowLimit)*step.Milliseconds()
				if _, err := candles.WarmupOne(model.CandleRequest{Series: key, FromUTCMS: from, ToUTCMS: to, Limit: cfg.Warmup.WindowLimit, IncludeIncomplete: true}); err != nil {
					logger.Debug("prewarm fetch deferred", "timeframe", timeframe, "reason", err.Error())
				}
			}
		}
		passes++
		m := candles.Metrics()
		logger.Info("prewarm pass finished", "passes", passes, "completed", m.WarmupCompleted, "skipped_covered", m.WarmupSkipped)
	}
}

// warmupStep maps the bounded prewarm timeframe vocabulary to its step.
func warmupStep(timeframe string) (time.Duration, bool) {
	switch timeframe {
	case "1m":
		return time.Minute, true
	case "5m":
		return 5 * time.Minute, true
	case "15m":
		return 15 * time.Minute, true
	case "30m":
		return 30 * time.Minute, true
	case "1h":
		return time.Hour, true
	case "1d":
		return 24 * time.Hour, true
	default:
		return 0, false
	}
}

func newLogger(level string) *slog.Logger {
	parsed := slog.LevelInfo
	switch level {
	case "DEBUG":
		parsed = slog.LevelDebug
	case "WARN", "WARNING":
		parsed = slog.LevelWarn
	case "ERROR":
		parsed = slog.LevelError
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parsed}))
}

func isRetryableUpstream(err error) bool {
	var typed *apperror.Error
	if errors.As(err, &typed) {
		return typed.Retryable
	}
	return false
}

func logFatal(message string) {
	slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("gateway fatal", "reason", message)
	os.Exit(1)
}

// parseSeriesKey reverses model.SeriesKey.Identity() (venue|market|instrument|
// timeframe|candle). It is used by the lease control plane to map a lease key
// back to the SeriesKey the routing table needs to compute the deterministic
// primary/standby owners.
func parseSeriesKey(identity string) (model.SeriesKey, error) {
	parts := strings.Split(identity, "|")
	if len(parts) != 5 {
		return model.SeriesKey{}, fmt.Errorf("invalid series identity %q", identity)
	}
	k := model.SeriesKey{Venue: parts[0], MarketType: parts[1], ProviderInstrumentID: parts[2], Timeframe: parts[3], CandleType: parts[4]}
	if err := k.Validate(); err != nil {
		return model.SeriesKey{}, err
	}
	return k, nil
}

// runLeaseRenewer re-confirms each held lease on a strict-majority cadence well
// before TTL expiry. A renewal that loses quorum drops the held lease so the
// gate stops the provider right (fail-closed liveness). It exits when ctx is
// cancelled at shutdown.
func runLeaseRenewer(ctx context.Context, c *lease.Coordinator, store *lease.NodeStore, logger *slog.Logger) {
	ttl := time.Duration(c.TTLMillis()) * time.Millisecond
	// Renew at ~1/3 of the TTL so a sequence of transient failures cannot
	// let a lease lapse silently before the next successful confirmation.
	tick := ttl / 3
	if tick <= 0 {
		tick = 10 * time.Second
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, key := range store.HeldKeys() {
				if _, err := c.Renew(ctx, key); err != nil {
					logger.Warn("lease renewal failed; dropping provider right", "series", key, "reason", err.Error())
				}
			}
		}
	}
}
