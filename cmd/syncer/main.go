package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/sunvim/evm_syncer/pkg/chain"
	"github.com/sunvim/evm_syncer/pkg/chain/bsc"
	"github.com/sunvim/evm_syncer/pkg/config"
	"github.com/sunvim/evm_syncer/pkg/logger"
	"github.com/sunvim/evm_syncer/pkg/metrics"
	"github.com/sunvim/evm_syncer/pkg/p2p"
	"github.com/sunvim/evm_syncer/pkg/storage"
	"github.com/sunvim/evm_syncer/pkg/syncer"
	"github.com/sunvim/evm_syncer/pkg/txpool"
	"github.com/sunvim/evm_syncer/pkg/worker"
	"go.uber.org/zap"
)

var (
	configPath = flag.String("config", "config/config.yaml", "Path to configuration file")
	version    = "1.0.0"
)

func main() {
	flag.Parse()

	// Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	if err := logger.Init(cfg.Logging.Level, cfg.Logging.Format); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	log := logger.Get()
	log.Info("starting evm_syncer",
		zap.String("version", version),
		zap.String("chain", cfg.Chain.Name),
		zap.Uint64("network_id", cfg.Chain.NetworkID),
	)

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize chain adapter
	chainAdapter, err := initChainAdapter(cfg)
	if err != nil {
		log.Fatal("failed to initialize chain adapter", zap.Error(err))
	}
	log.Info("chain adapter initialized", zap.String("chain", chainAdapter.ChainName()))

	// Initialize Pika storage
	pikaClient, err := storage.NewPikaClient(&storage.PikaConfig{
		Addr:         cfg.Storage.Pika.Addr,
		Password:     cfg.Storage.Pika.Password,
		DB:           cfg.Storage.Pika.DB,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		PoolSize:     cfg.Storage.Pika.MaxConnections,
	})
	if err != nil {
		log.Fatal("failed to initialize Pika client", zap.Error(err))
	}
	defer pikaClient.Close()
	log.Info("Pika client initialized", zap.String("addr", cfg.Storage.Pika.Addr))

	// Initialize worker pools
	pools := initWorkerPools(cfg, log)
	defer stopWorkerPools(pools, log)

	// Initialize P2P server
	p2pServer, err := initP2PServer(cfg, chainAdapter, log)
	if err != nil {
		log.Fatal("failed to initialize P2P server", zap.Error(err))
	}
	defer p2pServer.Stop()

	// Start P2P server
	if err := p2pServer.Start(); err != nil {
		log.Fatal("failed to start P2P server", zap.Error(err))
	}
	log.Info("P2P server started", zap.String("listen_addr", cfg.P2P.ListenAddr))

	// Initialize block storage
	blockStorage := storage.NewBlockStorage(pikaClient)
	txPoolStorage := storage.NewTxPoolStorage(pikaClient)

	// Initialize syncer
	syncConfig := &syncer.SyncConfig{
		Mode:        syncer.SyncModeFull,
		StartBlock:  cfg.Sync.StartBlock,
		BatchSize:   cfg.Sync.BatchSize,
		WorkerCount: cfg.Sync.ConcurrentDownloads,
	}
	blockSyncer, err := syncer.NewSyncer(syncConfig, chainAdapter, p2pServer.PeerManager(), blockStorage, log)
	if err != nil {
		log.Fatal("failed to initialize syncer", zap.Error(err))
	}

	// Initialize transaction pool components
	txValidator := txpool.NewValidator(cfg.Chain.NetworkID, log)
	broadcaster := txpool.NewBroadcaster(txPoolStorage, p2pServer.PeerManager(), pools["tx-broadcaster"], log)
	receiver := txpool.NewReceiver(txPoolStorage, p2pServer.PeerManager(), txValidator, log)

	// Start metrics server
	if cfg.Metrics.Enabled {
		go func() {
			if err := metrics.StartMetricsServer(cfg.Metrics.ListenAddr, log); err != nil {
				log.Error("metrics server failed", zap.Error(err))
			}
		}()
		log.Info("metrics server started", zap.String("addr", cfg.Metrics.ListenAddr))
	}

	// Start syncer
	go func() {
		if err := blockSyncer.Start(ctx); err != nil {
			log.Error("syncer failed", zap.Error(err))
		}
	}()

	// Start transaction broadcaster
	go func() {
		if err := broadcaster.Start(ctx); err != nil {
			log.Error("broadcaster failed", zap.Error(err))
		}
	}()

	// Start transaction receiver
	go func() {
		if err := receiver.Start(ctx); err != nil {
			log.Error("receiver failed", zap.Error(err))
		}
	}()

	// Progress reporting
	go reportProgress(ctx, blockSyncer, p2pServer.PeerManager(), log)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Info("shutdown signal received, gracefully stopping...")

	// Cancel context to trigger graceful shutdown
	cancel()

	// Wait for syncer to finish
	<-blockSyncer.Done()
	log.Info("syncer stopped")

	// Stop broadcaster and receiver
	broadcaster.Stop()
	receiver.Stop()

	log.Info("evm_syncer stopped successfully")
}

func initChainAdapter(cfg *config.Config) (chain.IChainAdapter, error) {
	chainConfig := &chain.ChainConfig{
		Name:        cfg.Chain.Name,
		NetworkID:   cfg.Chain.NetworkID,
		GenesisHash: common.HexToHash(cfg.Chain.GenesisHash),
		Bootnodes:   cfg.P2P.Bootnodes,
	}

	switch cfg.Chain.Name {
	case "bsc":
		return bsc.NewBSCAdapter(chainConfig)
	default:
		return nil, fmt.Errorf("unsupported chain: %s", cfg.Chain.Name)
	}
}

func initP2PServer(cfg *config.Config, chainAdapter chain.IChainAdapter, log *zap.Logger) (*p2p.Server, error) {
	p2pConfig := &p2p.Config{
		ListenAddr:      cfg.P2P.ListenAddr,
		MaxPeers:        cfg.P2P.MaxPeers,
		NetworkID:       cfg.Chain.NetworkID,
		Name:            fmt.Sprintf("evm_syncer/%s", version),
		Bootnodes:       chainAdapter.GetBootnodes(),
		MinDesiredPeers: 30,
		MaxDesiredPeers: 50,
	}

	return p2p.NewServer(p2pConfig, log)
}

func initWorkerPools(cfg *config.Config, log *zap.Logger) map[string]*worker.Pool {
	pools := make(map[string]*worker.Pool)

	// Block downloader pool
	pools["block-downloader"] = worker.NewPool(
		"block-downloader",
		cfg.WorkerPools.BlockDownloader.WorkerCount,
		cfg.WorkerPools.BlockDownloader.QueueSize,
		log,
	)
	pools["block-downloader"].Start()

	// Block validator pool
	pools["block-validator"] = worker.NewPool(
		"block-validator",
		cfg.WorkerPools.BlockValidator.WorkerCount,
		cfg.WorkerPools.BlockValidator.QueueSize,
		log,
	)
	pools["block-validator"].Start()

	// Transaction broadcaster pool
	pools["tx-broadcaster"] = worker.NewPool(
		"tx-broadcaster",
		cfg.WorkerPools.TxBroadcaster.WorkerCount,
		cfg.WorkerPools.TxBroadcaster.QueueSize,
		log,
	)
	pools["tx-broadcaster"].Start()

	// Peer messages pool
	pools["peer-messages"] = worker.NewPool(
		"peer-messages",
		cfg.WorkerPools.PeerMessages.WorkerCount,
		cfg.WorkerPools.PeerMessages.QueueSize,
		log,
	)
	pools["peer-messages"].Start()

	return pools
}

func stopWorkerPools(pools map[string]*worker.Pool, log *zap.Logger) {
	for name, pool := range pools {
		log.Info("stopping worker pool", zap.String("name", name))
		pool.Stop(30 * time.Second)
	}
}

func reportProgress(ctx context.Context, s *syncer.Syncer, pm *p2p.PeerManager, log *zap.Logger) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := s.GetStats()
			peerCount := pm.PeerCount()

			log.Info("sync progress",
				zap.Uint64("current", stats.CurrentBlock),
				zap.Uint64("target", stats.TargetBlock),
				zap.Float64("speed_bps", stats.BlocksPerSecond),
				zap.String("eta", stats.ETA.String()),
				zap.Int("peers", peerCount),
			)

			// Update Prometheus metrics
			metrics.SyncHeight.Set(float64(stats.CurrentBlock))
			metrics.PeerCount.Set(float64(peerCount))
			metrics.SyncSpeedBPS.Set(stats.BlocksPerSecond)
			if stats.TargetBlock > stats.CurrentBlock {
				metrics.SyncLag.Set(float64(stats.TargetBlock - stats.CurrentBlock))
			}
		}
	}
}
