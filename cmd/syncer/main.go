package main

import (
	"context"
	"flag"
	"fmt"
	"math/big"
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
		Address:      cfg.Storage.Pika.Addr,
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
	pools := initWorkerPools(ctx, cfg, log)
	defer stopWorkerPools(ctx, pools, log)

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

	// Initialize syncer
	syncConfig := &syncer.SyncConfig{
		Mode:        syncer.SyncModeFull,
		StartBlock:  cfg.Sync.StartBlock,
		BatchSize:   cfg.Sync.BatchSize,
		WorkerCount: cfg.Sync.ConcurrentDownloads,
	}
	blockSyncer, err := syncer.NewSyncer(syncConfig, chainAdapter, p2pServer.PeerManager(), pikaClient, log)
	if err != nil {
		log.Fatal("failed to initialize syncer", zap.Error(err))
	}

	// Initialize transaction pool components
	broadcaster := txpool.NewBroadcaster(pikaClient, p2pServer.PeerManager(), log)
	receiver := txpool.NewReceiver(pikaClient, big.NewInt(int64(cfg.Chain.NetworkID)), log)

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
		if err := broadcaster.Start(); err != nil {
			log.Error("broadcaster failed", zap.Error(err))
		}
	}()

	// Start transaction receiver
	go func() {
		if err := receiver.Start(); err != nil {
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
	// Convert bootnodes to string URLs
	bootnodes := chainAdapter.GetBootnodes()
	bootstrapURLs := make([]string, len(bootnodes))
	for i, node := range bootnodes {
		bootstrapURLs[i] = node.URLv4()
	}
	
	p2pConfig := &p2p.ServerConfig{
		ListenAddr:    cfg.P2P.ListenAddr,
		MaxPeers:      cfg.P2P.MaxPeers,
		NetworkID:     cfg.Chain.NetworkID,
		BootstrapURLs: bootstrapURLs,
	}

	return p2p.NewServer(p2pConfig, log)
}

func initWorkerPools(ctx context.Context, cfg *config.Config, log *zap.Logger) map[string]*worker.Pool {
	pools := make(map[string]*worker.Pool)

	// Block downloader pool
	blockDownloaderPool, _ := worker.NewPool(worker.PoolConfig{
		Name:        "block-downloader",
		WorkerCount: cfg.WorkerPools.BlockDownloader.WorkerCount,
		QueueSize:   cfg.WorkerPools.BlockDownloader.QueueSize,
		Logger:      log,
	})
	pools["block-downloader"] = blockDownloaderPool
	pools["block-downloader"].Start(ctx)

	// Block validator pool
	blockValidatorPool, _ := worker.NewPool(worker.PoolConfig{
		Name:        "block-validator",
		WorkerCount: cfg.WorkerPools.BlockValidator.WorkerCount,
		QueueSize:   cfg.WorkerPools.BlockValidator.QueueSize,
		Logger:      log,
	})
	pools["block-validator"] = blockValidatorPool
	pools["block-validator"].Start(ctx)

	// Transaction broadcaster pool
	txBroadcasterPool, _ := worker.NewPool(worker.PoolConfig{
		Name:        "tx-broadcaster",
		WorkerCount: cfg.WorkerPools.TxBroadcaster.WorkerCount,
		QueueSize:   cfg.WorkerPools.TxBroadcaster.QueueSize,
		Logger:      log,
	})
	pools["tx-broadcaster"] = txBroadcasterPool
	pools["tx-broadcaster"].Start(ctx)

	// Peer messages pool
	peerMessagesPool, _ := worker.NewPool(worker.PoolConfig{
		Name:        "peer-messages",
		WorkerCount: cfg.WorkerPools.PeerMessages.WorkerCount,
		QueueSize:   cfg.WorkerPools.PeerMessages.QueueSize,
		Logger:      log,
	})
	pools["peer-messages"] = peerMessagesPool
	pools["peer-messages"].Start(ctx)

	return pools
}

func stopWorkerPools(ctx context.Context, pools map[string]*worker.Pool, log *zap.Logger) {
	shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	
	for name, pool := range pools {
		log.Info("stopping worker pool", zap.String("name", name))
		pool.Shutdown(shutdownCtx)
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

			currentBlock := stats["current_block"].(uint64)
			targetBlock := stats["target_block"].(uint64)
			
			// Calculate sync speed from metrics if available
			var blocksPerSecond float64
			if metricsMap, ok := stats["metrics"].(map[string]interface{}); ok {
				if bps, ok := metricsMap["blocks_per_second"].(float64); ok {
					blocksPerSecond = bps
				}
			}

			log.Info("sync progress",
				zap.Uint64("current", currentBlock),
				zap.Uint64("target", targetBlock),
				zap.Float64("speed_bps", blocksPerSecond),
				zap.Int("peers", peerCount),
			)

			// Update Prometheus metrics
			metrics.SyncHeight.Set(float64(currentBlock))
			metrics.PeerCount.Set(float64(peerCount))
			metrics.SyncSpeedBPS.Set(blocksPerSecond)
			if targetBlock > currentBlock {
				metrics.SyncLag.Set(float64(targetBlock - currentBlock))
			}
		}
	}
}
