package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all configuration for the syncer
type Config struct {
	Chain       ChainConfig       `mapstructure:"chain"`
	P2P         P2PConfig         `mapstructure:"p2p"`
	Storage     StorageConfig     `mapstructure:"storage"`
	Sync        SyncConfig        `mapstructure:"sync"`
	WorkerPools WorkerPoolsConfig `mapstructure:"worker_pools"`
	Metrics     MetricsConfig     `mapstructure:"metrics"`
	Logging     LoggingConfig     `mapstructure:"logging"`
}

// ChainConfig contains chain-specific configuration
type ChainConfig struct {
	Name        string `mapstructure:"name"`
	NetworkID   uint64 `mapstructure:"network_id"`
	GenesisHash string `mapstructure:"genesis_hash"`
}

// P2PConfig contains P2P network configuration
type P2PConfig struct {
	ListenAddr string   `mapstructure:"listen_addr"`
	MaxPeers   int      `mapstructure:"max_peers"`
	Bootnodes  []string `mapstructure:"bootnodes"`
}

// StorageConfig contains storage configuration
type StorageConfig struct {
	Pika PikaConfig `mapstructure:"pika"`
}

// PikaConfig contains Pika (Redis) configuration
type PikaConfig struct {
	Addr           string `mapstructure:"addr"`
	Password       string `mapstructure:"password"`
	DB             int    `mapstructure:"db"`
	MaxConnections int    `mapstructure:"max_connections"`
	PipelineSize   int    `mapstructure:"pipeline_size"`
}

// SyncConfig contains synchronization configuration
type SyncConfig struct {
	Mode                string `mapstructure:"mode"`
	StartBlock          uint64 `mapstructure:"start_block"`
	ConcurrentDownloads int    `mapstructure:"concurrent_downloads"`
	BatchSize           int    `mapstructure:"batch_size"`
}

// WorkerPoolsConfig contains worker pool configurations
type WorkerPoolsConfig struct {
	BlockDownloader WorkerPoolConfig `mapstructure:"block_downloader"`
	BlockValidator  WorkerPoolConfig `mapstructure:"block_validator"`
	TxBroadcaster   WorkerPoolConfig `mapstructure:"tx_broadcaster"`
	PeerMessages    WorkerPoolConfig `mapstructure:"peer_messages"`
}

// WorkerPoolConfig contains configuration for a single worker pool
type WorkerPoolConfig struct {
	WorkerCount int `mapstructure:"worker_count"`
	QueueSize   int `mapstructure:"queue_size"`
}

// MetricsConfig contains metrics configuration
type MetricsConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	ListenAddr string `mapstructure:"listen_addr"`
}

// LoggingConfig contains logging configuration
type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

// Load loads configuration from file and environment variables
func Load(configPath string) (*Config, error) {
	v := viper.New()

	// Set defaults
	setDefaults(v)

	// Load from config file
	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath("./config")
		v.AddConfigPath(".")
	}

	// Read config file
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	// Override with environment variables
	v.SetEnvPrefix("EVM_SYNCER")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Unmarshal config
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// setDefaults sets default configuration values
func setDefaults(v *viper.Viper) {
	v.SetDefault("chain.name", "bsc")
	v.SetDefault("chain.network_id", 56)
	v.SetDefault("p2p.listen_addr", "0.0.0.0:30303")
	v.SetDefault("p2p.max_peers", 50)
	v.SetDefault("storage.pika.addr", "127.0.0.1:9221")
	v.SetDefault("storage.pika.db", 0)
	v.SetDefault("storage.pika.max_connections", 100)
	v.SetDefault("storage.pika.pipeline_size", 1000)
	v.SetDefault("sync.mode", "full")
	v.SetDefault("sync.start_block", 0)
	v.SetDefault("sync.concurrent_downloads", 50)
	v.SetDefault("sync.batch_size", 100)
	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.listen_addr", "0.0.0.0:9090")
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Chain.Name == "" {
		return fmt.Errorf("chain.name is required")
	}
	if c.Chain.NetworkID == 0 {
		return fmt.Errorf("chain.network_id is required")
	}
	if c.Storage.Pika.Addr == "" {
		return fmt.Errorf("storage.pika.addr is required")
	}
	if c.P2P.MaxPeers <= 0 {
		return fmt.Errorf("p2p.max_peers must be positive")
	}
	if c.Sync.BatchSize <= 0 {
		return fmt.Errorf("sync.batch_size must be positive")
	}
	return nil
}
