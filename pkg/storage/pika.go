package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sunvim/evm_syncer/pkg/logger"
	"go.uber.org/zap"
)

// PikaConfig holds the configuration for Pika client
type PikaConfig struct {
	Address      string
	Password     string
	DB           int
	PoolSize     int
	MinIdleConns int
	MaxRetries   int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// DefaultPikaConfig returns a default Pika configuration
func DefaultPikaConfig() *PikaConfig {
	return &PikaConfig{
		Address:      "localhost:9221",
		Password:     "",
		DB:           0,
		PoolSize:     10,
		MinIdleConns: 5,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}
}

// PikaClient wraps the Redis client with additional functionality for blockchain storage
type PikaClient struct {
	client *redis.Client
	logger *zap.Logger
}

// NewPikaClient creates a new Pika client with connection pooling
func NewPikaClient(cfg *PikaConfig) (*PikaClient, error) {
	if cfg == nil {
		cfg = DefaultPikaConfig()
	}

	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Address,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		MaxRetries:   cfg.MaxRetries,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to pika: %w", err)
	}

	log := logger.Get().With(zap.String("component", "pika"))
	log.Info("pika client connected successfully",
		zap.String("address", cfg.Address),
		zap.Int("pool_size", cfg.PoolSize))

	return &PikaClient{
		client: client,
		logger: log,
	}, nil
}

// Client returns the underlying Redis client
func (p *PikaClient) Client() *redis.Client {
	return p.client
}

// Pipeline creates a new Redis pipeline for batch operations
func (p *PikaClient) Pipeline() redis.Pipeliner {
	return p.client.Pipeline()
}

// TxPipeline creates a new Redis transaction pipeline
func (p *PikaClient) TxPipeline() redis.Pipeliner {
	return p.client.TxPipeline()
}

// Set stores a key-value pair
func (p *PikaClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	if err := p.client.Set(ctx, key, value, expiration).Err(); err != nil {
		p.logger.Error("failed to set key",
			zap.String("key", key),
			zap.Error(err))
		return fmt.Errorf("set key %s: %w", key, err)
	}
	return nil
}

// Get retrieves a value by key
func (p *PikaClient) Get(ctx context.Context, key string) (string, error) {
	val, err := p.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("key not found: %s", key)
		}
		p.logger.Error("failed to get key",
			zap.String("key", key),
			zap.Error(err))
		return "", fmt.Errorf("get key %s: %w", key, err)
	}
	return val, nil
}

// GetBytes retrieves a value as bytes by key
func (p *PikaClient) GetBytes(ctx context.Context, key string) ([]byte, error) {
	val, err := p.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("key not found: %s", key)
		}
		p.logger.Error("failed to get key",
			zap.String("key", key),
			zap.Error(err))
		return nil, fmt.Errorf("get key %s: %w", key, err)
	}
	return val, nil
}

// Del deletes one or more keys
func (p *PikaClient) Del(ctx context.Context, keys ...string) error {
	if err := p.client.Del(ctx, keys...).Err(); err != nil {
		p.logger.Error("failed to delete keys",
			zap.Strings("keys", keys),
			zap.Error(err))
		return fmt.Errorf("delete keys: %w", err)
	}
	return nil
}

// Exists checks if a key exists
func (p *PikaClient) Exists(ctx context.Context, keys ...string) (int64, error) {
	count, err := p.client.Exists(ctx, keys...).Result()
	if err != nil {
		p.logger.Error("failed to check key existence",
			zap.Strings("keys", keys),
			zap.Error(err))
		return 0, fmt.Errorf("check existence: %w", err)
	}
	return count, nil
}

// HSet sets a field in a hash
func (p *PikaClient) HSet(ctx context.Context, key string, values ...interface{}) error {
	if err := p.client.HSet(ctx, key, values...).Err(); err != nil {
		p.logger.Error("failed to hset",
			zap.String("key", key),
			zap.Error(err))
		return fmt.Errorf("hset key %s: %w", key, err)
	}
	return nil
}

// HGet retrieves a field from a hash
func (p *PikaClient) HGet(ctx context.Context, key, field string) (string, error) {
	val, err := p.client.HGet(ctx, key, field).Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("field not found: %s", field)
		}
		p.logger.Error("failed to hget",
			zap.String("key", key),
			zap.String("field", field),
			zap.Error(err))
		return "", fmt.Errorf("hget key %s field %s: %w", key, field, err)
	}
	return val, nil
}

// HGetAll retrieves all fields and values from a hash
func (p *PikaClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	val, err := p.client.HGetAll(ctx, key).Result()
	if err != nil {
		p.logger.Error("failed to hgetall",
			zap.String("key", key),
			zap.Error(err))
		return nil, fmt.Errorf("hgetall key %s: %w", key, err)
	}
	return val, nil
}

// LPush pushes values to the head of a list
func (p *PikaClient) LPush(ctx context.Context, key string, values ...interface{}) error {
	if err := p.client.LPush(ctx, key, values...).Err(); err != nil {
		p.logger.Error("failed to lpush",
			zap.String("key", key),
			zap.Error(err))
		return fmt.Errorf("lpush key %s: %w", key, err)
	}
	return nil
}

// RPush pushes values to the tail of a list
func (p *PikaClient) RPush(ctx context.Context, key string, values ...interface{}) error {
	if err := p.client.RPush(ctx, key, values...).Err(); err != nil {
		p.logger.Error("failed to rpush",
			zap.String("key", key),
			zap.Error(err))
		return fmt.Errorf("rpush key %s: %w", key, err)
	}
	return nil
}

// LPop pops a value from the head of a list
func (p *PikaClient) LPop(ctx context.Context, key string) (string, error) {
	val, err := p.client.LPop(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("list is empty: %s", key)
		}
		p.logger.Error("failed to lpop",
			zap.String("key", key),
			zap.Error(err))
		return "", fmt.Errorf("lpop key %s: %w", key, err)
	}
	return val, nil
}

// RPop pops a value from the tail of a list
func (p *PikaClient) RPop(ctx context.Context, key string) (string, error) {
	val, err := p.client.RPop(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("list is empty: %s", key)
		}
		p.logger.Error("failed to rpop",
			zap.String("key", key),
			zap.Error(err))
		return "", fmt.Errorf("rpop key %s: %w", key, err)
	}
	return val, nil
}

// LLen returns the length of a list
func (p *PikaClient) LLen(ctx context.Context, key string) (int64, error) {
	length, err := p.client.LLen(ctx, key).Result()
	if err != nil {
		p.logger.Error("failed to get list length",
			zap.String("key", key),
			zap.Error(err))
		return 0, fmt.Errorf("llen key %s: %w", key, err)
	}
	return length, nil
}

// SAdd adds members to a set
func (p *PikaClient) SAdd(ctx context.Context, key string, members ...interface{}) error {
	if err := p.client.SAdd(ctx, key, members...).Err(); err != nil {
		p.logger.Error("failed to sadd",
			zap.String("key", key),
			zap.Error(err))
		return fmt.Errorf("sadd key %s: %w", key, err)
	}
	return nil
}

// SIsMember checks if a value is a member of a set
func (p *PikaClient) SIsMember(ctx context.Context, key string, member interface{}) (bool, error) {
	isMember, err := p.client.SIsMember(ctx, key, member).Result()
	if err != nil {
		p.logger.Error("failed to check set membership",
			zap.String("key", key),
			zap.Error(err))
		return false, fmt.Errorf("sismember key %s: %w", key, err)
	}
	return isMember, nil
}

// SRem removes members from a set
func (p *PikaClient) SRem(ctx context.Context, key string, members ...interface{}) error {
	if err := p.client.SRem(ctx, key, members...).Err(); err != nil {
		p.logger.Error("failed to srem",
			zap.String("key", key),
			zap.Error(err))
		return fmt.Errorf("srem key %s: %w", key, err)
	}
	return nil
}

// Incr increments an integer value
func (p *PikaClient) Incr(ctx context.Context, key string) (int64, error) {
	val, err := p.client.Incr(ctx, key).Result()
	if err != nil {
		p.logger.Error("failed to increment key",
			zap.String("key", key),
			zap.Error(err))
		return 0, fmt.Errorf("incr key %s: %w", key, err)
	}
	return val, nil
}

// Ping checks the connection to Pika
func (p *PikaClient) Ping(ctx context.Context) error {
	if err := p.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}
	return nil
}

// Close closes the Pika client connection
func (p *PikaClient) Close() error {
	p.logger.Info("closing pika client connection")
	if err := p.client.Close(); err != nil {
		p.logger.Error("failed to close pika client", zap.Error(err))
		return fmt.Errorf("close pika client: %w", err)
	}
	return nil
}

// Stats returns connection pool statistics
func (p *PikaClient) Stats() *redis.PoolStats {
	return p.client.PoolStats()
}
