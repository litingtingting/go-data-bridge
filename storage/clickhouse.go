package storage

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap"

	"yourmodule/config"
)

type ClickHouseClient struct {
	conn   driver.Conn
	logger *zap.Logger
	cfg    config.ClickHouseConfig
}

func NewClickHouseClient(cfg config.ClickHouseConfig, logger *zap.Logger) (*ClickHouseClient, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: cfg.Hosts,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		MaxOpenConns: cfg.MaxOpenConns,
		MaxIdleConns: cfg.MaxIdleConns,
		ConnMaxLifetime: cfg.ConnMaxLifeTime,
		DialTimeout:     10 * time.Second,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})
	if err != nil {
		return nil, err
	}

	// 测试连接
	if err := conn.Ping(context.Background()); err != nil {
		return nil, err
	}

	return &ClickHouseClient{
		conn:   conn,
		logger: logger,
		cfg:    cfg,
	}, nil
}

// InsertBatch 批量插入数据，data 是 [][]interface{} 或具体结构体切片
func (c *ClickHouseClient) InsertBatch(ctx context.Context, columns []string, data [][]interface{}) error {
	if len(data) == 0 {
		return nil
	}
	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO "+c.cfg.Table)
	if err != nil {
		return err
	}
	for _, row := range data {
		if err := batch.Append(row...); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *ClickHouseClient) Close() error {
	return c.conn.Close()
}