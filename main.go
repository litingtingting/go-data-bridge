package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-data-bridge/config"
	"go-data-bridge/consumer"
	"go-data-bridge/storage"
	"go-data-bridge/batch"
	"go.uber.org/zap"
)

func main() {
	// 加载配置
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// 初始化日志
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// 初始化 ClickHouse 连接
	chClient, err := storage.NewClickHouseClient(cfg.ClickHouse, logger)
	if err != nil {
		logger.Fatal("连接 ClickHouse 失败", zap.Error(err))
	}
	defer chClient.Close()

	// 初始化批量处理器
	processor := batch.NewProcessor(chClient, cfg.App, logger)

	// 启动处理器（消费 channel 中的消息）
	ctx, cancel := context.WithCancel(context.Background())
	go processor.Start(ctx)

	// 初始化 Kafka 消费者
	consumerGroup, err := consumer.NewConsumerGroup(cfg.Kafka, processor, logger)
	if err != nil {
		logger.Fatal("创建 Kafka 消费者失败", zap.Error(err))
	}
	defer consumerGroup.Close()

	// 启动 Kafka 消费循环
	go func() {
		for {
			if err := consumerGroup.Consume(ctx); err != nil {
				logger.Error("Kafka 消费错误", zap.Error(err))
				time.Sleep(5 * time.Second) // 重试前等待
			}
			// 检查 ctx 是否结束
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// 优雅退出
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	logger.Info("收到退出信号，正在停止...")

	cancel() // 通知所有 goroutine 停止
	processor.Stop() // 等待批量处理完成
	logger.Info("程序已退出")
}