package batch

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"yourmodule/config"
	"yourmodule/storage"
)

type Message struct {
	Data      []byte
	Topic     string
	Partition int32
	Offset    int64
}

type Processor struct {
	ch          chan Message         // 接收消息的通道
	storage     *storage.ClickHouseClient
	cfg         config.AppConfig
	logger      *zap.Logger
	wg          sync.WaitGroup
	stopCh      chan struct{}
	flushTicker *time.Ticker
}

func NewProcessor(storage *storage.ClickHouseClient, cfg config.AppConfig, logger *zap.Logger) *Processor {
	ch := make(chan Message, cfg.BatchSize*2) // 缓冲队列
	return &Processor{
		ch:          ch,
		storage:     storage,
		cfg:         cfg,
		logger:      logger,
		stopCh:      make(chan struct{}),
		flushTicker: time.NewTicker(cfg.FlushInterval),
	}
}

// Start 启动多个 worker 并发处理
func (p *Processor) Start(ctx context.Context) {
	p.wg.Add(p.cfg.WorkerPoolSize)
	for i := 0; i < p.cfg.WorkerPoolSize; i++ {
		go p.worker(ctx, i)
	}
	p.wg.Wait()
}

// Submit 由 Kafka consumer 调用，将消息提交到 channel
func (p *Processor) Submit(data []byte, topic string, partition int32, offset int64) {
	select {
	case p.ch <- Message{Data: data, Topic: topic, Partition: partition, Offset: offset}:
	default:
		p.logger.Warn("消息队列已满，丢弃消息", zap.String("topic", topic), zap.Int64("offset", offset))
		// 实际生产环境应考虑阻塞或增加监控
	}
}

// worker 从 channel 读取消息，批量插入
func (p *Processor) worker(ctx context.Context, id int) {
	defer p.wg.Done()
	var batchMessages []Message
	var batchData [][]interface{} // 假设每条消息是 CSV 格式，需要解析成列数据
	// 实际中你需要根据业务解析 Message.Data 为具体列
	// 这里演示使用一个简化模型：直接将 message.Data 作为字符串存入一列
	// 实际使用时请替换为具体的解析逻辑

	flush := func() {
		if len(batchData) == 0 {
			return
		}
		// 执行批量插入
		err := p.storage.InsertBatch(ctx, []string{"data"}, batchData) // 假设只有一列 'data'
		if err != nil {
			p.logger.Error("批量写入 ClickHouse 失败", zap.Error(err))
			// 失败处理：可考虑重试或记录到死信队列
		} else {
			p.logger.Info("批量写入成功", zap.Int("count", len(batchData)))
		}
		// 清空批次
		batchMessages = batchMessages[:0]
		batchData = batchData[:0]
	}

	for {
		select {
		case <-ctx.Done():
			// 上下文取消，强制刷新剩余数据
			flush()
			return
		case <-p.flushTicker.C:
			// 定时刷新
			flush()
		case msg := <-p.ch:
			// 将消息转换为插入数据（此处简单示例）
			row := []interface{}{string(msg.Data)} // 实际需根据表结构调整
			batchMessages = append(batchMessages, msg)
			batchData = append(batchData, row)
			if len(batchData) >= p.cfg.BatchSize {
				flush()
			}
		}
	}
}

// Stop 通知所有 worker 停止，并等待他们完成
func (p *Processor) Stop() {
	p.flushTicker.Stop()
	close(p.stopCh) // 可选，目前使用 ctx.Done()
}