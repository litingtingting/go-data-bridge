package consumer

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	"go-data-bridge/config"
	"go-data-bridge/batch"
)

type ConsumerGroupHandler struct {
	processor *batch.Processor
	logger    *zap.Logger
}

func (h *ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *ConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		// 将消息提交给批量处理器
		h.processor.Submit(msg.Value, msg.Topic, msg.Partition, msg.Offset)
		// 手动提交偏移量（批量写入成功后才真正提交，这里先标记）
		sess.MarkMessage(msg, "")
	}
	return nil
}

type ConsumerGroup struct {
	group   sarama.ConsumerGroup
	topic   string
	handler *ConsumerGroupHandler
	logger  *zap.Logger
}

func NewConsumerGroup(cfg config.KafkaConfig, processor *batch.Processor, logger *zap.Logger) (*ConsumerGroup, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version, _ = sarama.ParseKafkaVersion(cfg.Version)
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = false // 手动提交
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	if cfg.SASL.Enable {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = cfg.SASL.User
		saramaConfig.Net.SASL.Password = cfg.SASL.Password
	}
	if cfg.TLS.Enable {
		saramaConfig.Net.TLS.Enable = true
	}

	group, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, saramaConfig)
	if err != nil {
		return nil, err
	}

	handler := &ConsumerGroupHandler{
		processor: processor,
		logger:    logger,
	}

	return &ConsumerGroup{
		group:   group,
		topic:   cfg.Topic,
		handler: handler,
		logger:  logger,
	}, nil
}

func (cg *ConsumerGroup) Consume(ctx context.Context) error {
	return cg.group.Consume(ctx, []string{cg.topic}, cg.handler)
}

func (cg *ConsumerGroup) Close() error {
	return cg.group.Close()
}