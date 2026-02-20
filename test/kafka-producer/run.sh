# 初始化 go mod
go mod init kafka-producer

# 获取依赖
go get github.com/IBM/sarama


# 编译
go build -o producer producer.go

# 运行（发送到本地 Kafka，每秒100条）
./producer -brokers=localhost:9092 -topic=user_events -rate=100

# 运行10分钟
./producer -brokers=localhost:9092 -topic=user_events -rate=500 -duration=10m

# 压测模式（每秒5000条，用20个worker）
./producer -brokers=localhost:9092 -topic=user_events -rate=5000 -workers=20 -batch=500