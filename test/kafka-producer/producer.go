package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// UserEvent 用户行为事件结构体
type UserEvent struct {
	UserHashID   string    `json:"user_hash_id"`   // 用户哈希ID（匿名化）
	Source       string    `json:"source"`         // 来源（如首页、详情页、推荐流）
	ButtonID     string    `json:"button_id"`      // 点击的按钮ID
	EventType    string    `json:"event_type"`     // 事件类型（click、view、scroll）
	DurationMs   int       `json:"duration_ms"`    // 停留时长（毫秒）
	PageURL      string    `json:"page_url"`       // 当前页面URL
	Referrer     string    `json:"referrer"`       // 来源页面
	DeviceType   string    `json:"device_type"`    // 设备类型（mobile、desktop、tablet）
	AppVersion   string    `json:"app_version"`    // 应用版本
	Timestamp    time.Time `json:"timestamp"`      // 事件时间
	EventTimeUnix int64    `json:"event_time_unix"` // Unix时间戳（毫秒）
}

// 预定义数据池（模拟真实分布）
var (
	// 来源页面
	sources = []string{"homepage", "search", "product_detail", "category", "recommend", "user_profile"}
	
	// 按钮ID（不同功能）
	buttonIDs = []string{
		"add_to_cart", "buy_now", "add_to_wishlist", "share", 
		"like", "comment", "follow", "enter_detail",
		"scroll_more", "back_to_top", "refresh",
	}
	
	// 事件类型（权重不同）
	eventTypes = []string{"click", "view", "scroll", "stay"}
	
	// 设备类型
	deviceTypes = []string{"mobile", "desktop", "tablet", "mobile", "mobile"} // 移动端占60%
	
	// 应用版本（模拟版本分布）
	appVersions = []string{"2.1.0", "2.1.1", "2.2.0", "2.2.1", "2.3.0"}
	
	// 用户哈希ID前缀（生成随机用户）
	userPrefixes = []string{"u_", "user_", "usr_", "uid_"}
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// generateUserEvent 生成一条用户行为事件
func generateUserEvent() UserEvent {
	userHashID := generateUserHash()
	eventType := randomChoice(eventTypes)
	source := randomChoice(sources)
	
	// 根据事件类型生成不同的按钮ID
	var buttonID string
	if eventType == "click" {
		buttonID = randomChoice(buttonIDs)
	} else if eventType == "view" {
		buttonID = "page_view"
	} else {
		buttonID = "none"
	}
	
	// 停留时长：点击类通常较短，浏览类可能较长
	var durationMs int
	switch eventType {
	case "click":
		durationMs = rand.Intn(1000) + 100 // 100-1100ms
	case "view":
		durationMs = rand.Intn(10000) + 2000 // 2-12秒
	case "scroll":
		durationMs = rand.Intn(5000) + 500 // 0.5-5.5秒
	default:
		durationMs = rand.Intn(3000) // 0-3秒
	}
	
	now := time.Now()
	
	return UserEvent{
		UserHashID:   userHashID,
		Source:       source,
		ButtonID:     buttonID,
		EventType:    eventType,
		DurationMs:   durationMs,
		PageURL:      generatePageURL(source),
		Referrer:     generateReferrer(),
		DeviceType:   randomChoice(deviceTypes),
		AppVersion:   randomChoice(appVersions),
		Timestamp:    now,
		EventTimeUnix: now.UnixMilli(),
	}
}

// generateUserHash 生成随机用户哈希ID（模拟匿名ID）
func generateUserHash() string {
	prefix := randomChoice(userPrefixes)
	// 生成16进制字符串，模拟哈希值
	hash := fmt.Sprintf("%x", rand.Int63())
	if len(hash) > 8 {
		hash = hash[:8]
	}
	return prefix + hash
}

// generatePageURL 根据来源生成页面URL
func generatePageURL(source string) string {
	base := "https://app.example.com"
	switch source {
	case "homepage":
		return base + "/"
	case "search":
		return base + "/search?q=" + randomString(3)
	case "product_detail":
		return base + "/product/" + strconv.Itoa(rand.Intn(10000)+1000)
	case "category":
		categories := []string{"electronics", "clothing", "books", "home"}
		return base + "/category/" + randomChoice(categories)
	case "recommend":
		return base + "/recommend"
	default:
		return base + "/other"
	}
}

// generateReferrer 生成来源页面
func generateReferrer() string {
	if rand.Float64() < 0.3 { // 30% 直接访问
		return ""
	}
	sites := []string{
		"https://google.com",
		"https://facebook.com",
		"https://twitter.com",
		"https://tiktok.com",
		"https://bing.com",
	}
	return randomChoice(sites)
}

// randomChoice 从切片中随机选择一个元素
func randomChoice(slice []string) string {
	return slice[rand.Intn(len(slice))]
}

// randomString 生成随机字符串（用于搜索词）
func randomString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyz")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func main() {
	// 命令行参数
	var (
		brokers    = flag.String("brokers", "localhost:9092", "Kafka brokers 逗号分隔")
		topic      = flag.String("topic", "user_events", "Kafka topic")
		rate       = flag.Int("rate", 100, "每秒发送消息数")
		duration   = flag.Duration("duration", 0, "运行时长（0表示一直运行）")
		workers    = flag.Int("workers", 10, "并发工作协程数")
		batchSize  = flag.Int("batch", 100, "每批次发送消息数")
	)
	flag.Parse()

	// 解析 brokers
	brokerList := strings.Split(*brokers, ",")
	
	// 配置 producer
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal       // 等待本地确认
	config.Producer.Compression = sarama.CompressionSnappy   // 压缩
	config.Producer.Flush.Frequency = 500 * time.Millisecond // 刷新频率
	config.Producer.Flush.Bytes = 1024 * 1024                // 1MB 触发刷新
	config.Producer.Flush.Messages = *batchSize               // 消息数触发刷新
	config.Producer.Return.Successes = true                    // 需要成功回调
	config.Producer.Return.Errors = true

	// 创建 producer
	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		log.Fatalf("创建 producer 失败: %v", err)
	}
	defer producer.Close()

	// 处理返回结果
	go func() {
		for {
			select {
			case success := <-producer.Successes():
				// 可以记录成功发送的偏移量，这里简单忽略
				_ = success
			case err := <-producer.Errors():
				log.Printf("发送失败: %v", err)
			}
		}
	}()

	// 计算发送间隔
	interval := time.Second / time.Duration(*rate)
	if interval < time.Microsecond {
		interval = time.Microsecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// 上下文控制运行时长
	ctx, cancel := context.WithCancel(context.Background())
	if *duration > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), *duration)
	}
	defer cancel()

	// 优雅退出
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("开始发送数据到 Kafka %s, topic: %s, 速率: %d msg/s", *brokers, *topic, *rate)
	
	// 统计变量
	var sentCount int64
	startTime := time.Now()
	
	// 使用多个 worker 并发生成数据
	msgChan := make(chan *sarama.ProducerMessage, 1000)
	for i := 0; i < *workers; i++ {
		go func(workerID int) {
			for msg := range msgChan {
				producer.Input() <- msg
			}
		}(i)
	}
	
	// 主循环生成消息
	go func() {
		for {
			select {
			case <-ticker.C:
				// 生成一条消息
				event := generateUserEvent()
				value, _ := json.Marshal(event)
				
				msg := &sarama.ProducerMessage{
					Topic: *topic,
					Key:   sarama.StringEncoder(event.UserHashID), // 用 user_hash_id 作为 key，保证同一用户顺序
					Value: sarama.ByteEncoder(value),
					Headers: []sarama.RecordHeader{
						{Key: []byte("source"), Value: []byte("go-producer")},
						{Key: []byte("version"), Value: []byte("1.0")},
					},
					Timestamp: time.Now(),
				}
				
				msgChan <- msg
				sentCount++
				
			case <-ctx.Done():
				close(msgChan)
				return
			}
		}
	}()

	// 等待退出信号
	select {
	case <-sigChan:
		log.Println("收到退出信号，正在停止...")
	case <-ctx.Done():
		log.Println("运行时长结束，正在停止...")
	}
	
	// 等待所有消息发送完成
	time.Sleep(2 * time.Second)
	
	elapsed := time.Since(startTime)
	actualRate := float64(sentCount) / elapsed.Seconds()
	log.Printf("总计发送 %d 条消息, 耗时 %.2f 秒, 平均速率 %.2f msg/s", sentCount, elapsed.Seconds(), actualRate)
}