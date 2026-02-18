package config

import (
	"time"

	"github.com/spf13/viper"
)

type KafkaConfig struct {
	Brokers   []string
	Topic     string
	GroupID   string `mapstructure:"group_id"`
	SASL      SASLConfig
	TLS       TLSConfig
	Version   string
}

type SASLConfig struct {
	Enable   bool
	User     string
	Password string
}

type TLSConfig struct {
	Enable bool
}

type ClickHouseConfig struct {
	Hosts          []string
	Database       string
	Table          string
	Username       string
	Password       string
	MaxOpenConns   int           `mapstructure:"max_open_conns"`
	MaxIdleConns   int           `mapstructure:"max_idle_conns"`
	ConnMaxLifeTime time.Duration `mapstructure:"conn_max_life_time"`
}

type AppConfig struct {
	BatchSize       int           `mapstructure:"batch_size"`
	FlushInterval   time.Duration `mapstructure:"flush_interval"`
	WorkerPoolSize  int           `mapstructure:"worker_pool_size"`
	LogLevel        string        `mapstructure:"log_level"`
}

type Config struct {
	Kafka      KafkaConfig
	ClickHouse ClickHouseConfig
	App        AppConfig
}

func Load() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AutomaticEnv() // 支持环境变量覆盖

	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}