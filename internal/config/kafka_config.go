package config

import (
	"fmt"
	"github.com/caarlos0/env/v6"
)

type KafkaConfig struct {
	KafkaURL string `env:"kafkaURL" envDefault:"localhost:29092"`
	Topic    string `env:"topic" envDefault:"topic1"`
	GroupID  string `env:"GroupID" envDefault:"mongo-group"`
}

func GetKafkaConfig() (*KafkaConfig, error) {
	cfg := KafkaConfig{}
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("error with parsing env variables in kafka config %w", err)
	}
	return &cfg, nil
}
