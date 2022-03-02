package main

import (
	"github.com/HekapOo-hub/Kafka/config"
	"github.com/HekapOo-hub/Kafka/consumer"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"time"
)

func main() {
	sarama.Logger = log.New()
	cfg, err := config.GetKafkaConfig()
	if err != nil {
		log.Warnf("couldn't get kafka config %v", err)
		return
	}
	service, err := consumer.StartBatchConsumer(*cfg)
	if err != nil {
		if err != nil {
			log.Warnf("start batch consumer %v", err)
			return
		}
	}
	time.Sleep(50000 * time.Second)
	err = service.Close()
	if err != nil {
		log.Warnf("closing consumer error %v", err)
	}
}
