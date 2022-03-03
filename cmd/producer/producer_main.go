package main

import (
	"github.com/HekapOo-hub/Kafka/config"
	"github.com/HekapOo-hub/Kafka/producer"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg, err := config.GetKafkaConfig()
	if err != nil {
		log.Warnf("couldn't get kafka config %v", err)
		return
	}
	service, err := producer.NewSendingService(cfg.KafkaURL)
	if err != nil {
		log.Warnf("creating service error %v", err)
		return
	}
	defer func() {
		err := service.Close()
		if err != nil {
			log.Warnf("closing service error %v", err)
		}
	}()
	var done = make(chan struct{})
	defer close(done)

	go service.StartProduce(done, cfg.Topic)

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	log.Info("received signal", <-c)
}
