package main

import (
	"fmt"
	"github.com/HekapOo-hub/Task45/internal/config"
	"github.com/HekapOo-hub/Task45/internal/service"

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
	service, err := service.StartBatchConsumer(*cfg)
	if err != nil {
		if err != nil {
			log.Warnf("start batch consumer %v", err)
			return
		}
	}

	defer func() {
		err = service.Close()
		if err != nil {
			log.Warnf("closing consumer error %v", err)
		}
	}()
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	fmt.Println("received signal", <-c)
}
