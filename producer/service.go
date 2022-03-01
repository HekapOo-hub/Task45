package producer

import (
	"encoding/json"
	"github.com/HekapOo-hub/Kafka/model"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"time"
)

type SendingService struct {
	//kafka    *kafka.Writer
	producer sarama.AsyncProducer
}

func NewSendingService(kafkaURL string) (*SendingService, error) {
	producer, err := sarama.NewAsyncProducer([]string{kafkaURL}, sarama.NewConfig())
	if err != nil {
		return nil, err
	}

	return &SendingService{
		producer: producer,
	}, nil
}

func (s *SendingService) StartProduce(done chan struct{}, topic string) {
	start := time.Now()

	for i := 0; ; i++ {
		offset := i % 2
		var command string
		if offset == 0 {
			command = "create"
		} else {
			command = "delete"
		}
		msg := model.Message{Value: i - offset, Command: command}
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			log.Warnf("start produce %v", err)
			continue
		}
		select {
		case <-done:
			return
		case s.producer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(msgBytes),
		}:
			if i%2000 == 0 {
				time.Sleep(4 * time.Second)
				log.Infof("produced %d messages with speed %.2f/s\n", i, float64(i)/time.Since(start).Seconds())
			}
		case err := <-s.producer.Errors():
			log.Warnf("Failed to send message to kafka, err: %s, msg: %s\n", err, msgBytes)
		}
	}
}

func (s *SendingService) Close() error {
	if s != nil {
		return s.producer.Close()
	}
	return nil
}
