package consumer

import (
	"context"
	"fmt"
	"github.com/HekapOo-hub/Kafka/config"
	"github.com/HekapOo-hub/Kafka/model"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type Service struct {
	cg sarama.ConsumerGroup
}

func NewConsumerService(broker string, topics []string, group string, handler GroupHandler) (*Service, error) {
	ctx := context.Background()
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V0_10_2_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	client, err := sarama.NewConsumerGroup([]string{broker}, group, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating consumer group client error %w", err)
	}

	go func() {
		for {
			err := client.Consume(ctx, topics, handler)
			if err != nil {
				log.Warnf("consume service error %v", err)
				if err == sarama.ErrClosedConsumerGroup {
					break
				} else {
					return
				}
			}
			if ctx.Err() != nil {
				log.Warnf("consume service error %v", ctx.Err())
				return
			}
			handler.Reset()
		}
	}()
	log.Infof("handler %v", handler)
	handler.WaitReady() // wait till the consumer has been set up

	return &Service{
		cg: client,
	}, nil
}

func (s *Service) Close() error {
	return s.cg.Close()
}

func StartBatchConsumer(cfg config.KafkaConfig) (*Service, error) {
	var count int

	//var start = time.Now()
	handler, err := NewBatchConsumerGroupHandler(&BatchConsumerConfig{
		MaxBufSize: 2000,
		Callback: func(messages []*SessionMessage) error {
			for i := range messages {
				if _, err := model.DecodeMessage(messages[i].Message.Value); err == nil {
					messages[i].Session.MarkMessage(messages[i].Message, "")
				}
			}
			count += len(messages)
			if count%2000 == 0 {
				//log.Infof("batch consumer consumed %d messages at speed %.2f/s", count, float64(count)/time.Since(start).Seconds())
			}
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	consumer, err := NewConsumerService(cfg.KafkaURL, []string{cfg.Topic}, cfg.GroupID, handler)
	if err != nil {
		return nil, fmt.Errorf("creating new consumer service error %w", err)
	}
	return consumer, nil
}
