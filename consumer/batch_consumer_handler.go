package consumer

import (
	"fmt"
	"github.com/HekapOo-hub/Kafka/model"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"sync"
)

type GroupHandler interface {
	sarama.ConsumerGroupHandler
	WaitReady()
	Reset()
}

type BatchConsumerConfig struct {
	BufferCapacity int // msg capacity
	MaxBufSize     int // max message size
	Callback       func([]*SessionMessage) error
}

type SessionMessage struct {
	Session sarama.ConsumerGroupSession
	Message *sarama.ConsumerMessage
}

type batchConsumerGroupHandler struct {
	repository *PostgresRepository

	cfg *BatchConsumerConfig

	ready chan bool

	// buffer

	msgBuf []*SessionMessage

	// lock to protect buffer operation
	mu sync.RWMutex

	// callback
	cb func([]*SessionMessage) error
}

func NewBatchConsumerGroupHandler(cfg *BatchConsumerConfig) (GroupHandler, error) {
	repo, err := NewPostgresRepository()
	if err != nil {
		return nil, fmt.Errorf("new batch consumer group handler %w", err)
	}
	handler := batchConsumerGroupHandler{
		ready:      make(chan bool, 0),
		cb:         cfg.Callback,
		repository: repo,
	}

	if cfg.BufferCapacity == 0 {
		cfg.BufferCapacity = 10000
	}
	handler.msgBuf = make([]*SessionMessage, 0, cfg.BufferCapacity)
	if cfg.MaxBufSize == 0 {
		cfg.MaxBufSize = 8000
	}

	handler.cfg = cfg

	return &handler, nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *batchConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(h.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *batchConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {

	err := h.repository.tx.Commit()
	if err != nil {
		return fmt.Errorf("close tx %w", err)
	}
	return nil
}

func (h *batchConsumerGroupHandler) WaitReady() {
	<-h.ready
	return
}

func (h *batchConsumerGroupHandler) Reset() {
	err := h.repository.SendBatch()
	if err != nil {
		log.Warnf("reset handler %v", err)
	}
	err = h.repository.OpenTx()
	if err != nil {
		log.Warnf("reset handler %v", err)
	}
	h.ready = make(chan bool, 0)
	return
}

func (h *batchConsumerGroupHandler) flushBuffer() {
	if len(h.msgBuf) > 0 {
		if err := h.cb(h.msgBuf); err == nil {
			h.msgBuf = make([]*SessionMessage, 0, h.cfg.BufferCapacity)
		}
	}
}

func (h *batchConsumerGroupHandler) insertMessage(msg *SessionMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgBuf = append(h.msgBuf, msg)
	if len(h.msgBuf) >= h.cfg.MaxBufSize {
		h.flushBuffer()
	}
}

func (h *batchConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	claimMsgChan := claim.Messages()

	for {
		message, ok := <-claimMsgChan
		if ok {
			h.insertMessage(&SessionMessage{
				Message: message,
				Session: session,
			})
			msg, err := model.DecodeMessage(message.Value)
			if err != nil {
				log.Warnf("consume claim %v", err)
				continue
			}
			err = h.repository.Add(*msg)
			if err != nil {
				log.Warnf("consume claim %v", err)
			}
		} else {
			return nil
		}
	}
}
