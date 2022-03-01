package consumer

import (
	"context"
	"fmt"
	"github.com/HekapOo-hub/Kafka/config"
	"github.com/HekapOo-hub/Kafka/model"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
)

type PostgresRepository struct {
	pool  *pgx.ConnPool
	batch *pgx.Batch
	tx    *pgx.Tx
}

func NewPostgresRepository() (*PostgresRepository, error) {
	cfg, err := config.GetPostgresConfig()
	if err != nil {
		log.Warnf("postgres config error: %v", err)
		return nil, fmt.Errorf("new postgres repository func %v", err)
	}
	connConfig := pgx.ConnConfig{
		Host:     cfg.Host,
		Database: cfg.DBName,
		User:     cfg.UserName,
		Password: cfg.Password,
	}
	poolConfig := pgx.ConnPoolConfig{
		ConnConfig:     connConfig,
		MaxConnections: 3,
	}

	pool, err := pgx.NewConnPool(poolConfig)
	if err != nil {
		log.Warnf("creating postgres connection pool error %v", err)
		return nil, fmt.Errorf("creating postgres connection pool error %w", err)
	}
	return &PostgresRepository{pool: pool}, nil
}

func (r *PostgresRepository) Add(message model.Message) error {
	if r.batch == nil {
		tx, err := r.pool.BeginEx(context.Background(), nil)
		if err != nil {
			return fmt.Errorf("postgres transaction %w", err)
		}
		r.batch = tx.BeginBatch()
	}
	var query string
	switch message.Command {
	case "create":
		query = "insert into messages (value) values ($1)"
	case "delete":
		query = "delete from messages where value=$1"
	}
	r.batch.Queue(query, []interface{}{message.Value}, nil, nil)
	return nil
}

func (r *PostgresRepository) SendBatch() error {
	err := r.batch.Send(context.Background(), nil)
	if err != nil {
		if e := r.tx.Rollback(); e != nil {
			log.Warnf("send batch %v", e)
		}

		if e := r.batch.Close(); e != nil {
			return fmt.Errorf("send batch %w", err)
		}
		return fmt.Errorf("send batch %w", err)
	}

	err = r.batch.Close()
	if err != nil {
		if e := r.tx.Rollback(); e != nil {
			return fmt.Errorf("send batch")
		}
		return fmt.Errorf("send batch %w", err)
	}
	err = r.tx.Commit()
	if err != nil {
		return fmt.Errorf("close tx %w", err)
	}
	return nil
}
func (r *PostgresRepository) OpenTx() error {
	var err error
	r.tx, err = r.pool.BeginEx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("open tx %w", err)
	}
	return nil
}
