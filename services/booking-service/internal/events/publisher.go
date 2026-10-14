package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// Message is the envelope every consumer receives. EventID is stable across
// redeliveries so consumers can de-duplicate (at-least-once delivery).
type Message struct {
	EventID     string          `json:"event_id"`
	Type        string          `json:"type"`
	AggregateID string          `json:"aggregate_id"`
	OccurredAt  time.Time       `json:"occurred_at"`
	Data        json.RawMessage `json:"data"`
}

type Publisher interface {
	Publish(ctx context.Context, msgs []Message) error
	Close() error
}

// KafkaPublisher writes synchronously with acks=all: Publish returning nil
// means the batch is durably replicated, so the outbox may mark it sent.
type KafkaPublisher struct{ w *kafka.Writer }

func NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher {
	return &KafkaPublisher{w: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // key = reservation id -> per-reservation ordering
		RequiredAcks: kafka.RequireAll,
		BatchSize:    500,
		BatchTimeout: 5 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
		// The relay retries the whole batch itself; keep the client fast-failing.
		MaxAttempts:            2,
		AllowAutoTopicCreation: true,
	}}
}

func (k *KafkaPublisher) Publish(ctx context.Context, msgs []Message) error {
	out := make([]kafka.Message, len(msgs))
	for i, m := range msgs {
		body, err := json.Marshal(m)
		if err != nil {
			return err
		}
		out[i] = kafka.Message{
			Key:   []byte(m.AggregateID),
			Value: body,
			Headers: []kafka.Header{
				{Key: "event_id", Value: []byte(m.EventID)},
				{Key: "event_type", Value: []byte(m.Type)},
			},
		}
	}
	return k.w.WriteMessages(ctx, out...)
}

func (k *KafkaPublisher) Close() error { return k.w.Close() }

// LogPublisher is used when no broker is configured (local runs, tests).
type LogPublisher struct{}

func (LogPublisher) Publish(_ context.Context, msgs []Message) error {
	for _, m := range msgs {
		slog.Debug("event", "type", m.Type, "aggregate", m.AggregateID, "event_id", m.EventID)
	}
	return nil
}

func (LogPublisher) Close() error { return nil }
