package events

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

type DLQProducer struct {
	writer *kafka.Writer
}

func NewDLQProducer(brokers, topic string) *DLQProducer {
	return &DLQProducer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll, // DLQ должна быть надёжной
		},
	}
}

func (p *DLQProducer) Send(ctx context.Context, original kafka.Message, reason, detail string) error {
	env := DLQEnvelope{
		OriginalTopic:     original.Topic,
		OriginalPartition: original.Partition,
		OriginalOffset:    original.Offset,
		Reason:            reason,
		ReasonDetail:      detail,
		RawPayload:        string(original.Value),
		FirstFailedAt:     time.Now().UTC(),
	}

	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	// Ключ сохраняем от оригинального сообщения — для трассировки.
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   original.Key,
		Value: raw,
	})
}

func (p *DLQProducer) Close() error {
	return p.writer.Close()
}
