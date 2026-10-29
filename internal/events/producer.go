package events

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// OrderEvent — то, что улетает в Kafka. Минимальный набор полей.
type OrderEvent struct {
	Type    string      `json:"type"` // "OrderCreated", "OrderUpdated", "OrderDeleted"
	OrderID int64       `json:"order_id"`
	Payload interface{} `json:"payload,omitempty"` // сам заказ (для Created/Updated) или nil
	Time    time.Time   `json:"time"`
}

type OrderProducer struct {
	writer *kafka.Writer
	topic  string
}

func NewOrderProducer(brokers, topic string) *OrderProducer {
	w := &kafka.Writer{
		Addr:     kafka.TCP(brokers),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		// Ждём подтверждения от всех in-sync реплик.
		// Для учебного проекта можно оставить RequireAll — надёжнее.
		RequiredAcks: kafka.RequireAll,
		// Асинхронность выключаем — хотим видеть ошибки сразу.
		Async: false,
	}
	return &OrderProducer{writer: w, topic: topic}
}

// Send отправляет событие. Ошибку возвращаем, но в сервисе её можно только залогировать.
func (p *OrderProducer) Send(ctx context.Context, evt OrderEvent) error {
	evt.Time = time.Now().UTC()
	raw, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	// Ключ = OrderID, чтобы все события одного заказа попадали в одну партицию
	// и обрабатывались в правильном порядке.
	msg := kafka.Message{
		Key:   []byte(formatOrderKey(evt.OrderID)),
		Value: raw,
	}

	// Таймаут на отправку — чтобы запрос не висел вечно, если Kafka недоступна.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		log.Printf("kafka produce error: %v", err)
		return err
	}
	return nil
}

func (p *OrderProducer) Close() error {
	return p.writer.Close()
}

func formatOrderKey(id int64) string {
	// Просто строка — kafka-go сам хэширует её в партицию.
	return "order-" + itoa(id)
}

func itoa(id int64) string {
	// Простейшая реализация без strconv, чтобы не плодить импорты в примере.
	// В реальном коде используйте strconv.FormatInt.
	if id == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for id > 0 {
		i--
		buf[i] = byte('0' + id%10)
		id /= 10
	}
	return string(buf[i:])
}
