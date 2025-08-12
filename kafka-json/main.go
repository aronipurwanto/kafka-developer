package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Product struct {
	ID    int     `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

func produceJSON(ctx context.Context, brokers, topic string) error {
	// Buat producer dengan config minimum yang masuk akal
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		// Optional tapi bagus:
		"acks":               "all",
		"enable.idempotence": true,
	})
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer p.Close()

	// Contoh data
	product := Product{ID: 1, Name: "Laptop", Price: 15_000_000}

	jsonBytes, err := json.Marshal(product)
	if err != nil {
		return fmt.Errorf("marshal json: %w", err)
	}

	// Siapkan channel untuk delivery report sinkron
	dr := make(chan kafka.Event, 1)

	// Kirim pesan
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		// Opsional: set key untuk partitioning konsisten
		Key:   []byte("product-" + fmt.Sprint(product.ID)),
		Value: jsonBytes,
	}, dr)
	if err != nil {
		return fmt.Errorf("produce: %w", err)
	}

	// Tunggu delivery report atau timeout via context
	select {
	case e := <-dr:
		m, ok := e.(*kafka.Message)
		if !ok {
			return fmt.Errorf("unexpected event type: %T", e)
		}
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
		}
		// Sukses
		fmt.Printf("delivered to %s [%d] @ offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)

	case <-ctx.Done():
		return fmt.Errorf("delivery timeout: %w", ctx.Err())
	}

	// Pastikan buffer drained (opsional jika pakai DR sinkron)
	p.Flush(15_000) // 15 detik

	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := produceJSON(ctx, "localhost:9092", "products"); err != nil {
		panic(err)
	}
}
