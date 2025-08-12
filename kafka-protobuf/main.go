// producer_proto.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"

	pb "kafkaserialization/proto"
)

func produceProto(ctx context.Context, brokers, topic string, product *pb.Product) error {
	// Marshal protobuf (deterministic untuk stabilitas hashing)
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(product)
	if err != nil {
		return fmt.Errorf("marshal protobuf: %w", err)
	}

	// Producer dengan konfigurasi aman & performa oke
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"acks":               "all",
		"enable.idempotence": true,
		"compression.type":   "zstd", // atau "lz4"
		"linger.ms":          20,
		"batch.num.messages": 10000,
		// timeout opsional:
		// "message.timeout.ms": 120000,
	})
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer p.Close()

	key := []byte(fmt.Sprintf("product-%d", product.Id))
	dr := make(chan kafka.Event, 1)

	// Kirim pesan
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          data,
		Headers: []kafka.Header{
			{Key: "content-type", Value: []byte("application/x-protobuf")},
			{Key: "proto-message", Value: []byte("kafkaserialization.proto.Product")},
		},
	}, dr)
	if err != nil {
		return fmt.Errorf("produce: %w", err)
	}

	// Tunggu delivery report dengan timeout via context
	select {
	case e := <-dr:
		m, ok := e.(*kafka.Message)
		if !ok {
			return fmt.Errorf("unexpected event type: %T", e)
		}
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
		}
		// log.Printf("delivered to %s [%d] @ %v", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)

	case <-ctx.Done():
		return fmt.Errorf("delivery timeout: %w", ctx.Err())
	}

	// Drain buffer (opsional karena kita sudah menunggu DR)
	p.Flush(15_000)
	return nil
}

func main() {
	product := &pb.Product{Id: 1, Name: "Laptop", Price: 15_000_000}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := produceProto(ctx, "localhost:9092", "products-proto", product); err != nil {
		log.Fatal(err)
	}
}
