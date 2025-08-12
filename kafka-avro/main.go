package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/riferrei/srclient"
)

func produceAvro(ctx context.Context, brokers, schemaURL, subject, topic string) error {
	// --- Connect Schema Registry
	sr := srclient.CreateSchemaRegistryClient(schemaURL)

	schema, err := sr.GetLatestSchema(subject)
	if err != nil {
		return fmt.Errorf("get latest schema for subject %q: %w", subject, err)
	}

	// --- Encode data ke Avro (native go -> avro binary)
	product := map[string]interface{}{
		"id":    int32(1), // pastikan tipe sesuai schema (mis. int32 untuk "int")
		"name":  "Laptop",
		"price": 15000000.0, // float64 untuk "double"
	}
	avroPayload, err := schema.Codec().BinaryFromNative(nil, product)
	if err != nil {
		return fmt.Errorf("encode avro: %w", err)
	}

	// --- Confluent wire format: magic byte (0) + schemaID (4 byte BE) + payload
	var framed bytes.Buffer
	framed.WriteByte(0x00)
	var sid [4]byte
	binary.BigEndian.PutUint32(sid[:], uint32(schema.ID()))
	framed.Write(sid[:])
	framed.Write(avroPayload)

	// --- Kafka Producer (optimized & safe defaults)
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"acks":               "all",
		"enable.idempotence": true,
		"compression.type":   "lz4", // atau "zstd"
		"linger.ms":          20,    // batching
		"batch.num.messages": 10000,
	})
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer p.Close()

	// --- Kirim pesan + tunggu delivery report
	dr := make(chan kafka.Event, 1)
	if err := p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		// Key opsional agar konsisten ke partition (mis. pakai id)
		Key:   []byte("product-1"),
		Value: framed.Bytes(),
		// Header opsional, kadang berguna untuk konsumen
		Headers: []kafka.Header{
			{Key: "content-type", Value: []byte("application/vnd.kafka.avro")},
			{Key: "schema-id", Value: []byte(fmt.Sprint(schema.ID()))},
		},
	}, dr); err != nil {
		return fmt.Errorf("produce: %w", err)
	}

	select {
	case e := <-dr:
		m, ok := e.(*kafka.Message)
		if !ok {
			return fmt.Errorf("unexpected event type: %T", e)
		}
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
		}
		// Optional log sukses
		// fmt.Printf("Delivered to %s [%d] @ %v\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	case <-ctx.Done():
		return fmt.Errorf("delivery timeout: %w", ctx.Err())
	}

	// Drain buffer (aman walau sudah pakai DR sinkron)
	p.Flush(15_000)
	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := produceAvro(ctx,
		"localhost:9092",
		"http://localhost:8081",
		"products-value", // subject Avro (value)
		"products",       // topic
	); err != nil {
		panic(err)
	}
}
