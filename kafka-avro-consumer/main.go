package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/riferrei/srclient"
)

func main() {
	// --- Schema Registry client
	schemaClient := srclient.CreateSchemaRegistryClient("http://localhost:8081")

	// --- Kafka Consumer
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        "localhost:9092",
		"group.id":                 "avro-consumer",
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       false, // commit manual setelah sukses
		"isolation.level":          "read_committed",
		"allow.auto.create.topics": false,
	})
	if err != nil {
		log.Fatalf("create consumer: %v", err)
	}
	defer c.Close()

	topic := "products-avro"
	if err := c.SubscribeTopics([]string{topic}, nil); err != nil {
		log.Fatalf("subscribe topics: %v", err)
	}

	// --- Graceful shutdown
	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("Consuming from %q ... Ctrl+C to exit", topic)

run:
	for {
		select {
		case <-sigch:
			log.Println("Signal received, exiting...")
			break run
		default:
			// Tunggu pesan max 500ms agar responsive terhadap sinyal
			msg, err := c.ReadMessage(500)
			if err != nil {
				// Timeout biasa akan mengembalikan err (kafka.Error) dengan code _PARTITION_EOF atau _TIMED_OUT
				ke, ok := err.(kafka.Error)
				if ok && (ke.Code() == kafka.ErrTimedOut || ke.Code() == kafka.ErrPartitionEOF) {
					continue
				}
				// Error lain: log dan lanjut
				if err != nil {
					log.Printf("read message error: %v", err)
				}
				continue
			}

			// Decode Avro (Confluent framing)
			native, err := decodeConfluentAvro(schemaClient, msg.Value)
			if err != nil {
				log.Printf("decode avro error at %s [%d] @%v: %v",
					*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, err)
				// Bisa pilih: skip commit supaya di-retry, atau commit agar lanjut. Di sini kita skip commit.
				continue
			}

			fmt.Printf("Message %s [%d] @%v key=%q value=%v\n",
				*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Key), native)

			// Commit offset setelah sukses diproses
			if _, err := c.CommitMessage(msg); err != nil {
				log.Printf("commit error: %v", err)
			}
		}
	}

	// Optional: wakeup & tunggu sebentar sebelum close
	c.Close()
	time.Sleep(200 * time.Millisecond)
}

func decodeConfluentAvro(sr *srclient.SchemaRegistryClient, data []byte) (interface{}, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("payload too short (%d bytes)", len(data))
	}
	if data[0] != 0x00 {
		return nil, fmt.Errorf("unknown magic byte: 0x%02x", data[0])
	}
	schemaID := binary.BigEndian.Uint32(data[1:5])

	schema, err := sr.GetSchema(int(schemaID))
	if err != nil {
		return nil, fmt.Errorf("get schema id %d: %w", schemaID, err)
	}

	// Sisanya adalah Avro payload
	native, _, err := schema.Codec().NativeFromBinary(data[5:])
	if err != nil {
		return nil, fmt.Errorf("avro decode: %w", err)
	}
	return native, nil
}
