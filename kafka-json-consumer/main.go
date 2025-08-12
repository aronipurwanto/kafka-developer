// consumer_json.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Product struct {
	ID    int     `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

func main() {
	// --- Kafka consumer config (aman & eksplisit)
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        "localhost:9092",
		"group.id":                 "json-consumer",
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       false, // commit manual setelah sukses
		"isolation.level":          "read_committed",
		"allow.auto.create.topics": false,
		// tuning opsional:
		// "max.poll.interval.ms":  300000,
		// "session.timeout.ms":    45000,
	})
	if err != nil {
		log.Fatalf("create consumer: %v", err)
	}
	defer c.Close()

	topic := "products-json"
	if err := c.SubscribeTopics([]string{topic}, nil); err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	log.Printf("Consuming from %q ... Ctrl+C to exit", topic)

	// --- Graceful shutdown
	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-sigch:
			log.Println("signal received, shutting down...")
			return
		default:
			// timeout kecil agar loop responsif terhadap sinyal
			msg, err := c.ReadMessage(500)
			if err != nil {
				// ignore timeout / EOF
				if ke, ok := err.(kafka.Error); ok {
					if ke.Code() == kafka.ErrTimedOut || ke.Code() == kafka.ErrPartitionEOF {
						continue
					}
				}
				log.Printf("read error: %v", err)
				continue
			}

			// --- Decode JSON dengan validator ketat
			var p Product
			dec := json.NewDecoder(bytes.NewReader(msg.Value))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&p); err != nil {
				log.Printf("json decode error at %s [%d] @%v: %v; raw=%q",
					*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, err, string(msg.Value))
				// pilih: tidak commit agar bisa di-retry; di sini kita skip commit
				continue
			}

			fmt.Printf("OK %s [%d] @%v key=%q -> %+v\n",
				*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Key), p)

			// --- Commit offset setelah sukses diproses
			if _, err := c.CommitMessage(msg); err != nil {
				log.Printf("commit error: %v", err)
			}
		}
	}
}
