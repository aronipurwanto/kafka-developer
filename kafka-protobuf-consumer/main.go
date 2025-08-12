// consumer_proto.go
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"

	pb "kafkaserialization/proto"
)

func main() {
	// Kafka consumer config yang lebih aman & eksplisit
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        "localhost:9092",
		"group.id":                 "proto-consumer",
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       false,          // commit manual setelah sukses
		"isolation.level":          "read_committed",
		"allow.auto.create.topics": false,
	})
	if err != nil {
		log.Fatalf("create consumer: %v", err)
	}
	defer c.Close()

	topic := "products-proto"
	if err := c.SubscribeTopics([]string{topic}, nil); err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	log.Printf("Consuming from %q ... Ctrl+C to exit", topic)

	// Graceful shutdown
	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-sigch:
			log.Println("signal received, shutting down...")
			return
		default:
			// timeout kecil supaya loop responsif terhadap sinyal
			msg, err := c.ReadMessage(500)
			if err != nil {
				// Abaikan timeout/EOF; log error lain
				if ke, ok := err.(kafka.Error); ok {
					if ke.Code() == kafka.ErrTimedOut || ke.Code() == kafka.ErrPartitionEOF {
						continue
					}
				}
				log.Printf("read error: %v", err)
				continue
			}

			// Decode Protobuf
			var p pb.Product
			if err := proto.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(msg.Value, &p); err != nil {
			log.Printf("protobuf decode error at %s [%d] @%v: %v",
				*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, err)
			// Pilihan: jangan commit agar bisa di-retry
			continue
		}

			fmt.Printf("OK %s [%d] @%v key=%q -> %+v\n",
				*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Key), p)

			// Commit offset setelah sukses diproses
			if _, err := c.CommitMessage(msg); err != nil {
				log.Printf("commit error: %v", err)
			}
		}
	}
}
