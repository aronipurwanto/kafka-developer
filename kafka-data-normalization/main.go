package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Consumer Setup (Source)
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "my-go-stream-app",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("❌ Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	if err := consumer.SubscribeTopics([]string{"input-topic"}, nil); err != nil {
		log.Fatalf("❌ Failed to subscribe to topics: %v", err)
	}

	// Producer Setup (Sink)
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		log.Fatalf("❌ Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Goroutine untuk delivery report producer
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("❌ Delivery failed: %v\n", ev.TopicPartition.Error)
				} else {
					log.Printf("✅ Delivered to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	outputTopic := "output-topic"

	// Main processing loop
	for {
		msg, err := consumer.ReadMessage(time.Second)
		if err == nil {
			fmt.Printf("📥 Received: %s\n", string(msg.Value))

			processedValue := []byte(fmt.Sprintf("Processed: %s", string(msg.Value)))

			// Produce message
			producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &outputTopic, Partition: kafka.PartitionAny},
				Value:          processedValue,
				Key:            msg.Key, // Preserve key
			}, nil)

		} else {
			// Cek tipe error secara aman
			switch e := err.(type) {
			case kafka.Error:
				if e.Code() != kafka.ErrTimedOut {
					log.Printf("⚠️ Kafka error: %v", e)
				}
				// kalau timeout, diamkan
			case *kafka.Error: // handle pointer to kafka.Error as well
				if e.Code() != kafka.ErrTimedOut {
					log.Printf("⚠️ Kafka error: %v", e)
				}
			default:
				log.Printf("❌ Consumer error: %v", err)
			}
		}
	}
}
