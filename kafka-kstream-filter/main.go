package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// LogEvent represents the incoming log data structure
type LogEvent struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Message   string `json:"message"`
	Source    string `json:"source"`
}

func main() {
	inputTopic := "raw-logs"       // Topic untuk log mentah
	outputTopic := "critical-logs" // Topic untuk log kritis

	for {
		err := runProcessor(inputTopic, outputTopic)
		log.Printf("🔄 Restarting consumer due to fatal error: %v\n", err)
		time.Sleep(5 * time.Second) // cooldown sebelum reconnect
	}
}

func runProcessor(inputTopic, outputTopic string) error {
	// --- Kafka Consumer Setup ---
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "go-log-processor-app",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	defer consumer.Close()

	if err := consumer.SubscribeTopics([]string{inputTopic}, nil); err != nil {
		return fmt.Errorf("failed to subscribe to topics %s: %w", inputTopic, err)
	}
	log.Printf("✅ Subscribed to topic: %s\n", inputTopic)

	// --- Kafka Producer Setup ---
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer producer.Close()

	// Goroutine untuk delivery report producer
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("❌ Delivery failed for message on %v: %v\n", ev.TopicPartition, ev.TopicPartition.Error)
				}
			}
		}
	}()

	log.Println("🚀 Starting log processing loop (Filter & Map KStream principles)...")
	for {
		msg, err := consumer.ReadMessage(time.Second)
		if err == nil {
			processMessage(msg, outputTopic, producer)
		} else {
			if kErr, ok := err.(kafka.Error); ok {
				// Timeout → skip
				if kErr.Code() == kafka.ErrTimedOut {
					continue
				}
				// Fatal error → trigger reconnect
				if kErr.IsFatal() {
					return fmt.Errorf("fatal Kafka error: %v", kErr)
				}
				// Non-fatal → log warning
				log.Printf("⚠️ Kafka Consumer error: %v (IsFatal: %t)\n", kErr, kErr.IsFatal())
			} else {
				log.Printf("❌ Non-Kafka Consumer error: %v\n", err)
			}
		}
	}
}

func processMessage(msg *kafka.Message, outputTopic string, producer *kafka.Producer) {
	log.Printf("📥 Received: %s\n", string(msg.Value))

	// Deserialization (assuming JSON log event)
	var logEvent LogEvent
	if err := json.Unmarshal(msg.Value, &logEvent); err != nil {
		log.Printf("❌ Failed to deserialize log event: %v. Message: %s\n", err, string(msg.Value))
		return
	}

	// Filter only "ERROR" level
	if strings.ToUpper(logEvent.Level) == "ERROR" {
		log.Printf("🔍 Filtered: Found ERROR log from %s\n", logEvent.Source)

		// Transform the message
		logEvent.Message = fmt.Sprintf("[CRITICAL] %s", logEvent.Message)
		newKey := []byte(logEvent.Source)

		// Serialization
		processedBytes, err := json.Marshal(logEvent)
		if err != nil {
			log.Printf("❌ Failed to serialize processed log: %v\n", err)
			return
		}

		// Produce to output topic
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &outputTopic, Partition: kafka.PartitionAny},
			Value:          processedBytes,
			Key:            newKey,
			Headers:        msg.Headers,
		}, nil)
		log.Printf("📤 Produced critical log to %s: %s\n", outputTopic, string(processedBytes))
	} else {
		log.Printf("⏭️ Skipping non-ERROR log: %s\n", string(msg.Value))
	}
}
