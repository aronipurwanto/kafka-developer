package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/signal"
	"strings"
	"syscall"
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

	// === Pilih sesuai compose ===
	// KRaft (3 broker):
	bootstrapServers := "localhost:9092,localhost:9093,localhost:9094"
	// ZooKeeper (5 broker):
	// bootstrapServers := "localhost:9092,localhost:9093,localhost:9094,localhost:9095,localhost:9096"

	// Buat topik jika belum ada
	if err := ensureTopics(bootstrapServers, []topicSpec{
		{Name: inputTopic, NumPartitions: 3, ReplicationFactor: 3},
		{Name: outputTopic, NumPartitions: 3, ReplicationFactor: 3},
	}, 15*time.Second); err != nil {
		log.Printf("⚠️ ensureTopics warning: %v\n", err)
	}

	// Graceful shutdown signals
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	for {
		err := runProcessor(ctx, bootstrapServers, inputTopic, outputTopic)
		if err == nil {
			return // selesai normal
		}
		log.Printf("🔄 Restarting consumer due to fatal error: %v\n", err)

		// Jika konteks diminta berhenti, keluar.
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func runProcessor(ctx context.Context, bootstrap, inputTopic, outputTopic string) error {
	// --- Kafka Consumer Setup (manual commit) ---
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        bootstrap,
		"group.id":                 "go-log-processor-app",
		"enable.auto.commit":       false, // manual commit setelah sukses
		"auto.offset.reset":        "earliest",
		"max.poll.interval.ms":     300000, // 5 menit
		"session.timeout.ms":       45000,  // stabil di docker
		"fetch.min.bytes":          1,      // tuning sesuai kebutuhan
		"fetch.wait.max.ms":        50,
		"go.events.channel.enable": false, // kita pakai ReadMessage
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	defer consumer.Close()

	if err := consumer.SubscribeTopics([]string{inputTopic}, nil); err != nil {
		return fmt.Errorf("failed to subscribe to topics %s: %w", inputTopic, err)
	}
	log.Printf("✅ Subscribed to topic: %s\n", inputTopic)

	// --- Kafka Producer Setup (safe & low-latency) ---
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  bootstrap,
		"enable.idempotence": true, // exactly-once per connection
		"acks":               "all",
		"linger.ms":          5, // batching ringan
		"batch.num.messages": 1000,
		"retries":            2147483647, // biar robust
		"message.timeout.ms": 120000,     // 2 menit
	})
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer func() {
		// pastikan semua message terkirim sebelum exit
		producer.Flush(10_000)
		producer.Close()
	}()

	// Goroutine untuk delivery report producer (error only)
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("❌ Delivery failed for %v: %v\n", ev.TopicPartition, ev.TopicPartition.Error)
				}
			}
		}
	}()

	log.Println("🚀 Starting log processing loop (Filter & Map KStream principles)...")

	// Loop utama consume -> process -> produce -> commit
	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 Context canceled, stopping processor...")
			return nil
		default:
		}

		msg, err := consumer.ReadMessage(1 * time.Second)
		if err != nil {
			// Timeout → lanjut
			if kErr, ok := err.(kafka.Error); ok {
				if kErr.Code() == kafka.ErrTimedOut {
					continue
				}
				if kErr.IsFatal() {
					return fmt.Errorf("fatal Kafka error: %v", kErr)
				}
				log.Printf("⚠️ Kafka Consumer error: %v (IsFatal: %t)\n", kErr, kErr.IsFatal())
				continue
			}
			// Non-kafka error (network blip dsb.)
			continue
		}

		// Proses & produce
		committable := processMessage(msg, outputTopic, producer)
		// Commit offset hanya jika berhasil di-process (committable = true)
		if committable {
			_, commitErr := consumer.CommitMessage(msg)
			if commitErr != nil {
				log.Printf("⚠️ Commit failed: %v\n", commitErr)
			}
		}
	}
}

func processMessage(msg *kafka.Message, outputTopic string, producer *kafka.Producer) bool {
	log.Printf("📥 Received: %s\n", string(msg.Value))

	// Deserialization (assuming JSON log event)
	var logEvent LogEvent
	if err := json.Unmarshal(msg.Value, &logEvent); err != nil {
		log.Printf("❌ Failed to deserialize log event: %v. Message: %s\n", err, string(msg.Value))
		return false
	}

	// Filter only "ERROR" level
	if strings.ToUpper(logEvent.Level) == "ERROR" {
		log.Printf("🔍 Filtered: Found ERROR log from %s\n", logEvent.Source)

		// Transform
		logEvent.Message = fmt.Sprintf("[CRITICAL] %s", logEvent.Message)
		newKey := []byte(logEvent.Source)

		// Serialization
		processedBytes, err := json.Marshal(logEvent)
		if err != nil {
			log.Printf("❌ Failed to serialize processed log: %v\n", err)
			return false
		}

		// Produce to output topic (key = Source → ordering per source)
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &outputTopic, Partition: kafka.PartitionAny},
			Value:          processedBytes,
			Key:            newKey,
			Headers:        msg.Headers,
		}, nil)
		if err != nil {
			log.Printf("❌ Produce error: %v\n", err)
			return false
		}
		log.Printf("📤 Produced critical log to %s: %s\n", outputTopic, string(processedBytes))
		return true
	}

	log.Printf("⏭️ Skipping non-ERROR log: %s\n", string(msg.Value))
	return true // tetap commit offset untuk non-ERROR
}

// -------- Admin (ensure topics) --------

type topicSpec struct {
	Name              string
	NumPartitions     int
	ReplicationFactor int
}

func ensureTopics(bootstrap string, specs []topicSpec, timeout time.Duration) error {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrap})
	if err != nil {
		return err
	}
	defer admin.Close()

	var cms []kafka.TopicSpecification
	for _, s := range specs {
		cms = append(cms, kafka.TopicSpecification{
			Topic:             s.Name,
			NumPartitions:     s.NumPartitions,
			ReplicationFactor: s.ReplicationFactor,
			// Configs: map[string]string{"cleanup.policy": "delete"},
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	results, err := admin.CreateTopics(ctx, cms, kafka.SetAdminOperationTimeout(timeout))
	if err != nil {
		return err
	}
	for _, r := range results {
		if r.Error.Code() != kafka.ErrNoError && r.Error.Code() != kafka.ErrTopicAlreadyExists {
			log.Printf("❌ CreateTopics %s failed: %v\n", r.Topic, r.Error)
		} else if r.Error.Code() == kafka.ErrTopicAlreadyExists {
			log.Printf("ℹ️ Topic %s already exists\n", r.Topic)
		} else {
			log.Printf("✅ Topic %s created\n", r.Topic)
		}
	}
	return nil
}
