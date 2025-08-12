package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// UserLoginEvent merepresentasikan event login user
type UserLoginEvent struct {
	UserID    string    `json:"user_id"`
	LoginTime time.Time `json:"login_time"`
	IPAddress string    `json:"ip_address"`
}

// UserLoginCount merepresentasikan output KTable-like
type UserLoginCount struct {
	UserID string `json:"user_id"`
	Count  int    `json:"count"`
}

// continuousAggregator menyimpan state login count per user
type continuousAggregator struct {
	UserCounts  map[string]int // State in-memory: UserID -> jumlah login
	OutputTopic string         // Nama topic output (compacted)
	Producer    *kafka.Producer
}

// NewContinuousAggregator membuat instance aggregator baru
func NewContinuousAggregator(outputTopic string, p *kafka.Producer) *continuousAggregator {
	return &continuousAggregator{
		UserCounts:  make(map[string]int),
		OutputTopic: outputTopic,
		Producer:    p,
	}
}

// ProcessEvent memproses event login dan update state + publish ke Kafka
func (ca *continuousAggregator) ProcessEvent(event UserLoginEvent) {
	// Increment counter untuk user tersebut
	ca.UserCounts[event.UserID]++
	currentCount := ca.UserCounts[event.UserID]

	log.Printf("📥 Event processed: UserID=%s. Current Login Count=%d\n", event.UserID, currentCount)

	// Buat record untuk dikirim ke compacted topic
	userCountMetric := UserLoginCount{
		UserID: event.UserID,
		Count:  currentCount,
	}

	// Serialize ke JSON
	jsonBytes, err := json.Marshal(userCountMetric)
	if err != nil {
		log.Printf("❌ Failed to marshal user count metric to JSON: %v\n", err)
		return
	}

	// Kirim ke Kafka dengan key = UserID (penting untuk compacted topic)
	producerKey := []byte(event.UserID)
	ca.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &ca.OutputTopic, Partition: kafka.PartitionAny},
		Key:            producerKey,
		Value:          jsonBytes,
	}, nil)

	log.Printf("✅ Published updated count for UserID=%s to %s: %s\n", event.UserID, ca.OutputTopic, string(jsonBytes))
}

// RecoverState memulihkan state dari compacted topic
func (ca *continuousAggregator) RecoverState(brokers string) {
	log.Printf("♻️ Recovering state from compacted topic '%s'...\n", ca.OutputTopic)

	// Buat consumer khusus untuk recovery (baca dari awal)
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          fmt.Sprintf("state-recovery-%d", time.Now().UnixNano()),
		"auto.offset.reset": "earliest",
		"isolation.level":   "read_committed", // Hanya baca data commit
	})
	if err != nil {
		log.Fatalf("❌ Failed to create recovery consumer: %v", err)
	}
	defer consumer.Close()

	if err := consumer.SubscribeTopics([]string{ca.OutputTopic}, nil); err != nil {
		log.Fatalf("❌ Failed to subscribe to output topic for recovery: %v", err)
	}

	for {
		msg, err := consumer.ReadMessage(2 * time.Second)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
				break // Timeout artinya recovery selesai
			}
			log.Printf("⚠️ Error reading during recovery: %v", err)
			break
		}

		// Deserialize UserLoginCount
		var countRecord UserLoginCount
		if err := json.Unmarshal(msg.Value, &countRecord); err != nil {
			log.Printf("❌ Failed to unmarshal during recovery: %v", err)
			continue
		}

		// Update state in-memory
		ca.UserCounts[countRecord.UserID] = countRecord.Count
	}

	log.Printf("✅ State recovery complete. Loaded %d users.\n", len(ca.UserCounts))
}

func main() {
	brokers := "localhost:9092"
	inputTopic := "user-logins"        // Input topic event login
	outputTopic := "user-login-counts" // Output compacted topic

	// --- Kafka Producer ---
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		log.Fatalf("❌ Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Goroutine untuk handle delivery reports
	go func() {
		for e := range producer.Events() {
			if ev, ok := e.(*kafka.Message); ok && ev.TopicPartition.Error != nil {
				log.Printf("❌ Delivery failed: %v\n", ev.TopicPartition.Error)
			}
		}
	}()

	// --- Aggregator ---
	aggregator := NewContinuousAggregator(outputTopic, producer)

	// Pulihkan state sebelum memproses event baru
	aggregator.RecoverState(brokers)

	// --- Kafka Consumer ---
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          "go-ktable-aggregator-app",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("❌ Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	if err := consumer.SubscribeTopics([]string{inputTopic}, nil); err != nil {
		log.Fatalf("❌ Failed to subscribe to input topic: %v", err)
	}

	log.Printf("🚀 Starting aggregator. Consuming from '%s', publishing to compacted '%s'.\n", inputTopic, outputTopic)

	// --- Main loop ---
	for {
		msg, err := consumer.ReadMessage(time.Second)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
				continue
			}
			log.Printf("⚠️ Consumer error: %v", err)
			continue
		}

		// Deserialize event login
		var loginEvent UserLoginEvent
		if err := json.Unmarshal(msg.Value, &loginEvent); err != nil {
			log.Printf("❌ Failed to deserialize: %v", err)
			continue
		}

		// Proses event
		aggregator.ProcessEvent(loginEvent)
	}
}
