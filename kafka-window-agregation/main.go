package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// SalesEvent represents the incoming sales data structure
type SalesEvent struct {
	OrderID   string    `json:"order_id"`
	ProductID string    `json:"product_id"`
	Category  string    `json:"category"`
	Amount    float64   `json:"amount"`
	EventTime time.Time `json:"event_time"`
}

// WindowedSalesMetric represents the aggregated output for a specific window
type WindowedSalesMetric struct {
	WindowStartTime time.Time `json:"window_start_time"`
	WindowEndTime   time.Time `json:"window_end_time"`
	Category        string    `json:"category"`
	TotalAmount     float64   `json:"total_amount"`
	Count           int       `json:"count"`
}

type windowAggregator struct {
	CurrentWindowSums      map[string]float64
	CurrentWindowCounts    map[string]int
	LastProcessedEventTime time.Time
	WindowDuration         time.Duration
	OutputTopic            string
	Producer               *kafka.Producer
}

func NewWindowAggregator(windowDuration time.Duration, outputTopic string, p *kafka.Producer) *windowAggregator {
	return &windowAggregator{
		CurrentWindowSums:      make(map[string]float64),
		CurrentWindowCounts:    make(map[string]int),
		WindowDuration:         windowDuration,
		OutputTopic:            outputTopic,
		Producer:               p,
		LastProcessedEventTime: time.Unix(0, 0),
	}
}

func (wa *windowAggregator) ProcessEvent(event SalesEvent) {
	if event.EventTime.IsZero() {
		log.Printf("⚠️ Event skipped: invalid EventTime for OrderID=%s", event.OrderID)
		return
	}

	windowStartUnixNano := event.EventTime.UnixNano() / int64(wa.WindowDuration) * int64(wa.WindowDuration)
	windowStartTime := time.Unix(0, windowStartUnixNano)
	windowEndTime := windowStartTime.Add(wa.WindowDuration)

	if event.EventTime.After(wa.LastProcessedEventTime) {
		wa.LastProcessedEventTime = event.EventTime
	}

	windowStateKeyStr := fmt.Sprintf("%s_%d", event.Category, windowStartTime.UnixNano())

	wa.CurrentWindowSums[windowStateKeyStr] += event.Amount
	wa.CurrentWindowCounts[windowStateKeyStr]++

	log.Printf("📥 Event processed: Category=%s, Amount=%.2f, Window=%s-%s",
		event.Category, event.Amount,
		windowStartTime.Format(time.TimeOnly), windowEndTime.Format(time.TimeOnly))
}

func (wa *windowAggregator) PunctureWindows() {
	keysToProcess := []string{}
	for key := range wa.CurrentWindowSums {
		keysToProcess = append(keysToProcess, key)
	}

	for _, key := range keysToProcess {
		parts := strings.Split(key, "_")
		if len(parts) != 2 {
			continue
		}
		category := parts[0]
		windowStartUnixNano, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}
		windowStartTime := time.Unix(0, windowStartUnixNano)
		windowEndTime := windowStartTime.Add(wa.WindowDuration)

		if windowEndTime.Before(wa.LastProcessedEventTime) || windowEndTime.Equal(wa.LastProcessedEventTime) {
			totalAmount := wa.CurrentWindowSums[key]
			count := wa.CurrentWindowCounts[key]

			metric := WindowedSalesMetric{
				WindowStartTime: windowStartTime,
				WindowEndTime:   windowEndTime,
				Category:        category,
				TotalAmount:     totalAmount,
				Count:           count,
			}

			jsonBytes, err := json.Marshal(metric)
			if err != nil {
				continue
			}

			producerKey := []byte(fmt.Sprintf("%s-%s", category, windowStartTime.Format("20060102150405")))
			wa.Producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &wa.OutputTopic, Partition: kafka.PartitionAny},
				Key:            producerKey,
				Value:          jsonBytes,
			}, nil)

			delete(wa.CurrentWindowSums, key)
			delete(wa.CurrentWindowCounts, key)

			log.Printf("✅ Window published: Category=%s Count=%d Total=%.2f",
				category, count, totalAmount)
		}
	}
	log.Printf("ℹ️ Active windows: %d", len(wa.CurrentWindowSums))
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT & SIGTERM
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("🛑 Shutdown signal received...")
		cancel()
	}()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "go-window-aggregator-app",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("❌ Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	inputTopic := "sales-transactions"
	if err := consumer.SubscribeTopics([]string{inputTopic}, nil); err != nil {
		log.Fatalf("❌ Failed to subscribe: %v", err)
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		log.Fatalf("❌ Failed to create producer: %v", err)
	}
	defer producer.Close()

	go func() {
		for e := range producer.Events() {
			if m, ok := e.(*kafka.Message); ok && m.TopicPartition.Error != nil {
				log.Printf("❌ Delivery failed: %v", m.TopicPartition.Error)
			}
		}
	}()

	windowDuration := 1 * time.Minute
	outputTopic := "hourly-sales-summary"
	aggregator := NewWindowAggregator(windowDuration, outputTopic, producer)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	log.Println("🚀 Aggregator started...")

	for {
		select {
		case <-ctx.Done():
			log.Println("🔄 Flushing producer before shutdown...")
			producer.Flush(5000)
			return
		case <-ticker.C:
			aggregator.PunctureWindows()
		default:
			msg, err := consumer.ReadMessage(500 * time.Millisecond)
			if err == nil {
				var salesEvent SalesEvent
				if err := json.Unmarshal(msg.Value, &salesEvent); err == nil {
					aggregator.ProcessEvent(salesEvent)
				}
			} else if kErr, ok := err.(kafka.Error); ok && kErr.Code() != kafka.ErrTimedOut {
				log.Printf("⚠️ Kafka error: %v", kErr)
			}
		}
	}
}
