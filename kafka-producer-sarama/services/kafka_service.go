package services

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

// IKafkaService defines the interface for Kafka operations.
type IKafkaService interface {
	PushMessage(topic string, message []byte) error
}

// KafkaService implements IKafkaService using Sarama.
type KafkaService struct {
	producer sarama.SyncProducer
	brokers  []string
}

// NewKafkaService creates a new KafkaService instance.
func NewKafkaService(brokers []string) (IKafkaService, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Membutuhkan semua replika in-sync untuk meng-ack pesan
	config.Producer.Retry.Max = 5                    // Jumlah percobaan ulang pengiriman pesan
	config.Producer.Return.Successes = true          // Harus diatur true untuk mendapatkan channel sukses
	config.Producer.Timeout = 5 * time.Second        // Berapa lama menunggu pesan dikirim

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to start Sarama producer: %w", err)
	}

	log.Println("Kafka producer connected successfully.")
	return &KafkaService{producer: producer, brokers: brokers}, nil
}

// PushMessage sends a message to the specified Kafka topic.
func (s *KafkaService) PushMessage(topic string, message []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := s.producer.SendMessage(msg)
	if err != nil {
		log.Printf("Failed to send message to Kafka topic '%s': %v", topic, err)
		return err
	}
	log.Printf("Message sent to topic '%s', partition %d, offset %d", topic, partition, offset)
	return nil
}
