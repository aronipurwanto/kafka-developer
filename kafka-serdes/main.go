package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// LogMessage struct represents the structure of your log data
type LogMessage struct {
	Level   string `json:"level"`
	Message string `json:"message"`
}

// processAndSerialize function demonstrates deserialization, transformation, and serialization
func processAndSerialize(msg *kafka.Message) ([]byte, error) {
	// Deserialization: Convert bytes from Kafka message value to LogMessage struct
	var logMsg LogMessage
	if err := json.Unmarshal(msg.Value, &logMsg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	// Simple transformation: Modify the message content
	logMsg.Message = "PROCESSED: " + logMsg.Message

	// Serialization: Convert the modified LogMessage struct back to bytes
	processedData, err := json.Marshal(logMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}
	return processedData, nil
}

// This is a minimal main function to show how processAndSerialize might be used
// In a real application, this would be part of your consumer loop
func main() {
	// Example usage (assuming you have a kafka.Message)
	sampleMsgValue := []byte(`{"level": "INFO", "message": "User logged in"}`)
	sampleMsg := &kafka.Message{Value: sampleMsgValue}

	processedBytes, err := processAndSerialize(sampleMsg)
	if err != nil {
		fmt.Printf("Error processing message: %v\n", err)
		return
	}
	fmt.Printf("Original: %s\n", string(sampleMsgValue))
	fmt.Printf("Processed: %s\n", string(processedBytes))

	// You would then produce 'processedBytes' to another Kafka topic
	// producer.Produce(&kafka.Message{Value: processedBytes}, nil)
}
