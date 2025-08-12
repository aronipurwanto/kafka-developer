package samples

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"strings"
)

func filterErrors(inputChan <-chan *kafka.Message) <-chan *kafka.Message {
	outputChan := make(chan *kafka.Message)
	go func() {
		for msg := range inputChan {
			// assume value is JSON string
			if strings.Contains(string(msg.Value), `"level": "ERROR"`) {
				outputChan <- msg
			}
		}
		close(outputChan)
	}()
	return outputChan
}

func transformLogFormat(inputChan <-chan *kafka.Message) <-chan *kafka.Message {
	outputChan := make(chan *kafka.Message)
	go func() {
		for msg := range inputChan {
			// transform message value
			newVal := []byte(strings.ToUpper(string(msg.Value)))
			msg.Value = newVal
			outputChan <- msg
		}
		close(outputChan)
	}()
	return outputChan
}
