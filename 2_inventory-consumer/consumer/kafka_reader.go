package consumer

import (
	"github.com/segmentio/kafka-go"
)

// NewKafkaReader initializes and returns a Kafka reader (consumer) for a given topic and group ID.
func NewKafkaReader(brokers []string, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,       // Minimum bytes to fetch
		MaxBytes: 10e6,    // Maximum bytes to fetch (10MB)
	})
}
