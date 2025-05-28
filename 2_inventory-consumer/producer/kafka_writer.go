package producer

import (
	"github.com/segmentio/kafka-go"
)

// NewKafkaWriter initializes and returns a Kafka writer (producer) for a given topic.
func NewKafkaWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(brokers...),     // Kafka broker address
		Topic:    topic,                     // Target topic
		Balancer: &kafka.LeastBytes{},       // Load balancing strategy
	}
}
