package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// PublishToKafka sends a message to the specified Kafka topic.
func PublishToKafka(topic string, message string) error {
	// Create a Kafka writer configuration
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},  // Kafka broker address
		Topic:    topic,                       // Target topic
		Balancer: &kafka.LeastBytes{},        // Load balancing strategy
	})
	defer writer.Close() // Ensure writer closes after use

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Send the message to Kafka
	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte("order-key"),      // Optional key
		Value: []byte(message),          // Message payload
	})
	if err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}

	fmt.Println("Message published to Kafka topic:", topic)
	return nil
}
