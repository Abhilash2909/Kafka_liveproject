package dlq

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddress     = "localhost:9092"
	deadLetterTopic   = "DeadLetterQueue"
	notificationTopic = "Notification"
	groupID           = "notification-group"
)

func PublishToDeadLetterQueue(original []byte, reason string) {
	writer := kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    deadLetterTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	dlqPayload := map[string]interface{}{
		"reason":         reason,
		"raw_message":    string(original),
		"topic":          notificationTopic,
		"consumer_group": groupID,
	}

	payloadBytes, err := json.Marshal(dlqPayload)
	if err != nil {
		log.Printf("❌ Failed to marshal DLQ message: %v", err)
		return
	}

	err = writer.WriteMessages(context.Background(), kafka.Message{Value: payloadBytes})
	if err != nil {
		log.Printf("❌ Failed to publish to DLQ: %v", err)
	} else {
		log.Println("✅ Message sent to DeadLetterQueue")
	}
}
