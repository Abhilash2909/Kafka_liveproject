package consumer

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"

	"3_notification-consumer/handler"
)

const (
	brokerAddress     = "localhost:9092"
	notificationTopic = "Notification"
	groupID           = "notification-group"
)

func ConsumeNotifications() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokerAddress},
		GroupID:  groupID,
		Topic:    notificationTopic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Println("🔄 Listening to Notification topic...")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("❌ Error reading message: %v", err)
			continue
		}

		log.Printf("📨 Received message at offset %d", m.Offset)
		handler.ProcessNotification(m.Value)
	}
}
