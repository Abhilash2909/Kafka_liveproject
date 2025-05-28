package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"

	"4_warehouse-consumer/dlq"
	"4_warehouse-consumer/models"

	"github.com/segmentio/kafka-go"
)

var (
	processedIDs = make(map[string]bool) // for idempotency
	mutex        = sync.Mutex{}
)

func HandleOrderMessage(ctx context.Context, m kafka.Message, notifWriter *kafka.Writer, dlqWriter *kafka.Writer) {
	var order models.OrderConfirmedEvent

	if err := json.Unmarshal(m.Value, &order); err != nil {
		log.Printf("❌ Failed to parse order: %v\n", err)
		dlq.PublishToDeadLetterQueue(ctx, dlqWriter, m.Value)
		return
	}

	// Idempotency check
	mutex.Lock()
	if processedIDs[order.ID] {
		mutex.Unlock()
		log.Printf("⚠️ Duplicate order skipped: %s\n", order.ID)
		return
	}
	processedIDs[order.ID] = true
	mutex.Unlock()

	log.Printf("📦 Order confirmed: ID=%s, CustomerID=%s, Items=%s\n",
		order.ID, order.CustomerID, strings.Join(order.Items, ", "))

	notif := models.NotificationEvent{
		ID:      "notif-" + order.ID,
		OrderID: order.ID,
		Message: fmt.Sprintf("Your order %s is being fulfilled", order.ID),
	}

	data, err := json.Marshal(notif)
	if err != nil {
		log.Printf("❌ Failed to marshal notification: %v\n", err)
		dlq.PublishToDeadLetterQueue(ctx, dlqWriter, m.Value)
		return
	}

	err = notifWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(order.ID),
		Value: data,
	})
	if err != nil {
		log.Printf("❌ Failed to publish notification: %v\n", err)

		errorEvent := map[string]string{
			"consumer_group": "warehouse-group",
			"raw_message":    string(m.Value),
			"reason":         "Publishing to Notification topic failed",
			"topic":          "OrderConfirmed",
		}
		payload, _ := json.Marshal(errorEvent)
		dlq.PublishToDeadLetterQueue(ctx, dlqWriter, payload)
	} else {
		log.Printf("📤 Notification sent for Order ID: %s\n", order.ID)
	}
}
