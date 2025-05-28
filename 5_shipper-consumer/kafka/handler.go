package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/segmentio/kafka-go"
	"5_shipper-consumer/models"
	"5_shipper-consumer/utils"
)

var processedIDs = make(map[string]bool)
var mu sync.Mutex

func HandleMessages(ctx context.Context, reader *kafka.Reader, notifWriter *kafka.Writer, dlqWriter *kafka.Writer) {
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("❌ Error reading message: %v\n", err)
			continue
		}

		var order models.OrderPickedAndPacked
		if err := json.Unmarshal(msg.Value, &order); err != nil {
			log.Printf("❌ Failed to parse order: %v\n", err)
			utils.PublishToDLQ(ctx, dlqWriter, msg.Value)
			continue
		}

		mu.Lock()
		if processedIDs[order.ID] {
			mu.Unlock()
			log.Printf("⚠️ Duplicate order skipped: %s\n", order.ID)
			continue
		}
		processedIDs[order.ID] = true
		mu.Unlock()

		log.Printf("🚚 Order ready to ship: ID=%s, CustomerID=%s, Items=%s, Warehouse=%s\n",
			order.ID, order.CustomerID, strings.Join(order.Items, ", "), order.Warehouse)

		notif := models.Notification{
			ID:      "notif-" + order.ID,
			OrderID: order.ID,
			Message: fmt.Sprintf("Your order %s has been shipped!", order.ID),
		}

		data, err := json.Marshal(notif)
		if err != nil {
			log.Printf("❌ Failed to marshal notification: %v\n", err)
			utils.PublishToDLQ(ctx, dlqWriter, msg.Value)
			continue
		}

		err = notifWriter.WriteMessages(ctx, kafka.Message{
			Key:   []byte(order.ID),
			Value: data,
		})
		if err != nil {
			log.Printf("❌ Failed to publish notification: %v\n", err)
			utils.PublishToDLQ(ctx, dlqWriter, msg.Value)
		} else {
			log.Printf("📤 Notification sent for Order ID: %s\n", order.ID)
		}
	}
}
