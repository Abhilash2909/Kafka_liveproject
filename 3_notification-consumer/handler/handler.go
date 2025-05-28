package handler

import (
	"encoding/json"
	"log"
	"sync"

	"3_notification-consumer/dlq"
	"3_notification-consumer/models"
)

var processedIDs = make(map[string]bool)
var mu sync.Mutex

func IsDuplicate(id string) bool {
	mu.Lock()
	defer mu.Unlock()
	return processedIDs[id]
}

func MarkAsProcessed(id string) {
	mu.Lock()
	defer mu.Unlock()
	processedIDs[id] = true
}

func ProcessNotification(msg []byte) {
	var event models.NotificationEvent

	// Deserialize JSON
	err := json.Unmarshal(msg, &event)
	if err != nil {
		log.Printf("❌ Failed to parse message: %v", err)
		dlq.PublishToDeadLetterQueue(msg, "JSON parsing failed")
		return
	}

	// Idempotency check
	if IsDuplicate(event.ID) {
		log.Printf("⚠️ Duplicate event skipped: %s", event.ID)
		return
	}

	// Business Logic
	log.Printf("📩 Sending notification: \"%s\" for Order ID: %s", event.Message, event.OrderID)

	// Mark as processed
	MarkAsProcessed(event.ID)
}
