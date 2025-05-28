package handler

import (
	"sync"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	ConfirmedOrdersCount int
	Mu                   sync.Mutex
)

// Order represents the structure of an order message.
type Order struct {
	OrderID  string `json:"orderId"`
	Customer string `json:"customer"`
	Amount   int    `json:"amount"`
}

// processedOrders stores IDs of already processed orders to avoid duplicates.
var processedOrders = make(map[string]bool)

// HandleOrder processes a Kafka message:
// 1. Validates JSON and required fields
// 2. Checks for duplicates
// 3. Writes valid orders to OrderConfirmed topic
// 4. Sends invalid or duplicate ones to the DeadLetterQueue (DLQ)
func HandleOrder(ctx context.Context, msg kafka.Message, writerConfirmed, writerDLQ, writerKPI, writerNotification *kafka.Writer) {
	var order Order

	// Step 1: Unmarshal JSON message
	if err := json.Unmarshal(msg.Value, &order); err != nil {
		log.Printf("❌ Invalid message format: %v", err)
		sendToDLQ(writerDLQ, msg.Value, "Invalid JSON format")
		SendKPIEvent(writerKPI, "InventoryError", "error_per_minute", 1) // KPI Increment
		return
	}

	// Step 2: Validate required field
	if order.OrderID == "" {
		log.Println("❌ Missing order ID")
		sendToDLQ(writerDLQ, msg.Value, "Missing order ID")
		SendKPIEvent(writerKPI, "InventoryError", "error_per_minute", 1)
		return
	}

	// Step 3: Check for duplicates
	if processedOrders[order.OrderID] {
		log.Printf("⚠️ Duplicate order: %s", order.OrderID)
		sendToDLQ(writerDLQ, msg.Value, "Duplicate order ID")
		SendKPIEvent(writerKPI, "InventoryError", "error_per_minute", 1)
		return
	}

	// Step 4: Mark as processed
	processedOrders[order.OrderID] = true

	// Step 5: Forward to OrderConfirmed topic
	orderJSON, _ := json.Marshal(order)
	err := writerConfirmed.WriteMessages(ctx, kafka.Message{
		Key:   []byte(order.OrderID),
		Value: orderJSON,
	})

	if err != nil {
		log.Printf("❌ Failed to write to OrderConfirmed: %v", err)
		sendToDLQ(writerDLQ, orderJSON, "Failed to forward to OrderConfirmed")
		SendKPIEvent(writerKPI, "InventoryError", "error_per_minute", 1)
		return
	}

	log.Printf("✅ Order confirmed: %+v", order)

	Mu.Lock()
	ConfirmedOrdersCount += 1
	log.Printf("Total confirmed orders: %d", ConfirmedOrdersCount)
	Mu.Unlock()
}

// sendToDLQ sends the original message to the DeadLetterQueue topic along with a reason.
func sendToDLQ(writer *kafka.Writer, message []byte, reason string) {
	dlqMsg := fmt.Sprintf(`{"error": "%s", "originalMessage": %s}`, reason, strings.ReplaceAll(string(message), `"`, `\"`))
	err := writer.WriteMessages(context.Background(), kafka.Message{
		Value: []byte(dlqMsg),
	})
	if err != nil {
		log.Printf("❌ Failed to write to DeadLetterQueue: %v", err)
	} else {
		log.Printf("📤 Sent to Dead Letter Queue.")
	}
}

// sendKPIEvent sends a KPI event to the InventoryKPI topic.
func SendKPIEvent(writer *kafka.Writer, kpiName, metricName string, value int) {
	kpiMsg := map[string]interface{}{
		"kpi_name":    kpiName,
		"metric_name": metricName,
		"value":       value,
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
	}
	jsonData, err := json.Marshal(kpiMsg)
	if err != nil {
		log.Printf("❌ Failed to marshal KPI event: %v", err)
		return
	}
	err = writer.WriteMessages(context.Background(), kafka.Message{
		Value: jsonData,
	})
	if err != nil {
		log.Printf("❌ Failed to send KPI event: %v", err)
	} else {
		log.Printf("📊 KPI Event sent.")
	}
}
