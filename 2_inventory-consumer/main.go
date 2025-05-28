package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"2_inventory-consumer/consumer"
	"2_inventory-consumer/handler"
	"2_inventory-consumer/producer"
)

func main() {
	// Kafka broker list
	brokers := []string{"localhost:9092"}

	// Kafka topic names
	orderReceivedTopic := "OrderReceived"
	orderConfirmedTopic := "OrderConfirmed"
	deadLetterTopic := "DeadLetterQueue"
	inventoryKPITopic := "InventoryKPI"
	notificationTopic := "Notification"

	// Initialize Kafka consumer (reader)
	reader := consumer.NewKafkaReader(brokers, orderReceivedTopic, "milestone4-consumer-group")
	defer reader.Close()

	// Initialize Kafka producers (writers)
	writerConfirmed := producer.NewKafkaWriter(brokers, orderConfirmedTopic)
	defer writerConfirmed.Close()

	writerDLQ := producer.NewKafkaWriter(brokers, deadLetterTopic)
	defer writerDLQ.Close()

	writerNotification := producer.NewKafkaWriter(brokers, notificationTopic)
	defer writerNotification.Close()

	writerKPI := producer.NewKafkaWriter(brokers, inventoryKPITopic)
	defer writerKPI.Close()

	fmt.Println("🔄 Consumer started. Listening for new orders...")

	go func() {
		for {
			// Wait for 60 seconds
			<-time.After(60 * time.Second)

			// Read and reset counter
			handler.Mu.Lock()
			count := handler.ConfirmedOrdersCount
			handler.ConfirmedOrdersCount = 0
			handler.Mu.Unlock()

			if count > 0 {
				handler.SendKPIEvent(writerKPI, "ConfirmedOrders", "orders_per_minute", count)
			}
		}
	}()

	// Infinite loop to keep reading messages from Kafka
	for {
		// Read a message from the OrderReceived topic
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("❌ Error reading message: %v", err)
			continue
		}

		// Process the received order
		handler.HandleOrder(context.Background(), msg, writerConfirmed, writerDLQ, writerKPI, writerNotification)

	}
}
