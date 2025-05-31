package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"4_warehouse-consumer/handler"
	"github.com/segmentio/kafka-go"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "OrderConfirmed",
		GroupID:  "warehouse-group",
		MinBytes: 1e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	notificationWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "Notification",
	})
	defer notificationWriter.Close()

	dlqWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "DeadLetterQueue",
	})
	defer dlqWriter.Close()

	latencyWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "LatencyKPI",
	})
	defer latencyWriter.Close()

	ctx := context.Background()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	fmt.Println("🚛 Warehouse Consumer is running and waiting for OrderConfirmed events...")

	for {
		select {
		case <-c:
			fmt.Println("🔴 Shutting down warehouse consumer...")
			return
		default:
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("❌ Error reading message: %v\n", err)
				continue
			}

			handler.HandleOrderMessage(ctx, m, notificationWriter, dlqWriter, latencyWriter)
		}
	}
}
