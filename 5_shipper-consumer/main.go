package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "os"
    "os/signal"
    "sync"

    "github.com/segmentio/kafka-go"
)

// Struct for the picked and packed event
type OrderPickedAndPacked struct {
    ID         string   `json:"id"`
    CustomerID string   `json:"customer_id"`
    Items      []string `json:"items"`
    Warehouse  string   `json:"warehouse"`
}

// Struct for notification
type Notification struct {
    ID      string `json:"id"`
    OrderID string `json:"order_id"`
    Message string `json:"message"`
}

// For idempotency
var processedIDs = make(map[string]bool)
var mu sync.Mutex

func main() {
    fmt.Println("🚚 Shipper Consumer is running and waiting for OrderPickedAndPacked events...")

    ctx := context.Background()

    // Kafka Reader (Consumer)
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{"localhost:9092"},
        Topic:   "OrderPickedAndPacked",
        GroupID: "shipper-group",
    })
    defer reader.Close()

    // Kafka Writer - Notification topic
    notificationWriter := &kafka.Writer{
        Addr:     kafka.TCP("localhost:9092"),
        Topic:    "Notification",
        Balancer: &kafka.LeastBytes{},
    }
    defer notificationWriter.Close()

    // Kafka Writer - DLQ topic
    dlqWriter := &kafka.Writer{
        Addr:     kafka.TCP("localhost:9092"),
        Topic:    "DeadLetterQueue",
        Balancer: &kafka.LeastBytes{},
    }
    defer dlqWriter.Close()

    // Graceful shutdown
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt)
    go func() {
        <-c
        log.Println("👋 Shutting down Shipper consumer...")
        reader.Close()
        notificationWriter.Close()
        dlqWriter.Close()
        os.Exit(0)
    }()

    for {
        msg, err := reader.ReadMessage(ctx)
        if err != nil {
            log.Printf("❌ Error reading message: %v\n", err)
            continue
        }

        var order OrderPickedAndPacked
        if err := json.Unmarshal(msg.Value, &order); err != nil {
            log.Printf("❌ Failed to parse order: %v\n", err)

            // Send to DLQ
            dlqWriter.WriteMessages(ctx, kafka.Message{
                Value: msg.Value,
            })
            log.Println("📥 Message sent to DeadLetterQueue")
            continue
        }

        // Idempotency check
        mu.Lock()
        if processedIDs[order.ID] {
            mu.Unlock()
            continue
        }
        processedIDs[order.ID] = true
        mu.Unlock()

        log.Printf("🚚 Order ready to ship: ID=%s, CustomerID=%s, Items=%s, Warehouse=%s\n",
            order.ID, order.CustomerID, order.Items, order.Warehouse)

        notif := Notification{
            ID:      "notif-" + order.ID,
            OrderID: order.ID,
            Message: fmt.Sprintf("Your order %s has been shipped!", order.ID),
        }

        notifBytes, err := json.Marshal(notif)
        if err != nil {
            log.Printf("❌ Failed to marshal notification: %v\n", err)

            // Send to DLQ
            dlqWriter.WriteMessages(ctx, kafka.Message{
                Value: msg.Value,
            })
            log.Println("📥 Message sent to DeadLetterQueue")
            continue
        }

        // Send to Notification topic
        err = notificationWriter.WriteMessages(ctx, kafka.Message{
            Key:   []byte(order.ID),
            Value: notifBytes,
        })
        if err != nil {
            log.Printf("❌ Failed to send notification: %v\n", err)
        } else {
            log.Printf("📤 Shipping notification sent for Order ID: %s\n", order.ID)
        }
    }
}
