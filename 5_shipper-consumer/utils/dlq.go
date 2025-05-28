package utils

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func PublishToDLQ(ctx context.Context, writer *kafka.Writer, value []byte) {
	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte("error"),
		Value: value,
		Time:  time.Now(),
	})
	if err != nil {
		log.Printf("❌ Failed to write to DLQ: %v\n", err)
	} else {
		log.Println("📥 Message sent to DeadLetterQueue")
	}
}
