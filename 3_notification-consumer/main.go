package main

import (
	"log"

	"3_notification-consumer/consumer"
)

func main() {
	log.Println("🚀 Notification Consumer Service Started")
	consumer.ConsumeNotifications()
}
