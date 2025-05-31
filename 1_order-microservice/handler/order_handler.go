package handler

import (
	"encoding/json"
	"net/http"
	"1_order-microservice/kafka"
	"1_order-microservice/model"
)

// HandleOrder processes the /order POST request.
// It validates and publishes the order data to Kafka.
func HandleOrder(w http.ResponseWriter, r *http.Request) {
	var order model.Order

	// Decode JSON payload into Order struct
	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		http.Error(w, "Invalid order payload", http.StatusBadRequest)
		return
	}

	// // Validate the order data
	// if err := model.ValidateOrder(order); err != nil {
	// 	http.Error(w, "Validation failed: "+err.Error(), http.StatusBadRequest)
	// 	return
	// }

	// Convert order struct to JSON bytes
	orderBytes, err := json.Marshal(order)
	if err != nil {
		http.Error(w, "Failed to encode order", http.StatusInternalServerError)
		return
	}

	// Publish the JSON message to Kafka topic
	if err := kafka.PublishToKafka("OrderReceived", string(orderBytes)); err != nil {
		http.Error(w, "Failed to publish order: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("Order received and published to Kafka!"))
}

