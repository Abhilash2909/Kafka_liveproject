package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"1_order-microservice/handler"
)

func main() {
	// Create a new router using chi
	r := chi.NewRouter()

	// Health check endpoint
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Order service is healthy!!"))
	})

	// Endpoint to receive order and publish to Kafka
	r.Post("/order", handler.HandleOrder)

	fmt.Println("Server is running on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", r)) // Start the HTTP server
}
