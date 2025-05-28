package models

// OrderConfirmedEvent is the event structure consumed by Warehouse Consumer
type OrderConfirmedEvent struct {
	ID         string   `json:"id"`
	CustomerID string   `json:"customer_id"`
	Items      []string `json:"items"`
}

// NotificationEvent is the event we publish to Notification topic
type NotificationEvent struct {
	ID      string `json:"id"`
	OrderID string `json:"order_id"`
	Message string `json:"message"`
}
