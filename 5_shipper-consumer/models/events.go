package models

// OrderPickedAndPacked defines the event structure consumed by Shipper Consumer
type OrderPickedAndPacked struct {
	ID         string   `json:"id"`
	CustomerID string   `json:"customer_id"`
	Items      []string `json:"items"`
	Warehouse  string   `json:"warehouse"`
}

// Notification defines the structure sent to the Notification topic
type Notification struct {
	ID      string `json:"id"`
	OrderID string `json:"order_id"`
	Message string `json:"message"`
}
