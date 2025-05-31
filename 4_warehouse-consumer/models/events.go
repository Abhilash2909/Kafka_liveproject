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


// KPIEvent is the event structure for KPI metrics
// It captures key performance indicators related to warehouse operations.
type KPIEvent struct {
	KPIName    string `json:"kpi_name"`
	MetricName string `json:"metric_name"`
	Value      int64  `json:"value"`
	Timestamp  string `json:"timestamp"`
}
