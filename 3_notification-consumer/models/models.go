package models

type NotificationEvent struct {
	ID      string `json:"id"`
	OrderID string `json:"order_id"`
	Message string `json:"message"`
}

type DeadLetterPayload struct {
	Reason      string `json:"reason"`
	RawMessage  string `json:"raw_message"`
	Topic       string `json:"topic"`
	ConsumerGrp string `json:"consumer_group"`
}
