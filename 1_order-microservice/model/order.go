package model

// import "errors"

// Order defines the structure of an incoming order
type Order struct {
	OrderID  string `json:"orderId"`
	Customer string `json:"customer"`
	Amount   int    `json:"amount"`
}

// ValidateOrder checks for required fields and valid values
// func ValidateOrder(order Order) error {
// 	if order.OrderID == "" {
// 		return errors.New("OrderID is required")
// 	}
// 	if order.Customer == "" {
// 		return errors.New("Customer name is required")
// 	}
// 	if order.Amount <= 0 {
// 		return errors.New("Amount must be greater than 0")
// 	}
// 	return nil
// }
