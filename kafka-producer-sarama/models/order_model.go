package models

import "gorm.io/gorm" // Pastikan ini diimpor!

// Customer model represents a user who places orders.
type Customer struct {
	gorm.Model         // Menyediakan ID, CreatedAt, UpdatedAt, DeletedAt
	Name       string  `json:"name" gorm:"not null"`
	Email      string  `json:"email" gorm:"unique;not null"`
	Orders     []Order `json:"orders"`
}

// Order represents a single order placed by a customer.
type Order struct {
	gorm.Model                 // Menyediakan ID, CreatedAt, UpdatedAt, DeletedAt
	CustomerID   uint          `json:"customer_id" gorm:"not null"`
	Customer     Customer      `json:"customer"` // Belongs To association
	TotalAmount  float64       `json:"total_amount" gorm:"type:decimal(10,2);default:0.00"`
	OrderDetails []OrderDetail `json:"order_details" gorm:"foreignKey:OrderID"` // Has Many association
}

// OrderDetail represents a single item in an order.
type OrderDetail struct {
	gorm.Model          // Menyediakan ID, CreatedAt, UpdatedAt, DeletedAt
	OrderID     uint    `json:"order_id"` // Foreign key for Order
	ProductID   uint    `json:"product_id" gorm:"not null"`
	ProductName string  `json:"product_name" gorm:"not null"`
	Quantity    int     `json:"quantity" gorm:"not null;default:1"`
	Price       float64 `json:"price" gorm:"type:decimal(10,2);not null"`
}
