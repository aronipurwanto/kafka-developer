package repository

import (
	"gorm.io/gorm"
	"kafka-producer-sarama/models"
)

// IOrderRepository defines the interface for order data operations.
type IOrderRepository interface {
	FindCustomerByID(id uint) (*models.Customer, error)
	CreateOrder(order *models.Order) error
	// Anda bisa menambahkan operasi database lain di sini (misalnya, GetOrder, UpdateOrder, dll.)
}

// OrderRepository implements IOrderRepository for GORM.
type OrderRepository struct {
	DB *gorm.DB
}

// NewOrderRepository creates a new OrderRepository instance.
func NewOrderRepository(db *gorm.DB) IOrderRepository {
	return &OrderRepository{DB: db}
}

// FindCustomerByID retrieves a customer by their ID.
func (r *OrderRepository) FindCustomerByID(id uint) (*models.Customer, error) {
	var customer models.Customer
	err := r.DB.First(&customer, id).Error
	return &customer, err
}

// CreateOrder creates a new order, including its details, in a transaction.
// GORM akan secara otomatis menyimpan OrderDetails karena asosiasi yang telah ditentukan di model.
func (r *OrderRepository) CreateOrder(order *models.Order) error {
	return r.DB.Transaction(func(tx *gorm.DB) error {
		return tx.Create(order).Error
	})
}
