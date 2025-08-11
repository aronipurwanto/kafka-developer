package services

import (
	"encoding/json"
	"fmt"
	"kafka-producer-sarama/models"
	"kafka-producer-sarama/repository"
)

// IOrderService defines the interface for order-related business logic.
type IOrderService interface {
	CreateOrder(customerID uint, details []models.OrderDetail) (*models.Order, error)
	// Anda bisa menambahkan metode logika bisnis lain di sini (misalnya, GetOrderHistory, CancelOrder, dll.)
}

// OrderService implements IOrderService.
type OrderService struct {
	orderRepo    repository.IOrderRepository // Dependency pada antarmuka repository
	kafkaService IKafkaService               // Dependency pada antarmuka layanan Kafka
	kafkaTopic   string
}

// NewOrderService creates a new OrderService instance.
func NewOrderService(repo repository.IOrderRepository, kafkaSvc IKafkaService, topic string) IOrderService {
	return &OrderService{
		orderRepo:    repo,
		kafkaService: kafkaSvc,
		kafkaTopic:   topic,
	}
}

// CreateOrder handles the business logic for creating a new order.
func (s *OrderService) CreateOrder(customerID uint, details []models.OrderDetail) (*models.Order, error) {
	// 1. Validasi keberadaan Customer
	customer, err := s.orderRepo.FindCustomerByID(customerID)
	if err != nil {
		return nil, fmt.Errorf("customer not found with ID %d: %w", customerID, err)
	}

	// 2. Hitung total harga dan validasi detail order
	var totalAmount float64
	for _, detail := range details {
		if detail.Quantity <= 0 {
			return nil, fmt.Errorf("product %s quantity must be positive", detail.ProductName)
		}
		if detail.Price < 0 {
			return nil, fmt.Errorf("product %s price cannot be negative", detail.ProductName)
		}
		totalAmount += detail.Price * float64(detail.Quantity)
	}
	if len(details) == 0 {
		return nil, fmt.Errorf("order must contain at least one product")
	}

	// 3. Buat model Order
	order := &models.Order{
		CustomerID:   customer.ID,
		TotalAmount:  totalAmount,
		OrderDetails: details,
	}

	// 4. Simpan order dan detailnya ke database (transaksi ditangani oleh repository)
	if err := s.orderRepo.CreateOrder(order); err != nil {
		return nil, fmt.Errorf("failed to save order to database: %w", err)
	}

	// 5. Marshal data order untuk Kafka
	orderJSON, err := json.Marshal(order)
	if err != nil {
		// Log error, tetapi jangan gagalkan pembuatan order karena sudah ada di DB
		return order, fmt.Errorf("failed to marshal order for Kafka: %w", err)
	}

	// 6. Push data order ke Kafka
	if err := s.kafkaService.PushMessage(s.kafkaTopic, orderJSON); err != nil {
		// Log error, tetapi jangan gagalkan pembuatan order karena sudah ada di DB
		return order, fmt.Errorf("failed to push order to Kafka: %w", err)
	}

	return order, nil
}
