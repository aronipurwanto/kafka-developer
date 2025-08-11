package tests

import (
	"errors"
	"kafka-producer-sarama/models"
	"kafka-producer-sarama/services"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gorm.io/gorm" // Gunakan GORM untuk SQLite in-memory untuk interaksi DB aktual dalam tes repository
)

// MockOrderRepository adalah implementasi mock dari repository.IOrderRepository
type MockOrderRepository struct {
	mock.Mock
}

func (m *MockOrderRepository) FindCustomerByID(id uint) (*models.Customer, error) {
	args := m.Called(id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.Customer), args.Error(1)
}

func (m *MockOrderRepository) CreateOrder(order *models.Order) error {
	args := m.Called(order)
	return args.Error(0)
}

// MockKafkaService adalah implementasi mock dari services.IKafkaService
type MockKafkaService struct {
	mock.Mock
}

func (m *MockKafkaService) PushMessage(topic string, message []byte) error {
	args := m.Called(topic, message)
	return args.Error(0)
}

// --- Tes Service ---

func TestOrderService_CreateOrder_Success(t *testing.T) {
	// 1. Setup Mock
	mockOrderRepo := new(MockOrderRepository)
	mockKafkaSvc := new(MockKafkaService)
	testKafkaTopic := "order-topic-test"

	// Definisikan perilaku mock untuk repository
	mockCustomer := &models.Customer{Model: gorm.Model{ID: 1}, Name: "Test Customer", Email: "test@example.com"}
	mockOrderRepo.On("FindCustomerByID", uint(1)).Return(mockCustomer, nil)
	mockOrderRepo.On("CreateOrder", mock.AnythingOfType("*models.Order")).Return(nil)

	// Definisikan perilaku mock untuk layanan Kafka
	mockKafkaSvc.On("PushMessage", testKafkaTopic, mock.AnythingOfType("[]uint8")).Return(nil) // []uint8 == []byte

	// 2. Inisialisasi Layanan dengan Mock
	orderSvc := services.NewOrderService(mockOrderRepo, mockKafkaSvc, testKafkaTopic)

	// 3. Siapkan Data Tes
	orderDetails := []models.OrderDetail{
		{ProductID: 101, ProductName: "Item A", Quantity: 1, Price: 100.0},
		{ProductID: 102, ProductName: "Item B", Quantity: 2, Price: 50.0},
	}
	customerID := uint(1)

	// 4. Panggil Metode Layanan
	createdOrder, err := orderSvc.CreateOrder(customerID, orderDetails)

	// 5. Asersi
	assert.NoError(t, err)
	assert.NotNil(t, createdOrder)
	assert.Equal(t, customerID, createdOrder.CustomerID)
	assert.Equal(t, 200.0, createdOrder.TotalAmount) // 1*100 + 2*50 = 200
	assert.Len(t, createdOrder.OrderDetails, 2)

	// Verifikasi semua ekspektasi mock terpenuhi
	mockOrderRepo.AssertExpectations(t)
	mockKafkaSvc.AssertExpectations(t)
}

func TestOrderService_CreateOrder_CustomerNotFound(t *testing.T) {
	mockOrderRepo := new(MockOrderRepository)
	mockKafkaSvc := new(MockKafkaService)
	testKafkaTopic := "order-topic-test"

	// Simulasikan error customer not found dari repository
	mockOrderRepo.On("FindCustomerByID", uint(99)).Return(nil, gorm.ErrRecordNotFound)

	orderSvc := services.NewOrderService(mockOrderRepo, mockKafkaSvc, testKafkaTopic)

	orderDetails := []models.OrderDetail{
		{ProductID: 101, ProductName: "Item A", Quantity: 1, Price: 100.0},
	}
	customerID := uint(99) // Customer tidak ada

	createdOrder, err := orderSvc.CreateOrder(customerID, orderDetails)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "customer not found with ID 99")
	assert.Nil(t, createdOrder)

	mockOrderRepo.AssertExpectations(t)
	mockKafkaSvc.AssertNotCalled(t, "PushMessage") // Kafka tidak boleh dipanggil
}

func TestOrderService_CreateOrder_DBSaveFails(t *testing.T) {
	mockOrderRepo := new(MockOrderRepository)
	mockKafkaSvc := new(MockKafkaService)
	testKafkaTopic := "order-topic-test"

	mockCustomer := &models.Customer{Model: gorm.Model{ID: 1}, Name: "Test Customer", Email: "test@example.com"}
	mockOrderRepo.On("FindCustomerByID", uint(1)).Return(mockCustomer, nil)
	// Simulasikan error penyimpanan DB
	mockOrderRepo.On("CreateOrder", mock.AnythingOfType("*models.Order")).Return(errors.New("database write error"))

	orderSvc := services.NewOrderService(mockOrderRepo, mockKafkaSvc, testKafkaTopic)

	orderDetails := []models.OrderDetail{
		{ProductID: 101, ProductName: "Item A", Quantity: 1, Price: 100.0},
	}
	customerID := uint(1)

	createdOrder, err := orderSvc.CreateOrder(customerID, orderDetails)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to save order to database")
	assert.Nil(t, createdOrder) // Order tidak boleh dikembalikan jika penyimpanan DB gagal

	mockOrderRepo.AssertExpectations(t)
	mockKafkaSvc.AssertNotCalled(t, "PushMessage") // Kafka tidak boleh dipanggil
}

func TestOrderService_CreateOrder_KafkaPushFails(t *testing.T) {
	mockOrderRepo := new(MockOrderRepository)
	mockKafkaSvc := new(MockKafkaService)
	testKafkaTopic := "order-topic-test"

	mockCustomer := &models.Customer{Model: gorm.Model{ID: 1}, Name: "Test Customer", Email: "test@example.com"}
	mockOrderRepo.On("FindCustomerByID", uint(1)).Return(mockCustomer, nil)
	mockOrderRepo.On("CreateOrder", mock.AnythingOfType("*models.Order")).Return(nil)
	// Simulasikan error push Kafka
	mockKafkaSvc.On("PushMessage", testKafkaTopic, mock.AnythingOfType("[]uint8")).Return(errors.New("kafka connection error"))

	orderSvc := services.NewOrderService(mockOrderRepo, mockKafkaSvc, testKafkaTopic)

	orderDetails := []models.OrderDetail{
		{ProductID: 101, ProductName: "Item A", Quantity: 1, Price: 100.0},
	}
	customerID := uint(1)

	createdOrder, err := orderSvc.CreateOrder(customerID, orderDetails)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to push order to Kafka")
	assert.NotNil(t, createdOrder) // Order masih harus dikembalikan karena sudah disimpan ke DB

	mockOrderRepo.AssertExpectations(t)
	mockKafkaSvc.AssertExpectations(t)
}

func TestOrderService_CreateOrder_EmptyOrderDetails(t *testing.T) {
	mockOrderRepo := new(MockOrderRepository)
	mockKafkaSvc := new(MockKafkaService)
	testKafkaTopic := "order-topic-test"

	mockCustomer := &models.Customer{Model: gorm.Model{ID: 1}, Name: "Test Customer", Email: "test@example.com"}
	mockOrderRepo.On("FindCustomerByID", uint(1)).Return(mockCustomer, nil)

	orderSvc := services.NewOrderService(mockOrderRepo, mockKafkaSvc, testKafkaTopic)

	orderDetails := []models.OrderDetail{} // Detail kosong
	customerID := uint(1)

	createdOrder, err := orderSvc.CreateOrder(customerID, orderDetails)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "order must contain at least one product")
	assert.Nil(t, createdOrder)

	mockOrderRepo.AssertExpectations(t)
	mockKafkaSvc.AssertNotCalled(t, "PushMessage")
}

func TestOrderService_CreateOrder_InvalidQuantity(t *testing.T) {
	mockOrderRepo := new(MockOrderRepository)
	mockKafkaSvc := new(MockKafkaService)
	testKafkaTopic := "order-topic-test"

	mockCustomer := &models.Customer{Model: gorm.Model{ID: 1}, Name: "Test Customer", Email: "test@example.com"}
	mockOrderRepo.On("FindCustomerByID", uint(1)).Return(mockCustomer, nil)

	orderSvc := services.NewOrderService(mockOrderRepo, mockKafkaSvc, testKafkaTopic)

	orderDetails := []models.OrderDetail{
		{ProductID: 101, ProductName: "Item A", Quantity: 0, Price: 100.0}, // Kuantitas tidak valid
	}
	customerID := uint(1)

	createdOrder, err := orderSvc.CreateOrder(customerID, orderDetails)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "quantity must be positive")
	assert.Nil(t, createdOrder)

	mockOrderRepo.AssertExpectations(t)
	mockKafkaSvc.AssertNotCalled(t, "PushMessage")
}
