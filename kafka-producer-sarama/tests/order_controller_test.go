package tests

import (
	"bytes"
	"encoding/json"
	"errors"
	"kafka-producer-sarama/controllers"
	"kafka-producer-sarama/models"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockOrderService adalah implementasi mock dari services.IOrderService
// Ini digunakan untuk menguji controller tanpa perlu menjalankan logika service yang sebenarnya,
// sehingga fokus pengujian hanya pada controller itu sendiri.
type MockOrderService struct {
	mock.Mock
}

// CreateOrder adalah metode mock yang meniru perilaku services.CreateOrder.
// Ketika metode ini dipanggil pada mock, ia akan mengembalikan nilai yang telah kita atur.
func (m *MockOrderService) CreateOrder(customerID uint, details []models.OrderDetail) (*models.Order, error) {
	args := m.Called(customerID, details)
	// Memeriksa apakah argumen pertama dari panggilan mock adalah nil.
	// Jika ya, berarti kita mengembalikan error.
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	// Mengembalikan Order dan Error yang telah kita atur untuk mock.
	return args.Get(0).(*models.Order), args.Error(1)
}

// --- Tes Controller ---

// TestOrderController_CreateOrder_Success menguji skenario sukses saat membuat order.
func TestOrderController_CreateOrder_Success(t *testing.T) {
	// 1. Setup Mock Service: Buat instance mock service.
	mockOrderSvc := new(MockOrderService)

	// Perilaku yang diharapkan: Atur agar metode CreateOrder pada mock
	// akan dipanggil dengan customerID 1 dan slice OrderDetail apapun (mock.AnythingOfType).
	// Kemudian, mock akan mengembalikan objek Order yang diharapkan dan nil (tidak ada error).
	expectedOrder := &models.Order{
		ID:          1,
		CustomerID:  1,
		TotalAmount: 1251.00,
		// Pastikan OrderDetails di sini sesuai dengan struktur models.OrderDetail.
		OrderDetails: []models.OrderDetail{
			{ProductID: 101, ProductName: "Laptop", Quantity: 1, Price: 1200.00},
			{ProductID: 102, ProductName: "Mouse", Quantity: 2, Price: 25.50},
		},
	}
	mockOrderSvc.On("CreateOrder", uint(1), mock.AnythingOfType("[]models.OrderDetail")).Return(expectedOrder, nil)

	// 2. Setup Controller dan Fiber App: Inisialisasi controller dengan mock service,
	// dan buat aplikasi Fiber untuk menangani rute.
	orderCtrl := controllers.NewOrderController(mockOrderSvc)
	app := fiber.New()
	app.Post("/orders", orderCtrl.CreateOrder) // Daftarkan rute POST /orders ke controller.

	// 3. Buat Payload Permintaan: Siapkan data JSON yang akan dikirim sebagai body request.
	orderPayload := struct {
		CustomerID   uint                 `json:"customer_id"`
		OrderDetails []models.OrderDetail `json:"order_details"`
	}{
		CustomerID: 1,
		OrderDetails: []models.OrderDetail{
			{ProductID: 101, ProductName: "Laptop", Quantity: 1, Price: 1200.00},
			{ProductID: 102, ProductName: "Mouse", Quantity: 2, Price: 25.50},
		},
	}
	payloadBytes, _ := json.Marshal(orderPayload) // Konversi payload ke bytes.

	// 4. Buat Permintaan HTTP: Buat objek httptest.NewRequest dengan metode, URL, dan body.
	req := httptest.NewRequest("POST", "/orders", bytes.NewReader(payloadBytes))
	req.Header.Set("Content-Type", "application/json") // Pastikan header Content-Type diatur.

	// 5. Lakukan permintaan: Gunakan app.Test untuk menjalankan permintaan HTTP secara in-memory.
	resp, err := app.Test(req, 10*time.Second) // Berikan batas waktu untuk menghindari hang.

	// 6. Asersi: Verifikasi hasil dari permintaan.
	assert.NoError(t, err)                                // Pastikan tidak ada error saat menjalankan tes.
	assert.Equal(t, fiber.StatusCreated, resp.StatusCode) // Pastikan status HTTP 201 Created.

	var responseOrder models.Order
	err = json.NewDecoder(resp.Body).Decode(&responseOrder)                    // Dekode respons JSON ke struct Order.
	assert.NoError(t, err)                                                     // Pastikan tidak ada error saat mendekode respons.
	assert.Equal(t, expectedOrder.ID, responseOrder.ID)                        // Verifikasi ID order.
	assert.Equal(t, expectedOrder.CustomerID, responseOrder.CustomerID)        // Verifikasi CustomerID.
	assert.Equal(t, expectedOrder.TotalAmount, responseOrder.TotalAmount)      // Verifikasi TotalAmount.
	assert.Len(t, responseOrder.OrderDetails, len(expectedOrder.OrderDetails)) // Verifikasi jumlah detail order.

	// Pastikan bahwa metode mock (CreateOrder) dipanggil sesuai ekspektasi.
	mockOrderSvc.AssertExpectations(t)
}

// TestOrderController_CreateOrder_InvalidBody menguji skenario dengan body request yang tidak valid.
func TestOrderController_CreateOrder_InvalidBody(t *testing.T) {
	mockOrderSvc := new(MockOrderService) // Mock ini tidak akan dipanggil dalam tes ini, tapi perlu diinisialisasi.
	orderCtrl := controllers.NewOrderController(mockOrderSvc)
	app := fiber.New()
	app.Post("/orders", orderCtrl.CreateOrder)

	// Payload JSON yang tidak valid.
	req := httptest.NewRequest("POST", "/orders", bytes.NewReader([]byte("{invalid json}")))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, 10*time.Second)

	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode) // Harapannya status 400 Bad Request.

	var responseMap map[string]string
	json.NewDecoder(resp.Body).Decode(&responseMap)
	assert.Contains(t, responseMap["error"], "Invalid request body format") // Verifikasi pesan error.

	// Pastikan metode service tidak pernah dipanggil karena body request sudah salah di awal.
	mockOrderSvc.AssertNotCalled(t, "CreateOrder")
}

// TestOrderController_CreateOrder_ServiceError menguji skenario di mana service mengembalikan error.
func TestOrderController_CreateOrder_ServiceError(t *testing.T) {
	mockOrderSvc := new(MockOrderService)
	// Simulasikan error dari layer service.
	serviceError := errors.New("failed to process order in service")
	// Atur mock agar mengembalikan nil (order) dan error serviceError.
	mockOrderSvc.On("CreateOrder", uint(1), mock.AnythingOfType("[]models.OrderDetail")).Return(nil, serviceError)

	orderCtrl := controllers.NewOrderController(mockOrderSvc)
	app := fiber.New()
	app.Post("/orders", orderCtrl.CreateOrder)

	orderPayload := struct {
		CustomerID   uint                 `json:"customer_id"`
		OrderDetails []models.OrderDetail `json:"order_details"`
	}{
		CustomerID: 1,
		OrderDetails: []models.OrderDetail{
			{ProductID: 1, ProductName: "Test", Quantity: 1, Price: 10.0},
		},
	}
	payloadBytes, _ := json.Marshal(orderPayload)

	req := httptest.NewRequest("POST", "/orders", bytes.NewReader(payloadBytes))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, 10*time.Second)

	assert.NoError(t, err)
	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode) // Harapannya status 500 Internal Server Error.

	var responseMap map[string]string
	json.NewDecoder(resp.Body).Decode(&responseMap)
	assert.Contains(t, responseMap["error"], serviceError.Error()) // Verifikasi pesan error.

	mockOrderSvc.AssertExpectations(t) // Pastikan metode service dipanggil seperti yang diharapkan.
}
