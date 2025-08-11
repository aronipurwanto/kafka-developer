package controllers

import (
	"github.com/gofiber/fiber/v2"
	"kafka-producer-sarama/models"
	"kafka-producer-sarama/services"
)

// OrderController handles HTTP requests related to orders.
type OrderController struct {
	orderService services.IOrderService // Dependency pada antarmuka layanan
}

// NewOrderController creates a new OrderController instance.
func NewOrderController(svc services.IOrderService) *OrderController {
	return &OrderController{orderService: svc}
}

// CreateOrder handles the POST /orders endpoint.
func (c *OrderController) CreateOrder(ctx *fiber.Ctx) error {
	var request struct {
		CustomerID   uint                 `json:"customer_id"`
		OrderDetails []models.OrderDetail `json:"order_details"`
	}

	// Parse body permintaan
	if err := ctx.BodyParser(&request); err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body format"})
	}

	// Panggil layer layanan untuk menangani logika bisnis
	order, err := c.orderService.CreateOrder(request.CustomerID, request.OrderDetails)
	if err != nil {
		// Kembalikan status error yang sesuai berdasarkan jenis error dari layanan
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	// Kembalikan respons sukses
	return ctx.Status(fiber.StatusCreated).JSON(order)
}
