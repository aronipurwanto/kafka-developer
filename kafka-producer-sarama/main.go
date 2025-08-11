package main

import (
	"log"
	"order-service/config"
	"order-service/controllers"
	"order-service/models"
	"order-service/repository"
	"order-service/services"

	"github.com/gofiber/fiber/v2"
)

func main() {
	// Muat konfigurasi dari config.yml
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Inisialisasi koneksi database
	db, err := repository.InitDB(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Migrasi tabel database secara otomatis
	log.Println("Running database migrations...")
	err = db.AutoMigrate(&models.Customer{}, &models.Order{}, &models.OrderDetail{})
	if err != nil {
		log.Fatalf("Failed to auto migrate database: %v", err)
	}
	log.Println("Database migration complete.")

	// Seed beberapa data customer awal untuk pengujian
	seedCustomers(db)

	// Inisialisasi layer Repository
	orderRepo := repository.NewOrderRepository(db)

	// Inisialisasi layer Kafka Service
	kafkaSvc, err := services.NewKafkaService(cfg.Kafka.Brokers)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka service: %v", err)
	}

	// Inisialisasi layer Order Service dengan dependensinya
	orderSvc := services.NewOrderService(orderRepo, kafkaSvc, cfg.Kafka.Topic)

	// Inisialisasi layer Controller dengan dependensinya
	orderCtrl := controllers.NewOrderController(orderSvc)

	// Inisialisasi aplikasi web Fiber
	app := fiber.New()

	// Definisikan rute API
	app.Post("/orders", orderCtrl.CreateOrder)

	// Mulai aplikasi Fiber
	port := ":3000"
	log.Printf("Server is starting on port %s", port)
	log.Fatal(app.Listen(port))
}

// seedCustomers creates some sample customers for testing purposes.
func seedCustomers(db *gorm.DB) {
	customers := []models.Customer{
		{Name: "Budi Santoso", Email: "budi@example.com"},
		{Name: "Siti Rahayu", Email: "siti@example.com"},
		{Name: "Joko Susilo", Email: "joko@example.com"},
	}

	for _, customer := range customers {
		var existingCustomer models.Customer
		// Periksa apakah customer sudah ada untuk mencegah duplikasi saat restart
		if db.Where("email = ?", customer.Email).First(&existingCustomer).Error != nil {
			if err := db.Create(&customer).Error; err != nil {
				log.Printf("Failed to seed customer %s: %v", customer.Name, err)
			} else {
				log.Printf("Seeded customer: %s (%s)", customer.Name, customer.Email)
			}
		}
	}
	log.Println("Sample customers seeding process finished.")
}
