# Order Service Kafka Producer

A simple microservice built with Go (Fiber) to demonstrate a Kafka producer implementation.

## Features
- **REST API:** Endpoint to create a new order.
- **Database:** Stores order details using GORM.
- **Kafka Producer:** Pushes new order data to a Kafka topic.
- **Configuration:** Uses Viper for externalized configuration via `config.yml`.

## Prerequisites
- Go (version 1.18+)
- MySQL database
- Apache Kafka & Zookeeper

## Setup and Run

1.  **Clone the repository:**
    ```sh
    git clone [https://github.com/your-username/order-service.git](https://github.com/your-username/order-service.git)
    cd order-service
    ```

2.  **Install dependencies:**
    ```sh
    go mod tidy
    ```

3.  **Configure:**
    Edit the `config/config.yml` file with your database and Kafka broker details.

4.  **Run the service:**
    ```sh
    go run main.go
    ```

## API Endpoints

### `POST /orders`

Creates a new order, saves it to the database, and sends it to the `order-topic` on Kafka.

**Request Body Example:**

```json
{
    "product_id": 123,
    "quantity": 1,
    "total": 99.99
}