package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

// Config holds all application configurations.
type Config struct {
	Database struct {
		Driver   string `mapstructure:"driver"`
		Host     string `mapstructure:"host"`
		Port     string `mapstructure:"port"`
		User     string `mapstructure:"user"`
		Password string `mapstructure:"password"`
		Name     string `mapstructure:"name"`
	} `mapstructure:"database"`
	Kafka struct {
		Brokers []string `mapstructure:"brokers"`
		Topic   string   `mapstructure:"topic"`
	} `mapstructure:"kafka"`
}

// LoadConfig reads configuration from config.yml.
func LoadConfig() (*Config, error) {
	viper.AddConfigPath("./config") // Path to config files
	viper.SetConfigName("config")   // Name of config file (without extension)
	viper.SetConfigType("yaml")     // Type of config file

	// Set default values (optional, but good practice)
	viper.SetDefault("database.driver", "mysql")
	viper.SetDefault("database.host", "127.0.0.1")
	viper.SetDefault("database.port", "3306")
	viper.SetDefault("database.user", "root")
	viper.SetDefault("database.password", "")
	viper.SetDefault("database.name", "order_db")
	viper.SetDefault("kafka.brokers", []string{"127.0.0.1:9092"})
	viper.SetDefault("kafka.topic", "order-topic")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore error or log it,
			// defaults will be used if set.
			fmt.Println("Config file not found, using default values.")
		} else {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// For security, if password is empty and not set via env, warn
	if cfg.Database.Password == "" && os.Getenv("DB_PASSWORD") == "" {
		fmt.Println("Warning: Database password is empty. Consider using environment variables for sensitive data.")
	}

	return &cfg, nil
}
