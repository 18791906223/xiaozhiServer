package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config 表示服务器配置
type Config struct {
	Server struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	} `json:"server"`
	MQTT struct {
		Broker   string `json:"broker"`
		ClientID string `json:"client_id"`
		Username string `json:"username"`
		Password string `json:"password"`
	} `json:"mqtt"`
	// 唤醒词相关配置
	WakeupWords    []string `json:"wakeup_words"`
	EnableGreeting bool     `json:"enable_greeting"`
}

// ServerAddress 返回服务器地址
func (c *Config) ServerAddress() string {
	return fmt.Sprintf("%s:%d", c.Server.Host, c.Server.Port)
}

// LoadConfig 从文件加载配置
func LoadConfig(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var config Config
	if err := json.NewDecoder(file).Decode(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

// SaveConfig 保存配置到文件
func (c *Config) SaveConfig(filename string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filename, data, 0644)
}
