package mqtt_server

import (
	"strings"

	"github.com/spf13/viper"
)

const (
	defaultAdminUsername = "admin"
	defaultAdminPassword = "test!@#"
)

func configuredAdminUsername() string {
	if username := strings.TrimSpace(viper.GetString("mqtt_server.username")); username != "" {
		return username
	}
	return defaultAdminUsername
}

func configuredAdminPassword() string {
	if password := strings.TrimSpace(viper.GetString("mqtt_server.password")); password != "" {
		return password
	}
	return defaultAdminPassword
}
