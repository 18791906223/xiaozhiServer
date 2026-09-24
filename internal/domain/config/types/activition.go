package types

// ActivationPayload/ActivationRequest 结构体定义

type ActivationPayload struct {
	Algorithm    string `json:"algorithm"`
	SerialNumber string `json:"serial_number"`
	Challenge    string `json:"challenge"`
	HMAC         string `json:"hmac"`
}
