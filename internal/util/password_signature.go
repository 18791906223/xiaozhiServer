package util

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

// GeneratePasswordSignature 生成密码签名
// 基于 clientId + '|' + username 和签名密钥生成HMAC-SHA256签名
func GeneratePasswordSignature(data, key string) string {
	// 使用HMAC-SHA256生成签名
	h := hmac.New(sha256.New, []byte(key))
	h.Write([]byte(data))
	signature := h.Sum(nil)

	// 返回base64编码的签名
	return base64.StdEncoding.EncodeToString(signature)
}

// ValidateMqttCredentials 验证MQTT凭据
// 根据提供的JavaScript验证逻辑实现
func ValidateMqttCredentials(clientId, username, password, signatureKey string) (*MqttCredentialInfo, error) {
	// 验证签名密钥
	if signatureKey == "" {
		return nil, fmt.Errorf("缺少签名密钥配置")
	}

	// 验证clientId
	if clientId == "" {
		return nil, fmt.Errorf("clientId必须是非空字符串")
	}

	// 验证clientId格式（必须包含@@@分隔符）
	clientIdParts := strings.Split(clientId, "@@@")
	if len(clientIdParts) != 3 {
		return nil, fmt.Errorf("clientId格式错误，必须包含@@@分隔符")
	}

	// 验证username
	if username == "" {
		return nil, fmt.Errorf("username必须是非空字符串")
	}

	// 尝试解码username（应该是base64编码的JSON）
	var userData map[string]interface{}
	decodedUsername, err := base64.StdEncoding.DecodeString(username)
	if err != nil {
		return nil, fmt.Errorf("username不是有效的base64编码: %v", err)
	}

	if err := json.Unmarshal(decodedUsername, &userData); err != nil {
		return nil, fmt.Errorf("username不是有效的base64编码JSON: %v", err)
	}

	// 验证密码签名
	signatureData := clientId + "|" + username
	expectedSignature := GeneratePasswordSignature(signatureData, signatureKey)
	if password != expectedSignature {
		return nil, fmt.Errorf("密码签名验证失败")
	}

	// 解析clientId中的信息
	groupId := clientIdParts[0]
	macAddress := strings.ReplaceAll(clientIdParts[1], "_", ":")
	uuid := clientIdParts[2]

	// 如果验证成功，返回解析后的有用信息
	return &MqttCredentialInfo{
		GroupId:    groupId,
		MacAddress: macAddress,
		UUID:       uuid,
		UserData:   userData,
	}, nil
}

// MqttCredentialInfo MQTT凭据信息
type MqttCredentialInfo struct {
	GroupId    string                 `json:"groupId"`
	MacAddress string                 `json:"macAddress"`
	UUID       string                 `json:"uuid"`
	UserData   map[string]interface{} `json:"userData"`
}

// GenerateMqttCredentials 生成MQTT凭据
// 用于OTA接口生成MQTT连接信息
func GenerateMqttCredentials(deviceId, clientId, ip, signatureKey string) (*MqttCredentials, error) {
	// 处理deviceId（替换冒号为下划线）
	deviceId = strings.ReplaceAll(deviceId, ":", "_")

	// 构建用户名数据（包含IP信息）
	userName := struct {
		Ip string `json:"ip"`
	}{
		Ip: ip,
	}
	userNameJson, err := json.Marshal(userName)
	if err != nil {
		return nil, fmt.Errorf("用户名序列化失败: %v", err)
	}
	base64UserName := base64.StdEncoding.EncodeToString(userNameJson)

	// 构建clientId，格式：GID_test@@@deviceId@@@clientId
	mqttClientId := fmt.Sprintf("GID_test@@@%s@@@%s", deviceId, clientId)

	// 生成密码签名
	var pwd string
	if signatureKey != "" {
		// 使用签名密钥生成密码
		signatureData := mqttClientId + "|" + base64UserName
		pwd = GeneratePasswordSignature(signatureData, signatureKey)
	} else {
		// 如果没有配置签名密钥，使用原来的逻辑作为fallback
		pwd = Sha256Digest([]byte(mqttClientId))
	}

	return &MqttCredentials{
		ClientId: mqttClientId,
		Username: base64UserName,
		Password: pwd,
	}, nil
}

// MqttCredentials MQTT凭据
type MqttCredentials struct {
	ClientId string `json:"client_id"`
	Username string `json:"username"`
	Password string `json:"password"`
}
