package mqtt_server

import (
	"bytes"
	"crypto/aes"
	"encoding/base64"
	"encoding/json"

	"xiaozhi-esp32-server-golang/internal/util"
	log "xiaozhi-esp32-server-golang/logger"

	mqttServer "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/spf13/viper"
)

// AuthHook 实现自定义鉴权逻辑
// 支持普通用户和超级管理员
// 普通用户: 用户名为 base64 后的 {"ip":"1.202.193.194"}，密码为 HMAC-SHA256 签名
// 超级管理员: 用户名 admin，密码 shijingbo!@#
type AuthHook struct {
	mqttServer.HookBase
}

func (h *AuthHook) ID() string {
	return "custom-auth-hook"
}

func (h *AuthHook) Provides(b byte) bool {
	return b == mqttServer.OnConnectAuthenticate
}

func (h *AuthHook) OnConnectAuthenticate(cl *mqttServer.Client, pk packets.Packet) bool {
	// 检查是否启用鉴权
	enableAuth := viper.GetBool("mqtt_server.enable_auth")
	if !enableAuth {
		//log.Infof("MQTT鉴权已禁用，允许所有连接")
		return true
	}

	username := string(pk.Connect.Username)
	password := string(pk.Connect.Password)
	clientId := string(pk.Connect.ClientIdentifier)

	// 超级管理员校验
	adminUsername := configuredAdminUsername()
	adminPassword := configuredAdminPassword()
	if username == adminUsername && password == adminPassword {
		log.Infof("超级管理员登录成功: %s", username)
		return true
	}
	if username == adminUsername {
		log.Warnf("MQTT管理员登录失败: username=%s, clientId=%s, 原因=密码错误", username, clientId)
		return false
	}

	// 普通用户校验 - 使用新的签名验证逻辑
	signatureKey := viper.GetString("mqtt_server.signature_key")
	if signatureKey != "" {
		credentialInfo, err := util.ValidateMqttCredentials(clientId, username, password, signatureKey)
		//log.Infof("MQTT用户验证开始: clientId=%s, username=%s, password=%s, signatureKey=%s",
		//	clientId, username, password, signatureKey)
		//log.Infof("MQTT用户验证开始: credentialInfo=%+v", credentialInfo)

		if err != nil {
			log.Warnf("MQTT凭据验证失败: username=%s, clientId=%s, err=%v", username, clientId, err)
			return false
		}

		log.Infof("MQTT用户验证成功: groupId=%s, macAddress=%s, uuid=%s",
			credentialInfo.GroupId, credentialInfo.MacAddress, credentialInfo.UUID)
		return true
	}

	// 如果没有配置签名密钥，回退到原来的AES验证逻辑
	log.Warnf("缺少OTA签名密钥配置，使用AES验证方式")
	return h.validateWithAes(username, password)
}

// validateWithAes 使用AES方式验证密码（向后兼容）
func (h *AuthHook) validateWithAes(username, password string) bool {
	// 普通用户校验
	decoded, err := base64.StdEncoding.DecodeString(username)
	if err != nil {
		return false
	}
	var userInfo map[string]string
	if err := json.Unmarshal(decoded, &userInfo); err != nil {
		return false
	}
	if _, ok := userInfo["ip"]; !ok {
		return false
	}
	// 校验 password 是否为 AES 加密后的 username
	if !checkAesPassword(username, password) {
		return false
	}
	return true
}

// checkAesPassword 校验 password 是否为 AES-ECB 加密后 base64(username)
func checkAesPassword(username, password string) bool {
	key := []byte("xiaozhi_aes_key_1") // 16字节密钥，实际建议配置
	ciphertext, err := aesEncryptECB([]byte(username), key)
	if err != nil {
		return false
	}
	cipherBase64 := base64.StdEncoding.EncodeToString(ciphertext)
	return cipherBase64 == password
}

// aesEncryptECB 实现 AES-ECB 加密
func aesEncryptECB(src, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	blockSize := block.BlockSize()
	// PKCS7 填充
	padding := blockSize - len(src)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	src = append(src, padtext...)
	encrypted := make([]byte, len(src))
	for bs, be := 0, blockSize; bs < len(src); bs, be = bs+blockSize, be+blockSize {
		block.Encrypt(encrypted[bs:be], src[bs:be])
	}
	return encrypted, nil
}
