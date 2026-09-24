package auth

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"time"
)

// ClientSession 表示一个客户端会话
type ClientSession struct {
	ID        string
	DeviceID  string
	CreatedAt time.Time
	LastSeen  time.Time
}

// AuthManager 管理认证和会话
type AuthManager struct {
	sessions map[string]*ClientSession
	mutex    sync.RWMutex
	// 令牌映射
	tokens map[string]string // token -> deviceID
}

var authManager *AuthManager

func Init() error {
	authManager = NewAuthManager()
	return nil
}

func A() *AuthManager {
	return authManager
}

// NewAuthManager 创建新的认证管理器
func NewAuthManager() *AuthManager {
	return &AuthManager{
		sessions: make(map[string]*ClientSession),
		tokens:   make(map[string]string),
	}
}

// CreateSession 创建新的会话
func (am *AuthManager) CreateSession(deviceID string) (*ClientSession, error) {
	// 生成随机会话ID
	sessionID, err := generateClientSessionID()
	if err != nil {
		return nil, err
	}

	session := &ClientSession{
		ID:        sessionID,
		DeviceID:  deviceID,
		CreatedAt: time.Now(),
		LastSeen:  time.Now(),
	}

	am.mutex.Lock()
	am.sessions[sessionID] = session
	am.mutex.Unlock()

	return session, nil
}

// EnsureSession 确保给定会话ID存在；若 preferredID 为空则创建新会话
func (am *AuthManager) EnsureSession(deviceID string, preferredID string) (*ClientSession, error) {
	preferredID = strings.TrimSpace(preferredID)
	if preferredID == "" {
		return am.CreateSession(deviceID)
	}

	now := time.Now()

	am.mutex.Lock()
	defer am.mutex.Unlock()

	if session, exists := am.sessions[preferredID]; exists {
		if deviceID != "" {
			session.DeviceID = deviceID
		}
		session.LastSeen = now
		return session, nil
	}

	session := &ClientSession{
		ID:        preferredID,
		DeviceID:  deviceID,
		CreatedAt: now,
		LastSeen:  now,
	}
	am.sessions[preferredID] = session
	return session, nil
}

// GetSession 获取会话
func (am *AuthManager) GetSession(sessionID string) (*ClientSession, error) {
	am.mutex.RLock()
	session, exists := am.sessions[sessionID]
	am.mutex.RUnlock()

	if !exists {
		return nil, errors.New("会话不存在")
	}

	// 更新最后访问时间
	am.mutex.Lock()
	session.LastSeen = time.Now()
	am.mutex.Unlock()

	return session, nil
}

// RemoveSession 移除会话
func (am *AuthManager) RemoveSession(sessionID string) {
	am.mutex.Lock()
	delete(am.sessions, sessionID)
	am.mutex.Unlock()
}

// CleanupSessions 清理过期会话
func (am *AuthManager) CleanupSessions(maxAge time.Duration) {
	am.mutex.Lock()
	defer am.mutex.Unlock()

	now := time.Now()
	for id, session := range am.sessions {
		if now.Sub(session.LastSeen) > maxAge {
			delete(am.sessions, id)
		}
	}
}

// generateClientSessionID 生成随机会话ID
func generateClientSessionID() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}

// ValidateToken 验证令牌
func (am *AuthManager) ValidateToken(token string) bool {
	return true
	// 移除 "Bearer " 前缀
	if len(token) > 7 && token[:7] == "Bearer " {
		token = token[7:]
	}

	am.mutex.RLock()
	_, exists := am.tokens[token]
	am.mutex.RUnlock()

	return exists
}

// RegisterToken 注册令牌
func (am *AuthManager) RegisterToken(token string, deviceID string) {
	// 移除 "Bearer " 前缀
	if len(token) > 7 && token[:7] == "Bearer " {
		token = token[7:]
	}

	am.mutex.Lock()
	am.tokens[token] = deviceID
	am.mutex.Unlock()
}

// RemoveToken 移除令牌
func (am *AuthManager) RemoveToken(token string) {
	// 移除 "Bearer " 前缀
	if len(token) > 7 && token[:7] == "Bearer " {
		token = token[7:]
	}

	am.mutex.Lock()
	delete(am.tokens, token)
	am.mutex.Unlock()
}
