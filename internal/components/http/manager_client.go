package http

import (
	"context"
	"time"
)

// ManagerClient Manager后端专用HTTP客户端
type ManagerClient struct {
	client *Client
}

// ManagerClientConfig Manager客户端配置
type ManagerClientConfig struct {
	BaseURL    string        // Manager后端地址
	AuthToken  string        // 认证Token（可选）
	Timeout    time.Duration // 请求超时时间
	MaxRetries int           // 最大重试次数
}

// NewManagerClient 创建Manager后端HTTP客户端
func NewManagerClient(cfg ManagerClientConfig) *ManagerClient {
	client := NewClient(ClientConfig{
		BaseURL:    cfg.BaseURL,
		AuthToken:  cfg.AuthToken,
		Timeout:    cfg.Timeout,
		MaxRetries: cfg.MaxRetries,
	})

	return &ManagerClient{
		client: client,
	}
}

// DoRequest 执行HTTP请求（封装通用客户端的DoRequest）
func (m *ManagerClient) DoRequest(ctx context.Context, opts RequestOptions) error {
	return m.client.DoRequest(ctx, opts)
}

// DoRequestRaw 执行HTTP请求并返回原始响应
func (m *ManagerClient) DoRequestRaw(ctx context.Context, opts RequestOptions) ([]byte, error) {
	return m.client.DoRequestRaw(ctx, opts)
}
