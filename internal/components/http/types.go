package http

import "time"

// ClientConfig HTTP客户端配置
type ClientConfig struct {
	BaseURL    string        // 基础URL
	AuthToken  string        // 认证Token（可选）
	Timeout    time.Duration // 请求超时时间
	MaxRetries int           // 最大重试次数（默认3次）
}

// RequestOptions 请求选项
type RequestOptions struct {
	Method      string            // HTTP方法
	Path        string            // 请求路径
	QueryParams map[string]string // 查询参数
	Headers     map[string]string // 自定义请求头
	Body        interface{}       // 请求体（会自动序列化为JSON）
	Response    interface{}       // 响应体（会自动反序列化）
}
