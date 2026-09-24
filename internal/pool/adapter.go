package pool

import (
	"xiaozhi-esp32-server-golang/internal/util"
)

// ResourceWrapper 泛型资源包装器
// T: 具体的资源类型（如 vad.VAD, asr.AsrProvider 等）
type ResourceWrapper[T any] struct {
	provider     T             // 实际的资源提供者（类型安全）
	configKey    string        // 配置键，用于标识资源池
	resourceType string        // 资源类型（vad/asr/llm/tts等）
	closeFunc    func(T) error // 关闭资源的函数
	isValidFunc  func(T) bool  // 验证资源是否有效的函数
	resetFunc    func(T) error // 重置资源状态的函数（可选）
}

// Close 关闭资源
func (r *ResourceWrapper[T]) Close() error {
	if r.closeFunc != nil {
		return r.closeFunc(r.provider)
	}
	return nil
}

// IsValid 检查资源是否有效
func (r *ResourceWrapper[T]) IsValid() bool {
	if r.isValidFunc != nil {
		return r.isValidFunc(r.provider)
	}
	var zero T
	return any(r.provider) != any(zero)
}

// GetProvider 获取实际的资源提供者（类型安全，无需类型断言）
func (r *ResourceWrapper[T]) GetProvider() T {
	return r.provider
}

// GetConfigKey 获取配置键
func (r *ResourceWrapper[T]) GetConfigKey() string {
	return r.configKey
}

// GetResourceType 获取资源类型
func (r *ResourceWrapper[T]) GetResourceType() string {
	return r.resourceType
}

// Reset 重置资源状态
func (r *ResourceWrapper[T]) Reset() error {
	if r.resetFunc != nil {
		return r.resetFunc(r.provider)
	}
	return nil
}

// CreatorFunc 泛型资源创建函数类型
// T: 资源类型
// 参数：resourceType, provider, config
// 返回：资源实例（类型 T）和错误
type CreatorFunc[T any] func(resourceType, provider string, config map[string]interface{}) (T, error)

// ResourceFactory 泛型资源工厂
type ResourceFactory[T any] struct {
	resourceType string
	provider     string
	config       map[string]interface{}
	configKey    string
	creator      CreatorFunc[T]
	closeFunc    func(T) error
	isValidFunc  func(T) bool
	resetFunc    func(T) error
}

// Create 创建资源
func (f *ResourceFactory[T]) Create() (util.Resource, error) {
	provider, err := f.creator(f.resourceType, f.provider, f.config)
	if err != nil {
		return nil, err
	}

	return &ResourceWrapper[T]{
		provider:     provider,
		configKey:    f.configKey,
		resourceType: f.resourceType,
		closeFunc:    f.closeFunc,
		isValidFunc:  f.isValidFunc,
		resetFunc:    f.resetFunc,
	}, nil
}

// Validate 验证资源
func (f *ResourceFactory[T]) Validate(resource util.Resource) bool {
	if wrapper, ok := resource.(*ResourceWrapper[T]); ok {
		if f.isValidFunc != nil {
			return f.isValidFunc(wrapper.provider)
		}
		return wrapper.IsValid()
	}
	return resource != nil && resource.IsValid()
}

// Reset 重置资源
func (f *ResourceFactory[T]) Reset(resource util.Resource) error {
	if wrapper, ok := resource.(*ResourceWrapper[T]); ok {
		if wrapper.resetFunc != nil {
			return wrapper.resetFunc(wrapper.provider)
		}
		return wrapper.Reset()
	}
	return nil
}
