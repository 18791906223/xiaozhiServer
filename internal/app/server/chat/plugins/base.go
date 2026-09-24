package plugins

import "xiaozhi-esp32-server-golang/internal/domain/chat/streamtransform"

// Init 初始化输出相关 transform。
func Init(registry *streamtransform.Registry) {
	if registry == nil {
		return
	}

	// 注册输出整形插件（文本分段 + tool call 收口）
	RegisterOutputSegmenter(registry)
}
