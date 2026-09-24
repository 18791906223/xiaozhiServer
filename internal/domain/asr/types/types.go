package types

const (
	EmptyReasonNone               = ""
	EmptyReasonNoServerResponse   = "no_server_response"
	EmptyReasonProviderEmptyFinal = "provider_empty_final"

	RetryReasonNone                           = ""
	RetryReasonDoubaoResponseCode45000081     = "doubao_response_code_45000081"
	RetryReasonDoubaoWaitingNextPacketTimeout = "doubao_waiting_next_packet_timeout"
	RetryReasonXunfeiServiceInstanceInvalid   = "xunfei_service_instance_invalid"
	RetryReasonAliyunQwen3ConnectionClosed    = "aliyun_qwen3_connection_closed"
)

// StreamingResult 流式识别结果
type StreamingResult struct {
	Text        string // 识别的文本
	IsFinal     bool   // 是否为最终结果
	Error       error  // 错误信息
	AsrType     string // asr 类型
	Mode        string // 模式
	EmptyReason string // 空结果原因，仅在 Text 为空时用于区分上游空结果/空转
	RetryReason string // 可恢复错误原因，仅在需要释放当前资源并重试时使用
}
