// 文件说明：本地 MCP 工具的统一响应协议。
//
// 职责：约定“本地 MCP 工具”返回给上层的那串 JSON 长什么样，并把该 JSON 反向解析回
// Go 结构体。本地工具（local_mcp_tool.go / local_mcp_media_control_tool.go）不允许
// 直接返回任意文本，必须用这里的 NewXxxResponse 构造函数产出标准响应，这样上层
// (tool.go 的 executeToolCall) 才能用统一规则判断“要不要继续问 LLM / 要不要播音频”。
//
// 核心类型：
//   - MCPResponse：四种响应共同实现的接口，也是上层唯一依赖的抽象；
//   - MCPResponseBase：四种响应共享的公共字段，抽出公共部分是为了让 ParseMCPResponse
//     能先只解析 type 字段再决定往下走哪个分支（见文件末尾）；
//   - MCPActionResponse / MCPAudioResponse / MCPContentResponse / MCPErrorResponse。
//
// 在流程中的位置：
//
//	LLM 返回 ToolCalls
//	  -> llm.go: handleLLMResponse
//	  -> tool.go: toolCallExecutor.Submit / executeToolCall
//	  -> tool.go: handleLocalToolResult -> ParseMCPResponse（本文件）
//	  -> 按响应里的控制标志决定：是否继续请求 LLM、是否播放音频
//
// 与其他文件的配合：
//   - tool.go：调用 ParseMCPResponse 并读取 GetType/IsTerminal/GetContent，是唯一的消费方；
//   - local_mcp_tool.go、local_mcp_media_control_tool.go：通过便利构造函数生产响应；
//   - 远程 MCP 工具不走这套协议，那边是标准 mcp_go.CallToolResult。
package chat

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	mcp_go "github.com/mark3labs/mcp-go/mcp"
)

// MCPResponseType 定义MCP响应的类型
// 用字符串而不是 int 枚举，是为了让响应 JSON 自描述：日志里直接能看懂是哪种响应，
// 也方便将来在外部（如调试工具、抓包）按 type 过滤。
type MCPResponseType string

const (
	// 动作类：需要执行特定动作，通常会终止后续处理
	// 语义：工具“做了一件有副作用的事”（退出对话、切角色、控制播放）。
	// 是否真的终止由响应体里的 FinalAction / NoFurtherResponse / SilenceLLM 决定，
	// 所以“通常”而非“一定”——例如清空历史成功后还要让 LLM 回一句确认语。
	MCPResponseTypeAction MCPResponseType = "action"
	// 音频资源类：需要执行特定动作，通常会终止后续处理, 也不需要返回stop
	// 语义：工具产出了“要播放的音频数据”，必须终止流程。
	// 为什么 audio 一定要终止：音频一旦交给媒体播放器播放，就抢占了 TTS 音频通道，
	// 此时若再让 LLM 说话会出现两路声音打架；而且播放器会独占输出、不需要额外的 tts_stop，
	// 所以解析侧（tool.go）见到 AudioContent 会直接置 shouldStopLLMProcessing=true。
	MCPResponseTypeAudio MCPResponseType = "audio"

	// 内容类：返回信息内容，允许后续处理
	// 语义：只是“带回了一些资料”（检索命中片段、查询结果）。它是非终止的，
	// 因为内容本身没意义，必须再交给 LLM 组织成人话才能播给用户听。
	MCPResponseTypeContent MCPResponseType = "content"
	// 错误类：处理错误情况
	// 语义：工具失败了。同样是非终止的——失败原因要回给 LLM，由模型用自然语言
	// 向用户道歉或换方案，比直接静默断掉体验好得多。
	MCPResponseTypeError MCPResponseType = "error"
)

// MCPResponseBase 所有MCP响应的基础结构
// 抽成独立类型的动机：ParseMCPResponse 先把它解析一遍拿到 Type，
// 就能像“读信封”一样在不知道正文结构时决定该按哪种响应去解 body。
type MCPResponseBase struct {
	// Type 响应类型，决定 JSON 该按哪个子结构解析
	Type MCPResponseType `json:"type"`
	// Success 工具是否执行成功（错误类响应固定为 false）
	Success bool `json:"success"`
	// Timestamp 响应产生时间（秒级），用于排查“工具到底有没有被调用、什么时候调的”
	Timestamp int64 `json:"timestamp"`
	// ToolName 产生该响应的工具名，方便多工具并发时把日志/结果对上号
	ToolName string `json:"tool_name"`
}

// MCPActionResponse 动作类响应 - 用于播放音乐、退出对话等需要执行动作的场景
type MCPActionResponse struct {
	MCPResponseBase
	// Action 动作标识（如 exit_conversation、switch_device_role）。
	// tool.go 会专门比对它是否等于 "exit_conversation" 来触发退出会话。
	Action string `json:"action"`
	// Message 给用户/LLM 看的结果描述（会被转成文本内容回填给 LLM）
	Message string `json:"message"`
	// Status 动作执行后的状态（exiting / completed …），仅作信息透传
	Status string `json:"status"`
	// Metadata 额外键值信息，供日志排查与前端展示，不参与流程判断
	Metadata map[string]string `json:"metadata,omitempty"`
	// 控制标志
	// FinalAction 是否是最终动作：为 true 表示本轮工具链到此为止
	FinalAction bool `json:"final_action"`
	// NoFurtherResponse 是否禁止再产生任何回复（含 LLM 再次请求）
	NoFurtherResponse bool `json:"no_further_response"`
	// SilenceLLM 是否让 LLM 闭嘴：用于“用户只想操作、不想被搭话”的场景
	// （典型是音乐暂停/切歌，端上已经在动，再补一句语音反而吵）。
	SilenceLLM bool `json:"silence_llm"`
	// UserState 用户状态变更提示（如 conversation_ended），供上层钩子观察
	UserState string `json:"user_state"`
	// Instruction 给 LLM 的额外指令文本，用于明确“别再多说话”之类的要求
	Instruction string `json:"instruction,omitempty"`
}

// MCPActionResponse 动作类响应 - 用于播放音乐、退出对话等需要执行动作的场景
type MCPAudioResponse struct {
	MCPResponseBase
	// Data 原始音频字节。JSON 序列化时会自动 base64，所以这里放二进制即可。
	Data []byte `json:"data"`
	// MusicName 音频标题/曲名，会被作为文本内容与音频一起返回给 LLM 当上下文
	MusicName string `json:"music_name"`
	// Action 动作标识（如 play_music），供上层区分是哪个工具产出的音频
	Action string `json:"action"`
	// Status 状态描述，此处常传“播放提示语”类文本
	Status string `json:"status"`
	// Metadata 额外信息，仅用于日志/排查
	Metadata map[string]string `json:"metadata,omitempty"`
	// 控制标志
	// FinalAction 是否终止本轮（音频类默认 true：播了音频就不该再让 LLM 接话）
	FinalAction bool `json:"final_action"`
}

// MCPContentResponse 内容类响应 - 用于获取时间、查询信息等返回数据的场景
type MCPContentResponse struct {
	MCPResponseBase
	// Data 结构化数据本体（如知识库命中的 hits 列表），给上层/调试用
	Data interface{} `json:"data"`
	// Message 给 LLM 阅读的文本摘要，GetContent 就是把它转成文本内容
	Message string `json:"message"`
}

// MCPErrorResponse 错误类响应 - 统一的错误处理
type MCPErrorResponse struct {
	MCPResponseBase
	// Error 人类可读的错误描述，会被转成文本内容交给 LLM 组织回复
	Error string `json:"error"`
	// ErrorCode 机器可读的错误码（PARSE_ERROR、SEARCH_FAILED…），便于客户端/日志分类
	ErrorCode string `json:"error_code,omitempty"`
	// Details 更详细的上下文（可选）
	Details string `json:"details,omitempty"`
	// Suggestion 给用户的建议 例如“请稍后重试”
	Suggestion string `json:"suggestion,omitempty"` // 给用户的建议
}

// MCPResponse 统一的MCP响应接口
// 上层（tool.go）只依赖这个接口，不需要关心具体是哪种响应，
// 新增响应类型时只要实现接口即可，不必改动执行器逻辑。
type MCPResponse interface {
	GetType() MCPResponseType
	GetSuccess() bool
	IsTerminal() bool // 是否是终止性操作
	ToJSON() (string, error)
	GetContent() []mcp_go.Content
	GetAction() string // 获取动作类型
}

// 实现MCPResponse接口
//
// IsTerminal 语义：FinalAction 或 NoFurtherResponse 任一为真即在执行器里判为“终止”，
// 于是不会再带工具结果去请求 LLM（例如退出对话、切角色这类已经闭环的动作）。
func (r *MCPActionResponse) GetType() MCPResponseType { return MCPResponseTypeAction }
func (r *MCPActionResponse) GetSuccess() bool         { return r.Success }
func (r *MCPActionResponse) IsTerminal() bool         { return r.FinalAction || r.NoFurtherResponse }
func (r *MCPActionResponse) GetAction() string        { return r.Action }

// GetContent 把动作响应转成 MCP 标准内容列表。
// 上层只认识 mcp_go.Content，所以这里做一次“协议翻译”：Message 包成 TextContent，
// 让 LLM 能看到“工具做了什么”的文本证据，从而给出自然的收尾语。
func (r *MCPActionResponse) GetContent() []mcp_go.Content {
	return []mcp_go.Content{
		mcp_go.TextContent{
			Type: "text",
			Text: r.Message,
		},
	}
}

// 为MCPAudioResponse添加接口方法实现
//
// IsTerminal 只看 FinalAction：音频类响应本身就是终止性动作（音频交给播放器后
// 不应再有 LLM 语音输出），所以不需要再额外看 NoFurtherResponse 这类开关。
func (r *MCPAudioResponse) GetType() MCPResponseType { return MCPResponseTypeAudio }
func (r *MCPAudioResponse) GetSuccess() bool         { return r.Success }
func (r *MCPAudioResponse) IsTerminal() bool         { return r.FinalAction }
func (r *MCPAudioResponse) GetAction() string        { return r.Action }

// GetContent 把音频响应转成 MCP 标准内容列表（文本 + 音频）。
// 为什么 audio 一定要 base64：MCP 的内容类型要经过 JSON 序列化/网络传输，
// JSON 不能直接承载二进制，必须编码成文本。tool.go 收到后会用
// base64.StdEncoding.DecodeString 还原成原始字节再交给播放器。
func (r *MCPAudioResponse) GetContent() []mcp_go.Content {
	return []mcp_go.Content{
		mcp_go.TextContent{
			Type: "text",
			Text: r.MusicName,
		},
		mcp_go.AudioContent{
			Type:     "audio",
			Data:     base64.StdEncoding.EncodeToString(r.Data),
			MIMEType: "audio/mpeg",
		},
	}
}

func (r *MCPContentResponse) GetType() MCPResponseType { return MCPResponseTypeContent }
func (r *MCPContentResponse) GetSuccess() bool         { return r.Success }
func (r *MCPContentResponse) IsTerminal() bool         { return false } // 内容类通常不终止
func (r *MCPContentResponse) GetAction() string        { return "" }    // 内容类没有动作

// GetContent 内容类只暴露文本摘要（Message），Data 属于结构化附属信息，
// 不进 LLM 上下文（避免把大段 JSON 塞给模型浪费 token）。
func (r *MCPContentResponse) GetContent() []mcp_go.Content {
	return []mcp_go.Content{
		mcp_go.TextContent{
			Type: "text",
			Text: r.Message,
		},
	}
}

func (r *MCPErrorResponse) GetType() MCPResponseType { return MCPResponseTypeError }
func (r *MCPErrorResponse) GetSuccess() bool         { return r.Success }
func (r *MCPErrorResponse) IsTerminal() bool         { return false } // 错误类允许后续处理
func (r *MCPErrorResponse) GetAction() string        { return "" }    // 错误类没有动作
func (r *MCPErrorResponse) GetContent() []mcp_go.Content {
	return []mcp_go.Content{
		mcp_go.TextContent{
			Type: "text",
			Text: r.Error,
		},
	}
}

// ToJSON 方法实现
// 各响应类型都只是标准库 json.Marshal 的薄封装：工具 Handle 函数的返回值必须是
// 字符串，所以统一在最后一步序列化。序列化失败时把 error 一起返回，由上层记日志。
func (r *MCPActionResponse) ToJSON() (string, error) {
	data, err := json.Marshal(r)
	return string(data), err
}

// 为MCPAudioResponse添加ToJSON方法
func (r *MCPAudioResponse) ToJSON() (string, error) {
	data, err := json.Marshal(r)
	return string(data), err
}

func (r *MCPContentResponse) ToJSON() (string, error) {
	data, err := json.Marshal(r)
	return string(data), err
}

func (r *MCPErrorResponse) ToJSON() (string, error) {
	data, err := json.Marshal(r)
	return string(data), err
}

// 便利构造函数
// 这些构造函数的意义：把“该填哪些字段、默认值是什么、Success 该填 true 还是 false”
// 这些易错细节集中在一处，工具实现里只需表达意图，不必手写结构体字面量。
// 另外它们会强制设置 Type 字段，避免工具写错类型导致上层解析错分支。

// NewActionResponse 创建动作类响应
// 参数含义：
//   - toolName：产生响应的工具名，回填到 base.ToolName 便于日志对账；
//   - action：动作标识，tool.go 用它判断是否为 "exit_conversation"；
//   - message：给 LLM/用户的结果描述；
//   - status：动作状态字符串，仅透传；
//   - terminal：一个开关同时决定 FinalAction / NoFurtherResponse / SilenceLLM 三个标志
//     ——为 true 表示“动作已闭环，无需任何后续 LLM 回复”（如退出、切角色），
//     为 false 表示“动作做完了，但还是让 LLM 说一句确认语”（如清空历史）。
func NewActionResponse(toolName, action, message, status string, terminal bool) *MCPActionResponse {
	return &MCPActionResponse{
		MCPResponseBase: MCPResponseBase{
			Type:      MCPResponseTypeAction,
			Success:   true,
			Timestamp: time.Now().Unix(),
			ToolName:  toolName,
		},
		Action:            action,
		Message:           message,
		Status:            status,
		FinalAction:       terminal,
		NoFurtherResponse: terminal,
		SilenceLLM:        terminal,
	}
}

// NewAudioResponse 创建音频类响应 - 修正返回类型
// 参数含义：
//   - toolName/action/status：同上；
//   - terminal：映射到 FinalAction，通常传 true（播音频即终止本轮）；
//   - data：原始音频字节，序列化时自动 base64。
func NewAudioResponse(toolName, action, status string, terminal bool, data []byte) *MCPAudioResponse {
	return &MCPAudioResponse{
		MCPResponseBase: MCPResponseBase{
			Type:      MCPResponseTypeAudio,
			Success:   true,
			Timestamp: time.Now().Unix(),
			ToolName:  toolName,
		},
		Data:        data,
		Action:      action,
		Status:      status,
		FinalAction: terminal,
	}
}

// NewContentResponse 创建内容类响应
// 参数含义：data 是结构化附属数据（给调试/前端用），message 是给 LLM 读的文本摘要。
// 没有 terminal 参数：内容类恒为非终止，必须再走一轮 LLM 才能变成回复。
func NewContentResponse(toolName string, data interface{}, message string) *MCPContentResponse {
	return &MCPContentResponse{
		MCPResponseBase: MCPResponseBase{
			Type:      MCPResponseTypeContent,
			Success:   true,
			Timestamp: time.Now().Unix(),
			ToolName:  toolName,
		},
		Data:    data,
		Message: message,
	}
}

// NewErrorResponse 创建错误类响应
// 注意 Success 固定 false；error 是给用户看的描述，errorCode 是给程序看的分类码，
// suggestion 是“用户可以怎么办”的建议（LLM 常直接引用它来安抚用户）。
func NewErrorResponse(toolName, error, errorCode, suggestion string) *MCPErrorResponse {
	return &MCPErrorResponse{
		MCPResponseBase: MCPResponseBase{
			Type:      MCPResponseTypeError,
			Success:   false,
			Timestamp: time.Now().Unix(),
			ToolName:  toolName,
		},
		Error:      error,
		ErrorCode:  errorCode,
		Suggestion: suggestion,
	}
}

// ParseMCPResponse 从JSON字符串解析MCP响应
//
// 实现分两步的理由：四种响应的字段差异很大（audio 有 Data、error 有 ErrorCode…），
// 不可能一次解出具体类型。于是先只解公共的 MCPResponseBase，像“拆信封看抬头”一样
// 拿到 Type，再据此把同一份 JSON 二次解析成对应的具体响应类型。
// 未知 type 时返回一个 MCPErrorResponse 并把错误也返回，让上层既能记录原因、
// 又不会因为拿到 nil 而崩。
func ParseMCPResponse(jsonStr string) (MCPResponse, error) {
	// 第一步：只解出公共字段，目的是拿到 Type
	var base MCPResponseBase
	if err := json.Unmarshal([]byte(jsonStr), &base); err != nil {
		return nil, err
	}

	// 第二步：按 Type 分发到具体响应结构
	switch base.Type {
	// 动作类：按 MCPActionResponse 解，保留 FinalAction/SilenceLLM 等控制标志
	case MCPResponseTypeAction:
		var response MCPActionResponse
		if err := json.Unmarshal([]byte(jsonStr), &response); err != nil {
			return nil, err
		}
		return &response, nil
	// 音频类：按 MCPAudioResponse 解，Data 会被自动 base64 解开成 []byte
	case MCPResponseTypeAudio:
		var response MCPAudioResponse
		if err := json.Unmarshal([]byte(jsonStr), &response); err != nil {
			return nil, err
		}
		return &response, nil
	// 内容类：按 MCPContentResponse 解
	case MCPResponseTypeContent:
		var response MCPContentResponse
		if err := json.Unmarshal([]byte(jsonStr), &response); err != nil {
			return nil, err
		}
		return &response, nil
	// 错误类：按 MCPErrorResponse 解
	case MCPResponseTypeError:
		var response MCPErrorResponse
		if err := json.Unmarshal([]byte(jsonStr), &response); err != nil {
			return nil, err
		}
		return &response, nil
	// 未知类型：兜底成错误响应 + error。
	// 之所以两种都返回：上层用 error 判断“这不是本地 MCP 协议”，
	// 从而回退到远程 MCP 的解析路径（见 tool.go 的 handleLocalToolResult / handleToolResult）。
	default:
		return NewErrorResponse("unknown", "未知的响应类型", "INVALID_TYPE", "请检查工具实现"), fmt.Errorf("未知的响应类型: %s", base.Type)
	}
}
