// 文件：types.go
// 职责：定义 ChatSessionOperator 接口——工具层调用会话能力的契约。
//
// 为什么需要这个接口（核心设计动机）：
// 本地 MCP 工具（local_mcp_tool.go）需要「关会话、清历史、切角色、检索知识库、控制音乐」这类
// 只有 ChatSession 才有的能力，但 ChatSession 又依赖 LLMManager 去执行工具调用。
// 若工具层直接引用 ChatSession，就会形成 ChatSession ↔ 工具层 的循环依赖（Go 不允许）。
// 因此抽出这个接口放在会话与工具之间：工具层只依赖接口，
// ChatSession 实现它并通过 context 传给工具，双方都只依赖抽象。
//
// 使用方式：ChatSession 把自己以 ctx.Value("chat_session_operator") 的形式放进上下文，
// 工具执行时取出来断言成 ChatSessionOperator 再调用对应方法。
//
// 在流程中的位置：LLM 工具调用阶段（llm.go / tool.go / local_mcp_tool.go）。
// 与哪些文件配合：
//   - local_mcp_tool.go：注册本地工具，并在 handler 里取出本接口调用。
//   - session_mcp_tool.go：接口方法在 ChatSession 上的具体实现。
//   - media_player.go / local_mcp_media_control_tool.go：媒体控制相关的参数与结果类型。
package chat

import (
	"context"

	config_types "xiaozhi-esp32-server-golang/internal/domain/config/types"
)

// ChatSessionOperator 定义 local mcp tool 需要的 ChatSession 操作接口
// 这个接口用于解耦 LLMManager 和 ChatSession，避免循环依赖
type ChatSessionOperator interface {
	// LocalMcpCloseChat 关闭聊天会话
	// 对应本地工具 "exit_conversation"：用户说「退出/再见」等由 LLM 决定调用时，走它结束会话。
	LocalMcpCloseChat() error

	// LocalMcpClearHistory 清空历史对话
	// 对应本地工具 "clear_conversation_history"：清掉内存上下文，
	// 让助手「忘掉」之前的对话（设备侧 UI 也会因清空而不再有旧消息）。
	LocalMcpClearHistory() error

	// LocalMcpPlayMusic 播放音乐
	// 对应本地工具 "play_music"：播放指定音乐/搜索关键词命中的音频。
	LocalMcpPlayMusic(ctx context.Context, params *PlayMusicParams) error

	// LocalMcpSwitchDeviceRole 按角色名称切换设备角色（支持模糊匹配）
	// 对应本地工具 "switch_device_role"：切换人格/音色（例如「用讲故事的语气说话」）。
	// 返回实际匹配到的角色名，便于告诉用户命中了哪个角色。
	LocalMcpSwitchDeviceRole(ctx context.Context, roleName string) (string, error)

	// LocalMcpRestoreDeviceDefaultRole 恢复设备默认角色
	// 对应本地工具 "restore_device_default_role"：把角色切回设备配置里的默认角色。
	LocalMcpRestoreDeviceDefaultRole(ctx context.Context) error

	// LocalMcpSearchKnowledge 检索当前智能体关联知识库
	// 对应本地工具 "search_knowledge"：按 query 做向量/关键词检索，返回命中片段。
	// 注意：没有可用知识库时上层会移除这个工具，因此它的存在与否也暗示了「是否有知识库」。
	LocalMcpSearchKnowledge(ctx context.Context, query string, topK int, knowledgeBaseIDs []uint) ([]config_types.KnowledgeSearchHit, error)

	// LocalMcpControlMusicPlayback 控制当前会话级媒体播放
	// 对应「媒体控制」能力（local_mcp_media_control_tool.go）：暂停/继续/切歌/歌单等。
	// realtime 门控（realtime_media_gate.go）识别到本地指令时，也直接调它执行动作。
	LocalMcpControlMusicPlayback(ctx context.Context, params *MusicPlaybackControlParams) (*MusicPlaybackControlResult, error)

	// 未来可以根据需要添加其他操作
	// GetDeviceID() string
	// IsActive() bool
}
