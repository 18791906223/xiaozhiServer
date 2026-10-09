// 文件说明：本地 MCP 工具的“集中注册”与实现（会话类工具）。
//
// 职责：把一批需要读写当前会话状态的工具注册进本地 MCP 管理器，并实现它们的
// 处理函数（Handle）。这些工具不直接持有 ChatSession，而是从 ctx 里取出
// ChatSessionOperator（定义见 types.go）回调会话能力，从而避免“工具层依赖会话层”
// 造成循环依赖。
//
// 核心类型/函数：
//   - LocalMcpTool：一条注册项（名字 + 描述 + 入参结构 + 处理函数）；
//   - InitChatLocalMCPTools：集中注册入口（带 local_mcp.<name> 配置开关）；
//   - RegisterLocalMcpFunc：单个工具注册的公共函数（media control 工具用它自注册）；
//   - SwitchDeviceRoleParams / SearchKnowledgeParams：工具的入参结构；
//   - exit/clear/switch/restore/search 五个 Handler：工具的实际逻辑。
//
// 在流程中的位置：
//
//	进程启动 -> app.go: chat.RegisterChatMCPTools() -> InitChatLocalMCPTools()
//	  -> mcp_manager.GetLocalMCPManager().RegisterToolFunc(...)  注册进全局表
//	  -> LLM 决定调用 -> tool.go: executeToolCall -> 上面注册的 Handler
//	  -> Handler 通过 ctx.Value("chat_session_operator") 调 session_mcp_tool.go 的实现
//	  -> 返回 mcp_response_types.go 定义的统一响应 JSON
//
// 与其他文件的配合：
//   - session_mcp_tool.go：Handler 背后真正的会话实现（切角色、检索知识库…）；
//   - mcp_response_types.go：所有 Handler 的返回值都用它的 NewXxxResponse 构造；
//   - local_mcp_media_control_tool.go：媒体控制工具不走这里的集中注册，而是用
//     RegisterLocalMcpFunc 在 init() 里自注册（见该文件的说明）；
//   - internal/domain/mcp：LocalMCPManager / RegisterToolFunc / LocalToolHandler。
package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	mcp_manager "xiaozhi-esp32-server-golang/internal/domain/mcp"
	log "xiaozhi-esp32-server-golang/logger"

	//"github.com/scroot/music-sd/pkg/netease"
	//"github.com/scroot/music-sd/pkg/qq"
	"github.com/spf13/viper"
)

// LocalMcpTool 一条“本地工具注册项”：注册一个工具所需的四要素。
//
// 为什么用这个小结构体而不是直接调 RegisterToolFunc：
// 好处是能把整批工具先放进 map，再统一做“配置开关判断 + 注册”循环，
// 注册逻辑只写一遍；也便于以后加字段（如分组、权限）而不改调用点。
type LocalMcpTool struct {
	// Name 工具名，会作为 LLM 的 function 名；必须与 localTools map 的 key 一致
	Name string
	// Description 工具描述，直接决定模型“什么场景该调用它”（写得越具体越不容易误用）
	Description string
	// Params 入参结构体（`struct{}{}` 表示无参数），会被转成 JSON Schema 告诉模型参数格式
	Params any
	// Handle 真正执行工具的函数（类型见 internal/domain/mcp.LocalToolHandler）
	Handle mcp_manager.LocalToolHandler
}

// InitChatLocalMCPTools 初始化聊天相关的本地MCP工具
//
// 注册到 mcp_manager.GetLocalMCPManager()（全局单例）。为什么这些工具必须是“本地”的：
// 退出对话、清空历史、切角色、检索知识库都依赖本进程内的会话/配置/检索能力，
// 没有对应的远程 MCP 服务可调，只有注册成本地工具，LLM 才能像调用普通工具一样调用它们。
func InitChatLocalMCPTools() {
	// 单例（内部用 sync.Once 懒初始化），所有设备/会话共享同一张工具注册表
	manager := mcp_manager.GetLocalMCPManager()

	log.Info("初始化聊天相关的本地MCP工具...")

	// 用 map 承载“待注册清单”，便于统一遍历并统一应用配置开关
	localTools := map[string]LocalMcpTool{
		/*"get_current_datetime": {
			Name:        "get_current_datetime",
			Description: "获取当前时间和日期信息",
			Params:      struct{}{},
			Handle:      getCurrentDateTimeHandler,
		},*/
		// 结束对话：LLM 识别到“不聊了 / 再见”时调用，走事件通知上层优雅关闭会话
		"exit_conversation": {
			Name:        "exit_conversation",
			Description: "当用户明确表示要结束对话、退出系统或告别时使用，用于优雅地关闭当前聊天会话",
			Params:      struct{}{},
			Handle:      exitConversationHandler,
		},
		// 清空历史：只清上下文，不结束会话（用户想“重新开始聊”）
		"clear_conversation_history": {
			Name:        "clear_conversation_history",
			Description: "当用户要求清空、清除或重置历史对话记录时使用，用于清空当前会话的所有历史对话内容",
			Params:      struct{}{},
			Handle:      clearConversationHistoryHandler,
		},
		// 切换设备角色：真正的匹配逻辑在 session_mcp_tool.go，支持模糊匹配
		"switch_device_role": {
			Name:        "switch_device_role",
			Description: "当用户要求把当前设备切换到某个角色时使用，参数 role_name 支持模糊匹配（会在全局角色和该设备所属用户角色中匹配）",
			Params:      SwitchDeviceRoleParams{},
			Handle:      switchDeviceRoleHandler,
		},
		// 恢复默认角色：撤销设备级角色覆盖，回到设备/用户配置里定义的角色
		"restore_device_default_role": {
			Name:        "restore_device_default_role",
			Description: "当用户要求恢复设备默认角色、取消当前设备角色覆盖时使用",
			Params:      struct{}{},
			Handle:      restoreDeviceDefaultRoleHandler,
		},
		// 知识库检索：描述里特意加“闲聊或纯创作场景不要调用”，
		// 因为检索有延迟，模型乱调会拖慢响应。
		"search_knowledge": {
			Name:        "search_knowledge",
			Description: "当用户问题需要事实依据、流程规则、参数细节、文档条款时，检索当前智能体关联知识库并返回相关片段；可选传 knowledge_base_ids 仅查指定知识库；闲聊或纯创作场景不要调用",
			Params:      SearchKnowledgeParams{},
			Handle:      searchKnowledgeHandler,
		},
		/*"play_music": {
			Name:        "play_music",
			Description: "当用户想听歌、无聊时、想放空大脑时使用，用于播放指定名称的音乐，当用户想随便听一首音乐时请推荐出具体的歌曲名称，当有多个音乐播放工具时优先使用此工具，**此工具调用耗时较长，需要先返回友好的过渡性提示语**",
			Params:      PlayMusicParams{},
			Handle:      playMusicHandler,
		},*/
	}

	for toolName, localTool := range localTools {
		// 只有当配置明确设为false时才跳过，配置不存在或为true时都启用
		// 配置语义：local_mcp.<工具名> = false 表示“关掉这个工具”；
		// 不写配置 = 默认启用（这样新增工具无需同步改配置文件即可生效，避免漏配）。
		if viper.IsSet("local_mcp."+toolName) && !viper.GetBool("local_mcp."+toolName) {
			continue
		}
		// 注册进全局表；注册后 mcp.GetToolByNameWithTransport 才能按名字找到它
		err := manager.RegisterToolFunc(
			localTool.Name,
			localTool.Description,
			localTool.Params,
			localTool.Handle,
		)
		if err != nil {
			// 单个工具注册失败不影响其它工具，记录日志继续
			log.Errorf("注册本地MCP工具 %s 失败: %+v", toolName, err)
		}
	}

	log.Info("聊天相关的本地MCP工具初始化完成")
}

// RegisterLocalMcpFunc 注册单个本地 MCP 工具。
// 与 InitChatLocalMCPTools 的集中注册不同，这个是给“希望自己管理注册时机”的工具用的
// （典型是 local_mcp_media_control_tool.go 在自己的 init() 里调用），
// 这样新增独立工具时不必修改本文件的注册清单，减少改同一处代码的冲突。
func RegisterLocalMcpFunc(name string, description string, params any, handle mcp_manager.LocalToolHandler) error {
	manager := mcp_manager.GetLocalMCPManager()

	err := manager.RegisterToolFunc(
		name,
		description,
		params,
		handle,
	)
	if err != nil {
		log.Errorf("注册本地MCP工具 %s 失败: %+v", name, err)
		return err
	}
	return nil
}

// SwitchDeviceRoleParams switch_device_role 的入参。
// required:"true" 会体现在生成的参数描述里，引导模型必须给出角色名。
type SwitchDeviceRoleParams struct {
	// RoleName 目标角色名称，服务端做模糊匹配（不要求用户说全称）
	RoleName string `json:"role_name" description:"目标角色名称，支持模糊匹配" required:"true"`
}

// SearchKnowledgeParams search_knowledge 的入参。
type SearchKnowledgeParams struct {
	// Query 检索内容，必填；为空时 Handler 直接返回错误响应而不去检索
	Query string `json:"query" description:"要检索的查询内容" required:"true"`
	// TopK 返回条数；omitempty 表示模型可以不传，Handler 里会给默认值 5
	TopK int `json:"top_k,omitempty" description:"返回条数，默认5"`
	// KnowledgeBaseIDs 只在这些知识库内检索（可选）。
	// 安全边界：即使模型传了别的知识库 ID，最终也会被限制在该智能体已关联的知识库内
	// （见 session_mcp_tool.go 的 LocalMcpSearchKnowledge 实现）。
	KnowledgeBaseIDs []uint `json:"knowledge_base_ids,omitempty" description:"可选：仅在这些知识库ID内检索（当前智能体已关联）"`
}

// playMusicHandler 播放音乐的处理函数
//
// 注意：该工具当前未在注册清单里启用（play_music 注册项被注释），
// 这个 Handler 与它依赖的 GetMusicAudioData 一起被保留下来备查/将来复用。
func playMusicHandler(ctx context.Context, argumentsInJSON string) (string, error) {
	log.Info("执行播放音乐工具")

	// 解析参数
	var params PlayMusicParams

	// 参数可能为空字符串（模型有时不传参数），此时保持零值即可，不必报错
	if argumentsInJSON != "" {
		if err := json.Unmarshal([]byte(argumentsInJSON), &params); err != nil {
			// 参数错也要返回“结构化的错误响应”而不是 Go error，
			// 这样上层能把它当普通工具结果回给 LLM，由模型向用户解释。
			response := NewErrorResponse("play_music", "参数解析失败", "PARSE_ERROR", "请检查参数格式是否正确")
			return response.ToJSON()
		}
	}

	log.Infof("找到ChatSessionOperator，正在调用LocalMcpPlayMusic方法播放音乐: %s", params.Name)
	// 直接拉取整首音频字节（旧实现：会阻塞较久，故工具描述里要求先给过渡提示语）
	audioData, realMusicName, err := GetMusicAudioData(ctx, &params)
	if err != nil {
		log.Errorf("获取音乐数据失败: %v", err)
		response := NewErrorResponse("play_music", fmt.Sprintf("获取音乐数据失败: %v", err), "PLAYBACK_ERROR", "请检查音乐名称或网络连接")
		return response.ToJSON()
	} else {
		// 成功播放 - 动作类响应，终止后续处理
		// 用 NewAudioResponse（而非 Action）的原因：音频数据要交给媒体播放器播放，
		// terminal=true 保证本轮不再让 LLM 说话，避免与音乐声重叠。
		response := NewAudioResponse("play_music", "play_music", fmt.Sprintf("开始播放音乐: %s", realMusicName), true, audioData)
		response.MusicName = realMusicName
		return response.ToJSON()
	}

}

// 以下大段是被注释保留的历史实现（“获取当前时间”工具）。
// 为什么保留而不删除：这是一份可复用的参考实现，它依赖的三个辅助函数
// （formatChineseDateTime / getWeekdayChinese / getWeekNumber）也一并保留；
// 当前“现在几点/今天几号”已由 system prompt 直接注入时间（见 llm.go GetMessages），
// 不再需要额外一次工具调用（工具调用会多一轮 LLM 往返，更慢）。
/*
// getCurrentDateTimeHandler 获取当前时间和日期的处理函数
func getCurrentDateTimeHandler(ctx context.Context, argumentsInJSON string) (string, error) {
	log.Info("执行获取当前时间日期工具")

	// 解析参数
	var params map[string]interface{}
	timezone := "Local" // 默认时区

	if argumentsInJSON != "" {
		if err := json.Unmarshal([]byte(argumentsInJSON), &params); err == nil {
			if tz, ok := params["timezone"].(string); ok && tz != "" {
				timezone = tz
			}
		}
	}

	now := time.Now()

	// 尝试解析指定的时区
	if timezone != "Local" {
		if loc, err := time.LoadLocation(timezone); err == nil {
			now = now.In(loc)
		} else {
			log.Warnf("无法加载时区 %s，使用本地时区", timezone)
		}
	}

	// 构造返回数据
	data := map[string]interface{}{
		"datetime": map[string]interface{}{
			"formatted":     now.Format("2006-01-02 15:04:05"),
			"iso8601":       now.Format(time.RFC3339),
			"chinese":       formatChineseDateTime(now),
			"unix":          now.Unix(),
			"year":          now.Year(),
			"month":         int(now.Month()),
			"day":           now.Day(),
			"hour":          now.Hour(),
			"minute":        now.Minute(),
			"second":        now.Second(),
			"weekday":       now.Weekday().String(),
			"weekday_zh":    getWeekdayChinese(now.Weekday()),
			"week_number":   getWeekNumber(now),
			"timezone":      timezone,
			"timezone_name": now.Location().String(),
		},
	}

	// 创建内容类响应
	response := NewContentResponse("get_current_datetime", data, fmt.Sprintf("当前时间：%s", formatChineseDateTime(now)))
	// response.Format = "datetime"
	// response.DisplayHint = "可用于显示当前日期时间信息"

	log.Infof("获取当前时间日期成功: %s", now.Format("2006-01-02 15:04:05"))
	return response.ToJSON(),nil
}
*/
// exitConversationHandler 退出对话的处理函数
//
// 设计要点（终止性工具的标准范式）：
//  1. 先构造“动作响应”，把控制标志置位，让上层停止继续请求 LLM；
//  2. 真正关会话的动作交给 ChatSessionOperator（本地工具不直接触碰会话对象）；
//  3. 即使拿不到 operator 也照样返回成功响应——工具本身成功识别了意图，
//     关会话失败只影响“是否立即断开”，不该让模型以为工具坏了。
func exitConversationHandler(ctx context.Context, argumentsInJSON string) (string, error) {
	log.Info("执行退出对话工具")

	// 解析参数
	var params map[string]interface{}
	reason := "用户主动退出" // 默认原因

	if argumentsInJSON != "" {
		if err := json.Unmarshal([]byte(argumentsInJSON), &params); err == nil {
			if r, ok := params["reason"].(string); ok && r != "" {
				reason = r
			}
		}
	}

	// 创建动作类响应 - 终止性操作
	// terminal=true 会同时打开 FinalAction / NoFurtherResponse / SilenceLLM：
	// 已经要退出了，任何额外的语音回复都是多余的。
	response := NewActionResponse("exit_conversation", "exit_conversation", "对话即将结束，感谢您的使用！", "exiting", true)
	response.UserState = "conversation_ended"
	// Instruction 是给模型看的“别再说话”的显式指令（双保险，避免模型仍生成告别语）
	response.Instruction = "对话已结束，请不要生成额外的文本回复"
	response.Metadata = map[string]string{
		"reason":           reason,
		"exit_code":        "0",
		"farewell_chinese": "再见！期待下次与您交流。",
		"farewell_english": "Goodbye! Looking forward to our next conversation.",
	}

	log.Infof("退出对话处理完成，原因: %s", reason)

	// 从context中获取ChatSessionOperator并调用Close方法
	// 工具层只依赖这个接口（types.go），因此不需要 import ChatSession，
	// 也就不会产生“工具包 <-> 会话包”的循环依赖。
	if chatSessionOperatorValue := ctx.Value("chat_session_operator"); chatSessionOperatorValue != nil {
		if chatSessionOperator, ok := chatSessionOperatorValue.(ChatSessionOperator); ok {
			log.Info("找到ChatSessionOperator，正在调用Close方法关闭会话")
			// defer：先让本函数把响应返回给调用方（工具结果要写进历史），
			// 再关会话；立刻关会导致“退出”这件事来不及被记录下来。
			defer chatSessionOperator.LocalMcpCloseChat()
		} else {
			log.Warn("从context中获取的chat_session_operator不是ChatSessionOperator类型")
		}
	} else {
		log.Warn("从context中未找到chat_session_operator")
	}

	responseStr, err := response.ToJSON()
	if err != nil {
		return "", err
	}

	return responseStr, nil
}

// clearConversationHistoryHandler 清空历史对话的处理函数
//
// 与退出对话的区别：这是“非终止”动作（terminal=false），
// 因为用户通常期望听到一句“已经清空了”的确认语，所以要让 LLM 继续说话。
func clearConversationHistoryHandler(ctx context.Context, argumentsInJSON string) (string, error) {
	log.Info("执行清空历史对话工具")

	// 解析参数
	var params map[string]interface{}
	reason := "用户主动清空历史" // 默认原因

	if argumentsInJSON != "" {
		if err := json.Unmarshal([]byte(argumentsInJSON), &params); err == nil {
			if r, ok := params["reason"].(string); ok && r != "" {
				reason = r
			}
		}
	}

	// 从context中获取ChatSessionOperator并调用LocalMcpClearHistory方法
	if chatSessionOperatorValue := ctx.Value("chat_session_operator"); chatSessionOperatorValue != nil {
		if chatSessionOperator, ok := chatSessionOperatorValue.(ChatSessionOperator); ok {
			log.Info("找到ChatSessionOperator，正在调用LocalMcpClearHistory方法清空历史")
			if err := chatSessionOperator.LocalMcpClearHistory(); err != nil {
				// 这里返回 Go error 而不是错误响应：
				// 属于“内部实现异常”，交给执行器统一记日志并生成失败文本。
				log.Errorf("清空历史对话失败: %v", err)
				return "", err
			} else {
				// 成功清空 - 动作类响应，但不终止对话
				response := NewActionResponse("clear_conversation_history", "clear_history", "历史对话已成功清空，您可以开始全新的对话。", "completed", false)
				response.Metadata = map[string]string{
					"reason": reason,
					"status": "cleared",
				}
				log.Info("历史对话清空成功")

				return response.ToJSON()
			}
		} else {
			log.Warn("从context中获取的chat_session_operator不是ChatSessionOperator类型")
			return "", fmt.Errorf("从context中获取的chat_session_operator不是ChatSessionOperator类型")
		}
	}
	log.Warn("从context中未找到chat_session_operator")
	return "", fmt.Errorf("从context中未找到chat_session_operator")
}

// switchDeviceRoleHandler 切换设备角色的处理函数
//
// 参数校验全部返回“错误响应”而非 Go error：这些是“模型给错了参数”的情况，
// 需要让模型看到原因并自己改正（例如换个角色名重试），而不是让整轮直接失败。
func switchDeviceRoleHandler(ctx context.Context, argumentsInJSON string) (string, error) {
	log.Info("执行切换设备角色工具")

	var params SwitchDeviceRoleParams
	if argumentsInJSON == "" {
		response := NewErrorResponse("switch_device_role", "缺少参数 role_name", "MISSING_ROLE_NAME", "请提供要切换的角色名称")
		return response.ToJSON()
	}
	if err := json.Unmarshal([]byte(argumentsInJSON), &params); err != nil {
		response := NewErrorResponse("switch_device_role", "参数解析失败", "PARSE_ERROR", "请检查 role_name 参数格式")
		return response.ToJSON()
	}
	// TrimSpace 后再判空：模型偶尔会传只有空格的字符串
	params.RoleName = strings.TrimSpace(params.RoleName)
	if params.RoleName == "" {
		response := NewErrorResponse("switch_device_role", "角色名称不能为空", "INVALID_ROLE_NAME", "请提供有效的 role_name")
		return response.ToJSON()
	}

	if chatSessionOperatorValue := ctx.Value("chat_session_operator"); chatSessionOperatorValue != nil {
		if chatSessionOperator, ok := chatSessionOperatorValue.(ChatSessionOperator); ok {
			// 模糊匹配 + 落库切角色 + 刷新设备配置都在 session_mcp_tool.go 里完成，
			// 返回值是“实际匹配到的角色名”，用于给用户确认（可能 != 请求的名字）。
			matchedRoleName, err := chatSessionOperator.LocalMcpSwitchDeviceRole(ctx, params.RoleName)
			if err != nil {
				log.Errorf("切换设备角色失败: %v", err)
				response := NewErrorResponse("switch_device_role", fmt.Sprintf("切换角色失败: %v", err), "SWITCH_ROLE_FAILED", "请尝试更换角色名称或稍后重试")
				return response.ToJSON()
			}

			// 非终止（false）：让 LLM 用新角色的人设回一句确认语，
			// 这样“切换角色”这个动作对用户是有反馈的。
			response := NewActionResponse(
				"switch_device_role",
				"switch_device_role",
				fmt.Sprintf("已切换到角色：%s", matchedRoleName),
				"completed",
				false,
			)
			response.Metadata = map[string]string{
				"requested_role_name": params.RoleName,
				"matched_role_name":   matchedRoleName,
			}
			return response.ToJSON()
		}
		return "", fmt.Errorf("从context中获取的chat_session_operator不是ChatSessionOperator类型")
	}

	return "", fmt.Errorf("从context中未找到chat_session_operator")
}

// restoreDeviceDefaultRoleHandler 恢复设备默认角色的处理函数
// 语义与切角色对称：非终止，让 LLM 以（恢复后的）默认人设回一句确认。
func restoreDeviceDefaultRoleHandler(ctx context.Context, argumentsInJSON string) (string, error) {
	log.Info("执行恢复设备默认角色工具")

	if chatSessionOperatorValue := ctx.Value("chat_session_operator"); chatSessionOperatorValue != nil {
		if chatSessionOperator, ok := chatSessionOperatorValue.(ChatSessionOperator); ok {
			if err := chatSessionOperator.LocalMcpRestoreDeviceDefaultRole(ctx); err != nil {
				log.Errorf("恢复设备默认角色失败: %v", err)
				response := NewErrorResponse("restore_device_default_role", fmt.Sprintf("恢复默认角色失败: %v", err), "RESTORE_ROLE_FAILED", "请稍后重试")
				return response.ToJSON()
			}

			response := NewActionResponse(
				"restore_device_default_role",
				"restore_device_default_role",
				"已恢复设备默认角色",
				"completed",
				false,
			)
			return response.ToJSON()
		}
		return "", fmt.Errorf("从context中获取的chat_session_operator不是ChatSessionOperator类型")
	}

	return "", fmt.Errorf("从context中未找到chat_session_operator")
}

// searchKnowledgeHandler 知识库检索的处理函数。
//
// 返回值形态的选择很关键：
//   - 命中内容用 ContentResponse（非终止）——检索结果本身没意义，必须再让 LLM
//     把它组织成自然语言回答；
//   - 参数错误用 ErrorResponse（非终止）——让模型知道该怎么改；
//   - operator 缺失用 Go error——那是内部接线问题，不是模型的错。
func searchKnowledgeHandler(ctx context.Context, argumentsInJSON string) (string, error) {
	log.Info("执行知识库检索工具")

	var params SearchKnowledgeParams
	if argumentsInJSON != "" {
		if err := json.Unmarshal([]byte(argumentsInJSON), &params); err != nil {
			response := NewErrorResponse("search_knowledge", "参数解析失败", "PARSE_ERROR", "请检查 query 参数格式")
			return response.ToJSON()
		}
	}
	params.Query = strings.TrimSpace(params.Query)
	if params.Query == "" {
		response := NewErrorResponse("search_knowledge", "query 不能为空", "INVALID_QUERY", "请提供要检索的内容")
		return response.ToJSON()
	}
	// 默认返回 5 条：模型经常不传 top_k，给个合理默认避免检索不出东西或返回过多。
	if params.TopK <= 0 {
		params.TopK = 5
	}

	chatSessionOperatorValue := ctx.Value("chat_session_operator")
	if chatSessionOperatorValue == nil {
		return "", fmt.Errorf("从context中未找到chat_session_operator")
	}
	chatSessionOperator, ok := chatSessionOperatorValue.(ChatSessionOperator)
	if !ok {
		return "", fmt.Errorf("从context中获取的chat_session_operator不是ChatSessionOperator类型")
	}

	// 真正的检索：知识库范围由会话侧决定（只允许检索该智能体已关联的知识库）
	hits, err := chatSessionOperator.LocalMcpSearchKnowledge(ctx, params.Query, params.TopK, params.KnowledgeBaseIDs)
	if err != nil {
		response := NewErrorResponse("search_knowledge", fmt.Sprintf("信息检索失败: %v", err), "SEARCH_FAILED", "请稍后重试")
		return response.ToJSON()
	}

	// data 里带结构化原始命中（调试/前端可用），message 才是给模型读的文本
	data := map[string]interface{}{
		"query": params.Query,
		"hits":  hits,
		"count": len(hits),
	}
	if len(hits) == 0 {
		// 检索为空也要返回 Content：让模型能回答“我没找到相关资料”，
		// 比静默失败或报错更自然。
		response := NewContentResponse("search_knowledge", data, "未找到足够相关信息")
		return response.ToJSON()
	}

	// 把命中拼成"1. 片段…"的编号文本。两条约束都是为了控制上下文长度：
	//   - 单条截断到 200 字符，避免一篇长文档挤满模型上下文；
	//   - 空片段直接跳过，避免出现"1. "这种无内容的行。
	var builder strings.Builder
	for i, hit := range hits {
		content := strings.TrimSpace(hit.Content)
		if content == "" {
			continue
		}
		if len(content) > 200 {
			content = content[:200] + "..."
		}
		builder.WriteString(fmt.Sprintf("%d. %s\n", i+1, content))
	}
	msg := strings.TrimSpace(builder.String())
	if msg == "" {
		msg = "已获取相关信息"
	}
	response := NewContentResponse("search_knowledge", data, msg)
	return response.ToJSON()
}

// getWeekNumber 获取周数
// 说明：仅供被注释保留的 getCurrentDateTimeHandler 使用（当前无调用方）。
func getWeekNumber(t time.Time) int {
	_, week := t.ISOWeek()
	return week
}

// formatChineseDateTime 格式化中文日期时间
// 说明：仅供被注释保留的 getCurrentDateTimeHandler 使用（当前无调用方）。
func formatChineseDateTime(t time.Time) string {
	weekdays := map[time.Weekday]string{
		time.Sunday:    "星期日",
		time.Monday:    "星期一",
		time.Tuesday:   "星期二",
		time.Wednesday: "星期三",
		time.Thursday:  "星期四",
		time.Friday:    "星期五",
		time.Saturday:  "星期六",
	}

	return fmt.Sprintf("%d年%d月%d日 %s %02d:%02d:%02d",
		t.Year(), int(t.Month()), t.Day(),
		weekdays[t.Weekday()],
		t.Hour(), t.Minute(), t.Second(),
	)
}

// getWeekdayChinese 获取中文星期几
// 说明：仅供被注释保留的 getCurrentDateTimeHandler 使用（当前无调用方）。
func getWeekdayChinese(weekday time.Weekday) string {
	weekdays := map[time.Weekday]string{
		time.Sunday:    "星期日",
		time.Monday:    "星期一",
		time.Tuesday:   "星期二",
		time.Wednesday: "星期三",
		time.Thursday:  "星期四",
		time.Friday:    "星期五",
		time.Saturday:  "星期六",
	}
	return weekdays[weekday]
}

// RegisterChatMCPTools 公共函数，供外部调用注册聊天MCP工具
//
// 存在的意义：把“聊天侧工具注册”收敛成一个对外函数，调用方（app.go 启动流程）
// 不需要知道内部有集中注册与自注册两种机制，只要调这一个入口即可。
func RegisterChatMCPTools() {
	InitChatLocalMCPTools()
}

// 播放音乐
//
// GetMusicAudioData 按歌名搜索并下载整首音频，返回 (音频字节, 真实歌名, error)。
// 说明：仅供被注释保留的 playMusicHandler 使用（当前无调用方）——
// 现在播放走的是“工具返回 AudioContent -> tool.go -> 媒体播放器”的链路，
// 这里保留的是早期“一次性把整首歌读进内存再播”的实现。
func GetMusicAudioData(ctx context.Context, musicParams *PlayMusicParams) ([]byte, string, error) {
	musicName := musicParams.Name
	//welcome := musicParams.Welcome
	welcome := ""
	log.Infof("搜索音乐: %s 中, welcome: %s", musicName, welcome)
	// 这里可以根据音乐名称获取音乐URL
	// 目前简化实现，假设musicName就是URL或者从配置中获取
	musicURL, realMusicName, ierr := getMusicURL(musicName)
	if ierr != nil {
		log.Errorf("获取音乐URL失败: %v", ierr)
		return nil, "", fmt.Errorf("获取音乐URL失败: %v", ierr)
	}

	log.Infof("搜索音乐成功 URL: %s, 音乐名称: %s", musicURL, realMusicName)

	// 复用全局单例 HTTP 客户端（连接池见 session_mcp_tool.go 的 getHTTPClient）
	client := getHTTPClient()
	req, err := http.NewRequest("GET", musicURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("创建请求失败: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("API请求失败: %v", err)
	}
	// 必须关 Body，否则连接无法归还连接池，会逐渐耗尽
	defer resp.Body.Close()

	// 整首读进内存：这也是该实现被弃用的原因之一（大文件占内存、首帧延迟高）
	audioData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("读取响应失败: %v", err)
	}

	log.Infof("获取音乐 %s 数据成功, 音频数据长度: %d", realMusicName, len(audioData))

	return audioData, realMusicName, nil
}

// 以下大段是被注释保留的历史实现（基于 music-sd 的 netease/qq 搜索 + 流式读取）。
// 为什么保留：它展示了接入第三方音乐库的方式（Search -> ParseMusic -> ReadCloser），
// 是将来恢复“搜索多源并选最优结果”时的参考；删除后这份调研就丢失了。
// 注意：启用它需要把文件顶部的 netease/qq import 一起取消注释。
/*
func GetMusicAudioData(ctx context.Context, musicParams *PlayMusicParams) ([]byte, string, error) {
	musicName := musicParams.Name
	//welcome := musicParams.Welcome
	welcome := ""
	log.Infof("搜索音乐: %s 中, welcome: %s", musicName, welcome)
	// 这里可以根据音乐名称获取音乐URL
	// 目前简化实现，假设musicName就是URL或者从配置中获取
	musicList := netease.Search(musicName)
	musicList = append(musicList, qq.Search(musicName)...)
	for id, music := range musicList {
		log.Infof("[%2d] %7s | %s %5sMB - %s - %s - %s\n", id, music.Source, music.Duration, music.Size, music.Title, music.Singer, music.Album)
	}

	if len(musicList) <= 0 {
		return nil, "", fmt.Errorf("没有找到音乐")
	}
	m := musicList[0]
	m.ParseMusic()
	rc, err := m.ReadCloser()
	if err != nil {
		return nil, "", fmt.Errorf("获取音乐数据失败: %v", err)
	}
	defer rc.Close()

	audioData, err := io.ReadAll(rc)
	if err != nil {
		return nil, "", fmt.Errorf("读取响应失败: %v", err)
	}

	log.Infof("获取音乐 %s 数据成功, 音频数据长度: %d", m.Name, len(audioData))

	return audioData, m.Name, nil

}
*/
