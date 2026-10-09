// 文件说明：本地“媒体控制”MCP 工具（暂停/继续/切歌/歌单）。
//
// 职责：把用户的“控制正在播放的音乐”这类指令变成一个 LLM 可调用的工具，
// 并规定它“只说必要的确认、不产生多余语音”。
//
// 与其他 local mcp 工具的差别（为什么单独一个文件 + 自注册）：
//   - local_mcp_tool.go 采用“集中注册”（在 InitChatLocalMCPTools 的清单里登记）；
//   - 本文件改用 RegisterLocalMcpFunc 在自己的 init() 里自注册。
//     好处：这个功能可以整文件独立增删/搬运，不需要改动注册清单，减少改同一处代码的冲突；
//     init() 会在包被导入时自动执行，所以只要编译进二进制就一定注册成功。
//
// 在流程中的位置：
//
//	用户说“暂停/下一首” -> LLM 选择调用 control_music_playback
//	  -> tool.go: executeToolCall
//	  -> musicPlaybackControlHandler（本文件）
//	  -> session_mcp_tool.go: controlMusicPlayback（真正操作播放器）
//	  -> 返回 mcp_response_types.go 的 ActionResponse（带 SilenceLLM 标志）
//	  -> tool.go 因 SilenceLLM 停止本轮，不再让 LLM 说话
//
// 与其他文件的配合：session_mcp_tool.go（控制实现与动作归一化）、
// mcp_response_types.go（响应协议）、media_player.go（播放器能力）。
package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	log "xiaozhi-esp32-server-golang/logger"
)

// localMcpMusicControlToolName 工具名常量。
// 用常量而不是到处写字符串的原因：这个名字同时出现在“注册”“响应里的 tool_name”
// 两处，写错一处会导致日志对不上号，抽成常量可以避免。
const localMcpMusicControlToolName = "control_music_playback"

// init 在包加载时把媒体控制工具自注册进本地 MCP 管理器。
//
// 是否启用仍受配置开关控制（RegisterLocalMcpFunc 内部不做开关判断，
// 但上层可以通过不再编译/移除该文件或后续扩展开关来停用）。
// 注册失败只记日志不求 panic：一个可选工具注册不上不应拖垮整个服务启动。
func init() {
	if err := RegisterLocalMcpFunc(
		localMcpMusicControlToolName,
		// 描述写得又长又具体（列举“继续播放/恢复播放/继续听/接着放/暂停/停止/上一首/
		// 下一首/播放歌单/把当前播放加入歌单”），是刻意的：
		// 工具描述是引导 LLM 的主要手段——必须让它覆盖用户的各种口语说法，
		// 并明确“必须调用此工具，不能只做文字回复”，否则模型可能只用文字回一句
		// “好的，已暂停”，而播放器根本没有任何动作。
		// 描述末尾同时给出反例（想播放新歌时不要用本工具），避免和搜索/点播类工具抢调用。
		"当用户要控制当前设备正在播放的音乐或音频时必须使用。对于“继续播放”“恢复播放”“继续听”“接着放”“暂停”“停止”“上一首”“下一首”“播放歌单”“播放歌单里的歌曲”“播放播放列表”“把当前播放加入歌单”等指令，必须调用此工具，不能只做文字回复。仅当用户想播放新的歌曲、搜索歌曲或点播具体音乐时，不要使用此工具。",
		MusicPlaybackControlParams{},
		musicPlaybackControlHandler,
	); err != nil {
		log.Errorf("注册媒体控制本地MCP工具失败: %v", err)
	}
}

// musicPlaybackControlHandler 媒体控制工具的处理函数。
//
// 与其它本地工具一致的模式：解析参数 -> 通过 ctx 里的 ChatSessionOperator
// 调用会话侧实现 -> 把结果包成统一响应（mcp_response_types.go）。
func musicPlaybackControlHandler(ctx context.Context, argumentsInJSON string) (string, error) {
	log.Infof("执行媒体控制工具, args=%s", argumentsInJSON)

	var params MusicPlaybackControlParams
	// 参数为空时保持零值交给下层报“不支持的动作”，比直接 panic/失败更友好
	if argumentsInJSON != "" {
		if err := json.Unmarshal([]byte(argumentsInJSON), &params); err != nil {
			response := NewErrorResponse(localMcpMusicControlToolName, "参数解析失败", "PARSE_ERROR", "请检查 action 参数格式是否正确")
			return response.ToJSON()
		}
	}

	chatSessionOperatorValue := ctx.Value("chat_session_operator")
	if chatSessionOperatorValue == nil {
		return "", fmt.Errorf("从context中未找到chat_session_operator")
	}

	chatSessionOperator, ok := chatSessionOperatorValue.(ChatSessionOperator)
	if !ok {
		return "", fmt.Errorf("从context中获取的chat_session_operator不是ChatSessionOperator类型")
	}

	// 真正操作播放器（含动作别名归一化、pause/stop 后清发送队列）
	result, err := chatSessionOperator.LocalMcpControlMusicPlayback(ctx, &params)
	if err != nil {
		log.Errorf("媒体控制失败: %v", err)
		response := NewErrorResponse(localMcpMusicControlToolName, fmt.Sprintf("媒体控制失败: %v", err), "MEDIA_CONTROL_FAILED", "请检查当前播放状态后重试")
		return response.ToJSON()
	}
	// 兜底：实现层理论上不会返回 nil，但一旦返回 nil 也不能让下面的 nil 解引用崩掉。
	// 这里造一个“状态未知但保持静音”的结果，语义上等价于“动作可能已生效”。
	if result == nil {
		result = &MusicPlaybackControlResult{
			Action:          normalizeMusicPlaybackAction(params.Action),
			Status:          "unknown",
			SilenceResponse: true,
		}
	}

	// 动作名优先取实现层归一化后的值（更准确）；取不到再回退到本地归一化。
	action := normalizeMusicPlaybackAction(params.Action)
	if result != nil && result.Action != "" {
		action = result.Action
	}

	// terminal 传 false：控制本身不是“终止性动作”，后面还需按 SilenceLLM 决定是否静音。
	// 两个标志分工见下方注释。
	response := NewActionResponse(
		localMcpMusicControlToolName,
		action,
		buildMusicPlaybackControlMessage(result),
		result.Status,
		false,
	)
	// SilenceLLM / NoFurtherResponse = result.SilenceResponse（当前恒为 true）：
	// 让 tool.go 判定 shouldStopLLMProcessing，从而不再请求 LLM、不产生任何语音。
	// 为什么必须静音：命令由端上播放器立即执行，用户不需要也往往不想听
	// “好的，已为您暂停”这类多余回复（尤其在 realtime 场景里会打断体验）。
	response.NoFurtherResponse = result.SilenceResponse
	response.SilenceLLM = result.SilenceResponse
	// Metadata 把播放器状态快照带出去，仅用于日志/排查，不参与流程判断
	response.Metadata = buildMusicPlaybackControlMetadata(result)

	return response.ToJSON()
}

// buildMusicPlaybackControlMessage 生成给用户/LLM 看的结果描述文本。
// 注意：虽然本轮通常会因 SilenceLLM 而静音，这段文本仍会作为工具结果写入对话历史，
// 因此它要准确描述“发生了什么动作、作用在哪首歌上”，便于后续追问时模型有据可依。
func buildMusicPlaybackControlMessage(result *MusicPlaybackControlResult) string {
	if result == nil {
		return "媒体控制已完成"
	}

	switch result.Action {
	case "resume":
		if result.CurrentTitle != "" {
			return fmt.Sprintf("已继续播放：%s", result.CurrentTitle)
		}
		return "已继续播放"
	case "pause":
		if result.CurrentTitle != "" {
			return fmt.Sprintf("已暂停：%s", result.CurrentTitle)
		}
		return "已暂停播放"
	case "stop":
		if result.CurrentTitle != "" {
			return fmt.Sprintf("已停止：%s", result.CurrentTitle)
		}
		return "已停止播放"
	case "prev":
		if result.CurrentTitle != "" {
			return fmt.Sprintf("已切到上一首：%s", result.CurrentTitle)
		}
		return "已切到上一首"
	case "next":
		if result.CurrentTitle != "" {
			return fmt.Sprintf("已切到下一首：%s", result.CurrentTitle)
		}
		return "已切到下一首"
	case "play_playlist":
		if result.CurrentTitle != "" {
			return fmt.Sprintf("已开始播放歌单：%s", result.CurrentTitle)
		}
		return "已开始播放歌单"
	case "enqueue_current":
		if result.AddedTitle != "" {
			return fmt.Sprintf("已将当前播放源加入歌单：%s", result.AddedTitle)
		}
		return "已将当前播放源加入歌单"
	default:
		return "媒体控制已完成"
	}
}

// buildMusicPlaybackControlMetadata 把结果拍平成 string->string 元数据。
//
// 为什么全部转成字符串：响应协议里 Metadata 的类型就是 map[string]string
// （见 mcp_response_types.go），统一转字符串能避免增加协议复杂度；
// 这些值只用于日志与排查，字符串形态完全够用。
// AddedTitle 为空时不写入——避免在日志里出现无意义的空字段。
func buildMusicPlaybackControlMetadata(result *MusicPlaybackControlResult) map[string]string {
	if result == nil {
		return nil
	}

	metadata := map[string]string{
		"action":          result.Action,
		"status":          result.Status,
		"current_title":   result.CurrentTitle,
		"current_index":   strconv.Itoa(result.CurrentIndex),
		"playlist_length": strconv.Itoa(result.PlaylistLength),
		"current_source":  result.CurrentSource,
		"position_ms":     strconv.FormatInt(result.PositionMs, 10),
	}
	if result.AddedTitle != "" {
		metadata["added_title"] = result.AddedTitle
	}
	return metadata
}
