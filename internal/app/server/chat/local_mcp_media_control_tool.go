package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	log "xiaozhi-esp32-server-golang/logger"
)

const localMcpMusicControlToolName = "control_music_playback"

func init() {
	if err := RegisterLocalMcpFunc(
		localMcpMusicControlToolName,
		"当用户要控制当前设备正在播放的音乐或音频时必须使用。对于“继续播放”“恢复播放”“继续听”“接着放”“暂停”“停止”“上一首”“下一首”“播放歌单”“播放歌单里的歌曲”“播放播放列表”“把当前播放加入歌单”等指令，必须调用此工具，不能只做文字回复。仅当用户想播放新的歌曲、搜索歌曲或点播具体音乐时，不要使用此工具。",
		MusicPlaybackControlParams{},
		musicPlaybackControlHandler,
	); err != nil {
		log.Errorf("注册媒体控制本地MCP工具失败: %v", err)
	}
}

func musicPlaybackControlHandler(ctx context.Context, argumentsInJSON string) (string, error) {
	log.Infof("执行媒体控制工具, args=%s", argumentsInJSON)

	var params MusicPlaybackControlParams
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

	result, err := chatSessionOperator.LocalMcpControlMusicPlayback(ctx, &params)
	if err != nil {
		log.Errorf("媒体控制失败: %v", err)
		response := NewErrorResponse(localMcpMusicControlToolName, fmt.Sprintf("媒体控制失败: %v", err), "MEDIA_CONTROL_FAILED", "请检查当前播放状态后重试")
		return response.ToJSON()
	}
	if result == nil {
		result = &MusicPlaybackControlResult{
			Action:          normalizeMusicPlaybackAction(params.Action),
			Status:          "unknown",
			SilenceResponse: true,
		}
	}

	action := normalizeMusicPlaybackAction(params.Action)
	if result != nil && result.Action != "" {
		action = result.Action
	}

	response := NewActionResponse(
		localMcpMusicControlToolName,
		action,
		buildMusicPlaybackControlMessage(result),
		result.Status,
		false,
	)
	response.NoFurtherResponse = result.SilenceResponse
	response.SilenceLLM = result.SilenceResponse
	response.Metadata = buildMusicPlaybackControlMetadata(result)

	return response.ToJSON()
}

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
