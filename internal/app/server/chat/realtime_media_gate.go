package chat

import (
	"context"
	"strings"
	"time"

	"xiaozhi-esp32-server-golang/internal/domain/eventbus"
	"xiaozhi-esp32-server-golang/internal/domain/play_music"
	log "xiaozhi-esp32-server-golang/logger"
)

type realtimeMusicControlRule struct {
	action   string
	keywords []string
}

var realtimeMcpAudioControlRules = []realtimeMusicControlRule{
	{
		action: "play_playlist",
		keywords: []string{
			"播放歌单",
			"播放歌单里的歌曲",
			"播放播放列表",
			"播放列表",
		},
	},
	{
		action: "enqueue_current",
		keywords: []string{
			"加入歌单",
			"加入播放列表",
			"添加到歌单",
			"添加到播放列表",
		},
	},
	{
		action: "resume",
		keywords: []string{
			"继续播放",
			"恢复播放",
			"继续听",
			"接着放",
			"接着播",
		},
	},
	{
		action: "pause",
		keywords: []string{
			"暂停",
			"先暂停",
			"先停一下",
		},
	},
	{
		action: "stop",
		keywords: []string{
			"停止播放",
			"停止",
			"停播",
			"别播了",
		},
	},
	{
		action: "next",
		keywords: []string{
			"下一首",
			"下首",
			"切到下一首",
			"切歌",
		},
	},
	{
		action: "prev",
		keywords: []string{
			"上一首",
			"上首",
			"切到上一首",
		},
	},
}

var realtimeMcpAudioExitKeywords = []string{
	"再见",
	"拜拜",
	"拜了",
	"回见",
	"退出",
	"退出对话",
	"退下吧",
}

func normalizeRealtimeMcpAudioText(text string) string {
	return removePunctuation(strings.ToLower(strings.TrimSpace(text)))
}

func detectRealtimeMcpAudioControlAction(text string) string {
	normalizedText := normalizeRealtimeMcpAudioText(text)
	if normalizedText == "" {
		return ""
	}

	for _, rule := range realtimeMcpAudioControlRules {
		for _, keyword := range rule.keywords {
			normalizedKeyword := normalizeRealtimeMcpAudioText(keyword)
			if normalizedKeyword == "" {
				continue
			}
			if strings.Contains(normalizedText, normalizedKeyword) {
				return rule.action
			}
		}
	}

	return ""
}

func isRealtimeMcpAudioExitCommand(text string) bool {
	normalizedText := normalizeRealtimeMcpAudioText(text)
	if normalizedText == "" {
		return false
	}

	for _, keyword := range realtimeMcpAudioExitKeywords {
		normalizedKeyword := normalizeRealtimeMcpAudioText(keyword)
		if normalizedKeyword == "" {
			continue
		}
		if strings.Contains(normalizedText, normalizedKeyword) {
			return true
		}
	}

	return false
}

func isRealtimeMcpAudioSourceType(sourceType MediaSourceType) bool {
	return sourceType == MediaSourceTypeMCPResource || sourceType == MediaSourceTypeInlineAudio
}

func isRealtimeMcpAudioPlaybackState(state MediaPlayerState) bool {
	if !isRealtimeMcpAudioSourceType(state.CurrentSourceType) {
		return false
	}

	return state.Status == play_music.StatusPlaying
}

func (s *ChatSession) hasRealtimeMcpAudioControlContext() bool {
	if s == nil || s.clientState == nil || !s.clientState.IsRealTime() || s.mediaPlayer == nil {
		return false
	}

	return s.mediaPlayer.HasRealtimeMcpAudioControlContext()
}

func (s *ChatSession) isRealtimeMcpAudioGateActive() bool {
	if s == nil || s.clientState == nil || !s.clientState.IsRealTime() || s.mediaPlayer == nil {
		return false
	}

	return s.mediaPlayer.ShouldGateRealtimeMcpAudioASR()
}

func (s *ChatSession) tryHandleRealtimeMcpAudioASR(ctx context.Context, text string) (bool, error) {
	if !s.hasRealtimeMcpAudioControlContext() {
		return false, nil
	}

	if isRealtimeMcpAudioExitCommand(text) {
		eventbus.Get().Publish(eventbus.TopicExitChat, &eventbus.ExitChatEvent{
			ClientState: s.clientState,
			Reason:      "realtime媒体播放中用户退出",
			TriggerType: "realtime_media_exit_words",
			UserText:    text,
			Timestamp:   time.Now(),
		})
		log.Infof("设备 %s realtime媒体播放门控命中退出指令: %s", s.clientState.DeviceID, text)
		return true, nil
	}

	action := detectRealtimeMcpAudioControlAction(text)
	if action != "" {
		_, err := controlMusicPlayback(ctx, s, &MusicPlaybackControlParams{Action: action})
		if err != nil {
			log.Warnf("设备 %s realtime媒体播放门控执行控制动作失败: action=%s, text=%s, err=%v", s.clientState.DeviceID, action, text, err)
			return true, nil
		}
		log.Infof("设备 %s realtime媒体播放门控执行控制动作: action=%s, text=%s", s.clientState.DeviceID, action, text)
		return true, nil
	}

	if !s.isRealtimeMcpAudioGateActive() {
		return false, nil
	}

	log.Debugf("设备 %s realtime媒体播放门控忽略ASR文本: %s", s.clientState.DeviceID, text)
	return true, nil
}
