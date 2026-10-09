// 文件：realtime_media_gate.go
// 职责：realtime（实时对话）模式下、正在播放 MCP 音频（音乐/资源音频）时，对 ASR 文本做本地门控：
//  1. 退出指令 → 直接发布退出事件，交给上层关会话；
//  2. 音乐控制指令（暂停/继续/切歌/歌单…）→ 本地直接控制播放器；
//  3. 其余识别文本 → 在门控生效时直接吞掉（不送 LLM）。
//
// 为什么要在本地识别这些指令：
// realtime 模式下音乐播放与对话并行，像「暂停」这类命令如果走 ASR → LLM → 工具调用 链路，
// 用户要等好几秒才听到反应，体验上完全不可接受。
// 音乐控制是确定的有限动作集，本地关键词匹配即可做到零延迟。
//
// 核心类型：realtimeMusicControlRule（动作 → 关键词表的映射规则）。
//
// 在流程中的位置：session.go 的 actionDoChat 路由链的最前面（优先级最高），
// 由 ChatSession.tryHandleRealtimeMcpAudioASR 统一入口调用。
//
// 与哪些文件配合：
//   - media_player.go：提供「当前是否在播 MCP 音频」与「是否需要门控」的判断。
//   - session_mcp_tool.go：controlMusicPlayback 执行真正的控制动作。
//   - util.go：removePunctuation 用于文本归一化。
//   - eventbus：退出指令通过 TopicExitChat 事件交给上层关闭会话。
package chat

import (
	"context"
	"strings"
	"time"

	"xiaozhi-esp32-server-golang/internal/domain/eventbus"
	"xiaozhi-esp32-server-golang/internal/domain/play_music"
	log "xiaozhi-esp32-server-golang/logger"
)

// realtimeMusicControlRule 描述一种「本地音乐控制动作」及其触发关键词。
// action 的取值与本地媒体控制工具约定一致（如 play_playlist / pause / next / prev…），
// 由 controlMusicPlayback 按这个字符串分发到具体动作。
type realtimeMusicControlRule struct {
	action   string
	keywords []string
}

// realtimeMcpAudioControlRules 是本地音乐控制指令表。
// 覆盖的动作分四类：歌单（播放歌单/加入歌单）、播放控制（继续/暂停/停止）、切歌（上一首/下一首）。
// 顺序即优先级：靠前的规则先命中，所以「播放歌单」「加入歌单」这类具体动作排在泛化短词之前，
// 避免被短词抢先匹配导致语义偏差（例如「加入歌单」被当成「播放」）。
// 关键词写成短语而不是单字，是为了降低误命中概率——仅凭一个「停」字就暂停播放太激进。
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

// realtimeMcpAudioExitKeywords 是 realtime 播音频期间的退出词。
// 之所以与普通对话的退出词分开维护：这里要能在「音乐正响」的环境下被识别，
// 所以选取的都是发音清晰、不易被音乐伴奏干扰误判的口语词（再见/拜拜/退出…）。
var realtimeMcpAudioExitKeywords = []string{
	"再见",
	"拜拜",
	"拜了",
	"回见",
	"退出",
	"退出对话",
	"退下吧",
}

// normalizeRealtimeMcpAudioText 归一化待匹配文本：去首尾空白 → 转小写 → 去标点。
// 为什么两步都要做：ASR 输出常带标点（「暂停，」），英文/数字的大小写也不固定，
// 不归一化就会漏命中；去掉空白与标点后剩下的连续汉字/字母，才适合做包含判断。
func normalizeRealtimeMcpAudioText(text string) string {
	return removePunctuation(strings.ToLower(strings.TrimSpace(text)))
}

// detectRealtimeMcpAudioControlAction 用关键词匹配判断文本属于哪个控制动作，无命中返回空串。
// 匹配用「包含」而非「相等」：ASR 常把指令裹在整句里（如「那个先暂停一下」），
// 包含匹配能覆盖这类口语化表达。
// 关键词也在运行时归一化，保证与文本使用同一套规则，不依赖调用方手写规范。
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

// isRealtimeMcpAudioExitCommand 判断文本是否是「退出对话」指令。
// 与动作识别一样采用「归一化 + 包含匹配」，容忍「好了再见吧」这类口语包装。
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

// isRealtimeMcpAudioSourceType 判断媒体来源是不是「MCP 相关的音频」
// （MCP 资源音频或内联音频）。只有这两类才涉及播放器控制，
// 普通 TTS/提示音不属于此列，因此不需要走本地音乐控制门控。
func isRealtimeMcpAudioSourceType(sourceType MediaSourceType) bool {
	return sourceType == MediaSourceTypeMCPResource || sourceType == MediaSourceTypeInlineAudio
}

// isRealtimeMcpAudioPlaybackState 判断媒体状态是否处在「正在播放 MCP 音频」。
// 只有这个状态才认为用户可能是在对音乐下指令（否则可能在跟助手正常对话）。
func isRealtimeMcpAudioPlaybackState(state MediaPlayerState) bool {
	if !isRealtimeMcpAudioSourceType(state.CurrentSourceType) {
		return false
	}

	return state.Status == play_music.StatusPlaying
}

// hasRealtimeMcpAudioControlContext 判断当前是否具备「本地控制音乐」的上下文：
// 必须是 realtime 模式、有媒体播放器、且播放器侧认为当前可以控制
// （存在活动播放，或存在可恢复的暂停播放）。
// 不满足时整条本地识别链路直接跳过，避免在非 realtime、或根本没在放音频时误吞用户的话。
func (s *ChatSession) hasRealtimeMcpAudioControlContext() bool {
	if s == nil || s.clientState == nil || !s.clientState.IsRealTime() || s.mediaPlayer == nil {
		return false
	}

	return s.mediaPlayer.HasRealtimeMcpAudioControlContext()
}

// isRealtimeMcpAudioGateActive 判断「ASR 门控」是否应当生效：
// 生效时，识别不出控制指令的文本会被吞掉（不发给 LLM），
// 因为此刻用户多半是在对音乐说话，而不是在跟助手对话。
func (s *ChatSession) isRealtimeMcpAudioGateActive() bool {
	if s == nil || s.clientState == nil || !s.clientState.IsRealTime() || s.mediaPlayer == nil {
		return false
	}

	return s.mediaPlayer.ShouldGateRealtimeMcpAudioASR()
}

// [6.2] realtime 门控：媒体播放期间先把 ASR 文本当本地播放指令处理，避免闲聊打断音乐。
//
// tryHandleRealtimeMcpAudioASR 是 realtime 播音频期间 ASR 文本的三段式处理，返回是否已被处理（即是否吞掉）。
// 顺序为什么必须是这样：
//  1. 先判退出指令：退出优先于一切——用户说「再见」时不该还在纠结要不要暂停音乐。
//  2. 再判控制动作：能识别成音乐指令就本地执行，实现零延迟，也不该再丢给 LLM 绕一圈。
//  3. 最后才是「门控吞掉」：实在识别不了的文本，若门控生效就丢弃。
//     这是有意的取舍——宁可漏掉一句闲聊，也不要让闲聊打断正在播放的音乐。
//     控制动作执行失败也返回 true（视为已处理），避免失败指令又被送进 LLM 造成二次尴尬。
//
// 返回 false 表示没接管，让文本正常走后续路由（LLM）。
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
