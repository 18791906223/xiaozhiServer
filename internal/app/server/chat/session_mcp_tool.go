// 文件说明：ChatSessionOperator 接口在 ChatManager 上的具体实现——本地 MCP 工具的“后端”。
//
// 职责：local_mcp_tool.go / local_mcp_media_control_tool.go 里注册的工具只负责
// “解析参数 + 构造统一响应”，真正读写会话/配置/检索/媒体播放的动作都实现在本文件。
// 之所以实现在 ChatManager 而不是 ChatSession 上：切换角色后要刷新设备配置、
// 清空历史要操作记忆，这些都依赖连接级对象（clientState / DeviceID）。
//
// 核心内容：
//   - MusicSearchResponse / MusicItem：第三方音乐搜索接口的响应结构；
//   - httpClient / httpClientOnce + getHTTPClient：带连接池的全局 HTTP 客户端单例；
//   - LocalMcp* 方法：ChatSessionOperator 接口（见 types.go）的实现；
//   - controlMusicPlayback / normalizeMusicPlaybackAction / flushQueuedMediaAudio：
//     媒体控制的内部实现、动作别名归一化、发送队列清理；
//   - getMusicURL：按歌名搜索并取回音频 URL。
//
// 在流程中的位置：
//
//	LLM 决定调用工具
//	  -> tool.go: executeToolCall（ctx 中带 chat_session_operator）
//	  -> local_mcp_tool.go / local_mcp_media_control_tool.go 的 Handler
//	  -> 通过接口调用到本文件的方法（真正干活）
//	  -> 结果回到 tool.go，由其决定是否继续请求 LLM
//
// 与其他文件的配合：types.go（接口契约）、local_mcp_tool.go（工具注册与入参）、
// mcp_response_types.go（响应协议）、media_player.go（媒体播放能力）、
// internal/domain/config（角色配置提供者）、internal/domain/rag（知识库检索）、
// internal/domain/memory/llm_memory（长期记忆）。
package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	user_config "xiaozhi-esp32-server-golang/internal/domain/config"
	config_types "xiaozhi-esp32-server-golang/internal/domain/config/types"
	llm_memory "xiaozhi-esp32-server-golang/internal/domain/memory/llm_memory"
	"xiaozhi-esp32-server-golang/internal/domain/rag"
	log "xiaozhi-esp32-server-golang/logger"

	"github.com/spf13/viper"
)

//此文件处理 local mcp tool 与 session绑定 的工具调用

// 音乐搜索API响应结构
type MusicSearchResponse struct {
	// Data 搜索结果列表，通常按相关度排序，取第 0 个作为命中
	Data []MusicItem `json:"data"`
	// Code 业务状态码，仅 200 表示成功
	Code int `json:"code"`
	// Error 失败时的错误描述
	Error string `json:"error"`
}

// MusicItem 一条音乐搜索结果。
// 字段大多来自第三方接口，这里只用了 URL/Title，其余字段保留是为了
// “接口返回什么就解析什么”，避免将来需要（如封面图、歌词、歌手）再改结构。
type MusicItem struct {
	// Type 音源类型（接口返回，如平台标识）
	Type string `json:"type"`
	// Link 第三方页面链接（不用于播放）
	Link string `json:"link"`
	// SongID 第三方歌曲 ID
	SongID string `json:"songid"`
	// Title 歌名，会作为“真实歌名”返回给上层用于显示/日志
	Title string `json:"title"`
	// Author 歌手
	Author string `json:"author"`
	// LRC 是否提供歌词
	LRC bool `json:"lrc"`
	// URL 可直接下载/播放的音频地址（播放实际使用的字段）
	URL string `json:"url"`
	// Pic 封面图地址
	Pic string `json:"pic"`
}

// 全局HTTP客户端
// 为什么做成单例：每次工具调用都新建 http.Client 会各自持有连接池，
// 高并发时 TCP 连接数暴涨；复用同一个客户端才能让 keep-alive 连接池真正生效。
var (
	// httpClient 全局唯一客户端，第一次调用 getHTTPClient 时创建
	httpClient *http.Client
	// httpClientOnce 保证初始化只执行一次（并发安全，不需要额外加锁）
	httpClientOnce sync.Once
)

// 获取配置了连接池的HTTP客户端
// 参数都是为“外网音乐搜索接口”调的：40s 以上默认超时太长，这里收紧到 30s，
// 避免一次工具调用把整轮对话拖住；idle 连接复用则减少 TLS 握手开销。
func getHTTPClient() *http.Client {
	httpClientOnce.Do(func() {
		transport := &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
		httpClient = &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		}
	})
	return httpClient
}

// 关闭会话
// 由本地工具 exit_conversation 触发（见 local_mcp_tool.go 的 exitConversationHandler）。
// 这里只做转发：真正的“退出”语义（关连接/保留会话）由 ChatManager.ExitChat 决定，
// 工具层不需要了解细节。
func (c *ChatManager) LocalMcpCloseChat() error {
	return c.ExitChat()
}

// 清空历史对话
// 由本地工具 clear_conversation_history 触发。
// 只清记忆（Redis/内存里的对话历史），不动连接、不动会话对象，
// 因此用户当前这轮对话仍可继续（并会收到“已清空”的确认语）。
func (c *ChatManager) LocalMcpClearHistory() error {
	llm_memory.Get().ResetMemory(c.ctx, c.DeviceID)
	return nil
}

// PlayMusicParams play_music 工具的入参。
// 说明：该工具目前在 local_mcp_tool.go 的注册清单里被注释停用，
// 但类型与下面的 LocalMcpPlayMusic 仍保留（接口实现 + 将来复用）。
type PlayMusicParams struct {
	// Name 要播放的音乐名称（或 URL），由 LLM 从用户话里抽取
	Name string `json:"name,omitempty" description:"音乐的名称"`
	//Welcome string `json:"welcome" description:"搜索音乐会耗时过长，用于安抚用户的提示语" required:"true"`
}

// MusicPlaybackControlParams control_music_playback 工具的入参。
// 描述里把各种说法都列出来（继续播放/恢复播放/继续听/接着放…），是为了引导 LLM
// 把用户口语归一到 action 字段；服务端还会做一次别名归一化（见 normalizeMusicPlaybackAction）。
type MusicPlaybackControlParams struct {
	// Action 控制动作。约定：resume / pause / stop / prev / next /
	// play_playlist / enqueue_current；play 与 continue 会被归一化成 resume。
	Action string `json:"action" description:"控制动作：resume(继续播放/恢复播放/继续听/接着放)、pause、stop、prev、next、play_playlist(播放歌单/播放歌单里的歌曲/播放播放列表)、enqueue_current；play 和 continue 也会归一化为 resume" required:"true"`
}

// MusicPlaybackControlResult 媒体控制的执行结果：既用于返回给工具的响应，
// 也用于日志/调试（把控制器里的最新状态快照带出来）。
type MusicPlaybackControlResult struct {
	// Action 归一化后的动作（以服务端为准，覆盖模型传进来的原始写法）
	Action string `json:"action"`
	// Status 播放器当前状态（playing/paused/stopped…）
	Status string `json:"status"`
	// CurrentTitle 当前曲目名，用于生成“已暂停：xxx”这类确认话术
	CurrentTitle string `json:"current_title,omitempty"`
	// CurrentIndex 当前曲目在播放列表中的下标
	CurrentIndex int `json:"current_index"`
	// PlaylistLength 播放列表长度
	PlaylistLength int `json:"playlist_length"`
	// CurrentSource 当前媒体源类型（standalone 播放源 / agent 歌单等，见 media_player.go）
	CurrentSource string `json:"current_source,omitempty"`
	// PositionMs 当前播放进度（毫秒）
	PositionMs int64 `json:"position_ms"`
	// AddedTitle enqueue_current 动作加入歌单的曲目名
	AddedTitle string `json:"added_title,omitempty"`
	// SilenceResponse 是否要求“不要再产生语音回复”。
	// 媒体控制（暂停/切歌等）由端上直接执行，再让 LLM 说一句话会显得聒噪，
	// 所以这里为 true，最终会映射到响应里的 SilenceLLM/NoFurtherResponse。
	SilenceResponse bool `json:"silence_response"`
}

// 播放音乐
//
// 说明：该实现当前没有被启用（play_music 工具注册项已注释，见 local_mcp_tool.go）。
// 它只负责“搜到 URL 并记日志”，真正的播放交给工具层/媒体播放器链路。
// 注意这里的错误传递方式：goroutine 里只打印 ierr，最终靠“musicURL 为空”来判定失败
// ——因为跨 goroutine 直接 return 无法把错误带回调用方，只能靠共享变量 + 结果检查。
func (c *ChatManager) LocalMcpPlayMusic(ctx context.Context, musicParams *PlayMusicParams) error {
	musicName := musicParams.Name
	//welcome := musicParams.Welcome
	welcome := ""
	log.Infof("搜索音乐: %s 中, welcome: %s", musicName, welcome)
	var musicURL, realMusicName string
	var wg sync.WaitGroup
	var ierr error
	// 计数 2 是因为下面起了两个 goroutine（第二个原本用于提前播报安抚语，现已留空），
	// 它们互不依赖，所以并发执行以缩短总耗时。
	wg.Add(2)
	go func() {
		defer wg.Done()
		// 这里可以根据音乐名称获取音乐URL
		// 目前简化实现，假设musicName就是URL或者从配置中获取
		musicURL, realMusicName, ierr = getMusicURL(musicName)
		if ierr != nil {
			log.Errorf("获取音乐URL失败: %v", ierr)
			return
		}

		return
	}()
	go func() {
		defer wg.Done()
		//c.session.ttsManager.handleTts(ctx, common.LLMResponseStruct{Text: welcome, IsStart: true})
	}()

	wg.Wait()

	// wg.Wait() 保证了上面的写入对这里是可见的，可以安全读取共享变量
	if musicURL == "" {
		log.Errorf("未找到音乐: %s", musicName)
		return fmt.Errorf("未找到音乐: %s", musicName)
	}

	log.Infof("找到音乐: %s, URL: %s", realMusicName, musicURL)

	return nil
}

// LocalMcpSwitchDeviceRole 按角色名称切换设备角色（支持模糊匹配）
//
// 被本地工具 switch_device_role 调用（见 local_mcp_tool.go）。
// 返回“实际匹配到的角色名”，因为用户说的名字可能是简称，模糊匹配后可能与请求不同，
// 上层需要用它给用户确认。
//
// 为什么切换后必须 ReloadDeviceConfig：
// 角色决定了 system prompt、音色、知识库等一系列配置，它们都缓存在 clientState 里；
// 不刷新的话，新角色要等到下次重连才生效，用户会觉得“说了切换却没变”。
func (c *ChatManager) LocalMcpSwitchDeviceRole(ctx context.Context, roleName string) (string, error) {
	roleName = strings.TrimSpace(roleName)
	if roleName == "" {
		return "", fmt.Errorf("role_name 不能为空")
	}

	// 配置提供者可能是本地文件/数据库等不同实现，用 viper 里的 type 选择
	configProvider, err := user_config.GetProvider(viper.GetString("config_provider.type"))
	if err != nil {
		return "", fmt.Errorf("获取配置提供者失败: %w", err)
	}

	// 模糊匹配 + 持久化设备角色（返回实际匹配到的角色名）
	matchedRoleName, err := configProvider.SwitchDeviceRoleByName(ctx, c.DeviceID, roleName)
	if err != nil {
		return "", err
	}

	// 刷新会话配置，让新角色的 prompt/音色/知识库立即生效
	if err := c.ReloadDeviceConfig(ctx); err != nil {
		return "", fmt.Errorf("角色已切换，但刷新会话配置失败: %w", err)
	}

	log.Infof("设备 %s 切换角色成功, 请求=%s, 匹配=%s", c.DeviceID, roleName, matchedRoleName)
	return matchedRoleName, nil
}

// LocalMcpRestoreDeviceDefaultRole 恢复设备默认角色
// 被本地工具 restore_device_default_role 调用。
// 与切角色同理：撤销设备级角色覆盖后也要 ReloadDeviceConfig，否则旧角色配置仍生效。
func (c *ChatManager) LocalMcpRestoreDeviceDefaultRole(ctx context.Context) error {
	configProvider, err := user_config.GetProvider(viper.GetString("config_provider.type"))
	if err != nil {
		return fmt.Errorf("获取配置提供者失败: %w", err)
	}

	if err := configProvider.RestoreDeviceDefaultRole(ctx, c.DeviceID); err != nil {
		return err
	}

	if err := c.ReloadDeviceConfig(ctx); err != nil {
		return fmt.Errorf("默认角色已恢复，但刷新会话配置失败: %w", err)
	}

	log.Infof("设备 %s 恢复默认角色成功", c.DeviceID)
	return nil
}

// LocalMcpSearchKnowledge 检索当前智能体绑定的知识库
// 被本地工具 search_knowledge 调用（见 local_mcp_tool.go 的 searchKnowledgeHandler）。
//
// 参数语义：
//   - query 检索文本；
//   - topK  返回条数（由工具层兜底默认 5）；
//   - knowledgeBaseIDs 可选的“进一步收窄”条件：只在其中指定的知识库内检索。
//     安全边界在这一行：候选范围固定是 c.clientState.DeviceConfig.KnowledgeBases
//     （即该智能体已关联的知识库），所以模型即使编造别的 ID 也检索不到越权数据。
func (c *ChatManager) LocalMcpSearchKnowledge(ctx context.Context, query string, topK int, knowledgeBaseIDs []uint) ([]config_types.KnowledgeSearchHit, error) {
	if c == nil || c.clientState == nil {
		return nil, fmt.Errorf("会话状态不可用")
	}
	return rag.Search(ctx, query, topK, c.clientState.DeviceConfig.KnowledgeBases, knowledgeBaseIDs)
}

// LocalMcpControlMusicPlayback 控制当前会话级媒体播放。
// 被本地工具 control_music_playback 调用（见 local_mcp_media_control_tool.go）。
// 这里只做转发：从 ChatManager 取到当前 ChatSession 后交给 controlMusicPlayback，
// 把“会话相关的播放逻辑”集中在一个不依赖 ChatManager 的函数里，便于单独测试。
func (c *ChatManager) LocalMcpControlMusicPlayback(ctx context.Context, params *MusicPlaybackControlParams) (*MusicPlaybackControlResult, error) {
	if c == nil {
		return nil, fmt.Errorf("chat manager 不可用")
	}
	return controlMusicPlayback(ctx, c.GetSession(), params)
}

// controlMusicPlayback 媒体控制的真正实现（操作 SessionMediaPlayer）。
//
// 为什么要先做动作归一化：LLM 输出的 action 写法不固定（play/continue/previous…），
// 归一化后 switch 分支才能穷举，避免“模型换了个说法就完全不生效”。
func controlMusicPlayback(ctx context.Context, session *ChatSession, params *MusicPlaybackControlParams) (*MusicPlaybackControlResult, error) {
	if session == nil || session.mediaPlayer == nil {
		return nil, fmt.Errorf("媒体播放器不可用")
	}
	if params == nil {
		return nil, fmt.Errorf("控制参数不能为空")
	}

	action := normalizeMusicPlaybackAction(params.Action)
	if action == "" {
		return nil, fmt.Errorf("不支持的控制动作: %s", params.Action)
	}

	// 默认 SilenceResponse=true：媒体控制属于“用户只想操作”的场景，
	// 成功的操作不需要 LLM 再补一句话。
	result := &MusicPlaybackControlResult{
		Action:          action,
		SilenceResponse: true,
	}

	switch action {
	case "resume":
		// 继续播放：恢复被暂停的播放（含“被打断自动恢复”的语义，见 media_player.go）
		if err := session.mediaPlayer.Play(ctx); err != nil {
			return nil, err
		}
	case "pause":
		if err := session.mediaPlayer.Pause(); err != nil {
			return nil, err
		}
		// 暂停后要清掉已入队但还没发出去的媒体音频，否则端上还会继续播一小段
		flushQueuedMediaAudio(session, action)
	case "stop":
		if err := session.mediaPlayer.Stop(ctx); err != nil {
			return nil, err
		}
		// 同理：停止后必须把发送队列里的残留音频清掉，否则“说停还在响”
		flushQueuedMediaAudio(session, action)
	case "prev":
		if err := session.mediaPlayer.Prev(ctx); err != nil {
			return nil, err
		}
	case "next":
		if err := session.mediaPlayer.Next(ctx); err != nil {
			return nil, err
		}
	case "play_playlist":
		// 播放该 agent 的歌单（跨设备共享，见 media_player.go 的 agent_playlist 模式）
		if err := session.mediaPlayer.PlayAgentPlaylist(ctx); err != nil {
			return nil, err
		}
	case "enqueue_current":
		// 把当前播放源加入歌单；成功后顺手尝试恢复播放。
		// 为什么在这里恢复：部分实现里“加入歌单”会短暂改变播放活动，
		// 若此前处于“被打断暂停”状态，用户会以为加入歌单把音乐弄停了。
		appendResult, err := session.mediaPlayer.AppendCurrentToPlaylist()
		if err != nil {
			return nil, err
		}
		result.AddedTitle = appendResult.AddedTitle
		if _, err := session.mediaPlayer.ResumeIfInterruptedPause(); err != nil {
			// 恢复失败不影响“加入歌单”这个主操作，只记警告
			log.Warnf("enqueue_current 自动恢复播放失败: %v", err)
		}
	}

	// 无论哪个分支，最后都从播放器取一次最新状态快照，回填给结果：
	// 这样工具响应里能带上“当前正在播什么、第几首”，便于日志与确认话术。
	state := session.mediaPlayer.GetState()
	result.Status = state.Status.String()
	result.CurrentTitle = state.CurrentTitle
	result.CurrentIndex = state.CurrentIndex
	result.PlaylistLength = len(state.Playlist)
	result.CurrentSource = string(state.CurrentSourceType)
	result.PositionMs = state.PositionMs
	return result, nil
}

// flushQueuedMediaAudio 清空 TTS/媒体音频的“待发送队列”。
//
// 为什么媒体控制在 pause/stop 之后需要它：下行音频是异步发送的——控制指令生效时，
// 发送队列里可能还压着几十到几百毫秒的音频帧（见 tts.go 的 runSenderLoop 流控）。
// 不清掉的话，设备在“已暂停/已停止”之后仍会把队列里的音频播完，听起来就是没停干净。
//
// 为什么要 300ms 超时 + 单独处理 context 错误：这是清理动作，不能因为发送协程
// 一时卡住就把整个工具调用拖死；超时只是“这次没清干净”，记警告即可，不算失败。
func flushQueuedMediaAudio(session *ChatSession, action string) {
	if session == nil || session.ttsManager == nil {
		return
	}

	interruptCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	if err := session.ttsManager.InterruptAndClearQueueSync(interruptCtx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			log.Warnf("媒体控制后清理音频发送队列超时: action=%s", action)
			return
		}
		if !errors.Is(err, context.Canceled) {
			log.Warnf("媒体控制后清理音频发送队列失败: action=%s, err=%v", action, err)
		}
	}
}

// normalizeMusicPlaybackAction 把模型给出的各种口语化/近义动作名归一到内部标准值。
//
// 为什么需要：这个值来自 LLM 的自由输出，同一个意图会有很多写法
// （play / resume / continue 都表示"继续"，previous 与 prev 等价…）。
// 归一化后，上层 switch 只需处理有限几个分支；无法识别的返回空串，由调用方报错。
func normalizeMusicPlaybackAction(action string) string {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "play", "resume", "continue":
		return "resume"
	case "pause":
		return "pause"
	case "stop":
		return "stop"
	case "prev", "previous":
		return "prev"
	case "next":
		return "next"
	case "play_playlist", "play_agent_playlist", "play_playlist_songs", "playlist":
		return "play_playlist"
	case "enqueue_current", "append_current", "add_current_to_playlist":
		return "enqueue_current"
	default:
		return ""
	}
}

// searchMusicFromAPI 从API搜索音乐
//
// getMusicURL 按歌名搜索并返回 (音频URL, 歌名, error)。
// 实现说明：
//   - 目标是一个第三方（非官方）音乐搜索接口，用 POST 表单 + 浏览器头伪装，
//     所以这里必须带 Referer/Origin/UA 等头，否则会被拒；
//   - 单独用 10s 的 context 超时：比全局 30s 更短，保证“搜歌”这一步不会长时间挂住对话；
//   - 只取第一条结果（Data[0]），够用且实现简单；多结果择优留待将来。
func getMusicURL(musicName string) (string, string, error) {
	client := getHTTPClient()

	// 构建请求体
	data := fmt.Sprintf("input=%s&filter=name&type=migu&page=1",
		url.QueryEscape(musicName))

	req, err := http.NewRequest("POST", "https://music.txqq.pro/",
		strings.NewReader(data))
	if err != nil {
		return "", "", fmt.Errorf("创建请求失败: %v", err)
	}

	// 设置请求头，模拟浏览器请求
	req.Header.Set("Accept", "application/json, text/javascript, */*; q=0.01")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	req.Header.Set("Origin", "https://music.txqq.pro")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("Referer", "https://music.txqq.pro/")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("sec-ch-ua", `"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"`)
	req.Header.Set("sec-ch-ua-mobile", "?0")
	req.Header.Set("sec-ch-ua-platform", `"Windows"`)

	// 设置超时
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("API请求失败: %v", err)
	}
	defer resp.Body.Close()

	// 非 200 直接失败：这里不做重试，因为工具调用本身已经有一轮 LLM 往返，重试会更慢
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("API请求失败，状态码: %d", resp.StatusCode)
	}

	// 解析响应
	var searchResp MusicSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&searchResp); err != nil {
		return "", "", fmt.Errorf("解析响应失败: %v", err)
	}

	// 业务码非 200 视为失败（第三方接口即使 HTTP 200 也可能返回业务错误）
	if searchResp.Code != 200 {
		return "", "", fmt.Errorf("API返回错误: %s", searchResp.Error)
	}

	if len(searchResp.Data) == 0 {
		return "", "", fmt.Errorf("未找到音乐: %s", musicName)
	}
	musicItem := searchResp.Data[0]
	// 返回第一个搜索结果的URL
	return musicItem.URL, musicItem.Title, nil
}
