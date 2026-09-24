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
	Data  []MusicItem `json:"data"`
	Code  int         `json:"code"`
	Error string      `json:"error"`
}

type MusicItem struct {
	Type   string `json:"type"`
	Link   string `json:"link"`
	SongID string `json:"songid"`
	Title  string `json:"title"`
	Author string `json:"author"`
	LRC    bool   `json:"lrc"`
	URL    string `json:"url"`
	Pic    string `json:"pic"`
}

// 全局HTTP客户端
var (
	httpClient     *http.Client
	httpClientOnce sync.Once
)

// 获取配置了连接池的HTTP客户端
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
func (c *ChatManager) LocalMcpCloseChat() error {
	return c.ExitChat()
}

// 清空历史对话
func (c *ChatManager) LocalMcpClearHistory() error {
	llm_memory.Get().ResetMemory(c.ctx, c.DeviceID)
	return nil
}

type PlayMusicParams struct {
	Name string `json:"name,omitempty" description:"音乐的名称"`
	//Welcome string `json:"welcome" description:"搜索音乐会耗时过长，用于安抚用户的提示语" required:"true"`
}

type MusicPlaybackControlParams struct {
	Action string `json:"action" description:"控制动作：resume(继续播放/恢复播放/继续听/接着放)、pause、stop、prev、next、play_playlist(播放歌单/播放歌单里的歌曲/播放播放列表)、enqueue_current；play 和 continue 也会归一化为 resume" required:"true"`
}

type MusicPlaybackControlResult struct {
	Action          string `json:"action"`
	Status          string `json:"status"`
	CurrentTitle    string `json:"current_title,omitempty"`
	CurrentIndex    int    `json:"current_index"`
	PlaylistLength  int    `json:"playlist_length"`
	CurrentSource   string `json:"current_source,omitempty"`
	PositionMs      int64  `json:"position_ms"`
	AddedTitle      string `json:"added_title,omitempty"`
	SilenceResponse bool   `json:"silence_response"`
}

// 播放音乐
func (c *ChatManager) LocalMcpPlayMusic(ctx context.Context, musicParams *PlayMusicParams) error {
	musicName := musicParams.Name
	//welcome := musicParams.Welcome
	welcome := ""
	log.Infof("搜索音乐: %s 中, welcome: %s", musicName, welcome)
	var musicURL, realMusicName string
	var wg sync.WaitGroup
	var ierr error
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

	if musicURL == "" {
		log.Errorf("未找到音乐: %s", musicName)
		return fmt.Errorf("未找到音乐: %s", musicName)
	}

	log.Infof("找到音乐: %s, URL: %s", realMusicName, musicURL)

	return nil
}

// LocalMcpSwitchDeviceRole 按角色名称切换设备角色（支持模糊匹配）
func (c *ChatManager) LocalMcpSwitchDeviceRole(ctx context.Context, roleName string) (string, error) {
	roleName = strings.TrimSpace(roleName)
	if roleName == "" {
		return "", fmt.Errorf("role_name 不能为空")
	}

	configProvider, err := user_config.GetProvider(viper.GetString("config_provider.type"))
	if err != nil {
		return "", fmt.Errorf("获取配置提供者失败: %w", err)
	}

	matchedRoleName, err := configProvider.SwitchDeviceRoleByName(ctx, c.DeviceID, roleName)
	if err != nil {
		return "", err
	}

	if err := c.ReloadDeviceConfig(ctx); err != nil {
		return "", fmt.Errorf("角色已切换，但刷新会话配置失败: %w", err)
	}

	log.Infof("设备 %s 切换角色成功, 请求=%s, 匹配=%s", c.DeviceID, roleName, matchedRoleName)
	return matchedRoleName, nil
}

// LocalMcpRestoreDeviceDefaultRole 恢复设备默认角色
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
func (c *ChatManager) LocalMcpSearchKnowledge(ctx context.Context, query string, topK int, knowledgeBaseIDs []uint) ([]config_types.KnowledgeSearchHit, error) {
	if c == nil || c.clientState == nil {
		return nil, fmt.Errorf("会话状态不可用")
	}
	return rag.Search(ctx, query, topK, c.clientState.DeviceConfig.KnowledgeBases, knowledgeBaseIDs)
}

func (c *ChatManager) LocalMcpControlMusicPlayback(ctx context.Context, params *MusicPlaybackControlParams) (*MusicPlaybackControlResult, error) {
	if c == nil {
		return nil, fmt.Errorf("chat manager 不可用")
	}
	return controlMusicPlayback(ctx, c.GetSession(), params)
}

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

	result := &MusicPlaybackControlResult{
		Action:          action,
		SilenceResponse: true,
	}

	switch action {
	case "resume":
		if err := session.mediaPlayer.Play(ctx); err != nil {
			return nil, err
		}
	case "pause":
		if err := session.mediaPlayer.Pause(); err != nil {
			return nil, err
		}
		flushQueuedMediaAudio(session, action)
	case "stop":
		if err := session.mediaPlayer.Stop(ctx); err != nil {
			return nil, err
		}
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
		if err := session.mediaPlayer.PlayAgentPlaylist(ctx); err != nil {
			return nil, err
		}
	case "enqueue_current":
		appendResult, err := session.mediaPlayer.AppendCurrentToPlaylist()
		if err != nil {
			return nil, err
		}
		result.AddedTitle = appendResult.AddedTitle
		if _, err := session.mediaPlayer.ResumeIfInterruptedPause(); err != nil {
			log.Warnf("enqueue_current 自动恢复播放失败: %v", err)
		}
	}

	state := session.mediaPlayer.GetState()
	result.Status = state.Status.String()
	result.CurrentTitle = state.CurrentTitle
	result.CurrentIndex = state.CurrentIndex
	result.PlaylistLength = len(state.Playlist)
	result.CurrentSource = string(state.CurrentSourceType)
	result.PositionMs = state.PositionMs
	return result, nil
}

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

	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("API请求失败，状态码: %d", resp.StatusCode)
	}

	// 解析响应
	var searchResp MusicSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&searchResp); err != nil {
		return "", "", fmt.Errorf("解析响应失败: %v", err)
	}

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
