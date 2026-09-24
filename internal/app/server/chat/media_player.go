package chat

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	types_audio "xiaozhi-esp32-server-golang/internal/data/audio"
	mcp_domain "xiaozhi-esp32-server-golang/internal/domain/mcp"
	"xiaozhi-esp32-server-golang/internal/domain/play_music"
	"xiaozhi-esp32-server-golang/internal/util"
	log "xiaozhi-esp32-server-golang/logger"

	mcpclient "github.com/mark3labs/mcp-go/client"
	mcp_go "github.com/mark3labs/mcp-go/mcp"
)

const defaultMediaPlayerPageSize = 16 * 1024

type MediaSourceType string

const (
	MediaSourceTypeMCPResource MediaSourceType = "mcp_resource"
	MediaSourceTypeHTTPURL     MediaSourceType = "http_url"
	MediaSourceTypeLocalFile   MediaSourceType = "local_file"
	MediaSourceTypeInlineAudio MediaSourceType = "inline_audio"
)

// 当前的播放模式——决定播放列表从哪来、播放怎么推进、切歌怎么工作
type mediaPlaybackMode string

const (
	mediaPlaybackModeStandalone    mediaPlaybackMode = "standalone"     // 独立播放——播一次就结束
	mediaPlaybackModeAgentPlaylist mediaPlaybackMode = "agent_playlist" // 智能体歌单——跨设备共享的播放列表
)

// "为什么暂停"——记录暂停的原因，从而决定恢复时该怎么处理。
type mediaPauseReason string

const (
	mediaPauseReasonNone      mediaPauseReason = ""
	mediaPauseReasonUser      mediaPauseReason = "user"
	mediaPauseReasonInterrupt mediaPauseReason = "interrupt"
)

type mediaRecoveryTrigger string

const (
	mediaRecoveryTriggerUser   mediaRecoveryTrigger = "user"
	mediaRecoveryTriggerAttach mediaRecoveryTrigger = "attach"
)

// "媒体源的完整描述"——告诉播放系统"要播什么、从哪来、怎么打开"。它是一条音频的统一表示，不管来源是 HTTP、本地文件还是 MCP 资源，都用这个结构描述。
/*
source := MediaSourceDescriptor{
    ID:         "song_20260922_001",
    Title:      "青花瓷",
    MIMEType:   "audio/mpeg",
    SourceType: MediaSourceTypeHTTPURL,
    Meta: map[string]string{
        "artist":   "周杰伦",
        "album":    "我很忙",
        "duration": "3:59",
        "cover":    "https://cdn.music.com/cover/qinghuaci.jpg",
    },
    HTTP: &HTTPMediaSource{
        URL: "https://cdn.music.com/song/qinghuaci.mp3",
    },
    // MCP / Local / Inline 都是 nil
}

source := MediaSourceDescriptor{
    ID:         "prompt_002",
    Title:      "开机提示音",
    MIMEType:   "audio/wav",
    SourceType: MediaSourceTypeLocalFile,
    Meta: map[string]string{
        "category": "system_prompt",
        "language": "zh-CN",
    },
    Local: &LocalMediaSource{
        Path: "/data/xiaozhi/audio/boot_prompt.wav",
    },
}

// 假设这段音频是某个工具调用返回的 MP3 字节
mp3Bytes := []byte{
    0xFF, 0xFB, 0x90, 0x64, // MP3 帧头
    0x00, 0x00, 0x00, 0x00,
    // ... 更多音频数据
}

source := MediaSourceDescriptor{
    ID:         "inline_003",
    Title:      "天气播报",
    MIMEType:   "audio/mpeg",
    SourceType: MediaSourceTypeInlineAudio,
    Meta: map[string]string{
        "source": "tool_response",
        "tool":   "get_weather",
    },
    Inline: &InlineAudioSource{
        Data: mp3Bytes,
    },
}

source := MediaSourceDescriptor{
    ID:         "mcp_004",
    Title:      "搜索结果语音播报",
    MIMEType:   "audio/mpeg",
    SourceType: MediaSourceTypeMCPResource,
    Meta: map[string]string{
        "query":  "今天的天气",
        "source": "parallel-search",
    },
    MCP: &MCPMediaSource{
        ServerName:       "parallel-search",
        EndpointSnapshot: "https://search.parallel.ai/mcp",
        ToolName:         "web_search",
        ResourceURI:      "audio://search/result/abc123",
        DirectAudioURL:   "",
        Description:      "北京今天晴，气温 15-25 度",
        ReadArgs: map[string]any{
            "query":     "今天的天气",
            "max_results": 5,
        },
        PageSize: 16384,
        Client:   nil, // 运行时从 mcp_domain.GetServerClientByName 获取
    },
}
*/
type MediaSourceDescriptor struct {
	ID         string            // 唯一标识"media_1234567890"
	Title      string            // 显示名 "青花瓷"
	MIMEType   string            // 格式  "audio/mpeg"、"audio/wav"
	SourceType MediaSourceType   // "http_url"
	Meta       map[string]string // {"artist": "周杰伦", "duration": "3:59"}

	MCP    *MCPMediaSource
	HTTP   *HTTPMediaSource
	Local  *LocalMediaSource
	Inline *InlineAudioSource
}

// MCP 资源信息
type MCPMediaSource struct {
	ServerName       string            // MCP 服务器名
	EndpointSnapshot string            // 端点快照
	ToolName         string            // 工具名
	ResourceURI      string            // 资源 URI
	DirectAudioURL   string            // 直接音频 URL（可选）
	Description      string            // 描述
	ReadArgs         map[string]any    // 读取参数
	PageSize         int               // 分页大小
	Client           *mcpclient.Client // MCP 客户端
}

type HTTPMediaSource struct {
	URL string
}

type LocalMediaSource struct {
	Path string
}

type InlineAudioSource struct {
	Data []byte
}

// 播放列表里的一项——把"一个媒体源"包装成"播放列表的一个条目"，加上 ID 和加入时间。
type MediaPlaylistItem struct {
	ID      string //唯一标识 同一个音频可能被加入歌单多次，用 Item.ID 区分
	Source  MediaSourceDescriptor
	AddedAt int64 // 加入时间 排序、清理、调试需要知道"什么时候加的"
}

// 播放器对外的状态快照——告诉外部"现在播放器在什么状态、在播什么、播到哪了"。它只用于查询，不参与控制。
/*
state := MediaPlayerState{
    Status:            "idle",        // play_music.StatusIdle
    Playlist:          nil,           // 无列表
    CurrentIndex:      -1,            // -1 表示无当前项
    CurrentTitle:      "",            // 空
    CurrentSourceType: "",            // 空
    PositionMs:        0,             // 0
    UpdatedAt:         1726992000000, // 2026-09-22 10:00:00.000
    ErrMsg:            "",            // 空
}

state := MediaPlayerState{
    Status:            "playing",                    // play_music.StatusPlaying
    Playlist:          nil,                          // standalone 模式，无列表
    CurrentIndex:      -1,                           // 独立模式，索引无意义
    CurrentTitle:      "青花瓷",                      // 从 source.Title 来
    CurrentSourceType: "http_url",                   // MediaSourceTypeHTTPURL
    PositionMs:        45000,                        // 已播 45 秒
    UpdatedAt:         1726992045000,                // 状态更新时间
    ErrMsg:            "",                           // 无错误
}

state := MediaPlayerState{
    Status:            "playing",
    Playlist:          nil,
    CurrentIndex:      -1,
    CurrentTitle:      "/data/xiaozhi/audio/boot_prompt.wav",  // 从路径派生
    CurrentSourceType: "local_file",                            // MediaSourceTypeLocalFile
    PositionMs:        2000,
    UpdatedAt:         1726992060000,
    ErrMsg:            "",
}

state := MediaPlayerState{
    Status:            "playing",
    Playlist:          nil,
    CurrentIndex:      -1,
    CurrentTitle:      "搜索结果语音播报",                       // 从 MCP Description 来
    CurrentSourceType: "mcp_resource",                           // MediaSourceTypeMCPResource
    PositionMs:        1500,
    UpdatedAt:         1726992075000,
    ErrMsg:            "",
}

state := MediaPlayerState{
    Status: "playing",
    Playlist: []MediaPlaylistItem{                    // ← 有完整列表
        {
            ID: "media_1726992000000000000_0",
            Source: MediaSourceDescriptor{
                ID:         "song_001",
                Title:      "青花瓷",
                MIMEType:   "audio/mpeg",
                SourceType: "http_url",
                Meta:       map[string]string{"artist": "周杰伦"},
                HTTP:       &HTTPMediaSource{URL: "https://music.com/qinghuaci.mp3"},
            },
            AddedAt: 1726992000000,
        },
        {
            ID: "media_1726992060000000000_1",
            Source: MediaSourceDescriptor{
                ID:         "song_002",
                Title:      "晴天",
                MIMEType:   "audio/mpeg",
                SourceType: "http_url",
                Meta:       map[string]string{"artist": "周杰伦"},
                HTTP:       &HTTPMediaSource{URL: "https://music.com/qingtian.mp3"},
            },
            AddedAt: 1726992060000,
        },
        {
            ID: "media_1726992120000000000_2",
            Source: MediaSourceDescriptor{
                ID:         "song_003",
                Title:      "稻香",
                MIMEType:   "audio/mpeg",
                SourceType: "http_url",
                Meta:       map[string]string{"artist": "周杰伦"},
                HTTP:       &HTTPMediaSource{URL: "https://music.com/daoxiang.mp3"},
            },
            AddedAt: 1726992120000,
        },
    },
    CurrentIndex:      1,                             // ← 当前播第 2 首
    CurrentTitle:      "晴天",
    CurrentSourceType: "http_url",
    PositionMs:        30000,                         // 已播 30 秒
    UpdatedAt:         1726992090000,
    ErrMsg:            "",
}

state := MediaPlayerState{
    Status:            "paused",                       // play_music.StatusPaused
    Playlist:          nil,
    CurrentIndex:      -1,
    CurrentTitle:      "青花瓷",
    CurrentSourceType: "http_url",
    PositionMs:        45000,                          // 暂停时的位置
    UpdatedAt:         1726992100000,
    ErrMsg:            "",
}

state := MediaPlayerState{
    Status:            "stopped",                      // play_music.StatusStopped
    Playlist:          nil,
    CurrentIndex:      -1,
    CurrentTitle:      "",                             // 停止后清空
    CurrentSourceType: "",
    PositionMs:        0,                              // 归零
    UpdatedAt:         1726992120000,
    ErrMsg:            "",
}

state := MediaPlayerState{
    Status:            "error",                        // play_music.StatusError
    Playlist:          nil,
    CurrentIndex:      -1,
    CurrentTitle:      "搜索结果语音播报",
    CurrentSourceType: "mcp_resource",
    PositionMs:        0,
    UpdatedAt:         1726992130000,
    ErrMsg:            "MCP client 不可用: search-server",  // ← 具体错误
}

state := MediaPlayerState{
    Status:            "error",
    Playlist:          nil,
    CurrentIndex:      -1,
    CurrentTitle:      "青花瓷",
    CurrentSourceType: "http_url",
    PositionMs:        0,
    UpdatedAt:         1726992140000,
    ErrMsg:            "HTTP 音频地址为空",
}

state := MediaPlayerState{
    Status:            "stopped",                      // 播完转 stopped
    Playlist:          []MediaPlaylistItem{...},       // 列表还在
    CurrentIndex:      2,                              // 停在最后一项
    CurrentTitle:      "稻香",
    CurrentSourceType: "http_url",
    PositionMs:        0,                              // 归零
    UpdatedAt:         1726992150000,
    ErrMsg:            "",
}

*/
type MediaPlayerState struct {
	Status            play_music.PlaybackStatus // 播放状态（playing/paused/stopped/error/idle）
	Playlist          []MediaPlaylistItem       // 当前播放列表
	CurrentIndex      int                       // 当前播第几项
	CurrentTitle      string                    // 当前媒体标题
	CurrentSourceType MediaSourceType           // 当前媒体来源类型
	PositionMs        int64                     // 已播放时长（毫秒）
	UpdatedAt         int64                     // 状态更新时间（毫秒）
	ErrMsg            string                    // 错误信息
}

// "把当前播放加入歌单"这个操作的返回结果——告诉调用方"加进去了什么、歌单现在多长、当前播放状态如何"。
/**
独立播放中加入歌单
result := PlaylistAppendResult{
    AddedTitle:      "青花瓷",                    // 刚加入的
    PlaylistLength:  1,                           // 歌单现在有 1 首
    CurrentIndex:    0,                           // 当前播第 0 项
    CurrentStatus:   "playing",                   // 仍在播放
    CurrentSource:   "http_url",                  // HTTP 来源
    CurrentPosition: 45000,                       // 已播 45 秒
}
歌单模式中追加一首
result := PlaylistAppendResult{
    AddedTitle:      "青花瓷",                    // 新加入的
    PlaylistLength:  3,                           // 歌单现在 3 首
    CurrentIndex:    1,                           // 当前仍播第 1 项（晴天）
    CurrentStatus:   "playing",                   // 仍在播放
    CurrentSource:   "http_url",
    CurrentPosition: 30000,                       // 已播 30 秒
}
暂停状态下加入
result := PlaylistAppendResult{
    AddedTitle:      "青花瓷",
    PlaylistLength:  2,
    CurrentIndex:    0,
    CurrentStatus:   "paused",                    // 保持暂停
    CurrentSource:   "http_url",
    CurrentPosition: 45000,                       // 暂停在 45 秒
}
*/
type PlaylistAppendResult struct {
	AddedTitle      string                    // 刚加入歌单的媒体标题
	PlaylistLength  int                       // 加入后歌单总长度
	CurrentIndex    int                       // 当前播放项在歌单中的索引
	CurrentStatus   play_music.PlaybackStatus // 当前播放状态
	CurrentSource   MediaSourceType           // 当前播放的来源类型
	CurrentPosition int64                     // 当前播放进度（毫秒）
}

// "播放句柄"——一个只读的完成信号，让调用方能够等待这次播放结束，而不用轮询状态。
type MediaPlaybackHandle struct {
	done <-chan struct{} // // 只读的完成信号通道
}

// 播放音频的格式配置——告诉播放系统"音频用什么采样率、每帧多长"。
type mediaPlaybackAudioConfig struct {
	SampleRate    int // 采样率（Hz）
	FrameDuration int // 每帧时长（毫秒）
}

// "一次播放活动"的运行时对象——每次开始播放都会创建一个，承载这次播放的生命周期、暂停状态、独占状态。
type activeMediaPlayback struct {
	ctx           context.Context    // 生命周期上下文
	cancel        context.CancelFunc // 取消函数
	done          chan struct{}      // 播放完成信号
	pauseMu       sync.Mutex         // 保护下面三个字段
	paused        bool               // 是否暂停   播放中, 暂停中(true)
	resumeCh      chan struct{}      // 恢复信号通道  解决"暂停的播放循环怎么被唤醒" 让循环"阻塞"在某个地方，直到收到恢复信号。waitIfPaused 和 setPaused两个方法就可以立即，等待暂停时for死循环进行阻塞主， 在setPaused两个方法就关闭通道，就可以调出死循环，继续往下执行
	exclusiveHeld bool               // 是否独占输出  解决"别重复申请独占"  播放媒体时，需要向 TTS 申请"独占输出通道"，避免和 TTS 互相干扰。 只有申请成功才可以给tts写入数据 这个就是申请独占attachment.bridge.BeginExclusiveMediaPlayback(ctx)
}

// "媒体输出桥"接口——定义播放系统怎么把音频发给 TTS 输出通道。它是一个抽象层，让播放系统不直接依赖 ttsManager。
type mediaOutputBridge interface {
	//申请独占
	BeginExclusiveMediaPlayback(ctx context.Context) error
	//释放独占
	EndExclusiveMediaPlayback()
	// 发句子开始, 告诉输出通道"一句话开始了"，附带文本（如"正在播放：青花瓷"）。 错误的回调
	SendSentenceStart(ctx context.Context, text string, onError func(error)) error
	// 发句子结束  出错时调用   onEnd：成功完成时调用（或带错误）
	SendSentenceEnd(ctx context.Context, text string, onError func(error), onEnd func(error)) error
	// 发音频帧 出错时调用
	SendAudioFrame(ctx context.Context, frame []byte, onError func(error)) error
}

// "播放系统"和"某个会话"之间的绑定凭证——把一个 ChatSession 和它的输出桥打包在一起，表示"这次播放绑定在哪个会话上"。
type mediaSessionAttachment struct {
	session *ChatSession      // 关联的会话
	bridge  mediaOutputBridge // 该会话的输出桥
}

// 是mediaOutputBridge 接口的唯一实现——把接口方法委托给 ChatSession.ttsManager，让播放系统能通过它向设备发音频。
type sessionMediaOutputBridge struct {
	session *ChatSession
}

// 按 agent（智能体）维度组织的歌单——同一个 agent 下的所有设备共享这个歌单，可以互相看到、接续播放。
type agentMediaPlaylist struct {
	mu        sync.RWMutex        // 读写锁，保护 items 和 updatedAt
	items     []MediaPlaylistItem // 歌单里的所有项
	updatedAt int64               // 最后更新时间（Unix 毫秒）
}

// 全局媒体播放协调器——管理所有 agent 的歌单和所有设备的播放运行时，是媒体播放系统的"总注册表"。
type mediaPlaybackCoordinator struct {
	ctx            context.Context                // 全局上下文
	mu             sync.Mutex                     // 保护两个 map
	agentPlaylists map[string]*agentMediaPlaylist // agentID → 歌单
	deviceRuntimes map[string]*deviceMediaRuntime // deviceID → 设备运行时
}

// 是单个设备的播放运行时——整个文件最核心的结构体，管理这台设备的播放状态、播放列表、播放活动、会话绑定。
type deviceMediaRuntime struct {
	coordinator *mediaPlaybackCoordinator
	deviceID    string

	mu                  sync.RWMutex
	state               MediaPlayerState         // 对外的状态快照
	mode                mediaPlaybackMode        // 当前是独立播放还是歌单播放。
	playlistAgentID     string                   // 当 mode == agent_playlist 时，指向哪个 agent 的歌单。
	standaloneItems     []MediaPlaylistItem      // 当 mode == standalone 时，本地保存的播放列表。  两者agent_playlist互斥——用一种，另一种清空。
	playbackIndex       int                      // 当前播到列表的第几项。
	audioConfig         mediaPlaybackAudioConfig // 当前播放的音频格式（采样率、帧时长）。
	currentSource       *MediaSourceDescriptor   // 当前正在播的媒体源。
	active              *activeMediaPlayback     // 当前正在进行的播放活动。
	pauseReason         mediaPauseReason         // 记录"为什么暂停"。
	attachment          *mediaSessionAttachment  // 当前绑定在哪个会话上。
	attachmentWaitCh    chan struct{}            // "绑定变了"的通知通道。
	resumeOnAttach      bool                     // 断线重连后是否要自动恢复播放。
	exclusiveAttachment *mediaSessionAttachment  // 当前独占输出通道的是哪个会话。
}

// 会话级的媒体播放器——它是对外门面，把播放请求委托给 deviceMediaRuntime，让 ChatSession 不用直接和底层打交道。
type SessionMediaPlayer struct {
	session     *ChatSession              // 所属会话
	coordinator *mediaPlaybackCoordinator // 全局协调器
	runtime     *deviceMediaRuntime       // 设备运行时（真正的实现）
}

var sharedMediaPlaybackCoordinator = newMediaPlaybackCoordinator()

// 是一个"已关闭的完成信号通道"——它的作用是兜底：当 MediaPlaybackHandle 无效时返回它，让 <-ch 立即返回，避免永久阻塞。, 展示没理解
var closedMediaPlaybackDoneCh = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

func newMediaPlaybackHandle(active *activeMediaPlayback) *MediaPlaybackHandle {
	if active == nil || active.done == nil {
		return &MediaPlaybackHandle{done: closedMediaPlaybackDoneCh}
	}
	return &MediaPlaybackHandle{done: active.done}
}

func (h *MediaPlaybackHandle) Done() <-chan struct{} {
	if h == nil || h.done == nil {
		return closedMediaPlaybackDoneCh
	}
	return h.done
}

func (h *MediaPlaybackHandle) Wait(ctx context.Context) error {
	if h == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case <-h.Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newActiveMediaPlayback(parent context.Context) *activeMediaPlayback {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	resumeCh := make(chan struct{})
	close(resumeCh)
	return &activeMediaPlayback{
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
		resumeCh: resumeCh,
	}
}

func (a *activeMediaPlayback) closeDone() {
	select {
	case <-a.done:
	default:
		close(a.done)
	}
}

func (a *activeMediaPlayback) setPaused(paused bool) bool {
	a.pauseMu.Lock()
	defer a.pauseMu.Unlock()

	if a.paused == paused {
		return false
	}

	if paused {
		a.paused = true
		a.resumeCh = make(chan struct{})
		return true
	}

	a.paused = false
	close(a.resumeCh)
	return true
}

func (a *activeMediaPlayback) isPaused() bool {
	a.pauseMu.Lock()
	defer a.pauseMu.Unlock()
	return a.paused
}

func (a *activeMediaPlayback) hasExclusiveHeld() bool {
	a.pauseMu.Lock()
	defer a.pauseMu.Unlock()
	return a.exclusiveHeld
}

func (a *activeMediaPlayback) setExclusiveHeld(held bool) bool {
	a.pauseMu.Lock()
	defer a.pauseMu.Unlock()

	if a.exclusiveHeld == held {
		return false
	}
	a.exclusiveHeld = held
	return true
}

func (a *activeMediaPlayback) waitIfPaused(ctx context.Context) error {
	for {
		a.pauseMu.Lock()
		paused := a.paused
		resumeCh := a.resumeCh
		a.pauseMu.Unlock()

		if !paused {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resumeCh:
		}
	}
}

func newMediaPlaybackCoordinator() *mediaPlaybackCoordinator {
	return &mediaPlaybackCoordinator{
		ctx:            context.Background(),
		agentPlaylists: make(map[string]*agentMediaPlaylist),
		deviceRuntimes: make(map[string]*deviceMediaRuntime),
	}
}

func newDeviceMediaRuntime(coordinator *mediaPlaybackCoordinator, deviceID string) *deviceMediaRuntime {
	return &deviceMediaRuntime{
		coordinator:      coordinator,
		deviceID:         deviceID,
		playbackIndex:    -1,
		attachmentWaitCh: make(chan struct{}),
		state: MediaPlayerState{
			Status:       play_music.StatusIdle,
			CurrentIndex: -1,
			UpdatedAt:    time.Now().UnixMilli(),
		},
	}
}

func (c *mediaPlaybackCoordinator) getOrCreateAgentPlaylist(agentID string) *agentMediaPlaylist {
	c.mu.Lock()
	defer c.mu.Unlock()

	playlist, ok := c.agentPlaylists[agentID]
	if ok {
		return playlist
	}

	playlist = &agentMediaPlaylist{}
	c.agentPlaylists[agentID] = playlist
	return playlist
}

func (c *mediaPlaybackCoordinator) getOrCreateRuntime(deviceID string) *deviceMediaRuntime {
	c.mu.Lock()
	defer c.mu.Unlock()

	runtime, ok := c.deviceRuntimes[deviceID]
	if ok {
		return runtime
	}

	runtime = newDeviceMediaRuntime(c, deviceID)
	c.deviceRuntimes[deviceID] = runtime
	return runtime
}

func (c *mediaPlaybackCoordinator) snapshotAgentPlaylist(agentID string) []MediaPlaylistItem {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return nil
	}

	playlist := c.getOrCreateAgentPlaylist(agentID)
	playlist.mu.RLock()
	defer playlist.mu.RUnlock()
	return cloneMediaPlaylistItems(playlist.items)
}

func (c *mediaPlaybackCoordinator) appendToAgentPlaylist(agentID string, source MediaSourceDescriptor) (MediaPlaylistItem, int, []MediaPlaylistItem, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return MediaPlaylistItem{}, -1, nil, fmt.Errorf("agentID 不能为空")
	}

	playlist := c.getOrCreateAgentPlaylist(agentID)
	nowMs := time.Now().UnixMilli()
	cloned := cloneMediaSourceDescriptor(source)
	if cloned.ID == "" {
		cloned.ID = fmt.Sprintf("media_%d", time.Now().UnixNano())
	}
	if cloned.Title == "" {
		cloned.Title = deriveMediaTitle(cloned)
	}

	item := MediaPlaylistItem{
		ID:      cloned.ID,
		Source:  cloned,
		AddedAt: nowMs,
	}

	playlist.mu.Lock()
	defer playlist.mu.Unlock()

	playlist.items = append(playlist.items, item)
	playlist.updatedAt = nowMs

	return cloneMediaPlaylistItem(item), len(playlist.items) - 1, cloneMediaPlaylistItems(playlist.items), nil
}

func (b *sessionMediaOutputBridge) BeginExclusiveMediaPlayback(ctx context.Context) error {
	if b == nil || b.session == nil || b.session.ttsManager == nil {
		return fmt.Errorf("媒体输出桥未初始化")
	}
	return b.session.ttsManager.BeginExclusiveMediaPlayback(ctx)
}

func (b *sessionMediaOutputBridge) EndExclusiveMediaPlayback() {
	if b == nil || b.session == nil || b.session.ttsManager == nil {
		return
	}
	b.session.ttsManager.EndExclusiveMediaPlayback()
}

func (b *sessionMediaOutputBridge) SendSentenceStart(ctx context.Context, text string, onError func(error)) error {
	if b == nil || b.session == nil || b.session.ttsManager == nil {
		return fmt.Errorf("媒体输出桥未初始化")
	}
	return b.session.ttsManager.EnqueueMediaSentenceStart(ctx, text, onError)
}

func (b *sessionMediaOutputBridge) SendSentenceEnd(ctx context.Context, text string, onError func(error), onEnd func(error)) error {
	if b == nil || b.session == nil || b.session.ttsManager == nil {
		return fmt.Errorf("媒体输出桥未初始化")
	}
	return b.session.ttsManager.EnqueueMediaSentenceEnd(ctx, text, onError, onEnd)
}

func (b *sessionMediaOutputBridge) SendAudioFrame(ctx context.Context, frame []byte, onError func(error)) error {
	if b == nil || b.session == nil || b.session.ttsManager == nil {
		return fmt.Errorf("媒体输出桥未初始化")
	}
	return b.session.ttsManager.EnqueueMediaFrame(ctx, frame, onError)
}

func NewSessionMediaPlayer(session *ChatSession) *SessionMediaPlayer {
	player := &SessionMediaPlayer{
		session:     session,
		coordinator: sharedMediaPlaybackCoordinator,
	}

	if session != nil && session.clientState != nil {
		player.runtime = player.coordinator.getOrCreateRuntime(session.clientState.DeviceID)
	}

	return player
}

func (p *SessionMediaPlayer) runtimeOrErr() (*deviceMediaRuntime, error) {
	if p == nil || p.runtime == nil {
		return nil, fmt.Errorf("media player 未初始化")
	}
	return p.runtime, nil
}

func (p *SessionMediaPlayer) audioConfig() mediaPlaybackAudioConfig {
	cfg := mediaPlaybackAudioConfig{
		SampleRate:    types_audio.SampleRate,
		FrameDuration: types_audio.FrameDuration,
	}
	if p == nil || p.session == nil || p.session.clientState == nil {
		return cfg
	}
	if p.session.clientState.OutputAudioFormat.SampleRate > 0 {
		cfg.SampleRate = p.session.clientState.OutputAudioFormat.SampleRate
	}
	if p.session.clientState.OutputAudioFormat.FrameDuration > 0 {
		cfg.FrameDuration = p.session.clientState.OutputAudioFormat.FrameDuration
	}
	return cfg
}

func (p *SessionMediaPlayer) currentAgentID() string {
	if p == nil || p.session == nil || p.session.clientState == nil {
		return ""
	}
	return strings.TrimSpace(p.session.clientState.AgentID)
}

func (p *SessionMediaPlayer) AttachSession() {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return
	}
	runtime.attachSession(p.session)
}

func (p *SessionMediaPlayer) DetachSession(preserve bool) {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return
	}
	runtime.detachSession(p.session, preserve)
}

func (p *SessionMediaPlayer) PlaySource(ctx context.Context, source MediaSourceDescriptor) error {
	_, err := p.PlaySourceWithHandle(ctx, source)
	return err
}

func (p *SessionMediaPlayer) ReplacePlaylistAndPlay(ctx context.Context, sources []MediaSourceDescriptor, startIndex int) error {
	_, err := p.ReplacePlaylistAndPlayWithHandle(ctx, sources, startIndex)
	return err
}

func (p *SessionMediaPlayer) PlaySourceWithHandle(ctx context.Context, source MediaSourceDescriptor) (*MediaPlaybackHandle, error) {
	return p.ReplacePlaylistAndPlayWithHandle(ctx, []MediaSourceDescriptor{source}, 0)
}

func (p *SessionMediaPlayer) ReplacePlaylistAndPlayWithHandle(ctx context.Context, sources []MediaSourceDescriptor, startIndex int) (*MediaPlaybackHandle, error) {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return nil, err
	}
	return runtime.ReplaceStandaloneQueueAndPlayWithHandle(ctx, sources, startIndex, p.audioConfig())
}

func (p *SessionMediaPlayer) Pause() error {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return err
	}
	return runtime.Pause()
}

func (p *SessionMediaPlayer) Play(ctx context.Context) error {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return err
	}
	return runtime.RecoverPlayback(ctx, p.audioConfig(), mediaRecoveryTriggerUser)
}

func (p *SessionMediaPlayer) PlayAgentPlaylist(ctx context.Context) error {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return err
	}
	return runtime.PlayAgentPlaylist(ctx, p.currentAgentID(), p.audioConfig())
}

func (p *SessionMediaPlayer) Resume() error {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return err
	}
	return runtime.Resume()
}

func (p *SessionMediaPlayer) ResumeIfInterruptedPause() (bool, error) {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return false, err
	}
	return runtime.ResumeIfInterruptedPause()
}

func (p *SessionMediaPlayer) Stop(ctx context.Context) error {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return err
	}
	return runtime.Stop(ctx)
}

func (p *SessionMediaPlayer) Suspend() error {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return err
	}
	return runtime.Suspend()
}

func (p *SessionMediaPlayer) Next(ctx context.Context) error {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return err
	}
	return runtime.Next(ctx, p.audioConfig())
}

func (p *SessionMediaPlayer) Prev(ctx context.Context) error {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return err
	}
	return runtime.Prev(ctx, p.audioConfig())
}

func (p *SessionMediaPlayer) GetState() MediaPlayerState {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return MediaPlayerState{
			Status:       play_music.StatusIdle,
			CurrentIndex: -1,
			UpdatedAt:    time.Now().UnixMilli(),
		}
	}
	return runtime.GetState()
}

func (p *SessionMediaPlayer) HasRealtimeMcpAudioControlContext() bool {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return false
	}
	return runtime.hasRealtimeMcpAudioControlContext()
}

func (p *SessionMediaPlayer) ShouldGateRealtimeMcpAudioASR() bool {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return false
	}
	return runtime.shouldGateRealtimeMcpAudioASR()
}

func (p *SessionMediaPlayer) AppendCurrentToPlaylist() (*PlaylistAppendResult, error) {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return nil, err
	}
	return runtime.AppendCurrentToAgentPlaylist(p.currentAgentID())
}

func (r *deviceMediaRuntime) attachSession(session *ChatSession) {
	if r == nil || session == nil || session.ttsManager == nil || session.serverTransport == nil {
		return
	}

	attachment := &mediaSessionAttachment{
		session: session,
		bridge:  &sessionMediaOutputBridge{session: session},
	}

	var shouldResume bool

	r.mu.Lock()
	if r.attachment != nil && r.attachment.session == session {
		r.mu.Unlock()
		return
	}
	r.attachment = attachment
	r.notifyAttachmentChangedLocked()
	shouldResume = r.resumeOnAttach
	r.mu.Unlock()

	log.Infof("设备 %s 媒体播放 attachment 已绑定", r.deviceID)

	if shouldResume {
		go func() {
			if err := r.RecoverPlayback(context.Background(), r.currentAudioConfig(), mediaRecoveryTriggerAttach); err != nil && !errors.Is(err, context.Canceled) {
				log.Warnf("设备 %s 恢复媒体播放失败: %v", r.deviceID, err)
			}
		}()
	}
}

func (r *deviceMediaRuntime) detachSession(session *ChatSession, preserve bool) {
	if r == nil || session == nil {
		return
	}

	var releaseAttachment *mediaSessionAttachment

	r.mu.Lock()
	currentAttachment := r.attachment
	if currentAttachment == nil || currentAttachment.session != session {
		r.mu.Unlock()
		return
	}

	wasPlaying := r.state.Status == play_music.StatusPlaying
	if preserve && r.active != nil && wasPlaying {
		r.active.setPaused(true)
		r.state.Status = play_music.StatusPaused
		r.state.UpdatedAt = time.Now().UnixMilli()
		r.resumeOnAttach = true
	}

	r.attachment = nil
	if r.exclusiveAttachment == currentAttachment {
		releaseAttachment = currentAttachment
		r.exclusiveAttachment = nil
		if r.active != nil {
			r.active.setExclusiveHeld(false)
		}
	}
	r.notifyAttachmentChangedLocked()
	r.mu.Unlock()

	log.Infof("设备 %s 媒体播放 attachment 已解绑, preserve=%v", r.deviceID, preserve)

	if releaseAttachment != nil {
		releaseAttachment.bridge.EndExclusiveMediaPlayback()
	}
}

func (r *deviceMediaRuntime) notifyAttachmentChangedLocked() {
	if r.attachmentWaitCh != nil {
		close(r.attachmentWaitCh)
	}
	r.attachmentWaitCh = make(chan struct{})
}

func (r *deviceMediaRuntime) ReplaceStandaloneQueueAndPlay(ctx context.Context, sources []MediaSourceDescriptor, startIndex int, cfg mediaPlaybackAudioConfig) error {
	_, err := r.ReplaceStandaloneQueueAndPlayWithHandle(ctx, sources, startIndex, cfg)
	return err
}

func (r *deviceMediaRuntime) ReplaceStandaloneQueueAndPlayWithHandle(ctx context.Context, sources []MediaSourceDescriptor, startIndex int, cfg mediaPlaybackAudioConfig) (*MediaPlaybackHandle, error) {
	if r == nil {
		return nil, fmt.Errorf("媒体播放器未初始化")
	}
	if len(sources) == 0 {
		return nil, r.Stop(ctx)
	}
	if startIndex < 0 || startIndex >= len(sources) {
		return nil, fmt.Errorf("无效的播放起始索引: %d", startIndex)
	}

	items := buildMediaPlaylistItems(sources)
	active := newActiveMediaPlayback(r.coordinator.ctx)

	r.mu.Lock()
	oldActive, oldExclusive := r.replaceActivePlaybackLocked(active)
	r.mode = mediaPlaybackModeStandalone
	r.playlistAgentID = ""
	r.standaloneItems = cloneMediaPlaylistItems(items)
	r.playbackIndex = startIndex
	r.audioConfig = normalizeMediaPlaybackAudioConfig(cfg, r.audioConfig)
	currentSource := cloneMediaSourceDescriptor(items[startIndex].Source)
	r.currentSource = &currentSource
	r.pauseReason = mediaPauseReasonNone
	r.resumeOnAttach = false
	r.state.Status = play_music.StatusPlaying
	r.state.Playlist = nil
	r.state.CurrentIndex = -1
	r.state.CurrentTitle = items[startIndex].Source.Title
	r.state.CurrentSourceType = items[startIndex].Source.SourceType
	r.state.PositionMs = 0
	r.state.ErrMsg = ""
	r.state.UpdatedAt = time.Now().UnixMilli()
	r.mu.Unlock()

	log.Infof("设备 %s 开始 standalone 媒体播放, source=%s, title=%s", r.deviceID, items[startIndex].Source.SourceType, items[startIndex].Source.Title)

	releaseMediaAttachment(oldExclusive)
	stopOldPlayback(oldActive)

	go r.runPlayback(active)
	return newMediaPlaybackHandle(active), nil
}

func (r *deviceMediaRuntime) PlayAgentPlaylistIndex(ctx context.Context, agentID string, startIndex int, cfg mediaPlaybackAudioConfig) error {
	if r == nil {
		return fmt.Errorf("媒体播放器未初始化")
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return fmt.Errorf("agentID 不能为空")
	}

	snapshot := r.coordinator.snapshotAgentPlaylist(agentID)
	if len(snapshot) == 0 {
		return fmt.Errorf("播放列表为空")
	}
	if startIndex < 0 || startIndex >= len(snapshot) {
		return fmt.Errorf("无效的播放起始索引: %d", startIndex)
	}

	active := newActiveMediaPlayback(r.coordinator.ctx)

	r.mu.Lock()
	oldActive, oldExclusive := r.replaceActivePlaybackLocked(active)
	r.mode = mediaPlaybackModeAgentPlaylist
	r.playlistAgentID = agentID
	r.standaloneItems = nil
	r.playbackIndex = startIndex
	r.audioConfig = normalizeMediaPlaybackAudioConfig(cfg, r.audioConfig)
	currentSource := cloneMediaSourceDescriptor(snapshot[startIndex].Source)
	r.currentSource = &currentSource
	r.pauseReason = mediaPauseReasonNone
	r.resumeOnAttach = false
	r.state.Status = play_music.StatusPlaying
	r.state.Playlist = cloneMediaPlaylistItems(snapshot)
	r.state.CurrentIndex = startIndex
	r.state.CurrentTitle = snapshot[startIndex].Source.Title
	r.state.CurrentSourceType = snapshot[startIndex].Source.SourceType
	r.state.PositionMs = 0
	r.state.ErrMsg = ""
	r.state.UpdatedAt = time.Now().UnixMilli()
	r.mu.Unlock()

	log.Infof("设备 %s 开始智能体歌单播放, agent=%s, index=%d, title=%s", r.deviceID, agentID, startIndex, snapshot[startIndex].Source.Title)

	releaseMediaAttachment(oldExclusive)
	stopOldPlayback(oldActive)

	go r.runPlayback(active)
	return nil
}

func (r *deviceMediaRuntime) PlayAgentPlaylist(ctx context.Context, agentID string, cfg mediaPlaybackAudioConfig) error {
	if r == nil {
		return fmt.Errorf("媒体播放器未初始化")
	}

	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return fmt.Errorf("agentID 不能为空")
	}

	snapshot := r.coordinator.snapshotAgentPlaylist(agentID)
	if len(snapshot) == 0 {
		return fmt.Errorf("播放列表为空")
	}

	startIndex := 0

	r.mu.RLock()
	if r.mode == mediaPlaybackModeAgentPlaylist && r.playlistAgentID == agentID && r.playbackIndex >= 0 && r.playbackIndex < len(snapshot) {
		startIndex = r.playbackIndex
	}
	r.mu.RUnlock()

	return r.PlayAgentPlaylistIndex(ctx, agentID, startIndex, cfg)
}

func (r *deviceMediaRuntime) Pause() error {
	return r.pausePlayback(true, mediaPauseReasonUser)
}

func (r *deviceMediaRuntime) Suspend() error {
	return r.pausePlayback(false, mediaPauseReasonInterrupt)
}

func (r *deviceMediaRuntime) Play(ctx context.Context, cfg mediaPlaybackAudioConfig) error {
	return r.RecoverPlayback(ctx, cfg, mediaRecoveryTriggerUser)
}

func (r *deviceMediaRuntime) RecoverPlayback(ctx context.Context, cfg mediaPlaybackAudioConfig, trigger mediaRecoveryTrigger) error {
	if r == nil {
		return fmt.Errorf("媒体播放器未初始化")
	}

	if trigger == mediaRecoveryTriggerAttach && !r.shouldResumeOnAttach() {
		return nil
	}

	r.mu.RLock()
	active := r.active
	mode := r.mode
	agentID := r.playlistAgentID
	playbackIndex := r.playbackIndex
	currentSource := cloneCurrentSource(r.currentSource)
	r.mu.RUnlock()

	if active != nil {
		if active.isPaused() {
			return r.recoverActivePlayback(trigger)
		}
		if trigger == mediaRecoveryTriggerAttach {
			r.clearResumeOnAttach()
		}
		log.Infof("设备 %s 跳过媒体恢复, trigger=%s, 原因=already_playing", r.deviceID, trigger)
		return nil
	}

	switch mode {
	case mediaPlaybackModeAgentPlaylist:
		snapshot := r.coordinator.snapshotAgentPlaylist(agentID)
		if len(snapshot) == 0 {
			return fmt.Errorf("播放列表为空")
		}
		if playbackIndex < 0 || playbackIndex >= len(snapshot) {
			playbackIndex = 0
		}
		log.Infof("设备 %s 执行媒体恢复, trigger=%s, mode=agent_playlist, index=%d", r.deviceID, trigger, playbackIndex)
		return r.PlayAgentPlaylistIndex(ctx, agentID, playbackIndex, cfg)
	default:
		if currentSource == nil {
			return fmt.Errorf("当前没有可播放的媒体")
		}
		log.Infof("设备 %s 执行媒体恢复, trigger=%s, mode=standalone, source=%s, title=%s", r.deviceID, trigger, currentSource.SourceType, currentSource.Title)
		return r.ReplaceStandaloneQueueAndPlay(ctx, []MediaSourceDescriptor{*currentSource}, 0, cfg)
	}
}

func (r *deviceMediaRuntime) Resume() error {
	return r.recoverActivePlayback(mediaRecoveryTriggerUser)
}

func (r *deviceMediaRuntime) recoverActivePlayback(trigger mediaRecoveryTrigger) error {
	if r == nil {
		return fmt.Errorf("媒体播放器未初始化")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.active == nil {
		return fmt.Errorf("当前没有正在播放的媒体")
	}
	if r.attachment == nil {
		return fmt.Errorf("当前暂无可用播放通道")
	}
	if !r.active.setPaused(false) {
		r.resumeOnAttach = false
		log.Infof("设备 %s 跳过媒体恢复, trigger=%s, 原因=already_playing", r.deviceID, trigger)
		return nil
	}
	r.pauseReason = mediaPauseReasonNone
	r.resumeOnAttach = false
	r.state.Status = play_music.StatusPlaying
	r.state.UpdatedAt = time.Now().UnixMilli()
	log.Infof("设备 %s 执行媒体恢复, trigger=%s, mode=resume_active", r.deviceID, trigger)
	return nil
}

func (r *deviceMediaRuntime) ResumeIfInterruptedPause() (bool, error) {
	if r == nil {
		return false, fmt.Errorf("媒体播放器未初始化")
	}

	r.mu.RLock()
	shouldResume := r.active != nil && r.pauseReason == mediaPauseReasonInterrupt
	r.mu.RUnlock()

	if !shouldResume {
		return false, nil
	}

	if err := r.recoverActivePlayback(mediaRecoveryTriggerUser); err != nil {
		return false, err
	}
	return true, nil
}

func (r *deviceMediaRuntime) shouldResumeOnAttach() bool {
	if r == nil {
		return false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.resumeOnAttach
}

func (r *deviceMediaRuntime) clearResumeOnAttach() {
	if r == nil {
		return
	}

	r.mu.Lock()
	r.resumeOnAttach = false
	r.mu.Unlock()
}

func (r *deviceMediaRuntime) Stop(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("媒体播放器未初始化")
	}

	var (
		active          *activeMediaPlayback
		exclusiveAttach *mediaSessionAttachment
	)

	r.mu.Lock()
	active = r.active
	exclusiveAttach = r.exclusiveAttachment
	if active != nil {
		active.setExclusiveHeld(false)
	}
	r.active = nil
	r.pauseReason = mediaPauseReasonNone
	r.exclusiveAttachment = nil
	r.resumeOnAttach = false
	r.state.Status = play_music.StatusStopped
	r.state.PositionMs = 0
	r.state.ErrMsg = ""
	r.state.UpdatedAt = time.Now().UnixMilli()
	r.mu.Unlock()

	releaseMediaAttachment(exclusiveAttach)
	stopOldPlayback(active)
	return nil
}

func (r *deviceMediaRuntime) Next(ctx context.Context, cfg mediaPlaybackAudioConfig) error {
	return r.jumpToRelative(ctx, 1, cfg)
}

func (r *deviceMediaRuntime) Prev(ctx context.Context, cfg mediaPlaybackAudioConfig) error {
	return r.jumpToRelative(ctx, -1, cfg)
}

func (r *deviceMediaRuntime) GetState() MediaPlayerState {
	if r == nil {
		return MediaPlayerState{
			Status:       play_music.StatusIdle,
			CurrentIndex: -1,
			UpdatedAt:    time.Now().UnixMilli(),
		}
	}

	r.mu.Lock()
	r.refreshPublicStateLocked()
	state := r.state
	r.mu.Unlock()

	state.Playlist = cloneMediaPlaylistItems(state.Playlist)
	return state
}

func (r *deviceMediaRuntime) hasRealtimeMcpAudioControlContext() bool {
	canControl, _ := r.realtimeMcpAudioGateStatus()
	return canControl
}

func (r *deviceMediaRuntime) shouldGateRealtimeMcpAudioASR() bool {
	_, shouldGate := r.realtimeMcpAudioGateStatus()
	return shouldGate
}

func (r *deviceMediaRuntime) realtimeMcpAudioGateStatus() (bool, bool) {
	if r == nil {
		return false, false
	}

	r.mu.RLock()
	sourceType := r.state.CurrentSourceType
	if sourceType == "" && r.currentSource != nil {
		sourceType = r.currentSource.SourceType
	}
	status := r.state.Status
	active := r.active
	attachment := r.attachment
	resumeOnAttach := r.resumeOnAttach
	r.mu.RUnlock()

	if !isRealtimeMcpAudioSourceType(sourceType) {
		return false, false
	}

	canControl := active != nil || resumeOnAttach || status == play_music.StatusPaused
	if active == nil || attachment == nil || status != play_music.StatusPlaying {
		return canControl, false
	}

	return true, !active.isPaused()
}

func (r *deviceMediaRuntime) AppendCurrentToAgentPlaylist(agentID string) (*PlaylistAppendResult, error) {
	if r == nil {
		return nil, fmt.Errorf("媒体播放器未初始化")
	}

	agentID = strings.TrimSpace(agentID)

	r.mu.RLock()
	currentSource := cloneCurrentSource(r.currentSource)
	currentStatus := r.state.Status
	currentPosition := r.state.PositionMs
	currentIndex := r.playbackIndex
	currentMode := r.mode
	currentPlaylistAgentID := r.playlistAgentID
	r.mu.RUnlock()

	if currentSource == nil {
		return nil, fmt.Errorf("当前没有可加入歌单的媒体")
	}
	if currentSource.SourceType == MediaSourceTypeInlineAudio {
		return nil, fmt.Errorf("当前音频来源不支持加入歌单")
	}

	item, index, snapshot, err := r.coordinator.appendToAgentPlaylist(agentID, *currentSource)
	if err != nil {
		return nil, err
	}

	targetPlaybackIndex := index
	preserveCurrentPlayback := currentMode == mediaPlaybackModeAgentPlaylist &&
		currentPlaylistAgentID == agentID &&
		currentIndex >= 0 &&
		currentIndex < len(snapshot)
	if preserveCurrentPlayback {
		targetPlaybackIndex = currentIndex
	}

	r.mu.Lock()
	r.mode = mediaPlaybackModeAgentPlaylist
	r.playlistAgentID = agentID
	r.playbackIndex = targetPlaybackIndex
	currentCopy := cloneMediaSourceDescriptor(*currentSource)
	r.currentSource = &currentCopy
	r.state.Playlist = cloneMediaPlaylistItems(snapshot)
	r.state.CurrentIndex = targetPlaybackIndex
	r.state.CurrentTitle = currentCopy.Title
	r.state.CurrentSourceType = currentCopy.SourceType
	r.state.UpdatedAt = time.Now().UnixMilli()
	r.mu.Unlock()

	log.Infof("设备 %s 将当前媒体加入智能体歌单, agent=%s, title=%s, index=%d", r.deviceID, strings.TrimSpace(agentID), item.Source.Title, index)

	return &PlaylistAppendResult{
		AddedTitle:      item.Source.Title,
		PlaylistLength:  len(snapshot),
		CurrentIndex:    targetPlaybackIndex,
		CurrentStatus:   currentStatus,
		CurrentSource:   item.Source.SourceType,
		CurrentPosition: currentPosition,
	}, nil
}

func (r *deviceMediaRuntime) pausePlayback(requireActive bool, reason mediaPauseReason) error {
	if r == nil {
		return fmt.Errorf("媒体播放器未初始化")
	}

	var exclusiveAttach *mediaSessionAttachment

	r.mu.Lock()
	active := r.active
	if active == nil {
		if requireActive {
			r.mu.Unlock()
			return fmt.Errorf("当前没有正在播放的媒体")
		}
		r.mu.Unlock()
		return nil
	}

	active.setPaused(true)
	exclusiveAttach = r.exclusiveAttachment
	r.exclusiveAttachment = nil
	active.setExclusiveHeld(false)
	r.pauseReason = reason
	r.resumeOnAttach = false
	r.state.Status = play_music.StatusPaused
	r.state.UpdatedAt = time.Now().UnixMilli()
	r.mu.Unlock()

	releaseMediaAttachment(exclusiveAttach)
	return nil
}

func (r *deviceMediaRuntime) jumpToRelative(ctx context.Context, delta int, cfg mediaPlaybackAudioConfig) error {
	if r == nil {
		return fmt.Errorf("媒体播放器未初始化")
	}

	r.mu.RLock()
	mode := r.mode
	agentID := r.playlistAgentID
	currentIndex := r.playbackIndex
	r.mu.RUnlock()

	if mode != mediaPlaybackModeAgentPlaylist {
		return fmt.Errorf("当前播放未加入智能体播放列表，请先执行 enqueue_current")
	}

	snapshot := r.coordinator.snapshotAgentPlaylist(agentID)
	if len(snapshot) == 0 {
		return fmt.Errorf("播放列表为空")
	}

	if currentIndex < 0 || currentIndex >= len(snapshot) {
		if delta >= 0 {
			currentIndex = 0
		} else {
			currentIndex = len(snapshot) - 1
		}
	}

	nextIndex := (currentIndex + delta) % len(snapshot)
	if nextIndex < 0 {
		nextIndex += len(snapshot)
	}

	return r.PlayAgentPlaylistIndex(ctx, agentID, nextIndex, cfg)
}

func (r *deviceMediaRuntime) replaceActivePlaybackLocked(active *activeMediaPlayback) (*activeMediaPlayback, *mediaSessionAttachment) {
	oldActive := r.active
	oldExclusive := r.exclusiveAttachment
	if oldActive != nil {
		oldActive.setExclusiveHeld(false)
	}
	r.active = active
	r.exclusiveAttachment = nil
	return oldActive, oldExclusive
}

func (r *deviceMediaRuntime) refreshPublicStateLocked() {
	switch r.mode {
	case mediaPlaybackModeAgentPlaylist:
		snapshot := r.coordinator.snapshotAgentPlaylist(r.playlistAgentID)
		r.state.Playlist = snapshot
		if len(snapshot) == 0 {
			r.state.CurrentIndex = -1
		} else if r.playbackIndex >= 0 && r.playbackIndex < len(snapshot) {
			r.state.CurrentIndex = r.playbackIndex
		} else {
			r.state.CurrentIndex = -1
		}
	default:
		r.state.Playlist = nil
		r.state.CurrentIndex = -1
	}
}

func (r *deviceMediaRuntime) currentPlaybackQueueLocked() []MediaPlaylistItem {
	switch r.mode {
	case mediaPlaybackModeAgentPlaylist:
		return r.coordinator.snapshotAgentPlaylist(r.playlistAgentID)
	default:
		return cloneMediaPlaylistItems(r.standaloneItems)
	}
}

func (r *deviceMediaRuntime) currentFrameDurationMs() int64 {
	r.mu.RLock()
	frameDuration := r.audioConfig.FrameDuration
	r.mu.RUnlock()
	if frameDuration <= 0 {
		return int64(types_audio.FrameDuration)
	}
	return int64(frameDuration)
}

func (r *deviceMediaRuntime) currentAudioConfig() mediaPlaybackAudioConfig {
	r.mu.RLock()
	cfg := r.audioConfig
	r.mu.RUnlock()
	return normalizeMediaPlaybackAudioConfig(cfg, mediaPlaybackAudioConfig{
		SampleRate:    types_audio.SampleRate,
		FrameDuration: types_audio.FrameDuration,
	})
}

func (r *deviceMediaRuntime) currentIndexForActive(active *activeMediaPlayback) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.active != active {
		return -1
	}
	return r.playbackIndex
}

func (r *deviceMediaRuntime) prepareTrack(active *activeMediaPlayback, index int) (MediaPlaylistItem, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.active != active {
		return MediaPlaylistItem{}, false
	}

	queue := r.currentPlaybackQueueLocked()
	if index < 0 || index >= len(queue) {
		return MediaPlaylistItem{}, false
	}

	item := cloneMediaPlaylistItem(queue[index])
	r.playbackIndex = index
	currentSource := cloneMediaSourceDescriptor(item.Source)
	r.currentSource = &currentSource
	r.state.CurrentTitle = item.Source.Title
	r.state.CurrentSourceType = item.Source.SourceType
	r.state.PositionMs = 0
	r.state.ErrMsg = ""
	if active.isPaused() {
		r.state.Status = play_music.StatusPaused
	} else {
		r.state.Status = play_music.StatusPlaying
	}
	r.state.UpdatedAt = time.Now().UnixMilli()
	r.refreshPublicStateLocked()
	return item, true
}

func (r *deviceMediaRuntime) advanceAfterTrack(active *activeMediaPlayback, index int) (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.active != active {
		return -1, false
	}

	queue := r.currentPlaybackQueueLocked()
	currentIndex := r.playbackIndex
	if currentIndex < 0 || currentIndex >= len(queue) {
		currentIndex = index
	}
	nextIndex := currentIndex + 1
	if nextIndex >= len(queue) {
		r.active = nil
		r.pauseReason = mediaPauseReasonNone
		r.resumeOnAttach = false
		r.state.Status = play_music.StatusStopped
		r.state.PositionMs = 0
		r.state.UpdatedAt = time.Now().UnixMilli()
		r.refreshPublicStateLocked()
		return -1, false
	}

	r.playbackIndex = nextIndex
	currentSource := cloneMediaSourceDescriptor(queue[nextIndex].Source)
	r.currentSource = &currentSource
	r.state.CurrentTitle = queue[nextIndex].Source.Title
	r.state.CurrentSourceType = queue[nextIndex].Source.SourceType
	r.state.PositionMs = 0
	r.state.Status = play_music.StatusPlaying
	r.state.UpdatedAt = time.Now().UnixMilli()
	r.refreshPublicStateLocked()
	return nextIndex, true
}

func (r *deviceMediaRuntime) markPlaybackError(active *activeMediaPlayback, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.active != active {
		return
	}

	r.active = nil
	r.pauseReason = mediaPauseReasonNone
	r.resumeOnAttach = false
	r.state.Status = play_music.StatusError
	r.state.ErrMsg = err.Error()
	r.state.UpdatedAt = time.Now().UnixMilli()
	r.refreshPublicStateLocked()
}

func (r *deviceMediaRuntime) markPlaybackStopped(active *activeMediaPlayback, status play_music.PlaybackStatus, errMsg string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.active != active {
		return
	}

	r.active = nil
	r.pauseReason = mediaPauseReasonNone
	r.resumeOnAttach = false
	r.state.Status = status
	r.state.ErrMsg = errMsg
	r.state.UpdatedAt = time.Now().UnixMilli()
	r.refreshPublicStateLocked()
}

func (r *deviceMediaRuntime) runPlayback(active *activeMediaPlayback) {
	defer active.closeDone()
	defer r.releaseExclusivePlayback(active)

	index := r.currentIndexForActive(active)
	for index >= 0 {
		if err := active.waitIfPaused(active.ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				r.markPlaybackStopped(active, play_music.StatusStopped, "")
			} else {
				r.markPlaybackError(active, err)
			}
			return
		}

		item, ok := r.prepareTrack(active, index)
		if !ok {
			return
		}

		if err := r.playItem(active, item); err != nil {
			if errors.Is(err, context.Canceled) {
				r.markPlaybackStopped(active, play_music.StatusStopped, "")
				return
			}
			r.markPlaybackError(active, err)
			return
		}

		nextIndex, hasNext := r.advanceAfterTrack(active, index)
		if !hasNext {
			return
		}
		index = nextIndex
	}
}

func (r *deviceMediaRuntime) waitForAttachment(active *activeMediaPlayback, ctx context.Context) (*mediaSessionAttachment, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		r.mu.RLock()
		if r.active != active {
			r.mu.RUnlock()
			return nil, context.Canceled
		}
		attachment := r.attachment
		waitCh := r.attachmentWaitCh
		r.mu.RUnlock()

		if attachment != nil {
			return attachment, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-waitCh:
		}
	}
}

func (r *deviceMediaRuntime) ensureExclusivePlayback(active *activeMediaPlayback, attachment *mediaSessionAttachment, ctx context.Context) error {
	if active == nil {
		return fmt.Errorf("当前没有正在播放的媒体")
	}
	if attachment == nil {
		return fmt.Errorf("当前暂无可用播放通道")
	}

	r.mu.RLock()
	sameAttachment := r.active == active && r.exclusiveAttachment == attachment && active.hasExclusiveHeld()
	r.mu.RUnlock()
	if sameAttachment {
		return nil
	}

	if err := attachment.bridge.BeginExclusiveMediaPlayback(ctx); err != nil {
		return err
	}

	var releaseAttachment *mediaSessionAttachment

	r.mu.Lock()
	if r.active != active || r.attachment != attachment {
		r.mu.Unlock()
		attachment.bridge.EndExclusiveMediaPlayback()
		return context.Canceled
	}

	if r.exclusiveAttachment != nil && r.exclusiveAttachment != attachment {
		releaseAttachment = r.exclusiveAttachment
	}
	r.exclusiveAttachment = attachment
	active.setExclusiveHeld(true)
	r.mu.Unlock()

	releaseMediaAttachment(releaseAttachment)
	return nil
}

func (r *deviceMediaRuntime) releaseExclusivePlayback(active *activeMediaPlayback) {
	if r == nil || active == nil {
		return
	}

	var attachment *mediaSessionAttachment

	r.mu.Lock()
	if !active.hasExclusiveHeld() {
		r.mu.Unlock()
		return
	}
	attachment = r.exclusiveAttachment
	r.exclusiveAttachment = nil
	active.setExclusiveHeld(false)
	r.mu.Unlock()

	releaseMediaAttachment(attachment)
}

func (r *deviceMediaRuntime) handleAttachmentFailure(active *activeMediaPlayback, attachment *mediaSessionAttachment, err error) {
	if r == nil || attachment == nil {
		return
	}

	var releaseAttachment *mediaSessionAttachment

	r.mu.Lock()
	if r.attachment != attachment || r.active != active {
		r.mu.Unlock()
		return
	}

	if r.exclusiveAttachment == attachment {
		releaseAttachment = attachment
		r.exclusiveAttachment = nil
		active.setExclusiveHeld(false)
	}

	if r.state.Status == play_music.StatusPlaying {
		active.setPaused(true)
		r.state.Status = play_music.StatusPaused
		r.state.UpdatedAt = time.Now().UnixMilli()
		r.resumeOnAttach = true
	}

	r.attachment = nil
	r.notifyAttachmentChangedLocked()
	r.mu.Unlock()

	releaseMediaAttachment(releaseAttachment)
	log.Warnf("设备 %s 媒体输出通道失效，等待重连恢复: %v", r.deviceID, err)
}

func (r *deviceMediaRuntime) playItem(active *activeMediaPlayback, item MediaPlaylistItem) error {
	source := item.Source
	title := deriveMediaTitle(source)
	playText := ""
	if title != "" {
		playText = fmt.Sprintf("正在播放音乐: %s", title)
	}

	audioChan, err := r.openSourceAudioStream(active.ctx, source, active)
	if err != nil {
		return err
	}

	gatedChan := r.wrapMediaAudioStream(active.ctx, audioChan, active)
	if err := r.streamMediaAudio(active.ctx, gatedChan, active, playText); err != nil {
		return err
	}

	log.Infof("媒体播放完成: %s", title)
	return nil
}

func (r *deviceMediaRuntime) streamMediaAudio(ctx context.Context, audioChan <-chan []byte, active *activeMediaPlayback, playText string) error {
	var (
		startedOn      *mediaSessionAttachment
		enqueuedFrames bool
	)
	frameDurationMs := r.currentFrameDurationMs()

	for {
		if err := active.waitIfPaused(ctx); err != nil {
			return err
		}

		var (
			frame []byte
			ok    bool
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case frame, ok = <-audioChan:
		}

		if !ok {
			if !enqueuedFrames {
				return nil
			}
			return r.waitMediaSentenceEnd(ctx, active, playText)
		}
		if len(frame) == 0 {
			continue
		}

		for {
			if err := active.waitIfPaused(ctx); err != nil {
				return err
			}

			attachment, err := r.waitForAttachment(active, ctx)
			if err != nil {
				return err
			}
			if err := r.ensureExclusivePlayback(active, attachment, ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				r.handleAttachmentFailure(active, attachment, err)
				continue
			}

			sendError := func(err error) {
				r.handleAttachmentFailure(active, attachment, err)
			}

			if startedOn != attachment && playText != "" {
				if err := attachment.bridge.SendSentenceStart(ctx, playText, sendError); err != nil {
					r.handleAttachmentFailure(active, attachment, err)
					continue
				}
				startedOn = attachment
			}

			if err := attachment.bridge.SendAudioFrame(ctx, frame, sendError); err != nil {
				r.handleAttachmentFailure(active, attachment, err)
				continue
			}

			enqueuedFrames = true
			r.incrementPlaybackPosition(active, frameDurationMs)
			break
		}
	}
}

func (r *deviceMediaRuntime) waitMediaSentenceEnd(ctx context.Context, active *activeMediaPlayback, playText string) error {
	if active == nil {
		return nil
	}

	r.mu.RLock()
	attachment := r.attachment
	isActive := r.active == active
	r.mu.RUnlock()
	if !isActive || attachment == nil {
		return nil
	}

	doneCh := make(chan error, 1)
	onEnd := func(err error) {
		select {
		case doneCh <- err:
		default:
		}
	}
	onError := func(err error) {
		r.handleAttachmentFailure(active, attachment, err)
	}

	if err := attachment.bridge.SendSentenceEnd(ctx, playText, onError, onEnd); err != nil {
		r.handleAttachmentFailure(active, attachment, err)
		return err
	}

	select {
	case err := <-doneCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *deviceMediaRuntime) openSourceAudioStream(ctx context.Context, source MediaSourceDescriptor, active *activeMediaPlayback) (<-chan []byte, error) {
	cfg := r.currentAudioConfig()
	audioFormat := util.GetAudioFormatByMimeType(source.MIMEType)

	switch source.SourceType {
	case MediaSourceTypeInlineAudio:
		if source.Inline == nil || len(source.Inline.Data) == 0 {
			return nil, fmt.Errorf("inline 音频数据为空")
		}
		return play_music.PlayMusicFromAudioData(ctx, source.Inline.Data, cfg.SampleRate, cfg.FrameDuration, audioFormat)
	case MediaSourceTypeHTTPURL:
		if source.HTTP == nil || strings.TrimSpace(source.HTTP.URL) == "" {
			return nil, fmt.Errorf("HTTP 音频地址为空")
		}
		return play_music.PlayMusicStream(ctx, source.HTTP.URL, cfg.SampleRate, cfg.FrameDuration, audioFormat)
	case MediaSourceTypeLocalFile:
		if source.Local == nil || strings.TrimSpace(source.Local.Path) == "" {
			return nil, fmt.Errorf("本地音频路径为空")
		}
		return openLocalMediaFileStream(ctx, source.Local.Path, cfg.SampleRate, cfg.FrameDuration, audioFormat)
	case MediaSourceTypeMCPResource:
		if source.MCP == nil {
			return nil, fmt.Errorf("MCP 音频源为空")
		}
		return r.openMCPResourceAudioStream(ctx, source.MCP, active, cfg.SampleRate, cfg.FrameDuration, audioFormat)
	default:
		return nil, fmt.Errorf("不支持的媒体源类型: %s", source.SourceType)
	}
}

func openLocalMediaFileStream(ctx context.Context, path string, sampleRate int, frameDuration int, audioFormat string) (<-chan []byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("打开本地音频文件失败: %v", err)
	}

	outputChan := make(chan []byte, 100)
	go func() {
		defer file.Close()
		decoder, err := util.CreateAudioDecoderWithSampleRate(ctx, file, outputChan, frameDuration, audioFormat, sampleRate)
		if err != nil {
			log.Errorf("创建本地音频解码器失败: %v", err)
			close(outputChan)
			return
		}
		if err := decoder.Run(time.Now().UnixMilli()); err != nil && !errors.Is(err, context.Canceled) {
			log.Errorf("本地音频解码失败: %v", err)
		}
	}()
	return outputChan, nil
}

func (r *deviceMediaRuntime) openMCPResourceAudioStream(ctx context.Context, source *MCPMediaSource, active *activeMediaPlayback, sampleRate int, frameDuration int, audioFormat string) (<-chan []byte, error) {
	if source == nil {
		return nil, fmt.Errorf("MCP 音频源为空")
	}
	if strings.TrimSpace(source.ResourceURI) == "" {
		return nil, fmt.Errorf("MCP Resource URI 为空")
	}

	pipeReader, pipeWriter := io.Pipe()
	audioChan, err := play_music.PlayMusicFromPipe(ctx, pipeReader, sampleRate, frameDuration, audioFormat)
	if err != nil {
		pipeWriter.CloseWithError(err)
		return nil, fmt.Errorf("创建 MCP 音频解码流失败: %v", err)
	}

	go r.streamMCPResourceToPipe(ctx, source, active, pipeWriter)
	return audioChan, nil
}

func (r *deviceMediaRuntime) streamMCPResourceToPipe(ctx context.Context, source *MCPMediaSource, active *activeMediaPlayback, pipeWriter *io.PipeWriter) {
	defer pipeWriter.Close()

	client := source.Client
	start := 0
	pageSize := source.PageSize
	if pageSize <= 0 {
		pageSize = defaultMediaPlayerPageSize
	}

	for {
		if err := active.waitIfPaused(ctx); err != nil {
			pipeWriter.CloseWithError(err)
			return
		}

		readArgs := cloneMediaSourceAnyMap(source.ReadArgs)
		readArgs["start"] = start
		readArgs["end"] = start + pageSize

		resourceResult, err := r.readMCPResourcePage(ctx, source, client, readArgs)
		if err != nil {
			pipeWriter.CloseWithError(err)
			return
		}
		if len(resourceResult.Contents) == 0 {
			return
		}

		hasData := false
		for _, content := range resourceResult.Contents {
			audioContent, ok := content.(mcp_go.BlobResourceContents)
			if !ok {
				continue
			}
			if len(audioContent.Blob) == 0 {
				continue
			}

			rawAudioData, err := base64.StdEncoding.DecodeString(audioContent.Blob)
			if err != nil {
				pipeWriter.CloseWithError(fmt.Errorf("解码 MCP 音频数据失败: %v", err))
				return
			}
			if string(rawAudioData) == McpReadResourceStreamDoneFlag {
				return
			}

			if err := active.waitIfPaused(ctx); err != nil {
				pipeWriter.CloseWithError(err)
				return
			}

			if _, err := pipeWriter.Write(rawAudioData); err != nil {
				pipeWriter.CloseWithError(fmt.Errorf("写入 MCP 音频流失败: %v", err))
				return
			}
			hasData = true
			if len(rawAudioData) < pageSize {
				return
			}
		}

		if !hasData {
			return
		}
		start += pageSize
	}
}

func (r *deviceMediaRuntime) readMCPResourcePage(ctx context.Context, source *MCPMediaSource, client *mcpclient.Client, readArgs map[string]any) (mcp_go.ReadResourceResult, error) {
	if client == nil && source.ServerName != "" {
		client = mcp_domain.GetServerClientByName(source.ServerName)
	}
	if client == nil {
		return mcp_go.ReadResourceResult{}, fmt.Errorf("MCP client 不可用: %s", source.ServerName)
	}

	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	resourceResult, err := client.ReadResource(readCtx, mcp_go.ReadResourceRequest{
		Params: mcp_go.ReadResourceParams{
			URI:       source.ResourceURI,
			Arguments: readArgs,
		},
	})
	if err == nil {
		if resourceResult == nil {
			return mcp_go.ReadResourceResult{}, nil
		}
		return *resourceResult, nil
	}

	if source.ServerName == "" || !strings.Contains(err.Error(), "session closed") {
		return mcp_go.ReadResourceResult{}, err
	}

	newClient, reconnErr := mcp_domain.ReconnectServerByName(source.ServerName)
	if reconnErr != nil {
		return mcp_go.ReadResourceResult{}, fmt.Errorf("MCP 资源读取失败且重连失败: %v", err)
	}
	source.Client = newClient

	readCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resourceResult, err = newClient.ReadResource(readCtx, mcp_go.ReadResourceRequest{
		Params: mcp_go.ReadResourceParams{
			URI:       source.ResourceURI,
			Arguments: readArgs,
		},
	})
	if err != nil {
		return mcp_go.ReadResourceResult{}, err
	}
	if resourceResult == nil {
		return mcp_go.ReadResourceResult{}, nil
	}
	return *resourceResult, nil
}

func (r *deviceMediaRuntime) wrapMediaAudioStream(ctx context.Context, input <-chan []byte, active *activeMediaPlayback) <-chan []byte {
	output := make(chan []byte)

	go func() {
		defer close(output)

		for {
			select {
			case <-ctx.Done():
				return
			case frame, ok := <-input:
				if !ok {
					return
				}
				if err := active.waitIfPaused(ctx); err != nil {
					return
				}
				select {
				case <-ctx.Done():
					return
				case output <- frame:
				}
			}
		}
	}()

	return output
}

func (r *deviceMediaRuntime) incrementPlaybackPosition(active *activeMediaPlayback, deltaMs int64) {
	if deltaMs <= 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.active != active {
		return
	}

	r.state.PositionMs += deltaMs
	r.state.UpdatedAt = time.Now().UnixMilli()
}

func buildMediaPlaylistItems(sources []MediaSourceDescriptor) []MediaPlaylistItem {
	items := make([]MediaPlaylistItem, 0, len(sources))
	nowMs := time.Now().UnixMilli()
	for i, source := range sources {
		cloned := cloneMediaSourceDescriptor(source)
		if cloned.ID == "" {
			cloned.ID = fmt.Sprintf("media_%d_%d", time.Now().UnixNano(), i)
		}
		if cloned.Title == "" {
			cloned.Title = deriveMediaTitle(cloned)
		}
		items = append(items, MediaPlaylistItem{
			ID:      cloned.ID,
			Source:  cloned,
			AddedAt: nowMs,
		})
	}
	return items
}

func normalizeMediaPlaybackAudioConfig(primary mediaPlaybackAudioConfig, fallback mediaPlaybackAudioConfig) mediaPlaybackAudioConfig {
	cfg := fallback
	if primary.SampleRate > 0 {
		cfg.SampleRate = primary.SampleRate
	}
	if primary.FrameDuration > 0 {
		cfg.FrameDuration = primary.FrameDuration
	}
	if cfg.SampleRate <= 0 {
		cfg.SampleRate = types_audio.SampleRate
	}
	if cfg.FrameDuration <= 0 {
		cfg.FrameDuration = types_audio.FrameDuration
	}
	return cfg
}

func cloneCurrentSource(source *MediaSourceDescriptor) *MediaSourceDescriptor {
	if source == nil {
		return nil
	}
	cloned := cloneMediaSourceDescriptor(*source)
	return &cloned
}

func releaseMediaAttachment(attachment *mediaSessionAttachment) {
	if attachment == nil {
		return
	}
	attachment.bridge.EndExclusiveMediaPlayback()
}

func stopOldPlayback(active *activeMediaPlayback) {
	if active == nil {
		return
	}
	active.cancel()
	waitForActivePlaybackDone(active, 2*time.Second)
}

func deriveMediaTitle(source MediaSourceDescriptor) string {
	if strings.TrimSpace(source.Title) != "" {
		return strings.TrimSpace(source.Title)
	}

	switch source.SourceType {
	case MediaSourceTypeHTTPURL:
		if source.HTTP != nil {
			if parsed, err := url.Parse(source.HTTP.URL); err == nil {
				if base := strings.TrimSpace(parsed.Path); base != "" {
					parts := strings.Split(base, "/")
					return strings.TrimSpace(parts[len(parts)-1])
				}
			}
			return strings.TrimSpace(source.HTTP.URL)
		}
	case MediaSourceTypeLocalFile:
		if source.Local != nil {
			return strings.TrimSpace(source.Local.Path)
		}
	case MediaSourceTypeMCPResource:
		if source.MCP != nil {
			if title := strings.TrimSpace(source.MCP.Description); title != "" {
				return title
			}
			if title := strings.TrimSpace(source.MCP.DirectAudioURL); title != "" {
				return title
			}
			return strings.TrimSpace(source.MCP.ResourceURI)
		}
	}

	return "未知音频"
}

func cloneMediaSourceDescriptor(source MediaSourceDescriptor) MediaSourceDescriptor {
	cloned := source
	cloned.Meta = cloneMediaSourceStringMap(source.Meta)
	if source.MCP != nil {
		mcpCopy := *source.MCP
		mcpCopy.ReadArgs = cloneMediaSourceAnyMap(source.MCP.ReadArgs)
		cloned.MCP = &mcpCopy
	}
	if source.HTTP != nil {
		httpCopy := *source.HTTP
		cloned.HTTP = &httpCopy
	}
	if source.Local != nil {
		localCopy := *source.Local
		cloned.Local = &localCopy
	}
	if source.Inline != nil {
		inlineCopy := *source.Inline
		inlineCopy.Data = append([]byte(nil), source.Inline.Data...)
		cloned.Inline = &inlineCopy
	}
	return cloned
}

func cloneMediaPlaylistItems(items []MediaPlaylistItem) []MediaPlaylistItem {
	if len(items) == 0 {
		return nil
	}
	cloned := make([]MediaPlaylistItem, 0, len(items))
	for _, item := range items {
		cloned = append(cloned, cloneMediaPlaylistItem(item))
	}
	return cloned
}

func cloneMediaPlaylistItem(item MediaPlaylistItem) MediaPlaylistItem {
	itemCopy := item
	itemCopy.Source = cloneMediaSourceDescriptor(item.Source)
	return itemCopy
}

func cloneMediaSourceStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneMediaSourceAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func waitForActivePlaybackDone(active *activeMediaPlayback, timeout time.Duration) {
	if active == nil {
		return
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	select {
	case <-active.done:
	case <-time.After(timeout):
	}
}
