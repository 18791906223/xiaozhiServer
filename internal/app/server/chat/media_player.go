// 本文件实现「会话级媒体播放器」：把音乐 / 提示音 / 工具返回的音频播到设备上。
//
// 职责：
//  1. 把四种来源的音频（MCP 资源 / HTTP URL / 本地文件 / 内联字节）统一抽象成
//     MediaSourceDescriptor，解码成设备输出格式（采样率 + 帧长）后按帧推送；
//  2. 维护「当前在播什么、播到第几项、播到多少毫秒、是播放还是暂停」的状态，
//     并对外提供 Pause / Play / Next / Prev / Stop / AppendCurrentToPlaylist 等控制方法；
//  3. 支持两种播放模式：standalone（本设备一次性播放）与 agent_playlist
//     （同一个 agentID 下的多台设备共享同一份歌单，可以接力、切歌）；
//  4. 设备断线时保留播放状态，重连后按需自动恢复（见 attachSession / detachSession）。
//
// 三层结构（从外到内）：
//
//	SessionMediaPlayer            // 门面：挂在 ChatSession 上，只做参数补齐（音频格式 / agentID）后转发
//	 └─ deviceMediaRuntime        // 每设备运行时：真正的状态机、播放主循环、会话绑定、独占申请
//	     └─ mediaPlaybackCoordinator // 全局注册表：按 agentID 存歌单、按 deviceID 存运行时
//
// 为什么这样分层：
//   - 播放状态必须比 ChatSession 活得久。ChatSession 会随连接关闭被销毁重建
//     （见 chat.go / session.go 的会话保留逻辑），所以真正的状态放在全局协调器
//     持有的 deviceMediaRuntime 上，会话只是"借用"它；
//   - agent 歌单要跨设备共享，必须有全局唯一的持有者，因此用包级单例
//     sharedMediaPlaybackCoordinator 统一管理。
//
// 与其它文件的配合：
//   - tts.go：媒体和 TTS 语音共用同一条下行输出通道（sessionAudioQueue + runSenderLoop）。
//     所以播放前必须先向 TTSManager 申请「独占」（BeginExclusiveMediaPlayback），
//     否则会出现"机器人在说话的同时音乐也在响"；音频帧也统一走 TTS 的入队接口
//     EnqueueMediaFrame，从而复用同一套流控逻辑。
//   - chat.go / session.go：会话 Start 时 AttachSession 绑定输出通道；
//     会话关闭 / 连接断开时 DetachSession（preserve=true 表示保留状态等重连恢复）。
//   - realtime_media_gate.go：realtime 模式下正在播 MCP 音频时，通过本文件暴露的
//     HasRealtimeMcpAudioControlContext / ShouldGateRealtimeMcpAudioASR 做 ASR 门控，
//     让"暂停 / 继续 / 下一首 / 退出"这类指令零延迟本地生效，不必走 LLM。
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

// defaultMediaPlayerPageSize 是 MCP 资源音频的「分页读取」大小（字节）。
//
// 为什么要分页：MCP 的 resource 读取是"一次读一段"的接口，而一首歌动辄几 MB，
// 不能一次性读进内存再解码（内存峰值大、首帧延迟高）。所以按 16KB 一段循环拉取，
// 读一段就写进解码管道一段，边读边解边播（见 streamMCPResourceToPipe）。
const defaultMediaPlayerPageSize = 16 * 1024

// MediaSourceType 标识媒体来源的种类。
//
// 播放器只为这四种来源各写一条"打开音频流"的分支（见 openSourceAudioStream），
// MediaSourceDescriptor 里对应的 MCP / HTTP / Local / Inline 字段按类型四选一填充，
// 其余三个保持 nil。这样上层（工具、MCP 资源回调）只要填好描述符，
// 播放器就不关心音频究竟是从网络、磁盘还是内存来的。
type MediaSourceType string

// 四种来源类型的取值，与 MediaSourceDescriptor 的四个可选字段一一对应：
//   - mcp_resource：MCP 资源，由 MCP 客户端按页 ReadResource 拉取音频；
//   - http_url：HTTP(S) 直链，直接 GET 拉流（音乐 URL 通常走这里）；
//   - local_file：服务端磁盘上的音频文件；
//   - inline_audio：音频字节已经在内存里（工具直接返回的音频内容）。
const (
	MediaSourceTypeMCPResource MediaSourceType = "mcp_resource"
	MediaSourceTypeHTTPURL     MediaSourceType = "http_url"
	MediaSourceTypeLocalFile   MediaSourceType = "local_file"
	MediaSourceTypeInlineAudio MediaSourceType = "inline_audio"
)

// 当前的播放模式——决定播放列表从哪来、播放怎么推进、切歌怎么工作
//
// 两种模式**互斥**（同一时刻只能是其中一种），因为它们的"列表来源"和"切歌语义"完全不同：
//   - standalone：列表存在本设备的 deviceMediaRuntime.standaloneItems 里，只属于这台设备，
//     播完最后一项就结束（状态转 stopped，active 置 nil），Next/Prev 不生效；
//   - agent_playlist：列表存在全局协调器的 agentMediaPlaylist 里（按 agentID 共享），
//     同一个 agent 下的多台设备看到的是同一份歌单，可以接力播放，
//     Next/Prev 在歌单内循环（见 jumpToRelative）。
//
// 为什么必须二选一：如果两份列表同时存在，"当前播的是哪一份、切歌该切哪一份"就无从判断，
// 多设备共享时还会互相踩。因此一旦进入 agent_playlist 模式，standaloneItems 会被清空
// （见 AppendCurrentToAgentPlaylist / PlayAgentPlaylistIndex）。
type mediaPlaybackMode string

// 两种播放模式的取值（互斥，见上面的说明）。
const (
	mediaPlaybackModeStandalone    mediaPlaybackMode = "standalone"     // 独立播放——播一次就结束
	mediaPlaybackModeAgentPlaylist mediaPlaybackMode = "agent_playlist" // 智能体歌单——跨设备共享的播放列表
)

// "为什么暂停"——记录暂停的原因，从而决定恢复时该怎么处理。
//
// 为什么必须区分"用户暂停"和"被打断暂停"，而不是简单地用一个 paused 布尔：
//   - mediaPauseReasonUser：用户自己按了暂停（或语音说了"暂停"）。这种暂停是用户意图，
//     后续哪怕设备重连、哪怕下一轮对话结束，都**不应该**擅自继续播放，
//     只有用户明确说"继续播放"才会恢复（ResumeIfInterruptedPause 会直接返回 false）。
//   - mediaPauseReasonInterrupt：系统为了给 TTS 语音让路而临时挂起（StopSpeaking → Suspend）。
//     这是"被打断"，不是用户意图，所以这轮对话结束 / 设备重连成功后应当自动恢复
//     （见 ResumeIfInterruptedPause 与 attachSession 里的 resumeOnAttach 分支）。
//
// 混用的后果很直观：用户明明说了暂停，助手一说完话就自己唱了起来。
type mediaPauseReason string

// 三种暂停原因的取值：
//   - ""（none）：当前没有暂停；
//   - user：用户主动暂停，重连或被 TTS 打断都不会自动恢复；
//   - interrupt：为给 TTS 让路而被挂起，可在对话收尾或设备重连后自动恢复。
const (
	mediaPauseReasonNone      mediaPauseReason = ""
	mediaPauseReasonUser      mediaPauseReason = "user"
	mediaPauseReasonInterrupt mediaPauseReason = "interrupt"
)

// mediaRecoveryTrigger 说明"这次恢复播放是谁要求的"。
//
// 因为两种来源的语义不同：用户要求必须真的恢复；而重连触发的恢复要先看
// resumeOnAttach 标记（用户暂停过、或被 Stop 过就不该自动响起来，见 RecoverPlayback）。
type mediaRecoveryTrigger string

// 恢复播放的两种触发来源取值：
//   - user：用户明确要求继续播放，无条件恢复；
//   - attach：设备重连绑定会话后自动恢复，恢复前要先看 resumeOnAttach 标记。
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
// MediaSourceDescriptor 是"一条音频的统一描述"：告诉播放器要播什么、从哪来、怎么打开。
//
// 上游（工具调用、MCP 资源回调）只要把它填好，播放器就能一视同仁地打开音频流
// （见 openSourceAudioStream）。MIMEType 决定解码格式，Meta 承载展示用元数据（歌手、时长、封面等）。
// 上面的 /* */ 示例给出了四种来源各自的填充方式。
//
// MCP / HTTP / Local / Inline 四个可选字段由 SourceType 决定用哪个，其余保持 nil：
//   - MCP：通过 MCP 客户端的 ReadResource 按页拉取音频；
//   - HTTP：直接 GET 拉流，音乐 URL 通常走这里；
//   - Local：读服务端磁盘上的音频文件；
//   - Inline：音频字节已经在内存里（工具直接返回）。
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

// HTTPMediaSource 描述一个可直接 HTTP GET 拉取的音频地址。
// 字段 URL 是音频地址（http/https），播放时拉流解码，不落盘。
type HTTPMediaSource struct {
	URL string
}

// LocalMediaSource 描述服务端磁盘上的一个音频文件。
// 字段 Path 是文件路径，由 openLocalMediaFileStream 打开并解码。
type LocalMediaSource struct {
	Path string
}

// InlineAudioSource 描述"已经在内存里"的音频字节（例如工具调用直接返回的音频内容）。
// 字段 Data 是原始音频字节（mp3/wav 等），解码后推给设备。
type InlineAudioSource struct {
	Data []byte
}

// 播放列表里的一项——把"一个媒体源"包装成"播放列表的一个条目"，加上 ID 和加入时间。
//
// 为什么条目要有自己的 ID（而不是直接复用 Source.ID）：同一个音频可以被重复加入歌单
// （用户连着说两次"加入歌单"），此时 Source.ID 相同，只能靠 Item.ID 区分是两个条目。
// 为什么要有 AddedAt：歌单按加入顺序播放/展示，出问题时也能知道"这条是什么时候进来的"。
// Source 字段存的是媒体源的深拷贝（见 cloneMediaSourceDescriptor），
// 因此条目一旦进入列表，外部再改原描述符也不会影响播放。
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
// MediaPlayerState 是播放器对外暴露的**只读状态快照**（GetState 返回的是深拷贝）。
//
// 各字段语义：
//   - Status：idle（还没播过）/ playing / paused / stopped（播完或被 Stop）/ error；
//   - Playlist：歌单模式下的列表快照；standalone 模式为 nil（没有共享歌单）；
//   - CurrentIndex：在 Playlist 中的下标；standalone 或列表为空时为 -1，别当 0 用；
//   - CurrentTitle / CurrentSourceType：当前在播的标题与来源类型（展示与 realtime 门控都要用）；
//   - PositionMs：按"已推送帧长"累加出的时长估算，不是设备端真实播放进度；
//   - ErrMsg：只在 Status==error 时有意义（见 markPlaybackError）；
//   - UpdatedAt：状态最后一次变化的时间（毫秒），排查时序问题用。
//
// 为什么强调"只读"：外部（工具层、realtime 门控）只拿它做判断，
// 字段都是值的拷贝，调用方怎么改都不会反向影响播放器内部状态。
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
// PlaylistAppendResult 是"把当前正在播的媒体加入歌单"操作（AppendCurrentToPlaylist）的返回结果。
//
// 它一次性回答了调用方最关心的三件事，便于工具层直接组织成对用户/LLM 的回复：
// 加了什么（AddedTitle）、歌单现在多长（PlaylistLength）、
// 加入动作有没有打断正在进行的播放（CurrentIndex/CurrentStatus/CurrentPosition 保持原值）。
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
//
// 生命周期：newActiveMediaPlayback 创建 → 由 go r.runPlayback(active) 跑播放主循环
// → 循环退出时 defer closeDone() 关闭 done 通知所有等待者，并归还独占输出权。
//
// 暂停机制的核心是 resumeCh：
//   - 暂停（setPaused(true)）时把它换成一个**新的、未关闭的** channel；
//     播放循环里所有 waitIfPaused 都会阻塞在这个新 channel 上；
//   - 恢复（setPaused(false)）时 close 它，所有阻塞点立刻被唤醒。
//
// 这样"暂停"既不需要轮询，也不需要 sync.Cond，是靠"通道阻塞 + 关闭唤醒"实现的。
// 注意每次暂停/恢复都会换新通道，所以等待方必须在循环里重新读取 resumeCh 引用。
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
//
// 为什么需要这层接口：deviceMediaRuntime 是跨会话存活的（状态比 ChatSession 活得久），
// 如果它直接持有 *ChatSession / *TTSManager，设备断线重连后就会指向已失效的对象。
// 改成"每次播放前由当前 attachment 提供一个有效的 bridge"，运行时与具体会话就解耦了。
// 代价是每个方法都要做 nil 防御（会话可能正在销毁）。
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
//
// 这里特意做成"薄委托"而不是包一层状态：
// TTSManager 自己会处理独占、流控、打断与指标收口，媒体侧不需要（也不应该）重复实现，
// 否则两条路径的状态很容易不一致。每个方法的 nil 判断是为了应对"会话正在销毁"的窗口期。
type sessionMediaOutputBridge struct {
	session *ChatSession
}

// 按 agent（智能体）维度组织的歌单——同一个 agent 下的所有设备共享这个歌单，可以互相看到、接续播放。
//
// 与 standalone 模式的差别：这份歌单不属于任何设备，而是挂在全局协调器上按 agentID 索引，
// 所以 A 设备加入的歌，B 设备能看到并接着播。
// mu 用 RWMutex：读取（快照）远多于写入（加入），读操作用 RLock 并发进行；
// 所有写入（appendToAgentPlaylist）都持写锁完成，保证 items 与 updatedAt 同步更新。
type agentMediaPlaylist struct {
	mu        sync.RWMutex        // 读写锁，保护 items 和 updatedAt
	items     []MediaPlaylistItem // 歌单里的所有项
	updatedAt int64               // 最后更新时间（Unix 毫秒）
}

// 全局媒体播放协调器——管理所有 agent 的歌单和所有设备的播放运行时，是媒体播放系统的"总注册表"。
//
// 为什么必须是全局唯一：歌单要跨设备共享，播放状态要跨会话存活
// （设备断线时 ChatSession 会被销毁，重连后再绑定回来）。
//
// 注意两个 map 都是只增不减：设备数和 agent 数都很有限，属于可控的内存占用；
// 换来的是"设备重连后拿到的还是同一个 runtime"，状态与歌单不会凭空丢失。
type mediaPlaybackCoordinator struct {
	ctx            context.Context                // 全局上下文
	mu             sync.Mutex                     // 保护两个 map
	agentPlaylists map[string]*agentMediaPlaylist // agentID → 歌单
	deviceRuntimes map[string]*deviceMediaRuntime // deviceID → 设备运行时
}

// 是单个设备的播放运行时——整个文件最核心的结构体，管理这台设备的播放状态、播放列表、播放活动、会话绑定。
//
// 读代码时请记住几条不变量（大量分支都是为维持它们而写）：
//   - mode 决定"列表从哪来"：standalone → standaloneItems；agent_playlist → 协调器里按 playlistAgentID 存的歌单，两者互斥；
//   - active == nil 表示当前没有播放循环在跑（Stop / 播完 / 出错都会把它置 nil）；
//   - attachment == nil 表示设备当前不在线、输出通道不可用，此时若 active 存在它会停在暂停态；
//   - resumeOnAttach == true 表示"重连后应自动恢复播放"，只有断线保留(preserve=true)
//     或输出通道失效时才会置位；用户主动 Stop/Pause 会清掉它，否则重连后会莫名自己响起来。
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
//
// 为什么要有门面而不是让会话直接用 runtime：
//   - 会话层不想关心"当前音频输出格式是什么、当前 agentID 是谁"，这些由门面统一补齐
//     （audioConfig / currentAgentID）；
//   - runtime 可能为 nil（会话缺少 clientState 时），门面用 runtimeOrErr 统一兜错。
//
// 注意：runtime 是跨会话共享的，同一个设备重建会话后仍指向同一个 runtime。
type SessionMediaPlayer struct {
	session     *ChatSession              // 所属会话
	coordinator *mediaPlaybackCoordinator // 全局协调器
	runtime     *deviceMediaRuntime       // 设备运行时（真正的实现）
}

// sharedMediaPlaybackCoordinator 是进程内唯一的媒体协调器实例。
// 必须是全局单例：歌单要跨设备共享、播放状态要跨会话存活，每个会话各自持有一份就无从共享
// （见 NewSessionMediaPlayer 从这里取 runtime）。
var sharedMediaPlaybackCoordinator = newMediaPlaybackCoordinator()

// 是一个"已关闭的完成信号通道"——它的作用是兜底：当 MediaPlaybackHandle 无效时返回它，让 <-ch 立即返回，避免永久阻塞。, 展示没理解
//
// 为什么能兜底：Go 里已关闭的 channel 可以立刻被读取（返回零值），
// 所以用它表示"这次播放已经结束 / 根本不存在"，调用方 select 等待时不会卡住。
// 典型场景：PlaySourceWithHandle 返回句柄时，如果这次播放已被别的调用 Stop 覆盖，
// 只能给一个形式上的句柄；调用方（如 tool.go）仍会 <-handle.Done()，
// 此时必须立刻返回，不能让整条工具调用链挂死。
//
// 为什么用立即执行的闭包初始化：包级变量不能先 make 再 close（那是两条语句），
// 用匿名函数包一层就能在一条声明里造出"已关闭的通道"。
var closedMediaPlaybackDoneCh = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// newMediaPlaybackHandle 用一次播放活动（activeMediaPlayback）构造对外句柄。
//
// active 为空、或它的 done 通道尚未建立时（例如这次播放已经被 Stop/替换抢先取消），
// 返回携带 closedMediaPlaybackDoneCh 的句柄，保证调用方的 Done/Wait 立刻返回而不是永久阻塞。
func newMediaPlaybackHandle(active *activeMediaPlayback) *MediaPlaybackHandle {
	if active == nil || active.done == nil {
		return &MediaPlaybackHandle{done: closedMediaPlaybackDoneCh}
	}
	return &MediaPlaybackHandle{done: active.done}
}

// Done 返回"本次播放结束"的只读信号通道。
//
// 返回 <-chan（只读）而不是 chan，是为了让调用方只能等待、不能关闭它
// （关闭信号只能由播放循环自己发出，见 closeDone）。
// 句柄无效时返回 closedMediaPlaybackDoneCh，使 <-h.Done() 立即返回。
func (h *MediaPlaybackHandle) Done() <-chan struct{} {
	if h == nil || h.done == nil {
		return closedMediaPlaybackDoneCh
	}
	return h.done
}

// Wait 阻塞等待本次播放结束，或等到 ctx 超时/被取消。
//
// 返回 nil 只表示"播放循环已经退出"，不代表播放成功——失败原因要看 GetState().ErrMsg；
// 返回 ctx.Err() 表示等待被取消，此时播放可能仍在继续（调用方不应据此认为已停）。
// ctx 为 nil 时退化为无限等待。
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

// newActiveMediaPlayback 创建一次播放活动。
//
// 做三件事：派生可取消的 ctx（用于打断/停止时立刻中断在途 IO）、建 done 通道、
// 把 resumeCh 初始化为**已关闭**状态——表示"初始就是播放中"，
// 这样第一次 waitIfPaused 拿到已关闭的通道会立即返回，不用特判初始状态。
// parent 为 nil 时退化为 context.Background()，避免调用方漏传上下文导致播放无法被取消。
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

// closeDone 关闭 done 通道，通知所有等待者"本次播放已经结束"。
//
// 先 select 探测再 close：close 一个已关闭的 channel 会 panic，
// 而这个方法可能从 runPlayback 的 defer 与 Stop 收尾路径同时到达，必须幂等。
func (a *activeMediaPlayback) closeDone() {
	select {
	case <-a.done:
	default:
		close(a.done)
	}
}

// setPaused 切换暂停状态，并返回"这次调用是否真的改变了状态"。
//
// 实现依赖的两步（也是整个暂停功能的关键）：
//  1. 暂停：把 resumeCh 换成**新的未关闭**通道 —— 播放循环里所有 waitIfPaused
//     都会阻塞在这个新通道上，等不到恢复就出不来；
//  2. 恢复：close(resumeCh) —— 所有阻塞点立刻被唤醒，无需轮询、无轮询间隔延迟。
//
// 提前 return false 表示状态没变化（重复暂停 / 重复恢复），
// 调用方据此避免重复记日志，也避免把"重复恢复"误判成"刚刚恢复成功"。
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

// isPaused 读取当前是否处于暂停。加锁读取，供状态判断与 realtime 门控使用。
func (a *activeMediaPlayback) isPaused() bool {
	a.pauseMu.Lock()
	defer a.pauseMu.Unlock()
	return a.paused
}

// hasExclusiveHeld 报告本次播放当前是否已持有 TTS 的独占输出权。
// 与 deviceMediaRuntime.exclusiveAttachment 配合，用来判断"还要不要再申请一次独占"
// （重复申请会被 TTSManager 直接报错拒绝）。
func (a *activeMediaPlayback) hasExclusiveHeld() bool {
	a.pauseMu.Lock()
	defer a.pauseMu.Unlock()
	return a.exclusiveHeld
}

// setExclusiveHeld 更新"是否已持有独占"的本地标记，返回是否发生变化。
//
// 它只改标记，不真正申请/释放独占：真正的动作在 ensureExclusivePlayback（申请）
// 与 releaseExclusivePlayback（释放）里做。标记的意义是让运行时知道
// "现在这条播放活动确实占着 TTS 的输出通道"，从而在停止/挂起时正确归还。
func (a *activeMediaPlayback) setExclusiveHeld(held bool) bool {
	a.pauseMu.Lock()
	defer a.pauseMu.Unlock()

	if a.exclusiveHeld == held {
		return false
	}
	a.exclusiveHeld = held
	return true
}

// waitIfPaused 是播放循环里"按需原地等待"的统一入口：
// 没暂停就立即返回，处于暂停则阻塞，直到被恢复或被 ctx 取消。
//
// 为什么用 for 循环、每轮都重新加锁读状态，而不是锁一次就读完：
//   - 暂停可能恰好发生在"读完状态"和"进入 select"之间，只读一次会漏掉这次暂停；
//   - 每轮都重新读取**最新的 resumeCh 引用**，这样即使 setPaused(false)
//     在我们 select 之前已经关闭旧通道并换了新通道，我们也不会傻等在新通道上，
//     导致错过这次恢复、出现"说了继续却不动"的问题。
//
// 因此：任何需要"能被暂停"的循环都必须通过本方法等待，不要自己 select resumeCh。
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

// newMediaPlaybackCoordinator 创建协调器。全局上下文用 context.Background()：
// 播放活动的生命周期由各自的 cancel 控制，不应随某个请求上下文被取消（否则用户一次请求超时就会把音乐掐掉）。
func newMediaPlaybackCoordinator() *mediaPlaybackCoordinator {
	return &mediaPlaybackCoordinator{
		ctx:            context.Background(),
		agentPlaylists: make(map[string]*agentMediaPlaylist),
		deviceRuntimes: make(map[string]*deviceMediaRuntime),
	}
}

// newDeviceMediaRuntime 创建一台设备的播放运行时。初始值刻意选成这样：
//   - playbackIndex = -1：表示"还没有当前项"，避免被误当成第 0 项；
//   - state.Status = idle、CurrentIndex = -1：对外表达"这台设备从没播过"；
//   - attachmentWaitCh 先建好（未关闭）：让等待输出的调用方有东西可等。
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

// getOrCreateAgentPlaylist 取出（不存在则创建）某个 agent 的共享歌单。
// 加锁的意义：并发下必须保证同一个 agent 只有一份歌单，
// 否则两台设备会各写各的列表，出现"我加的歌你看不到"。
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

// getOrCreateRuntime 取出（不存在则创建）某台设备的运行时。
// 设备重连时会命中"已存在"分支，从而拿回断线前的播放状态、歌单与恢复标记。
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

// snapshotAgentPlaylist 返回某个 agent 歌单的**深拷贝快照**。
//
// 为什么返回拷贝而不是内部切片：歌单是跨设备共享的数据，调用方（播放循环、GetState）
// 会持有它一段时间；直接返回内部切片的话，外部一改就污染了全局歌单，
// 也会和并发写入撞上数据竞争。深拷贝换来的是"读到的就是那一刻的列表"。
// agentID 为空白时直接返回 nil，避免所有"没有 agentID"的设备共用一份莫名其妙的歌单。
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

// appendToAgentPlaylist 把一条媒体源加入 agent 歌单，返回（新条目、其在歌单中的下标、歌单快照、错误）。
//
// 三个刻意的处理：
//  1. 先 cloneMediaSourceDescriptor 再入表 —— 歌单是跨设备共享的，若直接存调用方传进来的
//     描述符指针，别的设备/别的 goroutine 一改就会影响全局歌单；
//  2. ID / Title 缺失时补齐 —— ID 用纳秒时间戳保证唯一（同一首歌可以被反复加入），
//     Title 用 deriveMediaTitle 从 URL / 路径 / 描述推导，保证端上总有名字可显示；
//  3. 返回值里给的是条目的深拷贝，调用方改它不会影响歌单内部数据。
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

// [6.0] 取独占输出：向会话的 TTSManager 申请"独占下行输出"（媒体与 TTS 共用同一条发送通道）。
//
// BeginExclusiveMediaPlayback 向会话的 TTSManager 申请"独占下行输出"。
//
// 为什么媒体必须申请独占：媒体与 TTS 共用同一条发送队列，若不独占，
// 用户说话触发的新一轮 TTS 会插进音乐里，端上就变成"音乐和机器人同时响"。
// 申请成功后 TTS 会清掉待播内容并让路（详见 tts.go 的 BeginExclusiveMediaPlayback）。
func (b *sessionMediaOutputBridge) BeginExclusiveMediaPlayback(ctx context.Context) error {
	if b == nil || b.session == nil || b.session.ttsManager == nil {
		return fmt.Errorf("媒体输出桥未初始化")
	}
	return b.session.ttsManager.BeginExclusiveMediaPlayback(ctx)
}

// EndExclusiveMediaPlayback 归还独占，并唤醒此前被媒体挡住的所有 TTS 任务。
// 幂等：重复调用不会出错（TTSManager 内部也做了防御）。
func (b *sessionMediaOutputBridge) EndExclusiveMediaPlayback() {
	if b == nil || b.session == nil || b.session.ttsManager == nil {
		return
	}
	b.session.ttsManager.EndExclusiveMediaPlayback()
}

// SendSentenceStart 往输出通道插入一条 sentence_start：告诉端上"这段音频开始了，字幕显示这段文本"。
// 媒体用它把"正在播放音乐: 歌名"推给端上做字幕。
// onError 由 TTSManager 在发送失败时回调，媒体借此感知"输出通道失效"并转入等待重连。
func (b *sessionMediaOutputBridge) SendSentenceStart(ctx context.Context, text string, onError func(error)) error {
	if b == nil || b.session == nil || b.session.ttsManager == nil {
		return fmt.Errorf("媒体输出桥未初始化")
	}
	return b.session.ttsManager.EnqueueMediaSentenceStart(ctx, text, onError)
}

// SendSentenceEnd 插入一条 sentence_end，表示这段音频已经全部发送完毕。
//
// onEnd 是关键的同步点：TTSManager 会在"帧确实发完（含客户端缓冲水位走完）"之后才回调，
// 媒体必须等到它才能确认这一首真的播完、可以推进到下一首（见 waitMediaSentenceEnd），
// 否则会出现"上一首还在响就开始推下一首"的重叠。
func (b *sessionMediaOutputBridge) SendSentenceEnd(ctx context.Context, text string, onError func(error), onEnd func(error)) error {
	if b == nil || b.session == nil || b.session.ttsManager == nil {
		return fmt.Errorf("媒体输出桥未初始化")
	}
	return b.session.ttsManager.EnqueueMediaSentenceEnd(ctx, text, onError, onEnd)
}

// SendAudioFrame 把一帧音频交给 TTSManager 的发送队列（与 TTS 音频走同一条流控路径）。
// 不直接调用底层 SendAudio 的原因：绕过去就失去了流控（playbackTail）与打断失效
// （audioGeneration 代际号）的保护，容易在打断后继续把旧音频推给设备。
func (b *sessionMediaOutputBridge) SendAudioFrame(ctx context.Context, frame []byte, onError func(error)) error {
	if b == nil || b.session == nil || b.session.ttsManager == nil {
		return fmt.Errorf("媒体输出桥未初始化")
	}
	return b.session.ttsManager.EnqueueMediaFrame(ctx, frame, onError)
}

// NewSessionMediaPlayer 为会话创建媒体播放器门面。
//
// 构造时就从全局协调器取（或创建）本设备的 runtime —— 所以即使会话刚建、还没播过任何东西，
// runtime 也已经存在并与设备 ID 绑定；设备重连后新会话能立刻接管上一次的播放状态。
// session / clientState 缺失时 runtime 保持 nil，后续调用由 runtimeOrErr 统一报错。
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

// runtimeOrErr 取出本门面依赖的设备运行时；未初始化时返回错误。
// 所有对外方法都先过它，避免每处都重复写 nil 判断。
func (p *SessionMediaPlayer) runtimeOrErr() (*deviceMediaRuntime, error) {
	if p == nil || p.runtime == nil {
		return nil, fmt.Errorf("media player 未初始化")
	}
	return p.runtime, nil
}

// audioConfig 计算本次播放要用的音频格式（采样率 + 帧长）。
//
// 优先级：会话当前的输出音频格式（握手时协商好的）> 包级默认常量。
// 之所以每次都现算而不缓存：设备握手/重连后输出格式可能变化（不同 TTS provider 采样率不同），
// 媒体必须跟着变，否则端上播放会出现变速、变调。
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

// currentAgentID 返回当前会话所属的 agentID（歌单就是按它共享的）。取不到时返回空串。
func (p *SessionMediaPlayer) currentAgentID() string {
	if p == nil || p.session == nil || p.session.clientState == nil {
		return ""
	}
	return strings.TrimSpace(p.session.clientState.AgentID)
}

// AttachSession 把当前会话绑定为播放输出通道（会话 Start 时调用）。
// 绑定后如果 runtime 上还留着 resumeOnAttach 标记，会自动异步恢复上一次的播放（见 attachSession）。
func (p *SessionMediaPlayer) AttachSession() {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return
	}
	runtime.attachSession(p.session)
}

// DetachSession 解绑当前会话（会话关闭 / 连接断开时调用）。
// preserve=true 表示"保留播放状态、等设备重连后继续"；
// false 表示放弃这次播放（例如设备真的下线不再回来）。
func (p *SessionMediaPlayer) DetachSession(preserve bool) {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return
	}
	runtime.detachSession(p.session, preserve)
}

// PlaySource 播放单个媒体源（standalone 模式，播完即结束），不等播放结束。
// 需要等播完再做后续动作时用 PlaySourceWithHandle。
func (p *SessionMediaPlayer) PlaySource(ctx context.Context, source MediaSourceDescriptor) error {
	_, err := p.PlaySourceWithHandle(ctx, source)
	return err
}

// ReplacePlaylistAndPlay 用给定列表整体替换 standalone 列表，并从 startIndex 开始播。
// 注意是"替换"：调用后本设备之前那份 standalone 列表就没了（agent 歌单不受影响）。
func (p *SessionMediaPlayer) ReplacePlaylistAndPlay(ctx context.Context, sources []MediaSourceDescriptor, startIndex int) error {
	_, err := p.ReplacePlaylistAndPlayWithHandle(ctx, sources, startIndex)
	return err
}

// [6.1] 播放媒体：播单个源并返回句柄，调用方可用句柄等这次播放结束。
//
// PlaySourceWithHandle 播放单个媒体源并返回句柄，调用方可用它等待这次播放结束
// （工具调用需要"先播完再继续问 LLM"时就用这个，例如 tool.go 的音频结果回放）。
func (p *SessionMediaPlayer) PlaySourceWithHandle(ctx context.Context, source MediaSourceDescriptor) (*MediaPlaybackHandle, error) {
	return p.ReplacePlaylistAndPlayWithHandle(ctx, []MediaSourceDescriptor{source}, 0)
}

// ReplacePlaylistAndPlayWithHandle 与 ReplacePlaylistAndPlay 相同，但额外返回完成句柄。
// 这里把 audioConfig 一起传下去：播放必须锁定"当时协商好的"输出格式。
func (p *SessionMediaPlayer) ReplacePlaylistAndPlayWithHandle(ctx context.Context, sources []MediaSourceDescriptor, startIndex int) (*MediaPlaybackHandle, error) {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return nil, err
	}
	return runtime.ReplaceStandaloneQueueAndPlayWithHandle(ctx, sources, startIndex, p.audioConfig())
}

// Pause 用户主动暂停（当前没有在播时返回错误）。
// 会把 pauseReason 记为 user，因此重连或被 TTS 打断都不会让它自动恢复。
func (p *SessionMediaPlayer) Pause() error {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return err
	}
	return runtime.Pause()
}

// Play 用户主动要求继续播放（恢复自己暂停的内容，或重播当前媒体）。
// 与 Resume 的区别：Play 走完整的"重建播放"路径（能在没有 active 时重新起播），Resume 只恢复活动中的播放。
func (p *SessionMediaPlayer) Play(ctx context.Context) error {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return err
	}
	return runtime.RecoverPlayback(ctx, p.audioConfig(), mediaRecoveryTriggerUser)
}

// PlayAgentPlaylist 播放当前 agent 的共享歌单；若本设备已经在播同一份歌单，则从当前那首继续（不回到第一首）。
func (p *SessionMediaPlayer) PlayAgentPlaylist(ctx context.Context) error {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return err
	}
	return runtime.PlayAgentPlaylist(ctx, p.currentAgentID(), p.audioConfig())
}

// Resume 无条件恢复当前播放活动（不看暂停原因）。属于内部/快捷入口；
// 面向"用户说继续"的语义请用 Play，面向"打断后收尾"的语义请用 ResumeIfInterruptedPause。
func (p *SessionMediaPlayer) Resume() error {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return err
	}
	return runtime.Resume()
}

// ResumeIfInterruptedPause 只在"暂停原因是 interrupt"时恢复播放，返回是否真的恢复了。
//
// 为什么需要这个"条件恢复"：媒体为了给 TTS 让路会被挂起（interrupt），
// 这轮对话结束后应当把它接上；但如果是用户自己按的暂停（user），就绝不能自动恢复。
// 调用方（工具/会话收尾）只要调它，不需要自己判断 pauseReason。
func (p *SessionMediaPlayer) ResumeIfInterruptedPause() (bool, error) {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return false, err
	}
	return runtime.ResumeIfInterruptedPause()
}

// Stop 停止播放并清空状态（active 置 nil、进度归零、错误清空）。
// 与 Suspend 的区别：Stop 表示"不播了"，会清掉 resumeOnAttach，设备重连也不会自动恢复。
func (p *SessionMediaPlayer) Stop(ctx context.Context) error {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return err
	}
	return runtime.Stop(ctx)
}

// Suspend 临时挂起播放（暂停，但语义是"让路"而不是"用户不想听了"）。
// 典型来源：StopSpeaking 打断时为 TTS 让路，pauseReason 记为 interrupt，
// 之后可被 ResumeIfInterruptedPause 自动接上。当前没有播放时不报错（挂起本就无需处理）。
func (p *SessionMediaPlayer) Suspend() error {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return err
	}
	return runtime.Suspend()
}

// Next 切到歌单下一首（只在 agent_playlist 模式生效；在末尾会绕回第一首）。
func (p *SessionMediaPlayer) Next(ctx context.Context) error {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return err
	}
	return runtime.Next(ctx, p.audioConfig())
}

// Prev 切到歌单上一首（只在 agent_playlist 模式生效；在第一首会绕到最后一首）。
func (p *SessionMediaPlayer) Prev(ctx context.Context) error {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return err
	}
	return runtime.Prev(ctx, p.audioConfig())
}

// GetState 返回当前播放状态的快照（深拷贝，只读）。
// runtime 未初始化时返回 idle 快照而不是错误：调用方（工具层、realtime 门控）拿到"空状态"即可正常判断。
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

// HasRealtimeMcpAudioControlContext 报告"当前是否存在可被本地指令控制的 MCP 音频播放"。
//
// 用途：realtime 门控（realtime_media_gate.go）先看它；为 false 就完全不拦截用户的话，直接交给 LLM。
func (p *SessionMediaPlayer) HasRealtimeMcpAudioControlContext() bool {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return false
	}
	return runtime.hasRealtimeMcpAudioControlContext()
}

// ShouldGateRealtimeMcpAudioASR 报告"当前是否应当把 ASR 文本直接吞掉（不交给 LLM）"。
//
// 场景：realtime 下 MCP 音频正在播放且未暂停时，用户说的话如果既不是控制指令也不是退出词，
// 就不该为了回话而打断音乐，于是被门控丢弃（用户可以先说"暂停"再提问）。
func (p *SessionMediaPlayer) ShouldGateRealtimeMcpAudioASR() bool {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return false
	}
	return runtime.shouldGateRealtimeMcpAudioASR()
}

// AppendCurrentToPlaylist 把当前正在播的媒体加入当前 agent 的共享歌单，
// 同时把本设备切到 agent_playlist 模式（详见 deviceMediaRuntime.AppendCurrentToAgentPlaylist）。
func (p *SessionMediaPlayer) AppendCurrentToPlaylist() (*PlaylistAppendResult, error) {
	runtime, err := p.runtimeOrErr()
	if err != nil {
		return nil, err
	}
	return runtime.AppendCurrentToAgentPlaylist(p.currentAgentID())
}

// attachSession 把某个会话绑定为本设备的输出通道（会话启动时调用，见 session.go 的 Start）。
//
// 关键行为：
//  1. 组装 mediaSessionAttachment（会话 + 输出桥）写入 attachment，并通知 attachmentWaitCh
//     —— 正在"等输出通道"的播放循环会被唤醒，继续把音频推出去；
//  2. 重复绑定同一个会话时直接返回（幂等），避免无意义地反复通知；
//  3. 若断线时置过 resumeOnAttach，就异步发起一次恢复播放
//     —— 放 goroutine 是因为恢复要重建播放循环，不能阻塞会话启动路径。
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

// detachSession 解绑会话；preserve=true 表示"保留可恢复状态"（设备断线 / 会话关闭时的常规做法）。
//
// 为什么断线要保留而不是直接停：设备可能只是网络抖动，用户心理预期是"音乐还在放"。
// 具体做法：preserve 且当前确实在播时（wasPlaying 判断，暂停中的不必再处理），
// 把播放活动置为暂停（播放循环原地等待），并标 resumeOnAttach=true，等重连后自动恢复。
// 另外：解绑时必须归还独占——占着 TTS 输出通道不放会把后续所有 TTS 语音永久挡住。
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

// notifyAttachmentChangedLocked 通知"绑定发生了变化"：关闭旧的 attachmentWaitCh 并换上一个新通道。
//
// 为什么用"关闭 + 换新"而不是往通道里发一个值：
// 关闭能让**所有**等待者同时醒来（发送只能叫醒一个）；换新通道则保证下一次等待等的是新的变化，
// 不会因为读到已关闭的旧通道而立刻返回、空转一轮。
// 方法名带 Locked，表示调用方必须已经持有 r.mu。
func (r *deviceMediaRuntime) notifyAttachmentChangedLocked() {
	if r.attachmentWaitCh != nil {
		close(r.attachmentWaitCh)
	}
	r.attachmentWaitCh = make(chan struct{})
}

// ReplaceStandaloneQueueAndPlay 只关心成败的简化版，行为与 WithHandle 完全一致（见其注释）。
func (r *deviceMediaRuntime) ReplaceStandaloneQueueAndPlay(ctx context.Context, sources []MediaSourceDescriptor, startIndex int, cfg mediaPlaybackAudioConfig) error {
	_, err := r.ReplaceStandaloneQueueAndPlayWithHandle(ctx, sources, startIndex, cfg)
	return err
}

// ReplaceStandaloneQueueAndPlayWithHandle 用一份新列表替换 standalone 队列并立即开播，返回完成句柄。
//
// 流程与要点：
//  1. sources 为空等价于 Stop（没有内容可播），startIndex 越界直接报错——宁可失败也不要播错内容；
//  2. buildMediaPlaylistItems 先给每个源补齐条目 ID 与标题，再新建 activeMediaPlayback（本次播放的活动对象）；
//  3. 持锁一次性切换所有状态：mode=standalone、清空 playlistAgentID（保证与歌单模式互斥）、
//     记录起始下标、冻结音频格式、深拷贝当前媒体源、重置进度与错误；
//  4. 收尾顺序很重要：先释放旧活动占着的独占，再取消并等待旧播放循环退出，最后才起新循环。
//     这样新循环申请的独占不会被旧循环的收尾动作误释放（两者操作的是同一份 attachment）。
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

// PlayAgentPlaylistIndex 切到 agent 歌单模式，并从共享歌单的第 startIndex 项开始播。
//
// 与 standalone 版本的差别：列表来自协调器的共享快照（不是本设备私有的 standaloneItems），
// 并把整份快照写进对外状态，让端上和工具能看到完整歌单。
// 快照为空或下标非法时直接报错——共享歌单随时可能被别的设备改动/清空，必须重新校验。
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

// PlayAgentPlaylist 播放 agent 歌单。
//
// 起始下标的选择是有意为之：若本设备已经在播同一份歌单（mode/agentID 都对且下标仍有效），
// 就从当前那首继续——用户说"播放歌单"时不希望莫名其妙回到第一首；
// 否则从第 0 项开始。真正的切换交给 PlayAgentPlaylistIndex。
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

// Pause 用户主动暂停：暂停但要求必须有正在播放的内容（没有则报错，让上层能告诉用户"现在没有在放"）。
func (r *deviceMediaRuntime) Pause() error {
	return r.pausePlayback(true, mediaPauseReasonUser)
}

// Suspend 为让路而挂起（pauseReason=interrupt），没有正在播放的内容时不报错——
// 打断流程只关心"音乐别再响了"，不确定是否在播，用错误会污染调用链。
func (r *deviceMediaRuntime) Suspend() error {
	return r.pausePlayback(false, mediaPauseReasonInterrupt)
}

// Play 用户语义的"继续播放"：等价于 RecoverPlayback(..., mediaRecoveryTriggerUser)，也就是无条件恢复。
func (r *deviceMediaRuntime) Play(ctx context.Context, cfg mediaPlaybackAudioConfig) error {
	return r.RecoverPlayback(ctx, cfg, mediaRecoveryTriggerUser)
}

// RecoverPlayback 是恢复播放的总入口，两条路径：
//   - 播放活动还活着（active != nil）且处于暂停：直接解除暂停（recoverActivePlayback），
//     即"原地续播"——进度、独占、解码流都不用重建，这是最常用的快路径；
//   - 播放活动已经没了（Stop / 播完 / 出错 / 重连丢弃）：按当前模式与当前媒体源重建一次播放
//     （agent_playlist 用歌单快照重建，standalone 用 currentSource 重建）。
//
// trigger 决定前置条件：attach 触发要先看 resumeOnAttach（用户暂停过就不该自动响起来），
// user 触发则无条件执行。已经在播放（非暂停）时直接跳过，避免把正在播的内容从头重启一遍。
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

// Resume 无条件恢复当前播放活动（不看暂停原因）；没有可恢复的活动时报错。
func (r *deviceMediaRuntime) Resume() error {
	return r.recoverActivePlayback(mediaRecoveryTriggerUser)
}

// recoverActivePlayback 是"原地续播"的实现：只负责解除暂停并同步对外状态。
//
// 三个前置检查都不能省：
//   - active 为空 → 没有可续播的活动，报错让上层改走"重建播放"的路径；
//   - attachment 为空 → 设备不在线，此刻恢复也没人接收，报错；
//   - setPaused(false) 返回 false → 本来就没暂停，按成功返回（幂等，避免重复记日志）。
//
// 注意它持 r.mu 的同时调用 setPaused：后者用的是 active 自己的锁，不会与本锁互相等待。
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

// ResumeIfInterruptedPause 只在暂停原因是 interrupt 时恢复，返回是否真的恢复了。
// 先加读锁判断条件、释放锁后再去恢复：两步分开是为了避免持着 r.mu 走恢复路径
// （那条路径还要读写 attachment、状态等字段），减少锁嵌套与死锁风险。
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

// shouldResumeOnAttach 读取"重连后是否要自动恢复"的标记（加读锁，供 attachSession 与恢复流程判断）。
func (r *deviceMediaRuntime) shouldResumeOnAttach() bool {
	if r == nil {
		return false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.resumeOnAttach
}

// clearResumeOnAttach 清掉"重连后自动恢复"标记。
// 恢复已经执行过、或确认无需恢复（已经在播放）时调用，避免标记残留导致下次重连重复触发恢复。
func (r *deviceMediaRuntime) clearResumeOnAttach() {
	if r == nil {
		return
	}

	r.mu.Lock()
	r.resumeOnAttach = false
	r.mu.Unlock()
}

// Stop 停止播放：取消播放活动、归还独占、清空状态、复位恢复标记。
//
// 与 Suspend 的关键差别就在 resumeOnAttach：Stop 会把它清掉，表示"这次播放结束了，
// 重连也不要再自动响起来"。
// 实现上先在锁内把 active/exclusiveAttachment 取出来并置空，再到锁外做取消与等待
// （stopOldPlayback 会等播放循环退出，持锁等待容易把其他调用方一起卡住）。
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

// Next 切到下一首：jumpToRelative(+1)，只在歌单模式下有效（standalone 没有"下一首"的概念）。
func (r *deviceMediaRuntime) Next(ctx context.Context, cfg mediaPlaybackAudioConfig) error {
	return r.jumpToRelative(ctx, 1, cfg)
}

// Prev 切到上一首：jumpToRelative(-1)，同样只在歌单模式下有效。
func (r *deviceMediaRuntime) Prev(ctx context.Context, cfg mediaPlaybackAudioConfig) error {
	return r.jumpToRelative(ctx, -1, cfg)
}

// GetState 返回对外状态快照：先持锁按当前模式刷新派生字段（列表 / 下标），再整体拷贝并深拷贝列表。
// 深拷贝是必要的：state.Playlist 是切片，直接返回会与内部数据共享底层数组，外部一改就影响播放器。
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

// hasRealtimeMcpAudioControlContext 只取"能否被本地指令控制"这一个布尔（见 realtimeMcpAudioGateStatus）。
func (r *deviceMediaRuntime) hasRealtimeMcpAudioControlContext() bool {
	canControl, _ := r.realtimeMcpAudioGateStatus()
	return canControl
}

// shouldGateRealtimeMcpAudioASR 只取"是否要吞掉 ASR 文本、不交给 LLM"这一个布尔。
func (r *deviceMediaRuntime) shouldGateRealtimeMcpAudioASR() bool {
	_, shouldGate := r.realtimeMcpAudioGateStatus()
	return shouldGate
}

// realtimeMcpAudioGateStatus 计算 realtime 门控需要的两个判定，返回 (canControl, shouldGate)。
//
// 判定依据：
//   - 只认可 MCP 资源 / 内联音频两种来源（isRealtimeMcpAudioSourceType）：它们是工具或服务端
//     产生的音频内容，realtime 下需要支持语音控制；HTTP / 本地文件属于普通音乐，用工具命令控制即可；
//   - canControl：只要有播放活动、或标记了"重连后恢复"、或当前是暂停态，就认为用户还能控制它，
//     门控才会去识别"暂停 / 继续 / 下一首"这类指令；
//   - shouldGate：只有"正在播（attachment 有效且状态为 playing）且未暂停"时才吞 ASR 文本——
//     暂停状态下用户多半正是在下指令（继续 / 切歌），这时候吞掉就没人响应了。
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

// AppendCurrentToAgentPlaylist 把当前播放的媒体加入指定 agent 的共享歌单，并把本设备切换到歌单模式。
//
// 关键分支（都是为了"加入歌单不能打扰正在听的歌"）：
//   - 内联音频不允许加入：它的字节只在本次内存里，别的设备无法重放，直接报错比"加进去却播不出来"清楚；
//   - 若本设备本来就在播这个 agent 的歌单（preserveCurrentPlayback），加入后必须保持"当前项"不变，
//     于是 targetPlaybackIndex 沿用旧下标——新条目追加在末尾，不影响原有下标与正在播的歌。
//
// 状态里的 Playlist / CurrentIndex 也同步更新为共享歌单的最新快照，端上立刻能看到新列表。
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

// pausePlayback 是 Pause / Suspend 的共同实现。
//
// requireActive 决定"当前没有在播"时怎么表现：用户主动暂停要报错（上层才能告诉用户"现在没在放"），
// 为 TTS 让路的挂起则静默成功。
// 暂停时一定要归还独占：媒体已经不再推帧，却还占着 TTS 输出通道的话，机器人说话会被一直挡在外面。
// 同时清掉 resumeOnAttach：既然刚刚被暂停，就不该在重连时自动恢复——
// 自动恢复只应发生在"明确要求继续"的路径上。
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

// jumpToRelative 在歌单内相对跳转（Next 用 +1、Prev 用 -1），越界时按圈回绕。
//
// 为什么先取快照再算下标：歌单是共享的，随时可能被别的设备增删，
// 直接用"内存里的下标 + delta"会算错位置；取到最新快照后再取模，跳转结果才符合预期。
// 下标非法时不报错而是兜底（delta>=0 从 0 开始、否则从末尾开始）：
// 切歌是高频且低风险的操作，容错比失败体验更好。
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

// replaceActivePlaybackLocked 用新的播放活动替换旧的，并交出"旧活动 + 旧独占"让调用方在锁外收尾。
//
// 为什么要把旧的独占先摘掉（置 nil 并清标记）再返回：申请独占的动作在锁外做，
// 若这里不先解除记账，旧循环退出时的释放逻辑会误判断"独占已不属于自己"而把它漏放，
// 导致 TTS 永久被挡。调用方拿到返回值后必须执行 releaseMediaAttachment + stopOldPlayback。
// 名字带 Locked，调用方必须持有 r.mu。
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

// refreshPublicStateLocked 按当前模式刷新 state 里的派生字段（Playlist / CurrentIndex）。
//
// 为什么取状态前要刷：agent 歌单是共享的，别的设备可能刚加了歌，只在写入时同步会读到过期列表。
// standalone 模式对外不暴露列表（Playlist=nil、CurrentIndex=-1）——它只是"本次播放的队列"，
// 对用户/工具没有意义，暴露反而容易让人误以为是一份可操作歌单。
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

// currentPlaybackQueueLocked 返回当前"真正在用的播放队列"：
// agent_playlist 模式取共享歌单快照，standalone 模式取本设备列表的深拷贝。
// 调用方拿到的永远是拷贝，改它不会影响内部数据（播放循环会频繁调用它，不能返回内部切片）。
func (r *deviceMediaRuntime) currentPlaybackQueueLocked() []MediaPlaylistItem {
	switch r.mode {
	case mediaPlaybackModeAgentPlaylist:
		return r.coordinator.snapshotAgentPlaylist(r.playlistAgentID)
	default:
		return cloneMediaPlaylistItems(r.standaloneItems)
	}
}

// currentFrameDurationMs 返回当前帧长（毫秒），用于把"已推送多少帧"换算成播放时长估算。
// 配置缺失时退回默认帧长，保证进度统计不会停滞（也不会被 0 除）。
func (r *deviceMediaRuntime) currentFrameDurationMs() int64 {
	r.mu.RLock()
	frameDuration := r.audioConfig.FrameDuration
	r.mu.RUnlock()
	if frameDuration <= 0 {
		return int64(types_audio.FrameDuration)
	}
	return int64(frameDuration)
}

// currentAudioConfig 返回当前生效的音频格式（缺失项补默认值）。
// 用途：设备重连恢复播放时没有新的 cfg 传入，只能用上次记住的格式（见 attachSession 的恢复分支）。
func (r *deviceMediaRuntime) currentAudioConfig() mediaPlaybackAudioConfig {
	r.mu.RLock()
	cfg := r.audioConfig
	r.mu.RUnlock()
	return normalizeMediaPlaybackAudioConfig(cfg, mediaPlaybackAudioConfig{
		SampleRate:    types_audio.SampleRate,
		FrameDuration: types_audio.FrameDuration,
	})
}

// currentIndexForActive 取"这条播放活动当前对应的下标"。
//
// 为什么要比对 active：旧播放循环是在被 cancel 之后才慢慢退出的（取消是异步的），
// 它醒来时可能已经有一条新活动在跑；若不校验就返回下标，旧循环会用旧下标去动新活动，造成错播。
// 返回 -1 表示"这条活动已经不是当前活动了"，循环应当立刻退出。
func (r *deviceMediaRuntime) currentIndexForActive(active *activeMediaPlayback) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.active != active {
		return -1
	}
	return r.playbackIndex
}

// prepareTrack 在开始播某一项之前做"认领 + 置状态"：
// 确认活动仍是当前活动、下标合法，然后更新 playbackIndex / currentSource / 标题 / 来源类型，
// 进度归零、清掉旧错误，并按当前暂停与否设置 Status。
//
// 返回 false 表示这条活动已被替换或下标非法，调用方（播放循环）必须立刻退出。
// 它是"旧循环不许再动任何状态"的统一闸门，播放循环里几乎每一步都靠它兜底。
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

// advanceAfterTrack 在一首播完后推进到下一首，返回 (下一首下标, 是否还有下一首)。
//
// 两种结果：
//   - 还有下一首：更新下标、当前源、标题与状态，让循环继续；
//   - 已到末尾：把 active 置 nil、状态转 stopped、进度归零——"整个列表播完自动结束"就发生在这里，
//     也是"外界没有调 Stop 时播放循环的正常退出出口"。
//
// 同样先校验 active 身份，避免已作废的循环推进状态。
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

// markPlaybackError 记录播放失败：状态置 error、写入 ErrMsg、丢弃播放活动。
//
// 为什么错误要留在状态里而不只是打日志：工具层与端上要能把原因告诉用户
// （例如"MCP client 不可用"、"HTTP 音频地址为空"）。
// 这里刻意不动 CurrentTitle/CurrentSourceType/PositionMs，保留"是哪一首、播到哪失败的"，
// 排查问题时比清空更有用。
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

// markPlaybackStopped 是"正常收尾"：清掉播放活动，写入给定状态与可选错误信息。
//
// 与 markPlaybackError 的区别：它不把状态标成 error，用于 ctx 被取消（用户停止、被打断挂起）
// 这类预期内的结束，避免把"正常的停止"显示成故障。
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

// runPlayback 是每台设备唯一的播放主循环：从当前下标开始，逐首取源、开流、推帧，
// 一首播完就推进到下一首，直到列表播完 / 被取消 / 出错才退出。
//
// 三条退出路径与各自的状态收尾：
//   - 正常播完（advanceAfterTrack 返回 hasNext=false）：循环自然结束，状态已由 advance 置为 stopped；
//   - 上下文被取消（Stop 或被新播放替换）：markPlaybackStopped 置 stopped，不当作错误；
//   - 其他错误（取源失败、推帧失败）：markPlaybackError 置 error 并写入 ErrMsg。
//
// defer 的两件事必须执行：closeDone 让所有等待句柄立刻返回；releaseExclusivePlayback 归还 TTS 独占。
func (r *deviceMediaRuntime) runPlayback(active *activeMediaPlayback) {
	defer active.closeDone()
	defer r.releaseExclusivePlayback(active)

	// 取一次起始下标；若这条活动已经不是当前活动（已被替换），currentIndexForActive 返回 -1，循环直接结束。
	index := r.currentIndexForActive(active)
	for index >= 0 {
		// 暂停时在此原地等待（"暂停立即生效"就落在这里），同时也能被取消唤醒。
		if err := active.waitIfPaused(active.ctx); err != nil {
			// 取消是预期内的结束（用户停止 / 被新播放顶掉），不记为错误。
			if errors.Is(err, context.Canceled) {
				r.markPlaybackStopped(active, play_music.StatusStopped, "")
			} else {
				r.markPlaybackError(active, err)
			}
			return
		}

		// 认领这一项并刷新状态；ok=false 说明活动已作废或下标非法，立刻退出。
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

		// 这一首正常播完 → 推进到下一首；hasNext=false 表示列表已播完，循环结束（状态已在 advance 里置 stopped）。
		nextIndex, hasNext := r.advanceAfterTrack(active, index)
		if !hasNext {
			return
		}
		index = nextIndex
	}
}

// waitForAttachment 等待"输出通道可用"，返回当前绑定；活动已作废或被取消时返回错误。
//
// 为什么每轮循环都要重新读 attachment 与 waitCh：解绑 / 绑定会改变 attachment 并关闭旧 waitCh。
// 若只读一次，可能刚好在"读到 attachment==nil"之后错过紧随其后的绑定通知，于是永久等下去。
// 每轮同时校验 r.active != active：这条活动若已被替换，再等下去毫无意义，直接按取消返回。
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

// ensureExclusivePlayback 确保当前活动持有该 attachment 的独占输出权（幂等）。
//
// 为什么媒体必须申请独占：媒体与 TTS 共用同一条发送通道，不独占就会出现"音乐和机器人同时响"
// （见 tts.go 的 BeginExclusiveMediaPlayback / waitForMediaPlaybackRelease）。
// 实现里有两条快速路径和两个防御点：
//   - 同一个 attachment、活动未变、标记已置 → 直接返回，避免重复申请（TTS 对重复申请会报错拒绝）；
//   - 申请前先记下别的 attachment 的旧独占，成功后在锁外归还，避免旧会话的独占一直挡着 TTS；
//   - 申请成功后再校验一次 active/attachment 是否依然有效，无效就立刻归还并返回取消；
//   - 申请成功后把 exclusiveAttachment 记在运行时上，供停止 / 挂起时归还（漏了会让 TTS 永久失声）。
//
// ctx 只用于申请动作本身：TTS 可能正在收尾而短暂等待，必须能被打断。
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

// releaseExclusivePlayback 归还本次活动持有的独占。幂等：本身没持有就直接返回，
// 这样不会误释放"别的活动 / 别的会话"当前持有的独占。
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

// handleAttachmentFailure 处理"输出通道失效"（发送帧 / 句子时出错，通常是设备断开或会话正在销毁）。
//
// 处理策略是"不失败退出，而是等重连"：
//   - 先归还独占——通道都没了，占着只会挡住之后恢复的 TTS；
//   - 若当前状态是 playing，把播放活动置为暂停并标 resumeOnAttach，然后清掉 attachment、
//     通知绑定变化。播放循环随后会在 waitForAttachment 里一直等到设备重连（attachSession）再继续推帧，
//     从而实现"断线续播"；
//   - 若当前不在播放（例如已暂停），只需解绑，不需要额外标记。
//
// 同时校验 active/attachment 是否仍是当前的那一份，避免对已被替换的对象做失效处理。
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

// playItem 负责播完"一首"：打开音频流 → 包一层"可暂停"的壳 → 逐帧推给设备 → 等这一首彻底发完。
//
// 三步各有其原因：
//  1. openSourceAudioStream 把四种来源统一成"一帧一帧的 chan"；
//  2. wrapMediaAudioStream 让**解码线程**也能被暂停阻塞住——没有这层壳，暂停时解码侧仍会把帧塞满缓冲，
//     恢复后会一口气猛推，端上听到的就是叠在一起的声音；
//  3. streamMediaAudio 才是真正推帧、申请独占、发字幕、记进度的地方。
//
// playText 是给端上显示的字幕（"正在播放音乐: 歌名"）；标题为空时不发字幕。
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

// streamMediaAudio 是一首音频的推帧主循环：取一帧 → 确保有输出通道和独占 → 发字幕 / 发帧 → 记进度，
// 直到音频流关闭（这一首读完），再等 sentence_end 收尾。
//
// 几个刻意的设计：
//   - 每帧之前都 waitIfPaused：暂停时立刻停手，不会出现"暂停了还在狂推帧"；
//   - 内层 for + continue 而不是直接 return：发送失败（通道失效）时通过 handleAttachmentFailure
//     解绑并等待重连，然后重试**同一帧**，从而做到断线续播、不丢这一帧；
//   - startedOn 记录"字幕已经在哪个通道上发过"：重连后 attachment 换了，需要重新发一次字幕；
//   - enqueuedFrames 区分"一句都没发出去"和"发过一些"：一帧未发时直接返回（不需要发 sentence_end），
//     发过才需要等 end 回调，否则端上会收到没有 start 的 end。
func (r *deviceMediaRuntime) streamMediaAudio(ctx context.Context, audioChan <-chan []byte, active *activeMediaPlayback, playText string) error {
	// startedOn：sentence_start 已发往哪个输出通道（换通道后要重发）。
	// enqueuedFrames：这一首是否已经成功推过至少一帧（决定结束时要不要等 sentence_end）。
	var (
		startedOn      *mediaSessionAttachment
		enqueuedFrames bool
	)
	// 帧长只取一次：本次推送的"播放进度"按它累加（配置在播放期间不会变）。
	frameDurationMs := r.currentFrameDurationMs()

	for {
		// 每取一帧前先看是否暂停；取消（停止/被打断）也在这里感知。
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
			// 音频流关闭 = 这一首读完了。
			if !enqueuedFrames {
				// 一帧都没发出去（例如空音频），无需 sentence_end 配对，直接结束。
				return nil
			}
			// 等 end 回调：确保端上真的播完这一首，再让上层推进到下一首。
			return r.waitMediaSentenceEnd(ctx, active, playText)
		}
		if len(frame) == 0 {
			// 空帧直接跳过，避免把空数据塞进发送队列。
			continue
		}

		// 重试循环：通道失效时解绑等重连，成功后重发这一帧。
		for {
			if err := active.waitIfPaused(ctx); err != nil {
				return err
			}

			// 设备断线时 attachment 为 nil，这里会一直等到设备重连（attachSession 唤醒）。
			attachment, err := r.waitForAttachment(active, ctx)
			if err != nil {
				return err
			}
			// 申请独占输出：媒体播放期间 TTS 必须让路，否则两边声音会叠在一起。
			if err := r.ensureExclusivePlayback(active, attachment, ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				// 申请失败（通道已坏）→ 标记失效并等下一次重连，然后重试同一帧。
				r.handleAttachmentFailure(active, attachment, err)
				continue
			}

			// 发送失败的回调：交给统一的失效处理（归还独占 + 等重连），循环会 continue 重试。
			sendError := func(err error) {
				r.handleAttachmentFailure(active, attachment, err)
			}

			// 只有"换了通道"或"第一次发"时才发字幕，避免把同一句字幕重复推给端上。
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

			// 这一帧成功入队：记进度（供 GetState 展示）并跳出重试循环，去取下一帧。
			enqueuedFrames = true
			r.incrementPlaybackPosition(active, frameDurationMs)
			break
		}
	}
}

// waitMediaSentenceEnd 在这一首的帧全部推完后，发送 sentence_end 并等待它真正发完。
//
// 为什么一定要等 onEnd 回调：帧只是进了发送队列，TTSManager 还要按客户端缓冲水位慢慢发；
// 只有 onEnd 才代表"这段音频在端上已经放完"。不等就直接推下一首，会出现两首歌重叠。
// doneCh 用带缓冲（容量 1）的通道 + 非阻塞发送：onEnd 被重复回调时也不会把回调线程堵住。
// 若此时已经没有绑定（设备走了），直接返回 nil，不阻塞播放循环收尾。
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

// openSourceAudioStream 把任意一种媒体源统一变成"一帧一帧的音频 chan"——四种来源统一抽象的落点就在这里。
//
// 统一的办法：先用 util.GetAudioFormatByMimeType 从 MIMEType 推断解码格式，
// 再按当前会话的采样率 / 帧长重采样切帧。这样下游（streamMediaAudio）只面对帧流，
// 完全不需要知道音频是来自网络、磁盘还是内存。
// active 只有 MCP 分支用得上：分页读取过程中要能感知暂停与取消。
func (r *deviceMediaRuntime) openSourceAudioStream(ctx context.Context, source MediaSourceDescriptor, active *activeMediaPlayback) (<-chan []byte, error) {
	// 取本次播放的采样率 / 帧长，以及按 MIME 推断出的解码格式。
	cfg := r.currentAudioConfig()
	audioFormat := util.GetAudioFormatByMimeType(source.MIMEType)

	// 四种来源各一条分支。每条分支都先校验"该类型对应的字段是否存在"：
	// 描述符有可能被填错（例如声明 mcp_resource 却没给 MCP 结构），
	// 在这里报错比让下游解码时崩掉容易排查得多。
	switch source.SourceType {
	case MediaSourceTypeInlineAudio:
		// 内联字节：数据已在内存，直接交给解码器，不需要网络或磁盘 IO。
		if source.Inline == nil || len(source.Inline.Data) == 0 {
			return nil, fmt.Errorf("inline 音频数据为空")
		}
		return play_music.PlayMusicFromAudioData(ctx, source.Inline.Data, cfg.SampleRate, cfg.FrameDuration, audioFormat)
	case MediaSourceTypeHTTPURL:
		// HTTP 流：边下边解，适合 CDN 直链的音乐 URL。
		if source.HTTP == nil || strings.TrimSpace(source.HTTP.URL) == "" {
			return nil, fmt.Errorf("HTTP 音频地址为空")
		}
		return play_music.PlayMusicStream(ctx, source.HTTP.URL, cfg.SampleRate, cfg.FrameDuration, audioFormat)
	case MediaSourceTypeLocalFile:
		// 本地文件：打开磁盘文件后经统一解码器切帧（见 openLocalMediaFileStream，自己封装的目的是
		// 让 ctx 取消能同时中断"读文件 + 解码"）。
		if source.Local == nil || strings.TrimSpace(source.Local.Path) == "" {
			return nil, fmt.Errorf("本地音频路径为空")
		}
		return openLocalMediaFileStream(ctx, source.Local.Path, cfg.SampleRate, cfg.FrameDuration, audioFormat)
	case MediaSourceTypeMCPResource:
		// MCP 资源：需要按页从 MCP 服务端拉取（见 openMCPResourceAudioStream），
		// 所以要带 active，才能在拉取过程中暂停/取消。
		if source.MCP == nil {
			return nil, fmt.Errorf("MCP 音频源为空")
		}
		return r.openMCPResourceAudioStream(ctx, source.MCP, active, cfg.SampleRate, cfg.FrameDuration, audioFormat)
	default:
		return nil, fmt.Errorf("不支持的媒体源类型: %s", source.SourceType)
	}
}

// openLocalMediaFileStream 打开本地音频文件并起一个 goroutine 解码成帧流。
//
// 为什么自己封装而不是直接用 play_music 的现成函数：
// 文件句柄必须在本函数内关闭（defer file.Close 放在 goroutine 里，解码跑多久就持有多久），
// 且解码失败/取消时要 close(outputChan)，否则消费方会永远等一个不会再有数据的通道。
// 返回的通道缓冲 100 帧：给解码器一点提前量，避免磁盘抖动直接导致推帧断顿。
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

// openMCPResourceAudioStream 打开"MCP 资源"音频流：用一个 io.Pipe 把"分页拉取的字节"
// 和"解码器"接起来，实现边拉边解边播，不必等整首歌下载完。
//
// 为什么用 io.Pipe 而不是先全量读取：MCP resource 只能按页读，整首歌可能几 MB；
// 用管道可以让播放起点提前到"第一页到达"，同时内存占用恒定（只有管道内部缓冲）。
// 注意：写端出错必须用 CloseWithError 传出去，否则解码器只会看到"读到 EOF"，
// 把失败当成正常结束——那样上层会以为播放成功。
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

// streamMCPResourceToPipe 是 MCP 资源的分页拉取循环：反复 ReadResource(start..end)，
// 把返回的 base64 音频解码后写进管道，直到读完、命中结束标记或出错。
//
// 循环里的几条关键约定：
//   - 每页请求前都 waitIfPaused：暂停时连"拉下一页"都停下，避免白拉数据撑内存；
//   - ReadArgs 每次用 cloneMediaSourceAnyMap 复制后再改 start/end，不能直接改 source.ReadArgs，
//     否则会污染共享的描述符（同一首歌可能被多台设备同时播）；
//   - Blob 里装的是 base64 文本，要 StdEncoding.DecodeString 解出原始音频字节；
//   - 收到 McpReadResourceStreamDoneFlag 就是"没有更多数据了"，正常返回（defer 会关管道）；
//   - 一页实际数据不足 pageSize，说明这是最后一页，也可以收工。
func (r *deviceMediaRuntime) streamMCPResourceToPipe(ctx context.Context, source *MCPMediaSource, active *activeMediaPlayback, pipeWriter *io.PipeWriter) {
	// 无论正常结束还是出错都要关闭管道，否则解码侧会一直等数据，造成 goroutine 泄漏。
	defer pipeWriter.Close()

	// 优先用描述符里已带的客户端（工具上下文里可能已建好），没有再按服务器名查全局注册表。
	client := source.Client
	// 分页游标：start 是当前页的字节起点，pageSize 缺省为 16KB（见 defaultMediaPlayerPageSize）。
	start := 0
	pageSize := source.PageSize
	if pageSize <= 0 {
		pageSize = defaultMediaPlayerPageSize
	}

	for {
		// 暂停时连"拉下一页"都停下：宁可不预读，也不要让内存里堆着一堆还没播的音频。
		if err := active.waitIfPaused(ctx); err != nil {
			pipeWriter.CloseWithError(err)
			return
		}

		// 复制一份参数再写入本页范围：直接改 source.ReadArgs 会污染共享描述符。
		readArgs := cloneMediaSourceAnyMap(source.ReadArgs)
		readArgs["start"] = start
		readArgs["end"] = start + pageSize

		resourceResult, err := r.readMCPResourcePage(ctx, source, client, readArgs)
		if err != nil {
			pipeWriter.CloseWithError(err)
			return
		}
		if len(resourceResult.Contents) == 0 {
			// 没有内容 = 已经读到结尾（与"最后一页不足 pageSize"是两种收尾信号，都要处理）。
			return
		}

		// hasData 标记本页是否真的写入了音频，用来区分"这一页为空"（收尾）与"还有数据"（继续翻页）。
		hasData := false
		for _, content := range resourceResult.Contents {
			// 音频内容以 Blob（二进制块）形式返回，其他类型（文本等）与本用途无关，直接跳过。
			audioContent, ok := content.(mcp_go.BlobResourceContents)
			if !ok {
				continue
			}
			if len(audioContent.Blob) == 0 {
				continue
			}

			// Blob 里装的是 base64 文本，需解码成原始音频字节才能写进解码管道。
			rawAudioData, err := base64.StdEncoding.DecodeString(audioContent.Blob)
			if err != nil {
				pipeWriter.CloseWithError(fmt.Errorf("解码 MCP 音频数据失败: %v", err))
				return
			}
			// 服务端用这个特殊标记表示"音频流结束"，收到就当作正常收工。
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
			// 不足一页说明这是最后一页（分页读取的通用约定），直接收工。
			if len(rawAudioData) < pageSize {
				return
			}
		}

		if !hasData {
			// 这一页没有任何音频数据：视作流已结束，否则会在死循环里反复请求同一范围。
			return
		}
		// 推进到下一页的字节范围。
		start += pageSize
	}
}

// readMCPResourcePage 读取 MCP 资源的一页，返回原始 ReadResourceResult。
//
// 两件值得注意的事：
//   - 客户端来源：优先用描述符里带的 Client（工具上下文里可能已经拿到），
//     否则按 ServerName 去全局注册表查；两者都没有则报错；
//   - 自愈重连：MCP 会话过期时错误信息通常含 "session closed"，此时重连一次并用**同一页参数重试**。
//     重连成功后把新客户端写回 source.Client，后续分页就不必再重连了。
//
// 每次读取都套 30 秒超时：MCP 服务端无响应时不能让播放循环无限挂住。
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

// wrapMediaAudioStream 在原始帧流外再套一层"可暂停 + 可取消"的转发：
// 只有没暂停时才把帧转给下游；暂停期间读到的帧就不前推，上游通道缓冲会自然形成背压。
//
// 为什么需要它：暂停时若不卡住上游，解码器 / 网络仍会持续产帧并塞满缓冲，
// 恢复后这些积压帧会被一次性推给设备，听感上就是"暂停之后突然快进了一段"。
// 输出通道故意不带缓冲：让背压直接传到上游（解码线程），从源头上把生产速度降下来。
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

// incrementPlaybackPosition 按"成功推出去的帧"累加 PositionMs，作为播放进度的估算值。
//
// 为什么不用设备回报的真实进度：协议里没有播放进度回传，端上也不需要；
// 这个估算只用于展示和"暂停在多少秒"的提示，按帧长累加已足够准确。
// 校验 active 身份：已经作废的播放循环不许再改状态。
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

// buildMediaPlaylistItems 把一批媒体源包装成播放列表条目（补 ID、补标题、统一 AddedAt）。
//
// 两个补齐动作的原因：条目 ID 是"播放列表内的唯一标识"（同一首歌可以被加多次），
// 空 ID 会让 ShowState / 前端无法区分；标题为空则在派发前就推导好，避免每次播报时再算一遍。
// 注意这里对每个源都做了深拷贝：条目一旦入列表就不应再受调用方持有的原对象影响。
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

// normalizeMediaPlaybackAudioConfig 合并"本次请求的格式"与"上一次的格式"，并补上包级默认值。
//
// 为什么用 primary/fallback 而不是直接用 primary：调用方（恢复播放、工具调用）经常只能提供
// 部分字段、甚至给一个空结构体；此时应当沿用上一次的采样率 / 帧长，
// 而不是让 0 值把音频格式搞坏（采样率 0 会让端上直接播不出声）。
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

// cloneCurrentSource 深拷贝 currentSource 指针（nil 安全），用于在锁内取一份"当前媒体源"到锁外使用。
func cloneCurrentSource(source *MediaSourceDescriptor) *MediaSourceDescriptor {
	if source == nil {
		return nil
	}
	cloned := cloneMediaSourceDescriptor(*source)
	return &cloned
}

// releaseMediaAttachment 归还某个绑定占用的独占输出权（nil 安全）。
// 内部就是调桥的 EndExclusiveMediaPlayback；抽成函数是为了在锁外统一收尾，避免各处漏写。
func releaseMediaAttachment(attachment *mediaSessionAttachment) {
	if attachment == nil {
		return
	}
	attachment.bridge.EndExclusiveMediaPlayback()
}

// stopOldPlayback 取消并等待旧播放循环退出。
//
// 为什么必须等（waitForActivePlaybackDone）：被替换的循环可能正在推帧或等独占，
// 不等它退干净，新循环立刻又去申请独占、推帧，两条循环会短暂交叠，造成重复音频与状态互相覆盖。
// 等待有上限（2 秒）：旧循环若卡在 MCP 读取这类外部 IO 上，不能让整个播放切换被无限拖住。
func stopOldPlayback(active *activeMediaPlayback) {
	if active == nil {
		return
	}
	active.cancel()
	waitForActivePlaybackDone(active, 2*time.Second)
}

// deriveMediaTitle 推导"给用户看的标题"：优先用显式 Title，否则按来源类型兜底。
//
// 兜底顺序体现各来源各自最有用的信息：
//   - HTTP：取 URL 路径的最后一段（通常就是文件名），比整条 URL 可读；
//   - 本地文件：直接用路径（是工具/用户自己给的，认得出来）；
//   - MCP：优先 Description（服务端给的说明最像标题），其次 DirectAudioURL，最后退到 ResourceURI；
//   - 都没有则返回"未知音频"——保证端上字幕永远有内容，空标题会导致字幕整条不显示。
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

// cloneMediaSourceDescriptor 对媒体源做深拷贝。
//
// 为什么必须深拷贝：这个结构里有多处共享风险——
//   - Meta 与 MCP.ReadArgs 是 map，浅拷贝后两个描述符共用同一张表；
//   - MCP / HTTP / Local / Inline 都是指针，浅拷贝后指向同一个子结构；
//   - Inline.Data 是字节切片，后续 append 会与源共享底层数组。
//
// 而歌单是跨设备共享的、媒体源会被多个 goroutine 读取，任何一处共享都可能被意外改写
// （例如 MCP 分页时把 start/end 写进 ReadArgs）。因此逐字段复制是这里唯一安全的做法。
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

// cloneMediaPlaylistItems 深拷贝整个列表；空列表返回 nil，
// 保证对外状态里"没有歌单"始终表现为 nil（调用方不会把"非 nil 但空"的切片误当成有内容）。
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

// cloneMediaPlaylistItem 深拷贝单个条目（含其中的媒体源），供对外返回值与状态快照使用。
func cloneMediaPlaylistItem(item MediaPlaylistItem) MediaPlaylistItem {
	itemCopy := item
	itemCopy.Source = cloneMediaSourceDescriptor(item.Source)
	return itemCopy
}

// cloneMediaSourceStringMap 复制 string→string 映射；空表返回 nil，保持"没有元数据"的语义。
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

// cloneMediaSourceAnyMap 复制 string→any 映射（MCP 的 ReadArgs 就用它）。
// 只做浅拷贝是够的：调用方只会替换或新增顶层的 start/end 等键，不会去改值的内部结构。
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

// waitForActivePlaybackDone 等待一次播放活动结束，最多等 timeout（<=0 时用 2 秒兜底）。
//
// 为什么要有超时：调用方（stopOldPlayback、播放切换）不能因为旧循环卡在外部 IO 上就一起卡死。
// 超时后直接放弃等待即可——旧循环的 ctx 已经取消，它自己会走完收尾流程。
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
