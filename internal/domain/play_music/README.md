# 播放音乐功能

这个模块提供了从URL流式播放音乐的功能，支持从网络URL获取音频文件并实时解码为音频帧流。

## 功能特性

- ✅ **流式播放**: 支持从URL实时下载和播放音乐
- ✅ **格式支持**: 主要支持MP3格式，自动解码为Opus音频帧
- ✅ **音频解码**: 基于成熟的音频解码器，高效稳定
- ✅ **上下文控制**: 支持通过context取消和超时控制
- ✅ **连接池优化**: 使用HTTP连接池，提高网络性能
- ✅ **配置灵活**: 可配置帧时长和音频格式
- ✅ **统计信息**: 提供播放统计和状态监控

## 快速开始

### 1. 基础使用

```go
package main

import (
    "context"
    "fmt"
    
    "xiaozhi-esp32-server-golang/internal/domain/play_music"
)

func main() {
    // 创建音乐播放器
    config := play_music.DefaultMusicPlayerConfig()
    player := play_music.NewMusicPlayer(config.ToMap())
    
    // 开始播放音乐
    ctx := context.Background()
    audioChan, err := player.PlayMusicStream(ctx, "https://example.com/music.mp3")
    if err != nil {
        panic(err)
    }
    
    // 处理音频帧
    for audioFrame := range audioChan {
        fmt.Printf("收到音频帧: %d 字节\n", len(audioFrame))
        // 这里可以将音频帧发送到播放设备或其他处理
    }
}
```

### 2. 自定义配置

```go
// 创建自定义配置
config := &play_music.MusicPlayerConfig{
    FrameDuration: 20,   // 20ms帧时长
}

player := play_music.NewMusicPlayer(config.ToMap())

// 或者直接传入配置映射
player := play_music.NewMusicPlayer(map[string]interface{}{
    "frame_duration": 20,
    "audio_format":   "mp3",
})
```

### 3. 带统计信息的完整示例

```go
package main

import (
    "context"
    "fmt"
    "time"
    
    "xiaozhi-esp32-server-golang/internal/domain/play_music"
)

func main() {
    config := play_music.DefaultMusicPlayerConfig()
    player := play_music.NewMusicPlayer(config.ToMap())
    
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    audioChan, err := player.PlayMusicStream(ctx, "https://example.com/music.mp3")
    if err != nil {
        panic(err)
    }
    
    // 统计信息
    stats := &play_music.StreamingStats{
        StartTime: time.Now().UnixMilli(),
    }
    
    frameCount := 0
    for audioFrame := range audioChan {
        frameCount++
        stats.FramesGenerated = int64(frameCount)
        stats.BytesDecoded += int64(len(audioFrame))
        
        if frameCount == 1 {
            stats.FirstFrameTime = time.Now().UnixMilli()
            fmt.Printf("首帧延迟: %d ms\n", stats.FirstFrameTime - stats.StartTime)
        }
        
        // 处理音频帧...
    }
    
    fmt.Printf("播放完成，总帧数: %d\n", frameCount)
}
```

## API 参考

### MusicPlayer

主要的音乐播放器结构体。

#### 方法

##### `NewMusicPlayer(config map[string]interface{}) *MusicPlayer`

创建新的音乐播放器实例。

**参数:**
- `config`: 配置参数映射

**配置选项:**
- `frame_duration` (int): 帧时长(ms)，默认20
- `audio_format` (string): 音频格式，默认"mp3"

##### `PlayMusicStream(ctx context.Context, url string) (chan []byte, error)`

从URL开始流式播放音乐。

**参数:**
- `ctx`: 上下文对象，用于取消和超时控制
- `url`: 音乐文件的URL地址

**返回:**
- `chan []byte`: 音频帧数据通道
- `error`: 错误信息

##### `GetPlayerInfo() map[string]interface{}`

获取播放器配置信息。

##### `Stop() error`

停止播放器并清理资源。

### 配置类型

#### `MusicPlayerConfig`

```go
type MusicPlayerConfig struct {
    FrameDuration int    `json:"frame_duration"` // 帧时长(ms)
    AudioFormat   string `json:"audio_format"`   // 音频格式，默认"mp3"
}
```

#### `StreamingStats`

播放统计信息结构体，用于监控播放状态。

```go
type StreamingStats struct {
    BytesDownloaded int64         `json:"bytes_downloaded"`
    BytesDecoded    int64         `json:"bytes_decoded"`
    FramesGenerated int64         `json:"frames_generated"`
    StartTime       int64         `json:"start_time"`
    FirstFrameTime  int64         `json:"first_frame_time"`
    Status          PlaybackStatus `json:"status"`
    ErrorCount      int           `json:"error_count"`
}
```

## 测试

运行测试示例：

```bash
cd test/music_player
go run main.go "https://example.com/music.mp3"
```

## 支持的音频格式

目前主要支持：
- **MP3**: 完全支持，推荐使用
- **WAV**: 部分支持（通过通用解码器）

## 错误处理

播放器提供了简洁的错误处理机制：

1. **连接池优化**: 使用HTTP连接池提高网络稳定性
2. **上下文控制**: 支持通过context取消操作
3. **优雅退出**: 遇到错误时优雅关闭通道

## 性能优化建议

1. **合理设置帧时长**: 默认20ms适合大多数场景
2. **网络优化**: 使用稳定的网络连接，播放器已优化HTTP连接池
3. **内存管理**: 及时处理音频帧数据，避免通道阻塞
4. **并发控制**: 避免同时播放过多音频流

## 集成示例

### 与WebSocket集成

```go
func streamToWebSocket(audioChan <-chan []byte, ws *websocket.Conn) {
    for frame := range audioChan {
        err := ws.WriteMessage(websocket.BinaryMessage, frame)
        if err != nil {
            log.Errorf("发送WebSocket消息失败: %v", err)
            return
        }
    }
}
```

### 保存到文件

```go
func saveToFile(audioChan <-chan []byte, filename string) error {
    file, err := os.Create(filename)
    if err != nil {
        return err
    }
    defer file.Close()
    
    for frame := range audioChan {
        _, err := file.Write(frame)
        if err != nil {
            return err
        }
    }
    return nil
}
```

## 注意事项

1. **URL有效性**: 确保音频URL可访问且返回有效音频文件
2. **内存使用**: 长时间播放需要注意内存使用情况
3. **网络稳定性**: 使用稳定的网络连接以获得最佳播放体验
4. **上下文管理**: 及时取消不需要的播放任务

## 故障排除

### 常见问题

**Q: 播放没有声音**
A: 检查URL是否有效，音频格式是否支持

**Q: 播放延迟很高**
A: 检查网络连接，确保URL响应速度较快

**Q: 内存使用过高**
A: 检查音频帧处理是否及时，避免通道积压

## License

MIT License 