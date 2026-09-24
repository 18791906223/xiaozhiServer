# WebRTC VAD 资源池实现

这个实现为 WebRTC VAD (Voice Activity Detection) 提供了资源池管理功能，用于提高并发场景下的性能和资源利用率。

## 主要组件

### 1. WebRTCVAD
基础的 VAD 实现，现在实现了 `Resource` 接口：
- `IsValid()`: 检查资源是否有效
- `Close()`: 关闭并释放资源
- 线程安全的操作

### 2. WebRTCVADFactory
资源工厂，实现了 `ResourceFactory` 接口：
- `Create()`: 创建新的 VAD 实例
- `Validate()`: 验证资源有效性
- `Reset()`: 重置资源状态

### 3. WebRTCVADPool
VAD 资源池管理器：
- `AcquireVAD()`: 获取 VAD 实例
- `ReleaseVAD()`: 释放 VAD 实例
- `Stats()`: 获取统计信息
- `Close()`: 关闭资源池

### 4. VADManager
高级封装，提供便捷的使用接口：
- `ProcessAudio()`: 处理单个音频数据
- `ProcessAudioBatch()`: 批量处理音频数据
- `WithVAD()`: 使用回调函数处理 VAD

## 使用方法

### 基本使用

```go
// 创建 VAD 配置
config := WebRTCVADConfig{
    SampleRate: 16000,
    Mode:       2, // 中等敏感度
}

// 创建 VAD 管理器
manager, err := NewVADManager(config)
if err != nil {
    log.Fatal(err)
}
defer manager.Close()

// 处理音频数据
audioData := make([]float32, 320) // 16kHz, 20ms
isActive, err := manager.ProcessAudio(audioData)
if err != nil {
    log.Printf("VAD processing failed: %v", err)
    return
}

if isActive {
    fmt.Println("Voice activity detected!")
}
```

### 高级使用 - 直接使用资源池

```go
// 创建资源池
vadConfig := WebRTCVADConfig{
    SampleRate: 16000,
    Mode:       2,
}

poolConfig := &util.PoolConfig{
    MaxSize:          5,               // 最大实例数
    MinSize:          1,               // 预创建实例数
    MaxIdle:          3,               // 最大空闲实例数
    AcquireTimeout:   5 * time.Second, // 获取超时
    IdleTimeout:      2 * time.Minute, // 空闲超时
    ValidateOnBorrow: true,            // 获取时验证
}

pool, err := NewWebRTCVADPool(vadConfig, poolConfig)
if err != nil {
    log.Fatal(err)
}
defer pool.Close()

// 获取 VAD 实例
vad, err := pool.AcquireVAD()
if err != nil {
    log.Fatal(err)
}

// 使用 VAD
isActive, err := vad.IsVAD(audioData)

// 释放 VAD 实例
pool.ReleaseVAD(vad)
```

### 并发使用

```go
manager, err := NewVADManager(config)
if err != nil {
    log.Fatal(err)
}
defer manager.Close()

// 多个 goroutine 并发处理
for i := 0; i < numWorkers; i++ {
    go func(workerID int) {
        audioData := generateAudioData() // 生成音频数据
        
        active, err := manager.ProcessAudio(audioData)
        if err != nil {
            log.Printf("Worker %d failed: %v", workerID, err)
            return
        }
        
        fmt.Printf("Worker %d: active = %v\n", workerID, active)
    }(i)
}
```

## 配置参数

### WebRTCVADConfig
- `SampleRate`: 采样率 (8000, 16000, 32000, 48000)
- `Mode`: VAD 敏感度模式 (0: 最不敏感, 3: 最敏感)

### PoolConfig
- `MaxSize`: 最大资源数量
- `MinSize`: 最小资源数量（预创建）
- `MaxIdle`: 最大空闲资源数量
- `AcquireTimeout`: 获取资源超时时间
- `IdleTimeout`: 资源空闲超时时间
- `ValidateOnBorrow`: 获取时是否验证资源
- `ValidateOnReturn`: 归还时是否验证资源

## 优势

1. **资源复用**: 避免频繁创建和销毁 VAD 实例
2. **并发安全**: 支持多个 goroutine 并发使用
3. **自动管理**: 自动清理空闲超时的资源
4. **性能监控**: 提供详细的统计信息
5. **配置灵活**: 支持自定义池大小和超时参数

## 性能统计

使用 `GetStats()` 方法获取资源池统计信息：

```go
stats := manager.GetStats()
fmt.Printf("Pool stats: %+v\n", stats)
// 输出示例:
// {
//   "total_resources": 3,
//   "available_resources": 2,
//   "in_use_resources": 1,
//   "max_size": 5,
//   "min_size": 1,
//   "max_idle": 3,
//   "is_closed": false
// }
```

## 错误处理

主要的错误类型：
- 获取超时：`acquire timeout after 5s`
- 资源池已关闭：`pool is closed`
- 无效资源类型：`invalid resource type`
- VAD 初始化失败：`failed to initialize WebRTC VAD`

## 最佳实践

1. **合理设置池大小**: 根据并发需求设置 `MaxSize`
2. **及时释放资源**: 使用 `defer` 确保资源被释放
3. **监控统计信息**: 定期检查池的使用情况
4. **优雅关闭**: 程序退出时调用 `Close()` 方法
5. **错误处理**: 处理获取超时等异常情况

## 示例代码

查看 `example_usage.go` 文件中的完整示例：
- 基本使用示例
- 批量处理示例
- 回调函数使用示例
- 并发使用示例

## 测试

运行测试：
```bash
go test -v ./internal/domain/vad/webrtc_vad/
```

运行性能测试：
```bash
go test -bench=. ./internal/domain/vad/webrtc_vad/
``` 