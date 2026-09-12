package manager

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"github.com/cloudwego/eino/schema"
	"github.com/go-audio/audio"
	"github.com/go-audio/wav"

	"xiaozhi-esp32-server-golang/internal/domain/asr"
	"xiaozhi-esp32-server-golang/internal/domain/llm"
	"xiaozhi-esp32-server-golang/internal/domain/tts"
	"xiaozhi-esp32-server-golang/internal/domain/vad/inter"
	"xiaozhi-esp32-server-golang/internal/pool"
	log "xiaozhi-esp32-server-golang/logger"
)

// DefaultTestWavPath 配置测试用固定 WAV 路径（16kHz 单声道，约 1–3 秒），可选
const DefaultTestWavPath = "internal/testdata/config_test.wav"

// DefaultTestText LLM/TTS 固定测试文本
const DefaultTestText = "配置测试"
const (
	defaultLLMTestTimeout  = 15 * time.Second
	thinkingLLMTestTimeout = 30 * time.Second
)

// 用于 VAD/ASR 的备用 PCM：约 1 秒模拟语音 16kHz 单声道，无文件时使用
// 使用合成噪声模拟真实语音，以便 ASR 服务端能正常处理（特别是 Manual 模式需要 commit）
var fallbackPCM = func() []float32 {
	pcm := make([]float32, 16000)
	// 生成模拟语音信号：使用多个正弦波叠加 + 噪声
	// 模拟中文"配置测试"的基本频率范围
	// 增加幅度以使服务端能识别为有效音频（Manual 模式要求更高）
	for i := range pcm {
		t := float64(i) / 16000.0
		// 基频 + 谐波模拟语音，大幅增加幅度
		sample := float32(0.5 * math.Sin(2*math.Pi*t*400))   // 基频 400Hz，幅度 0.5
		sample += float32(0.25 * math.Sin(2*math.Pi*t*800))  // 谐波，幅度 0.25
		sample += float32(0.15 * math.Sin(2*math.Pi*t*1200)) // 谐波，幅度 0.15
		sample += float32(0.1 * math.Sin(2*math.Pi*t*2000))  // 谐波，幅度 0.1
		// 添加噪声，大幅增加噪声水平
		sample += (float32(i%100) - 50) / 2000 // 噪声幅度增加到 0.05
		// 应用包络（淡入淡出）
		env := float32(1.0)
		if i < 1000 {
			env = float32(i) / 1000
		} else if i > 15000 {
			env = float32(16000-i) / 1000
		}
		pcm[i] = sample * env
	}
	log.Debugf("[config_test] fallbackPCM 生成: len=%d", len(pcm))
	return pcm
}()

// loadTestWav 加载固定 WAV 为 float32 PCM，若文件不存在则返回 nil 与 nil error（调用方用 fallbackPCM）
func loadTestWav(path string) ([]float32, error) {
	if path == "" {
		path = DefaultTestWavPath
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil
	}
	defer f.Close()
	dec := wav.NewDecoder(f)
	if !dec.IsValidFile() {
		return nil, nil
	}
	dec.ReadInfo()
	wavFmt := dec.Format()
	frameSize := int(wavFmt.SampleRate) * 20 / 1000 * wavFmt.NumChannels
	buf := &audio.IntBuffer{Format: wavFmt, SourceBitDepth: 16, Data: make([]int, frameSize)}
	var out []float32
	for {
		n, err := dec.PCMBuffer(buf)
		if err == io.EOF || n == 0 {
			break
		}
		if err != nil {
			return nil, err
		}
		for i := 0; i < n; i++ {
			out = append(out, float32(buf.Data[i])/32767.0)
		}
	}
	return out, nil
}

// RunConfigTest 根据下发的 data（与实时配置一致）执行 VAD/ASR/LLM/TTS 轻量测试，返回每类按 config_id 的结果
func RunConfigTest(data map[string]interface{}, testText string) (vadResult, asrResult, llmResult, ttsResult map[string]interface{}) {
	vadResult = make(map[string]interface{})
	asrResult = make(map[string]interface{})
	llmResult = make(map[string]interface{})
	ttsResult = make(map[string]interface{})

	if testText == "" {
		testText = DefaultTestText
	}
	log.Debugf("[config_test] RunConfigTest 开始 test_text=%q data.keys=%v", testText, mapKeys(data))
	// 打印接收到的各类型 config_id 及脱敏后的配置内容，便于 debug
	for _, typ := range []string{"vad", "asr", "llm", "tts"} {
		// 获取配置vad
		v, _ := data[typ].(map[string]interface{})
		if v == nil {
			continue
		}
		// 创建一个字符串数组
		ids := make([]string, 0, len(v))
		for k := range v {
			// 加入
			ids = append(ids, k)
		}
		log.Debugf("[config_test] 收到 data[%s] config_ids=%v", typ, ids)
	}
	// 对数据进行脱敏
	if redacted := redactSensitive(data); redacted != nil {
		if b, err := json.Marshal(redacted); err == nil {
			log.Debugf("[config_test] 收到 data 脱敏后: %s", string(b))
		}
	}
	// 加载wav音频
	pcm, _ := loadTestWav(DefaultTestWavPath)
	if pcm == nil || len(pcm) == 0 {
		log.Debugf("[config_test] WAV 文件加载失败或为空，使用 fallbackPCM")
		pcm = fallbackPCM
	}
	log.Debugf("[config_test] 使用 PCM 数据: len=%d", len(pcm))

	// VAD：统计处理耗时（从调用 IsVAD 到返回）
	if v, ok := data["vad"].(map[string]interface{}); ok {
		for configID, val := range v {
			if configID == "provider" {
				continue
			}
			cfg, ok := val.(map[string]interface{})
			if !ok {
				vadResult[configID] = map[string]interface{}{"ok": false, "message": "配置格式无效"}
				continue
			}
			wrapper, err := pool.Acquire[inter.VAD]("vad", configID, cfg)
			if err != nil {
				vadResult[configID] = map[string]interface{}{"ok": false, "message": err.Error()}
				continue
			}
			vad := wrapper.GetProvider()
			testSamples := vadTestSampleCount(configID, cfg)
			t0 := time.Now()
			_, err = vad.IsVAD(pcm[:min(testSamples, len(pcm))])
			elapsedMs := time.Since(t0).Milliseconds()
			pool.Release(wrapper)
			if err != nil {
				vadResult[configID] = map[string]interface{}{"ok": false, "message": err.Error(), "first_packet_ms": elapsedMs}
			} else {
				vadResult[configID] = map[string]interface{}{"ok": true, "message": "通过", "first_packet_ms": elapsedMs}
			}
		}
	}

	// ASR：使用 StreamingRecognize 做轻量测试，统计整体耗时
	if v, ok := data["asr"].(map[string]interface{}); ok {
		for configID, val := range v {
			if configID == "provider" {
				continue
			}
			cfg, ok := val.(map[string]interface{})
			if !ok {
				asrResult[configID] = map[string]interface{}{"ok": false, "message": "配置格式无效"}
				log.Debugf("[config_test] ASR config_id=%s 配置格式无效", configID)
				continue
			}
			// 资源池 creator 需要引擎类型（funasr/doubao），用 config_id 会报「不支持的ASR引擎类型」
			asrEngineType := "funasr"
			if p, ok := cfg["provider"].(string); ok && p != "" {
				asrEngineType = p
			}
			wrapper, err := pool.Acquire[asr.AsrProvider]("asr", asrEngineType, cfg)
			if err != nil {
				asrResult[configID] = map[string]interface{}{"ok": false, "message": err.Error()}
				continue
			}
			asrProvider := wrapper.GetProvider()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			audioCh := make(chan []float32)
			go func() {
				const chunk = 3200 // 约 200ms @ 16kHz
				for i := 0; i < len(pcm); i += chunk {
					end := i + chunk
					if end > len(pcm) {
						end = len(pcm)
					}
					audioCh <- pcm[i:end]
				}
				close(audioCh)
			}()
			t0 := time.Now()
			resultChan, err := asrProvider.StreamingRecognize(ctx, audioCh)
			pool.Release(wrapper)
			if err != nil {
				cancel()
				asrResult[configID] = map[string]interface{}{"ok": false, "message": err.Error(), "first_packet_ms": time.Since(t0).Milliseconds()}
				continue
			}
			var asrErr error
			for r := range resultChan {
				if r.Error != nil {
					asrErr = r.Error
					break
				}
			}
			elapsedMs := time.Since(t0).Milliseconds()
			cancel()
			if asrErr != nil {
				asrResult[configID] = map[string]interface{}{"ok": false, "message": asrErr.Error(), "first_packet_ms": elapsedMs}
			} else {
				asrResult[configID] = map[string]interface{}{"ok": true, "message": "通过", "first_packet_ms": elapsedMs}
			}
		}
	}

	// LLM
	if v, ok := data["llm"].(map[string]interface{}); ok {
		n := 0
		for k := range v {
			if k != "provider" {
				n++
			}
		}
		log.Debugf("[config_test] LLM 待测 config 数: %d", n)
		for configID, val := range v {
			if configID == "provider" {
				continue
			}
			cfg, ok := val.(map[string]interface{})
			if !ok {
				llmResult[configID] = map[string]interface{}{"ok": false, "message": "配置格式无效"}
				log.Debugf("[config_test] LLM config_id=%s 配置格式无效", configID)
				continue
			}
			testCfg := cloneConfigMap(cfg)
			testCfg["__enable_reasoning_content_detection"] = true
			wrapper, err := pool.Acquire[llm.LLMProvider]("llm", configID, testCfg)
			if err != nil {
				llmResult[configID] = map[string]interface{}{"ok": false, "message": err.Error()}
				log.Debugf("[config_test] LLM config_id=%s Acquire 失败: %v", configID, err)
				continue
			}
			llmProvider := wrapper.GetProvider()
			timeout := defaultLLMTestTimeout
			if llmThinkingEnabled(cfg) {
				timeout = thinkingLLMTestTimeout
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			t0 := time.Now()
			msgChan := llmProvider.ResponseWithContext(ctx, "config_test", []*schema.Message{
				{Role: "user", Content: testText},
			}, nil)
			var gotMessage bool
			var firstMsg *schema.Message
			var firstPacketMs int64
			for msg := range msgChan {
				if msg != nil {
					firstMsg = msg
					gotMessage = true
					firstPacketMs = time.Since(t0).Milliseconds()
					break
				}
			}
			cancel()
			pool.Release(wrapper)
			resultBase := map[string]interface{}{"first_packet_ms": firstPacketMs}
			if reporter, ok := llmProvider.(interface{ HasReasoningContent() bool }); ok {
				resultBase["reasoning_content_returned"] = reporter.HasReasoningContent()
			}
			if gotMessage && llm.IsLLMErrorMessage(firstMsg) {
				errMsg := llm.LLMErrorMessage(firstMsg)
				resultBase["ok"] = false
				resultBase["message"] = errMsg
				llmResult[configID] = resultBase
				log.Debugf("[config_test] LLM config_id=%s 失败(透传错误): %s", configID, errMsg)
			} else if gotMessage {
				resultBase["ok"] = true
				resultBase["message"] = "通过"
				llmResult[configID] = resultBase
				log.Debugf("[config_test] LLM config_id=%s 通过", configID)
			} else if ctx.Err() == context.DeadlineExceeded {
				resultBase["ok"] = false
				resultBase["message"] = "超时"
				llmResult[configID] = resultBase
				log.Debugf("[config_test] LLM config_id=%s 超时", configID)
			} else {
				resultBase["ok"] = false
				resultBase["message"] = "未收到响应或调用失败"
				llmResult[configID] = resultBase
				log.Debugf("[config_test] LLM config_id=%s 失败(未收到响应)", configID)
			}
		}
	} else {
		log.Debugf("[config_test] LLM data.llm 缺失或非 map, ok=%v", ok)
	}

	// TTS
	if v, ok := data["tts"].(map[string]interface{}); ok {
		for configID, val := range v {
			if configID == "provider" {
				continue
			}
			cfg, ok := val.(map[string]interface{})
			if !ok {
				ttsResult[configID] = map[string]interface{}{"ok": false, "message": "配置格式无效"}
				continue
			}
			wrapper, err := pool.Acquire[tts.TTSProvider]("tts", configID, cfg)
			if err != nil {
				ttsResult[configID] = map[string]interface{}{"ok": false, "message": err.Error()}
				continue
			}
			ttsProvider := wrapper.GetProvider()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			outputChan, err := ttsProvider.TextToSpeechStream(ctx, testText, 24000, 1, 60)
			if err != nil {
				cancel()
				pool.Release(wrapper)
				ttsResult[configID] = map[string]interface{}{"ok": false, "message": err.Error()}
				log.Warnf("TTS config test %s: %v", configID, err)
				continue
			}
			t0 := time.Now()
			var totalBytes int
			var firstPacketMs int64 = -1
			for chunk := range outputChan {
				if chunk != nil {
					if firstPacketMs < 0 {
						firstPacketMs = time.Since(t0).Milliseconds()
					}
					totalBytes += len(chunk)
				}
			}
			cancel()
			pool.Release(wrapper)
			if firstPacketMs < 0 {
				firstPacketMs = time.Since(t0).Milliseconds()
			}
			if totalBytes == 0 {
				ttsResult[configID] = map[string]interface{}{"ok": false, "message": "未收到有效音频或合成失败", "first_packet_ms": firstPacketMs}
				log.Debugf("[config_test] TTS config_id=%s 失败(未收到有效音频)", configID)
			} else {
				ttsResult[configID] = map[string]interface{}{"ok": true, "message": "通过", "first_packet_ms": firstPacketMs}
			}
		}
	}

	return vadResult, asrResult, llmResult, ttsResult
}

func cloneConfigMap(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return map[string]interface{}{}
	}
	dst := make(map[string]interface{}, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func vadTestSampleCount(configID string, cfg map[string]interface{}) int {
	switch vadProviderForTest(configID, cfg) {
	case "silero_vad":
		if intFromMap(cfg, "sample_rate", 16000) == 8000 {
			return 256
		}
		return 512
	case "ten_vad":
		return intFromMap(cfg, "hop_size", 512)
	default:
		return 320
	}
}

func vadProviderForTest(configID string, cfg map[string]interface{}) string {
	if provider, ok := cfg["provider"].(string); ok {
		provider = strings.ToLower(strings.TrimSpace(provider))
		if provider != "" {
			return provider
		}
	}
	if _, ok := cfg["silero_vad"]; ok {
		return "silero_vad"
	}
	if _, ok := cfg["ten_vad"]; ok {
		return "ten_vad"
	}

	configID = strings.ToLower(strings.TrimSpace(configID))
	switch {
	case strings.Contains(configID, "silero"):
		return "silero_vad"
	case strings.Contains(configID, "ten"):
		return "ten_vad"
	default:
		return ""
	}
}

func intFromMap(cfg map[string]interface{}, key string, fallback int) int {
	switch value := cfg[key].(type) {
	case int:
		if value > 0 {
			return value
		}
	case int64:
		if value > 0 {
			return int(value)
		}
	case int32:
		if value > 0 {
			return int(value)
		}
	case float64:
		if value > 0 {
			return int(value)
		}
	case float32:
		if value > 0 {
			return int(value)
		}
	}
	return fallback
}

func llmThinkingEnabled(cfg map[string]interface{}) bool {
	rawThinking, ok := cfg["thinking"].(map[string]interface{})
	if !ok || len(rawThinking) == 0 {
		return false
	}

	mode, _ := rawThinking["mode"].(string)
	mode = strings.ToLower(strings.TrimSpace(mode))
	return mode != "" && mode != "default"
}

// mapKeys 返回 map 的键列表，用于 debug 日志
func mapKeys(m map[string]interface{}) []string {
	if m == nil {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// 敏感字段名（小写），脱敏后用于日志
var sensitiveKeys = map[string]bool{
	"api_key": true, "access_token": true, "token": true, "password": true, "secret": true,
}

// redactSensitive 深拷贝 data 并将敏感字段值替换为 "***"，用于 debug 日志
func redactSensitive(data map[string]interface{}) map[string]interface{} {
	if data == nil {
		return nil
	}
	out := make(map[string]interface{}, len(data))
	for k, v := range data {
		out[k] = redactValue(v)
	}
	return out
}

func redactValue(v interface{}) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		m := make(map[string]interface{}, len(x))
		for k, val := range x {
			if sensitiveKeys[strings.ToLower(k)] {
				m[k] = "***"
			} else {
				m[k] = redactValue(val)
			}
		}
		return m
	case []interface{}:
		arr := make([]interface{}, len(x))
		for i, val := range x {
			arr[i] = redactValue(val)
		}
		return arr
	default:
		return v
	}
}
