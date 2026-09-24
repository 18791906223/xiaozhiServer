package play_music

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"bytes"

	"xiaozhi-esp32-server-golang/internal/util"
	log "xiaozhi-esp32-server-golang/logger"
)

// 全局HTTP客户端，实现连接池
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
			//Timeout:   30 * time.Second,
		}
	})
	return httpClient
}

// PlayMusicStream 从URL播放音乐，返回音频流通道
// frameDuration: 每帧时长（毫秒），默认20ms
// audioFormat: 音频格式，支持 "mp3"
func PlayMusicStream(ctx context.Context, url string, sampleRate int, frameDuration int, audioFormat string) (outputChan chan []byte, err error) {
	// 参数校验和默认值设置
	if frameDuration <= 0 {
		frameDuration = 20 // 默认20ms帧时长
	}
	if audioFormat == "" {
		audioFormat = "mp3" // 默认MP3格式
	}

	startTs := time.Now().UnixMilli()

	// 创建HTTP请求
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %v", err)
	}

	req.Header.Set("Accept", "audio/*")
	req.Header.Set("User-Agent", "MusicPlayer/1.0")

	// 使用连接池创建客户端
	client := getHTTPClient()

	// 创建输出通道
	outputChan = make(chan []byte, 100)

	// 启动goroutine处理流式响应
	go func() {
		// 发送请求
		resp, err := client.Do(req)
		if err != nil {
			log.Errorf("发送请求失败: %v", err)
			close(outputChan)
			return
		}
		defer func() {
			resp.Body.Close()
		}()

		// 检查响应状态码
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			log.Errorf("API请求失败，状态码: %d, 响应: %s", resp.StatusCode, string(body))
			close(outputChan)
			return
		}

		// 检查响应内容类型和内容长度
		contentLength := resp.ContentLength

		// 记录响应长度到日志
		log.Debugf("收到音乐流响应，Content-Length: %d", contentLength)

		// 判断Content-Length是否合理
		if contentLength == 0 {
			log.Errorf("音乐流返回空响应，Content-Length为0")
			close(outputChan)
			return
		}

		// MP3文件头至少需要100字节才能正常解析
		// -1表示未知长度（例如分块传输）
		if contentLength > 0 && contentLength < 100 {
			log.Errorf("音乐流响应太小无法解析为MP3: %d字节", contentLength)
			close(outputChan)
			return
		}

		log.Infof("开始播放音乐: %s", url)

		// 根据音频格式处理流式响应
		if audioFormat == "mp3" {
			// 创建 MP3 解码器，传入 context 而不是 done 通道
			mp3Decoder, err := util.CreateAudioDecoderWithSampleRate(ctx, resp.Body, outputChan, frameDuration, audioFormat, sampleRate)
			if err != nil {
				log.Errorf("创建MP3解码器失败: %v", err)
				close(outputChan)
				return
			}

			// 启动解码过程
			if err := mp3Decoder.Run(startTs); err != nil {
				log.Errorf("MP3解码失败: %v", err)
				return
			}

			select {
			case <-ctx.Done():
				log.Debugf("音乐播放取消, URL: %s", url)
				return
			default:
				log.Infof("音乐播放完成耗时: %d ms", time.Now().UnixMilli()-startTs)
			}
		} else {
			log.Errorf("当前仅支持MP3格式的流式播放，传入格式: %s", audioFormat)
			close(outputChan)
		}
	}()

	return outputChan, nil
}

func PlayMusicFromAudioData(ctx context.Context, audioData []byte, sampleRate int, frameDuration int, audioFormat string) (outputChan chan []byte, err error) {
	// 参数校验和默认值设置
	if frameDuration <= 0 {
		frameDuration = 20 // 默认20ms帧时长
	}
	if audioFormat == "" {
		audioFormat = "mp3" // 默认MP3格式
	}

	// 添加调试信息
	log.Debugf("PlayMusicFromAudioData: 音频数据长度=%d字节, 采样率=%d, 帧时长=%dms, 格式=%s",
		len(audioData), sampleRate, frameDuration, audioFormat)

	// 检查音频数据是否为空
	if len(audioData) == 0 {
		log.Errorf("音频数据为空，无法播放")
		return nil, fmt.Errorf("音频数据为空")
	}

	startTs := time.Now().UnixMilli()

	// 创建输出通道
	outputChan = make(chan []byte, 100)

	// 启动goroutine处理流式响应
	go func() {
		// 从 audioData 创建一个 io.ReadCloser
		audioReader := io.NopCloser(bytes.NewReader(audioData))

		// 根据音频格式处理流式响应
		if audioFormat == "mp3" {
			// 创建 MP3 解码器，传入 context 而不是 done 通道
			mp3Decoder, err := util.CreateAudioDecoderWithSampleRate(ctx, audioReader, outputChan, frameDuration, audioFormat, sampleRate)
			if err != nil {
				log.Errorf("创建MP3解码器失败: %v", err)
				return
			}

			// 启动解码过程
			if err := mp3Decoder.Run(startTs); err != nil {
				log.Errorf("MP3解码失败: %v", err)
				return
			}

			select {
			case <-ctx.Done():
				log.Debugf("音乐播放取消")
				return
			default:
				log.Infof("音乐播放完成耗时: %d ms", time.Now().UnixMilli()-startTs)
			}
		} else {
			log.Errorf("当前仅支持MP3格式的流式播放，传入格式: %s", audioFormat)
		}
	}()

	return outputChan, nil
}

func PlayMusicFromPipe(ctx context.Context, pipeReader *io.PipeReader, sampleRate int, frameDuration int, audioFormat string) (outputChan chan []byte, err error) {
	// 参数校验和默认值设置
	if frameDuration <= 0 {
		frameDuration = 20 // 默认20ms帧时长
	}
	if audioFormat == "" {
		audioFormat = "mp3" // 默认MP3格式
	}

	// 添加调试信息
	log.Debugf("PlayMusicFromPipe: 采样率=%d, 帧时长=%dms, 格式=%s",
		sampleRate, frameDuration, audioFormat)

	startTs := time.Now().UnixMilli()

	// 创建输出通道
	outputChan = make(chan []byte, 100)

	// 启动goroutine处理流式响应
	go func() {
		// 根据音频格式处理流式响应
		if audioFormat == "mp3" {
			// 创建 MP3 解码器，传入 context 而不是 done 通道
			mp3Decoder, err := util.CreateAudioDecoderWithSampleRate(ctx, pipeReader, outputChan, frameDuration, audioFormat, sampleRate)
			if err != nil {
				log.Errorf("创建MP3解码器失败: %v", err)
				return
			}

			// 启动解码过程
			if err := mp3Decoder.Run(startTs); err != nil {
				log.Errorf("MP3解码失败: %v", err)
				return
			}

			select {
			case <-ctx.Done():
				log.Debugf("音乐播放取消")
				return
			default:
				log.Infof("音乐播放完成耗时: %d ms", time.Now().UnixMilli()-startTs)
			}
		} else {
			log.Errorf("当前仅支持MP3格式的流式播放，传入格式: %s", audioFormat)
		}
	}()

	return outputChan, nil
}
