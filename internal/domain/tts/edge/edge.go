package edge

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"xiaozhi-esp32-server-golang/internal/util"
	log "xiaozhi-esp32-server-golang/logger"

	"github.com/difyz9/edge-tts-go/pkg/communicate"
)

// EdgeTTSProvider Edge TTS 提供者
// 支持一次性和流式TTS，输出Opus帧
// 配置参数：voice, rate, volume, pitch, connectTimeout, receiveTimeout
type EdgeTTSProvider struct {
	Voice          string
	Rate           string
	Volume         string
	Pitch          string
	ConnectTimeout int
	ReceiveTimeout int
}

// NewEdgeTTSProvider 创建EdgeTTSProvider
func NewEdgeTTSProvider(config map[string]interface{}) *EdgeTTSProvider {
	voice, _ := config["voice"].(string)
	rate, _ := config["rate"].(string)
	volume, _ := config["volume"].(string)
	pitch, _ := config["pitch"].(string)
	connectTimeout, _ := config["connect_timeout"].(int)
	receiveTimeout, _ := config["receive_timeout"].(int)
	if rate == "" {
		rate = "+0%"
	}
	if volume == "" {
		volume = "+0%"
	}
	if pitch == "" {
		pitch = "+0Hz"
	}
	if connectTimeout == 0 {
		connectTimeout = 10
	}
	if receiveTimeout == 0 {
		receiveTimeout = 60
	}
	return &EdgeTTSProvider{
		Voice:          voice,
		Rate:           rate,
		Volume:         volume,
		Pitch:          pitch,
		ConnectTimeout: connectTimeout,
		ReceiveTimeout: receiveTimeout,
	}
}

// TextToSpeech 一次性合成，返回Opus帧
func (p *EdgeTTSProvider) TextToSpeech(ctx context.Context, text string, sampleRate int, channels int, frameDuration int) ([][]byte, error) {
	startTs := time.Now().UnixMilli()
	// 临时MP3文件
	tmpFile := fmt.Sprintf("/tmp/edge-tts-%d.mp3", time.Now().UnixNano())
	defer os.Remove(tmpFile)

	comm, err := communicate.NewCommunicate(
		text,
		p.Voice,
		p.Rate,
		p.Volume,
		p.Pitch,
		"", // proxy
		p.ConnectTimeout,
		p.ReceiveTimeout,
	)
	if err != nil {
		log.Errorf("EdgeTTS Communicate创建失败: %v", err)
		return nil, err
	}
	// 保存MP3
	err = comm.Save(ctx, tmpFile, "")
	if err != nil {
		log.Errorf("EdgeTTS保存MP3失败: %v", err)
		return nil, err
	}
	// MP3转Opus
	f, err := os.Open(tmpFile)
	if err != nil {
		return nil, fmt.Errorf("打开MP3失败: %v", err)
	}
	defer f.Close()
	pipeReader, pipeWriter := io.Pipe()
	outputChan := make(chan []byte, 1000)
	// 写入MP3数据到pipe
	go func() {
		_, _ = io.Copy(pipeWriter, f)
		pipeWriter.Close()
	}()
	mp3Decoder, err := util.CreateAudioDecoder(ctx, pipeReader, outputChan, frameDuration, "mp3")
	if err != nil {
		return nil, fmt.Errorf("创建MP3解码器失败: %v", err)
	}
	var opusFrames [][]byte
	done := make(chan struct{})
	go func() {
		for frame := range outputChan {
			opusFrames = append(opusFrames, frame)
		}
		done <- struct{}{}
	}()
	if err := mp3Decoder.Run(startTs); err != nil {
		return nil, fmt.Errorf("MP3解码失败: %v", err)
	}
	<-done
	return opusFrames, nil
}

// TextToSpeechStream 流式合成，返回Opus帧chan
func (p *EdgeTTSProvider) TextToSpeechStream(ctx context.Context, text string, sampleRate int, channels int, frameDuration int) (chan []byte, error) {
	startTs := time.Now().UnixMilli()
	comm, err := communicate.NewCommunicate(
		text,
		p.Voice,
		p.Rate,
		p.Volume,
		p.Pitch,
		"", // proxy
		p.ConnectTimeout,
		p.ReceiveTimeout,
	)
	if err != nil {
		log.Errorf("EdgeTTS Communicate创建失败: %v", err)
		return nil, err
	}

	chunkChan, errChan := comm.Stream(ctx)
	outputChan := make(chan []byte, 100)
	pipeReader, pipeWriter := io.Pipe()
	// MP3转Opus解码器
	go func() {
		defer func() {
			pipeWriter.Close()
			log.Debugf("EdgeTTS流式合成结束, 耗时: %d ms", time.Now().UnixMilli()-startTs)
			if err := <-errChan; err != nil {
				log.Errorf("EdgeTTS流式合成出错: %v", err)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				log.Debugf("EdgeTTS Stream context done, exit")
				return
			default:
				select {
				case chunk, ok := <-chunkChan:
					if !ok {
						log.Debugf("EdgeTTS Stream channel closed, exit")
						return
					}
					if chunk.Type == "audio" {
						_, _ = pipeWriter.Write(chunk.Data)
					}
				}
			}
		}

	}()
	// 启动MP3→Opus解码
	go func() {
		mp3Decoder, err := util.CreateAudioDecoder(ctx, pipeReader, outputChan, frameDuration, "mp3")
		if err != nil {
			log.Errorf("EdgeTTS MP3解码器创建失败: %v", err)
			return
		}
		if err := mp3Decoder.Run(startTs); err != nil {
			log.Errorf("EdgeTTS MP3解码失败: %v", err)
		}
		log.Debugf("EdgeTTS MP3解码结束, 耗时: %d ms", time.Now().UnixMilli()-startTs)
	}()
	return outputChan, nil
}

// SetVoice 设置音色参数
func (p *EdgeTTSProvider) SetVoice(voiceConfig map[string]interface{}) error {
	if voice, ok := voiceConfig["voice"].(string); ok && voice != "" {
		p.Voice = voice
		return nil
	}
	return fmt.Errorf("无效的音色配置: 缺少 voice")
}

// Close 关闭资源（无状态 Provider，无需关闭）
func (p *EdgeTTSProvider) Close() error {
	return nil
}

// IsValid 检查资源是否有效
func (p *EdgeTTSProvider) IsValid() bool {
	return p != nil
}
