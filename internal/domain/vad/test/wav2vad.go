package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/go-audio/audio"
	"github.com/go-audio/wav"
	"github.com/hackers365/silero-vad-go/speech"
	"gopkg.in/hraban/opus.v2"
)

// readCloserWrapper 为 bytes.Reader 提供 Close 方法以实现 ReadCloser 接口
type readCloserWrapper struct {
	*bytes.Reader
}

// Close 实现 io.Closer 接口
func (r *readCloserWrapper) Close() error {
	return nil
}

// newReadCloserWrapper 创建一个新的 ReadCloser 包装
func newReadCloserWrapper(data []byte) *readCloserWrapper {
	return &readCloserWrapper{bytes.NewReader(data)}
}

// WavToOpus 将WAV音频数据转换为标准Opus格式
// 返回Opus帧的切片集合，每个切片是一个Opus编码帧
func WavToOpus(wavData []byte, sampleRate int, channels int, bitRate int) ([][]byte, error) {

	sd, err := speech.NewDetector(speech.DetectorConfig{
		ModelPath:            "silero_vad.onnx",
		SampleRate:           16000,
		Threshold:            0.5,
		MinSilenceDurationMs: 250,
		SpeechPadMs:          150,
	})
	if err != nil {
		log.Fatalf("failed to create speech detector: %s", err)
	}

	// 创建WAV解码器
	wavReader := bytes.NewReader(wavData)
	wavDecoder := wav.NewDecoder(wavReader)
	if !wavDecoder.IsValidFile() {
		return nil, fmt.Errorf("无效的WAV文件")
	}

	// 读取WAV文件信息
	wavDecoder.ReadInfo()
	format := wavDecoder.Format()
	wavSampleRate := int(format.SampleRate)
	wavChannels := int(format.NumChannels)

	// 如果提供的参数与文件参数不一致，使用文件中的参数
	if sampleRate == 0 {
		sampleRate = wavSampleRate
	}
	if channels == 0 {
		channels = wavChannels
	}

	//打印wavDecoder信息
	fmt.Println("WAV格式:", format)

	enc, err := opus.NewEncoder(sampleRate, channels, opus.AppAudio)
	if err != nil {
		return nil, fmt.Errorf("创建Opus编码器失败: %v", err)
	}

	dec, err := opus.NewDecoder(sampleRate, channels)
	if err != nil {
		return nil, fmt.Errorf("创建Opus编码器失败: %v", err)
	}

	// 设置比特率
	if bitRate > 0 {
		if err := enc.SetBitrate(bitRate); err != nil {
			return nil, fmt.Errorf("设置比特率失败: %v", err)
		}
	}

	// 创建输出帧切片数组
	opusFrames := make([][]byte, 0)

	perFrameDuration := 60
	// PCM缓冲区 - Opus帧大小(60ms)
	frameSize := sampleRate * perFrameDuration / 1000
	pcmBuffer := make([]int16, frameSize*channels)
	pcmBufferFloat32 := make([]float32, frameSize*channels)
	opusBuffer := make([]byte, 1000) // 足够大的缓冲区存储编码后的数据

	// 读取音频缓冲区
	audioBuf := &audio.IntBuffer{Data: make([]int, frameSize*channels), Format: format}

	fmt.Println("开始转换...")

	pcmAllData := make([]float32, 0)
	for {
		// 读取WAV数据
		n, err := wavDecoder.PCMBuffer(audioBuf)
		if err == io.EOF || n == 0 {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("读取WAV数据失败: %v", err)
		}

		// 将int转换为int16
		for i := 0; i < len(audioBuf.Data); i++ {
			if i < len(pcmBuffer) {
				pcmBuffer[i] = int16(audioBuf.Data[i])
			}
		}

		// 编码为Opus格式
		n, err = enc.Encode(pcmBuffer, opusBuffer)
		if err != nil {
			return nil, fmt.Errorf("编码失败: %v", err)
		}

		// 将当前帧复制到新的切片中并添加到帧数组
		frameData := make([]byte, n)
		copy(frameData, opusBuffer[:n])
		opusFrames = append(opusFrames, frameData)

		//将opus解码至pcm
		n, err = dec.DecodeFloat32(frameData, pcmBufferFloat32)
		if err != nil {
			return nil, fmt.Errorf("解码失败: %v", err)
		}

		fmt.Printf("pcmBufferFloat32 len: %d\n", len(pcmBufferFloat32[:n]))

		segments, err := sd.Detect(pcmBufferFloat32[:n])
		if err != nil {
			//log.Fatalf("Detect failed: %s", err)
		}
		fmt.Printf("detect voice: %v\n", segments)

		pcmAllData = append(pcmAllData, pcmBufferFloat32[:n]...)
	}

	segments, err := sd.Detect(pcmAllData)
	if err != nil {
		log.Fatalf("Detect failed: %s", err)
	}
	fmt.Printf("detect voice: %v\n", segments)

	//将frameData输出至test.opus
	opusFile, err := os.OpenFile("output.opus", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to create opus file: %s", err)
	}
	opusFile.Write(opusFrames[0])
	opusFile.Close()

	/*
		//将pcm数据输出至test.pcm
		pcmFile, err := os.OpenFile("test.pcm", os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("failed to create pcm file: %s", err)
		}

		defer pcmFile.Close()
		dec, err := opus.NewDecoder(sampleRate, channels)
		if err != nil {
			return nil, fmt.Errorf("创建Opus解码器失败: %v", err)
		}

		pcmBuffer = make([]int16, 10240)
		for _, data := range opusFrames {
			//将opus数据decode成pcm
			n, err := dec.Decode(data, pcmBuffer)
			if err != nil {
				return nil, fmt.Errorf("解码失败: %v", err)
			}
			frameData := make([]int16, len(pcmBuffer)*2)
			copy(frameData, pcmBuffer[:n])
			_, err = pcmFile.Write(frameData)
			if err != nil {
				log.Fatalf("failed to write to pcm file: %s", err)
			}
		}*/

	return opusFrames, nil
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("invalid arguments provided: expecting one file path")
	}

	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatalf("failed to open sample audio file: %s", err)
	}
	defer f.Close()

	//读取文件全部内容
	mp3Data, err := io.ReadAll(f)
	if err != nil {
		log.Fatalf("failed to read mp3 file: %s", err)
	}

	//将mp3转换为opus
	opusData, err := WavToOpus(mp3Data, 16000, 1, 0)
	if err != nil {
		log.Fatalf("failed to convert mp3 to opus: %s", err)
	}

	//打印opus数据
	fmt.Printf("opusData: %d\n", len(opusData))

	//将Opus数据decode成pcm

	//将所有数据输出至test.opus
	/*opusFile, err := os.OpenFile("test.opus", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to create opus file: %s", err)
	}
	defer opusFile.Close()

	for _, data := range opusData {
		_, err := opusFile.Write(data)
		if err != nil {
			log.Fatalf("failed to write to opus file: %s", err)
		}
	}*/
}
