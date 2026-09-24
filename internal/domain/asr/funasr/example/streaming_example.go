package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/go-audio/audio"
	"github.com/go-audio/wav"

	"xiaozhi-esp32-server-golang/internal/domain/asr/funasr"
)

// readWavFile 读取WAV文件并转换为PCM []float32数据
func readWavFile(filePath string) ([]float32, error) {
	// 打开WAV文件
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("打开WAV文件失败: %v", err)
	}
	defer file.Close()

	// 创建WAV解码器
	wavDecoder := wav.NewDecoder(file)
	if !wavDecoder.IsValidFile() {
		return nil, fmt.Errorf("无效的WAV文件")
	}

	// 读取WAV文件信息
	wavDecoder.ReadInfo()
	format := wavDecoder.Format()

	fmt.Printf("WAV格式: 采样率=%dHz, 通道数=%d\n",
		int(format.SampleRate), format.NumChannels)

	// 读取所有PCM数据
	var allPcmData []float32

	// 使用20ms帧大小作为缓冲区
	perFrameDuration := 20
	frameSize := int(format.SampleRate) * perFrameDuration / 1000
	audioBuf := &audio.IntBuffer{
		Format:         format,
		SourceBitDepth: 16,
		Data:           make([]int, frameSize*format.NumChannels),
	}

	fmt.Printf("使用帧大小: %d 采样点 (%.1fms)\n", frameSize, float64(perFrameDuration))
	fmt.Println("开始读取WAV数据...")

	for {
		// 读取WAV数据
		n, err := wavDecoder.PCMBuffer(audioBuf)
		if err == io.EOF || n == 0 {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("读取WAV数据失败: %v", err)
		}

		// 将int数据转换为float32 (范围-1.0到1.0)
		for i := 0; i < n; i++ {
			// 将int转换为float32，范围从[-32768, 32767]到[-1.0, 1.0]
			floatSample := float32(audioBuf.Data[i]) / 32767.0
			allPcmData = append(allPcmData, floatSample)
		}
	}

	fmt.Printf("成功读取WAV文件，总采样点数: %d, 时长: %.2f秒\n",
		len(allPcmData), float64(len(allPcmData))/float64(format.SampleRate))

	return allPcmData, nil
}

func main() {
	// 定义命令行参数
	var (
		host = flag.String("host", "192.168.208.214", "FunASR服务器IP地址")
		port = flag.String("port", "10096", "FunASR服务器端口")
		mode = flag.String("mode", "offline", "识别模式 (online/offline)")
		file = flag.String("file", "test.wav", "要识别的WAV文件路径")
	)

	// 解析命令行参数
	flag.Parse()

	// 显示使用说明
	if len(os.Args) < 2 {
		fmt.Println("用法: ./streaming_example [选项]")
		fmt.Println("选项:")
		flag.PrintDefaults()
		fmt.Println("\n示例:")
		fmt.Println("  ./streaming_example -host=192.168.1.100 -port=10095 -file=audio.wav")
		fmt.Println("  ./streaming_example -mode=online -file=test.wav")
		return
	}

	config := funasr.FunasrConfig{
		Host:          *host,
		Port:          *port,
		Mode:          *mode,
		SampleRate:    16000,
		ChunkSize:     []int{5, 10, 5},
		ChunkInterval: 10,
		Timeout:       30,
		AutoEnd:       false,
	}

	// 使用配置创建ASR实例
	asr, err := funasr.NewFunasr(config)
	if err != nil {
		fmt.Printf("创建ASR实例失败: %v\n", err)
		return
	}

	fmt.Printf("目标服务器: %s:%s, 模式: %s\n", config.Host, config.Port, config.Mode)

	// 使用命令行参数指定的音频文件路径
	audioFilePath := *file

	// 检查音频文件是否存在
	if _, err := os.Stat(audioFilePath); os.IsNotExist(err) {
		fmt.Printf("音频文件 %s 不存在\n", audioFilePath)
		fmt.Println("请提供有效的音频文件路径")
		return
	}

	// 读取WAV文件并转换为PCM数据
	pcmData, err := readWavFile(audioFilePath)
	if err != nil {
		fmt.Printf("读取WAV文件失败: %v\n", err)
		return
	}

	// 执行识别
	result, err := asr.Process(pcmData)
	if err != nil {
		fmt.Printf("识别失败: %v\n", err)
		return
	}

	// 格式化并打印结果
	fmt.Println("识别结果:")
	fmt.Println(strings.Repeat("-", 40))
	fmt.Println(result)
	fmt.Println(strings.Repeat("-", 40))
}
