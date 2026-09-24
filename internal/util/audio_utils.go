package util

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	log "xiaozhi-esp32-server-golang/logger"

	"github.com/go-audio/audio"
	"github.com/go-audio/wav"
	"github.com/gopxl/beep"
	"github.com/gopxl/beep/mp3"
	"gopkg.in/hraban/opus.v2"
)

// min returns the smaller of x or y.
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

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

	// 设置比特率
	if bitRate > 0 {
		if err := enc.SetBitrate(bitRate); err != nil {
			return nil, fmt.Errorf("设置比特率失败: %v", err)
		}
	}

	// 创建输出帧切片数组
	opusFrames := make([][]byte, 0)

	perFrameDuration := 20
	// PCM缓冲区 - Opus帧大小(60ms)
	frameSize := sampleRate * perFrameDuration / 1000
	pcmBuffer := make([]int16, frameSize*channels)
	opusBuffer := make([]byte, 1000) // 足够大的缓冲区存储编码后的数据

	// 读取音频缓冲区
	audioBuf := &audio.IntBuffer{Data: make([]int, frameSize*channels), Format: format}

	fmt.Println("开始转换...")
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
	}

	return opusFrames, nil
}

type AudioDecoder struct {
	streamer           beep.StreamSeekCloser
	format             beep.Format
	enc                *opus.Encoder
	pipeReader         io.ReadCloser
	perFrameDurationMs int
	AudioFormat        string
	targetSampleRate   int
	TargetAudioFormat  string

	outputOpusChan chan []byte     //opus一帧一帧的输出
	ctx            context.Context // 新增：上下文控制
}

// CreateMP3Decoder 创建一个通过 Done 通道控制的 MP3 解码器
// 为了兼容旧代码，保留此方法
func CreateAudioDecoder(ctx context.Context, pipeReader io.ReadCloser, outputOpusChan chan []byte, perFrameDurationMs int, AudioFormat string) (*AudioDecoder, error) {
	return &AudioDecoder{
		pipeReader:         pipeReader,
		outputOpusChan:     outputOpusChan,
		perFrameDurationMs: perFrameDurationMs,
		AudioFormat:        AudioFormat,
		ctx:                ctx,
		TargetAudioFormat:  "opus",
	}, nil
}

// CreateMP3Decoder 创建一个通过 Done 通道控制的 MP3 解码器
// 为了兼容旧代码，保留此方法
func CreateAudioDecoderWithSampleRate(ctx context.Context, pipeReader io.ReadCloser, outputOpusChan chan []byte, perFrameDurationMs int, AudioFormat string, targetSampleRate int) (*AudioDecoder, error) {
	return &AudioDecoder{
		pipeReader:         pipeReader,
		outputOpusChan:     outputOpusChan,
		perFrameDurationMs: perFrameDurationMs,
		AudioFormat:        AudioFormat,
		targetSampleRate:   targetSampleRate,
		ctx:                ctx,
		TargetAudioFormat:  "opus",
	}, nil
}

func (d *AudioDecoder) WithFormat(format beep.Format) *AudioDecoder {
	d.format = format
	return d
}

func (d *AudioDecoder) WithTargetAudioFormat(targetAudioFormat string) *AudioDecoder {
	d.TargetAudioFormat = targetAudioFormat
	return d
}

func (d *AudioDecoder) Run(startTs int64) error {
	if d.AudioFormat == "wav" {
		d.RunWavDecoder(startTs, false)
	} else if d.AudioFormat == "pcm" {
		d.RunWavDecoder(startTs, true)
	} else if d.AudioFormat == "mp3" {
		return d.RunMp3Decoder(startTs)
	} else if d.AudioFormat == "opus" {
		return d.RunOpusDecoder(startTs)
	} else if d.AudioFormat == "ogg_opus" {
		return d.RunOggOpusDecoder(startTs)
	}
	return nil
}

// WriteLengthPrefixedFrame 将单帧音频数据写成“4字节长度头 + payload”格式，便于流式传给通用解码器。
func WriteLengthPrefixedFrame(writer io.Writer, frame []byte) error {
	if writer == nil {
		return fmt.Errorf("writer 不能为空")
	}
	if len(frame) == 0 {
		return fmt.Errorf("frame 不能为空")
	}

	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], uint32(len(frame)))
	if _, err := writer.Write(header[:]); err != nil {
		return fmt.Errorf("写入帧长度失败: %v", err)
	}
	if _, err := writer.Write(frame); err != nil {
		return fmt.Errorf("写入帧数据失败: %v", err)
	}
	return nil
}

func readLengthPrefixedFrame(reader io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}

	frameLen := binary.LittleEndian.Uint32(header[:])
	if frameLen == 0 {
		return nil, fmt.Errorf("帧长度不能为0")
	}
	if frameLen > 64*1024 {
		return nil, fmt.Errorf("帧长度过大: %d", frameLen)
	}

	frame := make([]byte, int(frameLen))
	if _, err := io.ReadFull(reader, frame); err != nil {
		return nil, err
	}
	return frame, nil
}

func (d *AudioDecoder) RunOpusDecoder(startTs int64) error {
	defer func() {
		close(d.outputOpusChan)
		if d.pipeReader != nil {
			d.pipeReader.Close()
		}
	}()

	sourceSampleRate := int(d.format.SampleRate)
	if sourceSampleRate < 1 {
		sourceSampleRate = 16000
		log.Warnf("Opus 输入采样率为0，按 16000 Hz 处理")
	}

	channels := d.format.NumChannels
	if channels < 1 {
		channels = 1
		log.Warnf("Opus 输入通道数为0，按单声道处理")
	}

	return d.runOpusPacketStream(startTs, sourceSampleRate, channels, func() ([]byte, error) {
		packet, err := readLengthPrefixedFrame(d.pipeReader)
		if err == io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("读取Opus帧失败: 数据不完整")
		}
		if err != nil {
			return nil, err
		}
		return packet, nil
	})
}

func (d *AudioDecoder) RunOggOpusDecoder(startTs int64) error {
	defer func() {
		close(d.outputOpusChan)
		if d.pipeReader != nil {
			d.pipeReader.Close()
		}
	}()

	packetReader := &oggOpusPacketReader{reader: d.pipeReader}
	info, err := packetReader.Prepare()
	if err != nil {
		return fmt.Errorf("解析 Ogg Opus 头失败: %v", err)
	}

	log.Debugf("Ogg Opus解码器开始，原始采样率: %d, 原始通道: %d, 目标采样率: %d, 目标格式: %s", info.SampleRate, info.Channels, d.getTargetSampleRate(info.SampleRate), d.TargetAudioFormat)

	return d.runOpusPacketStream(startTs, info.SampleRate, info.Channels, packetReader.NextPacket)
}

func (d *AudioDecoder) runOpusPacketStream(startTs int64, sourceSampleRate int, channels int, nextPacket func() ([]byte, error)) error {
	firstPacket, err := nextPacket()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}

	if d.canPassthroughOpusPacket(sourceSampleRate, channels, firstPacket) {
		return d.passThroughOpusPackets(startTs, firstPacket, nextPacket)
	}
	if d.canRepacketizeOpusPacket(sourceSampleRate, channels, firstPacket) {
		return d.repacketizeOpusPackets(startTs, sourceSampleRate, firstPacket, nextPacket)
	}

	return d.transcodeOpusPackets(startTs, sourceSampleRate, channels, firstPacket, nextPacket)
}

func (d *AudioDecoder) passThroughOpusPackets(startTs int64, firstPacket []byte, nextPacket func() ([]byte, error)) error {
	var firstFrame bool
	emitPacket := func(packet []byte) error {
		if len(packet) == 0 {
			return nil
		}
		if !firstFrame {
			firstFrame = true
			log.Infof("tts云端->首帧直通完成耗时: %d ms", time.Now().UnixMilli()-startTs)
		}
		frameData := make([]byte, len(packet))
		copy(frameData, packet)
		select {
		case <-d.ctx.Done():
			log.Debugf("opus passthrough context done, exit")
			return nil
		case d.outputOpusChan <- frameData:
		}
		return nil
	}

	if err := emitPacket(firstPacket); err != nil {
		return err
	}

	for {
		select {
		case <-d.ctx.Done():
			log.Debugf("opus passthrough context done, exit")
			return nil
		default:
		}

		packet, err := nextPacket()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := emitPacket(packet); err != nil {
			return err
		}
	}
}

func (d *AudioDecoder) transcodeOpusPackets(startTs int64, sourceSampleRate int, channels int, firstPacket []byte, nextPacket func() ([]byte, error)) error {
	targetSampleRate := d.getTargetSampleRate(sourceSampleRate)
	frameDurationMs := d.perFrameDurationMs
	if frameDurationMs <= 0 {
		frameDurationMs = 60
	}
	sourceFrameSize := sourceSampleRate * frameDurationMs / 1000
	if sourceFrameSize <= 0 {
		return fmt.Errorf("无效的 Opus 帧时长: %d ms", frameDurationMs)
	}

	outputChannels := 1
	var enc *opus.Encoder
	var err error
	if d.TargetAudioFormat == "opus" {
		enc, err = opus.NewEncoder(targetSampleRate, outputChannels, opus.AppAudio)
		if err != nil {
			return fmt.Errorf("创建Opus编码器失败: %v", err)
		}
		d.enc = enc
	}

	opusDecoder, err := opus.NewDecoder(sourceSampleRate, channels)
	if err != nil {
		return fmt.Errorf("创建Opus解码器失败: %v", err)
	}

	maxDecodeSamples := channels * sourceSampleRate * 120 / 1000
	if maxDecodeSamples < channels*sourceSampleRate/50 {
		maxDecodeSamples = channels * sourceSampleRate / 50
	}
	decodedBuffer := make([]int16, maxDecodeSamples)
	pcmBuffer := make([]int16, 0, sourceFrameSize*2)
	opusBuffer := make([]byte, 1000)
	var firstFrame bool

	log.Debugf("Opus转码开始，原始采样率: %d, 目标采样率: %d, 原始通道: %d, 帧大小: %d, 目标格式: %s", sourceSampleRate, targetSampleRate, channels, sourceFrameSize, d.TargetAudioFormat)

	emitFrame := func(frame []int16) error {
		if len(frame) == 0 {
			return nil
		}

		outputPCM := append([]int16(nil), frame...)
		if targetSampleRate > 0 && targetSampleRate != sourceSampleRate {
			pcmBytes := Int16SliceToBytes(outputPCM)
			pcmFloat32 := PCM16BytesToFloat32(pcmBytes)
			pcmFloat32 = ResampleLinearFloat32(pcmFloat32, sourceSampleRate, targetSampleRate)
			outputPCM = Float32SliceToInt16Slice(pcmFloat32)
		}

		if !firstFrame {
			firstFrame = true
			log.Infof("tts云端->首帧解码完成耗时: %d ms", time.Now().UnixMilli()-startTs)
		}

		switch d.TargetAudioFormat {
		case "opus":
			n, encodeErr := enc.Encode(outputPCM, opusBuffer)
			if encodeErr != nil {
				return fmt.Errorf("Opus重编码失败: %v", encodeErr)
			}
			frameData := make([]byte, n)
			copy(frameData, opusBuffer[:n])
			select {
			case <-d.ctx.Done():
				log.Debugf("opusDecoder context done, exit")
				return nil
			case d.outputOpusChan <- frameData:
			}
		case "pcm":
			pcmData := Int16SliceToBytes(outputPCM)
			select {
			case <-d.ctx.Done():
				log.Debugf("opusDecoder context done, exit")
				return nil
			case d.outputOpusChan <- pcmData:
			}
		default:
			return fmt.Errorf("不支持的目标音频格式: %s", d.TargetAudioFormat)
		}

		return nil
	}

	flushFrames := func(flushLast bool) error {
		for len(pcmBuffer) >= sourceFrameSize {
			frame := append([]int16(nil), pcmBuffer[:sourceFrameSize]...)
			if err := emitFrame(frame); err != nil {
				return err
			}
			pcmBuffer = pcmBuffer[sourceFrameSize:]
		}
		if flushLast && len(pcmBuffer) > 0 {
			padded := make([]int16, sourceFrameSize)
			copy(padded, pcmBuffer)
			if err := emitFrame(padded); err != nil {
				return err
			}
			pcmBuffer = pcmBuffer[:0]
		}
		return nil
	}

	processPacket := func(packet []byte) error {
		n, err := opusDecoder.Decode(packet, decodedBuffer)
		if err != nil {
			return fmt.Errorf("解码Opus帧失败: %v", err)
		}
		if n <= 0 {
			return nil
		}

		if channels == 1 {
			pcmBuffer = append(pcmBuffer, decodedBuffer[:n]...)
		} else {
			for i := 0; i < n; i++ {
				base := i * channels
				var sampleSum int32
				for ch := 0; ch < channels; ch++ {
					sampleSum += int32(decodedBuffer[base+ch])
				}
				pcmBuffer = append(pcmBuffer, int16(sampleSum/int32(channels)))
			}
		}
		return flushFrames(false)
	}

	if err := processPacket(firstPacket); err != nil {
		return err
	}

	for {
		select {
		case <-d.ctx.Done():
			log.Debugf("opusDecoder context done, exit")
			return nil
		default:
		}

		packet, err := nextPacket()
		if err == io.EOF {
			log.Debugf("Opus流读取结束，处理剩余数据")
			return flushFrames(true)
		}
		if err != nil {
			return err
		}
		if err := processPacket(packet); err != nil {
			return err
		}
	}
}

func (d *AudioDecoder) repacketizeOpusPackets(startTs int64, sourceSampleRate int, firstPacket []byte, nextPacket func() ([]byte, error)) error {
	targetDurationMs := d.perFrameDurationMs
	if targetDurationMs <= 0 {
		return fmt.Errorf("无效的目标 Opus 帧时长: %d ms", targetDurationMs)
	}

	rp, err := newOpusRepacketizer()
	if err != nil {
		return err
	}
	defer rp.close()

	currentDurationMs := 0
	prevTOC := byte(0)
	var firstFrame bool

	emitCurrent := func() error {
		if rp.nbFrames() == 0 {
			return nil
		}
		packet, err := rp.out()
		if err != nil {
			return fmt.Errorf("输出重组后的 Opus packet 失败: %v", err)
		}
		if len(packet) == 0 {
			rp.reset()
			currentDurationMs = 0
			prevTOC = 0
			return nil
		}
		if !firstFrame {
			firstFrame = true
			log.Infof("tts云端->首帧重组完成耗时: %d ms", time.Now().UnixMilli()-startTs)
		}
		frameData := make([]byte, len(packet))
		copy(frameData, packet)
		select {
		case <-d.ctx.Done():
			log.Debugf("opus repacketize context done, exit")
			return nil
		case d.outputOpusChan <- frameData:
		}
		rp.reset()
		currentDurationMs = 0
		prevTOC = 0
		return nil
	}

	appendPacket := func(packet []byte) error {
		if len(packet) == 0 {
			return nil
		}
		packetDurationMs, err := opusPacketDurationMs(packet, sourceSampleRate)
		if err != nil {
			return err
		}
		if packetDurationMs <= 0 {
			return fmt.Errorf("非法 Opus packet 时长: %d ms", packetDurationMs)
		}
		if packetDurationMs > targetDurationMs {
			return fmt.Errorf("Opus packet 时长 %d ms 大于目标帧长 %d ms，无法仅通过重组处理", packetDurationMs, targetDurationMs)
		}

		needFlush := rp.nbFrames() > 0 && (((prevTOC & 0xFC) != (packet[0] & 0xFC)) || currentDurationMs+packetDurationMs > targetDurationMs)
		if needFlush {
			if err := emitCurrent(); err != nil {
				return err
			}
		}

		if err := rp.cat(packet); err != nil {
			return fmt.Errorf("提交 Opus packet 到 repacketizer 失败: %v", err)
		}
		prevTOC = packet[0]
		currentDurationMs += packetDurationMs
		if currentDurationMs == targetDurationMs {
			return emitCurrent()
		}
		return nil
	}

	if err := appendPacket(firstPacket); err != nil {
		return err
	}

	for {
		select {
		case <-d.ctx.Done():
			log.Debugf("opus repacketize context done, exit")
			return nil
		default:
		}

		packet, err := nextPacket()
		if err == io.EOF {
			return emitCurrent()
		}
		if err != nil {
			return err
		}
		if err := appendPacket(packet); err != nil {
			return err
		}
	}
}

func (d *AudioDecoder) getTargetSampleRate(sourceSampleRate int) int {
	targetSampleRate := sourceSampleRate
	if d.targetSampleRate > 0 {
		targetSampleRate = d.targetSampleRate
	}
	return targetSampleRate
}

func (d *AudioDecoder) canPassthroughOpusPacket(sourceSampleRate int, channels int, firstPacket []byte) bool {
	if d.TargetAudioFormat != "opus" {
		return false
	}
	if channels != 1 {
		return false
	}
	if d.getTargetSampleRate(sourceSampleRate) != sourceSampleRate {
		return false
	}
	if d.perFrameDurationMs <= 0 {
		return true
	}

	packetDurationMs, err := opusPacketDurationMs(firstPacket, sourceSampleRate)
	if err != nil {
		log.Debugf("解析 Opus packet 时长失败，回退转码: %v", err)
		return false
	}
	if packetDurationMs != d.perFrameDurationMs {
		log.Debugf("Opus packet 时长不匹配，回退转码: packet=%dms target=%dms", packetDurationMs, d.perFrameDurationMs)
		return false
	}
	return true
}

func (d *AudioDecoder) canRepacketizeOpusPacket(sourceSampleRate int, channels int, firstPacket []byte) bool {
	if d.TargetAudioFormat != "opus" {
		return false
	}
	if channels != 1 {
		return false
	}
	if d.getTargetSampleRate(sourceSampleRate) != sourceSampleRate {
		return false
	}
	targetDurationMs := d.perFrameDurationMs
	if targetDurationMs <= 0 || targetDurationMs > 120 {
		return false
	}

	packetDurationMs, err := opusPacketDurationMs(firstPacket, sourceSampleRate)
	if err != nil {
		log.Debugf("解析 Opus packet 时长失败，回退转码: %v", err)
		return false
	}
	if packetDurationMs <= 0 || packetDurationMs >= targetDurationMs {
		return false
	}
	return true
}

func opusPacketDurationMs(packet []byte, sampleRate int) (int, error) {
	if len(packet) == 0 {
		return 0, fmt.Errorf("空 Opus packet")
	}
	if sampleRate <= 0 {
		sampleRate = 48000
	}

	samplesPerFrame := opusPacketSamplesPerFrame(packet[0], sampleRate)
	frameCount, err := opusPacketFrameCount(packet)
	if err != nil {
		return 0, err
	}
	totalSamples := samplesPerFrame * frameCount
	return totalSamples * 1000 / sampleRate, nil
}

func opusPacketSamplesPerFrame(toc byte, sampleRate int) int {
	if toc&0x80 != 0 {
		return (sampleRate << ((toc >> 3) & 0x03)) / 400
	}
	if toc&0x60 == 0x60 {
		if toc&0x08 != 0 {
			return sampleRate / 50
		}
		return sampleRate / 100
	}

	audioSize := (toc >> 3) & 0x03
	if audioSize == 3 {
		return sampleRate * 60 / 1000
	}
	return (sampleRate << audioSize) / 100
}

func opusPacketFrameCount(packet []byte) (int, error) {
	if len(packet) == 0 {
		return 0, fmt.Errorf("空 Opus packet")
	}

	switch packet[0] & 0x03 {
	case 0:
		return 1, nil
	case 1, 2:
		return 2, nil
	default:
		if len(packet) < 2 {
			return 0, fmt.Errorf("Opus packet 长度不足，无法解析 frame count")
		}
		return int(packet[1] & 0x3F), nil
	}
}

type opusStreamInfo struct {
	SampleRate int
	Channels   int
}

type oggPage struct {
	HeaderType byte
	Segments   []byte
	Body       []byte
}

type oggOpusPacketReader struct {
	reader   io.Reader
	queue    [][]byte
	carry    []byte
	info     opusStreamInfo
	headSeen bool
	tagsSeen bool
}

func (r *oggOpusPacketReader) Prepare() (opusStreamInfo, error) {
	for !r.headSeen || !r.tagsSeen {
		if err := r.readNextPage(); err != nil {
			if err == io.EOF {
				return opusStreamInfo{}, fmt.Errorf("Ogg Opus 流缺少必要头部")
			}
			return opusStreamInfo{}, err
		}
	}

	if r.info.SampleRate <= 0 {
		r.info.SampleRate = 48000
	}
	if r.info.Channels <= 0 {
		r.info.Channels = 1
	}
	return r.info, nil
}

func (r *oggOpusPacketReader) NextPacket() ([]byte, error) {
	for len(r.queue) == 0 {
		if err := r.readNextPage(); err != nil {
			if err == io.EOF {
				if len(r.carry) > 0 {
					return nil, io.ErrUnexpectedEOF
				}
				return nil, io.EOF
			}
			return nil, err
		}
	}

	packet := r.queue[0]
	r.queue = r.queue[1:]
	return packet, nil
}

func (r *oggOpusPacketReader) readNextPage() error {
	page, err := readOggPage(r.reader)
	if err != nil {
		return err
	}

	packet := r.carry
	if len(packet) == 0 && page.HeaderType&0x01 != 0 {
		return fmt.Errorf("收到缺少前序数据的 Ogg continuation page")
	}

	offset := 0
	for _, segmentLen := range page.Segments {
		end := offset + int(segmentLen)
		if end > len(page.Body) {
			return fmt.Errorf("Ogg page 数据长度不完整")
		}
		packet = append(packet, page.Body[offset:end]...)
		offset = end
		if segmentLen < 255 {
			completePacket := append([]byte(nil), packet...)
			if err := r.handlePacket(completePacket); err != nil {
				return err
			}
			packet = nil
		}
	}

	if offset != len(page.Body) {
		return fmt.Errorf("Ogg page 数据存在未消费尾部: offset=%d total=%d", offset, len(page.Body))
	}

	r.carry = packet
	return nil
}

func (r *oggOpusPacketReader) handlePacket(packet []byte) error {
	switch {
	case !r.headSeen:
		info, err := parseOpusHeadPacket(packet)
		if err != nil {
			return err
		}
		r.info = info
		r.headSeen = true
	case !r.tagsSeen:
		if !bytes.HasPrefix(packet, []byte("OpusTags")) {
			return fmt.Errorf("缺少 OpusTags 包")
		}
		r.tagsSeen = true
	default:
		if len(packet) > 0 {
			r.queue = append(r.queue, packet)
		}
	}
	return nil
}

func parseOpusHeadPacket(packet []byte) (opusStreamInfo, error) {
	if len(packet) < 19 {
		return opusStreamInfo{}, fmt.Errorf("OpusHead 包长度不足: %d", len(packet))
	}
	if !bytes.HasPrefix(packet, []byte("OpusHead")) {
		return opusStreamInfo{}, fmt.Errorf("缺少 OpusHead 包")
	}

	channels := int(packet[9])
	if channels < 1 {
		channels = 1
	}
	sampleRate := int(binary.LittleEndian.Uint32(packet[12:16]))
	if sampleRate <= 0 {
		sampleRate = 48000
	}

	return opusStreamInfo{
		SampleRate: NormalizeOpusSampleRate(sampleRate),
		Channels:   channels,
	}, nil
}

func readOggPage(reader io.Reader) (oggPage, error) {
	header := make([]byte, 27)
	n, err := io.ReadFull(reader, header)
	if err != nil {
		if err == io.EOF && n == 0 {
			return oggPage{}, io.EOF
		}
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			return oggPage{}, io.ErrUnexpectedEOF
		}
		return oggPage{}, err
	}

	if !bytes.Equal(header[:4], []byte("OggS")) {
		return oggPage{}, fmt.Errorf("非法 OggS 头")
	}
	if header[4] != 0 {
		return oggPage{}, fmt.Errorf("不支持的 Ogg 版本: %d", header[4])
	}

	segmentCount := int(header[26])
	segments := make([]byte, segmentCount)
	if _, err := io.ReadFull(reader, segments); err != nil {
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			return oggPage{}, io.ErrUnexpectedEOF
		}
		return oggPage{}, err
	}

	bodyLen := 0
	for _, segmentLen := range segments {
		bodyLen += int(segmentLen)
	}

	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(reader, body); err != nil {
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			return oggPage{}, io.ErrUnexpectedEOF
		}
		return oggPage{}, err
	}

	return oggPage{
		HeaderType: header[5],
		Segments:   segments,
		Body:       body,
	}, nil
}

func (d *AudioDecoder) RunWavDecoder(startTs int64, isRaw bool) error {
	defer func() {
		close(d.outputOpusChan)
		if d.pipeReader != nil {
			d.pipeReader.Close()
		}
	}()
	var sampleRate int
	var channels int

	if !isRaw {
		// WAV文件头部固定为44字节
		headerSize := 44
		header := make([]byte, headerSize)
		_, err := io.ReadFull(d.pipeReader, header)
		if err != nil {
			return fmt.Errorf("读取WAV头部失败: %v", err)
		}

		// 从WAV头部获取基本参数
		// 采样率: 字节24-27
		sampleRate = int(uint32(header[24]) | uint32(header[25])<<8 | uint32(header[26])<<16 | uint32(header[27])<<24)
		// 通道数: 字节22-23
		channels = int(uint16(header[22]) | uint16(header[23])<<8)
		if channels < 1 {
			channels = 1
			log.Warnf("WAV头部通道数为0，按单声道处理")
		}
		if sampleRate < 1 {
			sampleRate = 24000
			log.Warnf("WAV头部采样率为0，按 24000 Hz 处理")
		}
		log.Debugf("WAV格式: %d Hz, %d 通道", sampleRate, channels)
	} else {
		// 对于原始PCM数据，使用format中的参数
		sampleRate = int(d.format.SampleRate)
		channels = d.format.NumChannels
		if channels < 1 {
			channels = 1
			log.Warnf("PCM 通道数为0，按单声道处理")
		}
		if sampleRate < 1 {
			sampleRate = 24000
			log.Warnf("PCM 采样率为0，按 24000 Hz 处理")
		}
		log.Debugf("原始PCM格式: %d Hz, %d 通道", sampleRate, channels)
	}

	// 始终使用单通道输出
	outputChannels := 1
	if channels > 1 {
		log.Debugf("将多声道音频转换为单声道输出")
	}

	opusSampleRate := int(sampleRate)
	if d.targetSampleRate > 0 {
		opusSampleRate = d.targetSampleRate
	}

	// 根据目标格式决定是否创建Opus编码器
	var enc *opus.Encoder
	var err error
	if d.TargetAudioFormat == "opus" {
		enc, err = opus.NewEncoder(opusSampleRate, outputChannels, opus.AppAudio)
		if err != nil {
			return fmt.Errorf("创建Opus编码器失败: %v", err)
		}
		d.enc = enc
	}

	//opus相关配置及缓冲区
	frameDurationMs := d.perFrameDurationMs               //每帧时长(ms)
	frameSize := int(sampleRate) * frameDurationMs / 1000 //每帧采样点数（基于原始采样率）
	pcmBuffer := make([]int16, frameSize*outputChannels)  //PCM缓冲区
	opusBuffer := make([]byte, 1000)                      //Opus输出缓冲区

	log.Debugf("WAV/PCM解码器开始，原始采样率: %d, 目标采样率: %d, 帧大小: %d, 目标格式: %s", sampleRate, opusSampleRate, frameSize, d.TargetAudioFormat)

	// 用于读取原始PCM数据的缓冲区
	bytesPerPoint := 2 * channels // 16位采样=2字节，多声道按一个采样点聚合
	rawBuffer := make([]byte, frameSize*bytesPerPoint)
	remainderBytes := make([]byte, 0, bytesPerPoint*4) // 保存未对齐的残留字节，避免打乱后续采样边界
	currentFramePos := 0
	var firstFrame bool

	flushLastFrame := func() error {
		if currentFramePos <= 0 {
			return nil
		}

		// 创建一个完整的帧缓冲区，用0填充剩余部分
		paddedFrame := make([]int16, len(pcmBuffer))
		copy(paddedFrame, pcmBuffer[:currentFramePos]) // 将有效数据复制到开头，剩余部分默认为0

		var opusPcmBuffer []int16 = paddedFrame
		if d.targetSampleRate > 0 && d.targetSampleRate != sampleRate {
			pcmBytes := Int16SliceToBytes(opusPcmBuffer)
			pcmFloat32 := PCM16BytesToFloat32(pcmBytes)
			pcmFloat32 = ResampleLinearFloat32(pcmFloat32, sampleRate, d.targetSampleRate)
			opusPcmBuffer = Float32SliceToInt16Slice(pcmFloat32)
		}

		// 根据目标格式输出数据
		if d.TargetAudioFormat == "opus" {
			// 编码最后一帧
			n, encodeErr := enc.Encode(opusPcmBuffer, opusBuffer)
			if encodeErr != nil {
				log.Errorf("编码剩余数据失败: %v", encodeErr)
				return fmt.Errorf("编码剩余数据失败: %v", encodeErr)
			}
			frameData := make([]byte, n)
			copy(frameData, opusBuffer[:n])
			select {
			case <-d.ctx.Done():
				log.Debugf("wavDecoder context done, exit")
				return nil
			case d.outputOpusChan <- frameData:
			}
			return nil
		}
		if d.TargetAudioFormat == "pcm" {
			// 直接输出PCM数据
			pcmData := Int16SliceToBytes(opusPcmBuffer)
			select {
			case <-d.ctx.Done():
				log.Debugf("wavDecoder context done, exit")
				return nil
			case d.outputOpusChan <- pcmData:
			}
		}
		return nil
	}

	for {
		select {
		case <-d.ctx.Done():
			log.Debugf("wavDecoder context done, exit")
			return nil
		default:
			// 读取PCM数据
			n, readErr := d.pipeReader.Read(rawBuffer)
			if n <= 0 && readErr == nil {
				continue
			}

			var chunk []byte
			if n > 0 {
				chunk = rawBuffer[:n]
				if len(remainderBytes) > 0 {
					combined := make([]byte, 0, len(remainderBytes)+len(chunk))
					combined = append(combined, remainderBytes...)
					combined = append(combined, chunk...)
					chunk = combined
					remainderBytes = remainderBytes[:0]
				}

				alignedBytes := (len(chunk) / bytesPerPoint) * bytesPerPoint
				if alignedBytes < len(chunk) {
					remainderBytes = append(remainderBytes[:0], chunk[alignedBytes:]...)
					chunk = chunk[:alignedBytes]
				}
			}

			// 将字节数据转换为int16采样点（保证按采样点边界对齐）
			samplesRead := len(chunk) / bytesPerPoint
			for i := 0; i < samplesRead; i++ {
				// 对于多通道,取平均值
				var sampleSum int32
				for ch := 0; ch < channels; ch++ {
					pos := i*bytesPerPoint + ch*2
					sample := int16(uint16(chunk[pos]) | uint16(chunk[pos+1])<<8)
					sampleSum += int32(sample)
				}

				// 计算多通道平均值
				avgSample := int16(sampleSum / int32(channels))
				pcmBuffer[currentFramePos] = avgSample
				currentFramePos++

				// 如果缓冲区已满,进行编码或输出
				if currentFramePos == len(pcmBuffer) {
					if !firstFrame {
						firstFrame = true
						log.Infof("tts云端->首帧解码完成耗时: %d ms", time.Now().UnixMilli()-startTs)
					}

					var opusPcmBuffer []int16 = pcmBuffer
					if d.targetSampleRate > 0 && d.targetSampleRate != sampleRate {
						pcmBytes := Int16SliceToBytes(opusPcmBuffer)
						pcmFloat32 := PCM16BytesToFloat32(pcmBytes)
						pcmFloat32 = ResampleLinearFloat32(pcmFloat32, sampleRate, d.targetSampleRate)
						opusPcmBuffer = Float32SliceToInt16Slice(pcmFloat32)
					}

					if d.TargetAudioFormat == "opus" {
						// Opus编码输出
						opusLen, err := enc.Encode(opusPcmBuffer, opusBuffer)
						if err != nil {
							log.Errorf("WAV/PCM解码编码失败: %v", err)
							// 编码失败时，跳过这一帧但继续处理
							currentFramePos = 0 // 重置帧位置
							continue
						}

						// 将当前帧复制到新的切片中
						frameData := make([]byte, opusLen)
						copy(frameData, opusBuffer[:opusLen])
						select {
						case <-d.ctx.Done():
							log.Debugf("wavDecoder context done, exit")
							return nil
						case d.outputOpusChan <- frameData:
						}
					} else if d.TargetAudioFormat == "pcm" {
						// 直接输出PCM数据
						pcmData := Int16SliceToBytes(opusPcmBuffer)
						select {
						case <-d.ctx.Done():
							log.Debugf("wavDecoder context done, exit")
							return nil
						case d.outputOpusChan <- pcmData:
						}
					}
					currentFramePos = 0
				}
			}

			if readErr == io.EOF {
				log.Debugf("WAV/PCM流读取结束，处理剩余数据")
				if len(remainderBytes) > 0 {
					log.Warnf("WAV/PCM存在未对齐残留字节，已丢弃: %d", len(remainderBytes))
				}
				return flushLastFrame()
			}
			if readErr != nil {
				return fmt.Errorf("读取PCM数据失败: %v", readErr)
			}
		}
	}
}

func (d *AudioDecoder) RunMp3Decoder(startTs int64) error {
	defer func() {
		close(d.outputOpusChan)
		if d.pipeReader != nil {
			d.pipeReader.Close()
		}
	}()

	decoder, format, err := mp3.Decode(d.pipeReader)
	if err != nil {
		return fmt.Errorf("创建MP3解码器失败: %v", err)
	}
	log.Debugf("MP3格式: %d Hz, %d 通道", format.SampleRate, format.NumChannels)
	d.streamer = decoder
	d.format = format

	// 流式解码MP3
	defer func() {
		d.streamer.Close()
	}()

	// 获取MP3音频信息
	sampleRate := format.SampleRate
	channels := format.NumChannels

	// 始终使用单通道输出
	outputChannels := 1
	if channels > 1 {
		log.Debugf("将双声道音频转换为单声道输出")
	}

	opusSampleRate := int(sampleRate)
	if d.targetSampleRate > 0 {
		opusSampleRate = d.targetSampleRate
	}

	// 根据目标格式决定是否创建Opus编码器
	var enc *opus.Encoder
	if d.TargetAudioFormat == "opus" {
		enc, err = opus.NewEncoder(opusSampleRate, outputChannels, opus.AppAudio)
		if err != nil {
			return fmt.Errorf("创建Opus编码器失败: %v", err)
		}
		d.enc = enc
	}

	//opus相关配置及缓冲区 创建缓冲区用于接收音频采样
	frameDurationMs := d.perFrameDurationMs               //60ms
	frameSize := int(sampleRate) * frameDurationMs / 1000 // 60ms帧大小
	// 临时PCM存储，将音频转换为PCM格式
	pcmBuffer := make([]int16, frameSize*outputChannels)

	//mp3读缓冲区
	mp3Buffer := make([][2]float64, 2048)

	//opus输出缓冲区
	opusBuffer := make([]byte, 1000)

	currentFramePos := 0 // 当前填充到pcmBuffer的位置
	var firstFrame bool
	frameCount := 0

	log.Debugf("MP3解码器开始，原始采样率: %d, 目标采样率: %d, 帧大小: %d, 目标格式: %s", int(sampleRate), opusSampleRate, frameSize, d.TargetAudioFormat)

	for {
		select {
		case <-d.ctx.Done():
			log.Debugf("mp3Decoder context done, exit")
			return nil
		default:
			// 从MP3读取PCM数据
			n, ok := d.streamer.Stream(mp3Buffer)

			if !ok {
				log.Debugf("MP3流读取结束，处理剩余数据")
				// 处理剩余不足一帧的数据
				if currentFramePos > 0 {
					// 创建一个完整的帧缓冲区，用0填充剩余部分
					paddedFrame := make([]int16, len(pcmBuffer))
					copy(paddedFrame, pcmBuffer[:currentFramePos]) // 将有效数据复制到开头，剩余部分默认为0

					var opusPcmBuffer []int16 = paddedFrame
					if d.targetSampleRate > 0 && d.targetSampleRate != int(sampleRate) {
						pcmBytes := Int16SliceToBytes(opusPcmBuffer)
						pcmFloat32 := PCM16BytesToFloat32(pcmBytes)
						pcmFloat32 = ResampleLinearFloat32(pcmFloat32, int(sampleRate), d.targetSampleRate)
						opusPcmBuffer = Float32SliceToInt16Slice(pcmFloat32)
					}

					// 根据目标格式输出数据
					if d.TargetAudioFormat == "opus" {
						// 编码补齐后的完整帧
						n, err := enc.Encode(opusPcmBuffer, opusBuffer)
						if err != nil {
							log.Errorf("编码剩余数据失败: %v", err)
							return fmt.Errorf("编码剩余数据失败: %v", err)
						} else {
							frameData := make([]byte, n)
							copy(frameData, opusBuffer[:n])

							select {
							case <-d.ctx.Done():
								log.Debugf("mp3Decoder context done, exit")
								return nil
							case d.outputOpusChan <- frameData:
								frameCount++
								log.Debugf("MP3解码完成，总共处理 %d 帧", frameCount)
							}
						}
					} else if d.TargetAudioFormat == "pcm" {
						// 直接输出PCM数据
						pcmData := Int16SliceToBytes(opusPcmBuffer)
						select {
						case <-d.ctx.Done():
							log.Debugf("mp3Decoder context done, exit")
							return nil
						case d.outputOpusChan <- pcmData:
							frameCount++
							log.Debugf("MP3解码完成，总共处理 %d 帧", frameCount)
						}
					}
				}
				return nil
			}

			if n == 0 {
				continue
			}

			// 将浮点音频数据转换为PCM格式(16位整数)
			for i := 0; i < n; i++ {
				// 先在浮点数阶段计算平均值，避免整数相加时溢出
				monoSampleFloat := (mp3Buffer[i][0] + mp3Buffer[i][1]) * 0.5

				// 进行音量限制，确保不超出范围
				if monoSampleFloat > 1.0 {
					monoSampleFloat = 1.0
				} else if monoSampleFloat < -1.0 {
					monoSampleFloat = -1.0
				}

				// 将浮点平均值转换为16位整数
				monoSample := int16(monoSampleFloat * 32767.0)
				pcmBuffer[currentFramePos] = monoSample
				currentFramePos++

				// 如果pcmBuffer已满一帧，则进行编码或输出
				if currentFramePos == len(pcmBuffer) {
					if !firstFrame {
						firstFrame = true
						log.Infof("tts云端->首帧解码完成耗时: %d ms", time.Now().UnixMilli()-startTs)
					}

					var opusPcmBuffer []int16 = pcmBuffer
					if d.targetSampleRate > 0 && d.targetSampleRate != int(sampleRate) {
						pcmBytes := Int16SliceToBytes(opusPcmBuffer)
						pcmFloat32 := PCM16BytesToFloat32(pcmBytes)
						pcmFloat32 = ResampleLinearFloat32(pcmFloat32, int(sampleRate), d.targetSampleRate)
						opusPcmBuffer = Float32SliceToInt16Slice(pcmFloat32)
					}

					if d.TargetAudioFormat == "opus" {
						// Opus编码输出
						opusLen, err := enc.Encode(opusPcmBuffer, opusBuffer)
						if err != nil {
							log.Errorf("MP3解码编码失败: %v", err)
							// 编码失败时，跳过这一帧但继续处理
							currentFramePos = 0 // 重置帧位置
							continue
						}

						// 将当前帧复制到新的切片中并添加到帧数组
						frameData := make([]byte, opusLen)
						copy(frameData, opusBuffer[:opusLen])

						select {
						case <-d.ctx.Done():
							log.Debugf("mp3Decoder context done, exit")
							return nil
						case d.outputOpusChan <- frameData:
							frameCount++
							if frameCount%100 == 0 {
								log.Debugf("MP3解码已处理 %d 帧", frameCount)
							}
						}
					} else if d.TargetAudioFormat == "pcm" {
						// 直接输出PCM数据
						pcmData := Int16SliceToBytes(opusPcmBuffer)
						select {
						case <-d.ctx.Done():
							log.Debugf("mp3Decoder context done, exit")
							return nil
						case d.outputOpusChan <- pcmData:
							frameCount++
							if frameCount%100 == 0 {
								log.Debugf("MP3解码已处理 %d 帧", frameCount)
							}
						}
					}

					currentFramePos = 0 // 重置帧位置
				}
			}
		}
	}
}

// GetAudioFormatByMimeType 根据MIME类型获取音频格式
func GetAudioFormatByMimeType(mimeType string) string {
	switch mimeType {
	case "audio/mpeg", "audio/mp3", "audio/mpeg3", "audio/x-mpeg-3":
		return "mp3"
	case "audio/wav", "audio/wave", "audio/x-wav":
		return "wav"
	case "audio/pcm", "audio/x-pcm":
		return "pcm"
	case "audio/ogg", "application/ogg":
		return "ogg_opus"
	case "audio/opus":
		return "opus"
	default:
		// 默认返回mp3格式
		return "mp3"
	}
}

// writeSeekerBuffer 实现io.WriteSeeker接口，包装bytes.Buffer
type writeSeekerBuffer struct {
	*bytes.Buffer
	pos int64
}

func newWriteSeekerBuffer() *writeSeekerBuffer {
	return &writeSeekerBuffer{
		Buffer: bytes.NewBuffer(nil),
		pos:    0,
	}
}

func (w *writeSeekerBuffer) Write(p []byte) (n int, err error) {
	// 如果当前位置在缓冲区末尾，直接追加
	if w.pos == int64(w.Buffer.Len()) {
		n, err = w.Buffer.Write(p)
		w.pos += int64(n)
		return n, err
	}

	// 如果当前位置在缓冲区中间，需要在该位置写入
	// 获取当前缓冲区数据的副本（避免直接修改底层缓冲区）
	data := make([]byte, w.Buffer.Len())
	copy(data, w.Buffer.Bytes())

	// 如果写入会超出当前缓冲区，需要扩展
	endPos := w.pos + int64(len(p))
	if endPos > int64(len(data)) {
		// 扩展缓冲区
		extra := int(endPos - int64(len(data)))
		data = append(data, make([]byte, extra)...)
	}

	// 在指定位置写入数据
	copy(data[w.pos:], p)

	// 更新缓冲区
	w.Buffer.Reset()
	w.Buffer.Write(data)

	n = len(p)
	w.pos += int64(n)
	return n, nil
}

func (w *writeSeekerBuffer) Seek(offset int64, whence int) (int64, error) {
	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = w.pos + offset
	case io.SeekEnd:
		newPos = int64(w.Buffer.Len()) + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	if newPos < 0 {
		return 0, fmt.Errorf("negative position")
	}

	// 如果新位置超出当前缓冲区长度，需要扩展
	if newPos > int64(w.Buffer.Len()) {
		// 扩展缓冲区
		extra := int(newPos - int64(w.Buffer.Len()))
		w.Buffer.Write(make([]byte, extra))
	}

	w.pos = newPos
	return w.pos, nil
}

// PCMFloat32BytesToWav 将PCM float32字节数组转换为WAV格式
// audioData: PCM float32格式的字节数组（每个float32占4字节，小端序）
// sampleRate: 采样率
// channels: 通道数
// 返回: WAV格式的字节数组
func PCMFloat32BytesToWav(audioData []byte, sampleRate, channels int) ([]byte, error) {
	if len(audioData) == 0 {
		return nil, fmt.Errorf("音频数据为空")
	}

	// 将字节数组转换为float32切片（小端序，每个float32占4字节）
	if len(audioData)%4 != 0 {
		// 如果不是4的倍数，截断到最近的4的倍数
		audioData = audioData[:len(audioData)-len(audioData)%4]
	}
	float32Data := make([]float32, len(audioData)/4)
	for i := 0; i < len(float32Data); i++ {
		bits := uint32(audioData[i*4]) | uint32(audioData[i*4+1])<<8 | uint32(audioData[i*4+2])<<16 | uint32(audioData[i*4+3])<<24
		float32Data[i] = math.Float32frombits(bits)
	}

	// 将float32转换为int16
	int16Data := Float32SliceToInt16Slice(float32Data)

	// 创建WAV编码器（使用writeSeekerBuffer作为输出）
	wavBuffer := newWriteSeekerBuffer()
	wavEncoder := wav.NewEncoder(wavBuffer, sampleRate, 16, channels, 1)

	// 创建音频缓冲区
	audioBuf := &audio.IntBuffer{
		Format: &audio.Format{
			NumChannels: channels,
			SampleRate:  sampleRate,
		},
		SourceBitDepth: 16,
		Data:           make([]int, len(int16Data)),
	}

	// 将int16数据转换为int切片
	for i, sample := range int16Data {
		audioBuf.Data[i] = int(sample)
	}

	// 写入WAV文件
	if err := wavEncoder.Write(audioBuf); err != nil {
		return nil, fmt.Errorf("写入WAV数据失败: %v", err)
	}

	if err := wavEncoder.Close(); err != nil {
		return nil, fmt.Errorf("关闭WAV编码器失败: %v", err)
	}

	return wavBuffer.Buffer.Bytes(), nil
}

// OpusFramesToWav 将Opus帧数组转换为WAV格式
// opusFrames: Opus格式的音频帧数组（每个元素是一个Opus帧）
// sampleRate: 采样率
// channels: 通道数
// 返回: WAV格式的字节数组
// 参考: test/test_audio/audio_utils.go 中的 OpusToWav 实现
func OpusFramesToWav(opusFrames [][]byte, sampleRate, channels int) ([]byte, error) {
	if len(opusFrames) == 0 {
		return nil, fmt.Errorf("音频数据为空")
	}

	// 创建Opus解码器
	opusDecoder, err := opus.NewDecoder(sampleRate, channels)
	if err != nil {
		return nil, fmt.Errorf("创建Opus解码器失败: %v", err)
	}

	// 创建WAV编码器（使用writeSeekerBuffer作为输出）
	wavBuffer := newWriteSeekerBuffer()
	wavEncoder := wav.NewEncoder(wavBuffer, sampleRate, 16, channels, 1)

	// 创建音频缓冲区
	audioBuf := &audio.IntBuffer{
		Format: &audio.Format{
			NumChannels: channels,
			SampleRate:  sampleRate,
		},
		SourceBitDepth: 16,
		Data:           make([]int, 0),
	}

	// PCM缓冲区用于解码（使用60ms作为估算，足够大以容纳一帧）
	perFrameDuration := 60
	pcmBuffer := make([]int16, channels*sampleRate*perFrameDuration/1000)

	// 遍历所有Opus帧并解码
	for _, opusFrame := range opusFrames {
		if len(opusFrame) == 0 {
			continue
		}

		// 解码Opus帧
		n, err := opusDecoder.Decode(opusFrame, pcmBuffer)
		if err != nil {
			return nil, fmt.Errorf("解码Opus帧失败: %v", err)
		}

		// 将PCM数据转换为int格式并添加到缓冲区
		for i := 0; i < n; i++ {
			audioBuf.Data = append(audioBuf.Data, int(pcmBuffer[i]))
		}
	}

	// 写入WAV文件
	if len(audioBuf.Data) > 0 {
		if err := wavEncoder.Write(audioBuf); err != nil {
			return nil, fmt.Errorf("写入WAV数据失败: %v", err)
		}
	}

	if err := wavEncoder.Close(); err != nil {
		return nil, fmt.Errorf("关闭WAV编码器失败: %v", err)
	}

	return wavBuffer.Buffer.Bytes(), nil
}
