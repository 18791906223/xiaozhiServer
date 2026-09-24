package main

import (
	"fmt"
	"log"
	"os"

	"github.com/hackers365/silero-vad-go/speech"

	"github.com/go-audio/wav"
)

const (
	sampleRate      = 16000
	chunkDuration   = 0.06                            // 60ms in seconds
	samplesPerChunk = int(sampleRate * chunkDuration) // 960 samples per chunk
	windowStep      = 0.03                            // 30ms step size
	samplesPerStep  = int(sampleRate * windowStep)    // 480 samples per step
)

func main() {
	sd, err := speech.NewDetector(speech.DetectorConfig{
		ModelPath:            "silero_vad.onnx",
		SampleRate:           sampleRate,
		Threshold:            0.5,
		MinSilenceDurationMs: 100,
		SpeechPadMs:          150,
		LogLevel:             speech.LogLevelInfo,
	})
	if err != nil {
		log.Fatalf("failed to create speech detector: %s", err)
	}
	defer sd.Destroy()

	if len(os.Args) != 2 {
		log.Fatalf("invalid arguments provided: expecting one file path")
	}

	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatalf("failed to open sample audio file: %s", err)
	}
	defer f.Close()

	dec := wav.NewDecoder(f)

	if ok := dec.IsValidFile(); !ok {
		log.Fatalf("invalid WAV file")
	}

	buf, err := dec.FullPCMBuffer()
	if err != nil {
		log.Fatalf("failed to get PCM buffer")
	}

	pcmBuf := buf.AsFloat32Buffer()
	totalSamples := len(pcmBuf.Data)

	// Create a buffer to hold the current window
	window := make([]float32, samplesPerChunk)

	// Process audio using sliding window
	for i := 0; i < totalSamples; i += samplesPerStep {
		// Reset VAD state before each detection
		if err := sd.Reset(); err != nil {
			log.Printf("Failed to reset VAD state at window starting at %0.2fs: %s", float64(i)/float64(sampleRate), err)
			continue
		}

		// Calculate window end position
		end := i + samplesPerChunk
		if end > totalSamples {
			// Pad the last window with zeros if needed
			copy(window, pcmBuf.Data[i:totalSamples])
			for j := totalSamples - i; j < samplesPerChunk; j++ {
				window[j] = 0
			}
		} else {
			copy(window, pcmBuf.Data[i:end])
		}

		fmt.Println("window len: ", len(window))

		// Perform VAD detection on the current window
		segments, err := sd.Detect(window)
		if err != nil {
			log.Printf("Detect failed at window starting at %0.2fs: %s", float64(i)/float64(sampleRate), err)
			continue
		}

		// Adjust timestamps to account for window position
		windowStartTime := float64(i) / float64(sampleRate)
		for _, s := range segments {
			adjustedStart := s.SpeechStartAt + windowStartTime
			log.Printf("Speech detected at %0.2fs", adjustedStart)
			if s.SpeechEndAt > 0 {
				adjustedEnd := s.SpeechEndAt + windowStartTime
				log.Printf("Speech ends at %0.2fs", adjustedEnd)
			}
		}
	}
}
