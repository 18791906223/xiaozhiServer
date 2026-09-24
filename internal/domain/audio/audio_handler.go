package audio

import (
	"errors"

	"gopkg.in/hraban/opus.v2"
)

type AudioProcesser struct {
	sampleRate       int
	channels         int
	perFrameDuration int
	decoder          *opus.Decoder
	encoder          *opus.Encoder
}

func GetAudioProcesser(sampleRate int, channels int, perFrameDuration int) (*AudioProcesser, error) {
	decoder, err := opus.NewDecoder(sampleRate, channels)
	if err != nil {
		return nil, err
	}
	encoder, err := opus.NewEncoder(sampleRate, channels, opus.AppAudio)
	if err != nil {
		return nil, err
	}

	return &AudioProcesser{
		sampleRate:       sampleRate,
		channels:         channels,
		perFrameDuration: perFrameDuration,
		decoder:          decoder,
		encoder:          encoder,
	}, nil
}

func (a *AudioProcesser) Decoder(audio []byte, pcmData []int16) (int, error) {
	if a.decoder == nil {
		return 0, errors.New("decoder is nil")
	}
	return a.decoder.Decode(audio, pcmData)
}

func (a *AudioProcesser) DecoderFloat32(audio []byte, pcmData []float32) (int, error) {
	if a.decoder == nil {
		return 0, errors.New("decoder is nil")
	}
	return a.decoder.DecodeFloat32(audio, pcmData)
}

func (a *AudioProcesser) Encoder(pcmData []int16, audio []byte) (int, error) {
	if a.encoder == nil {
		return 0, errors.New("encoder is nil")
	}
	return a.encoder.Encode(pcmData, audio)
}
