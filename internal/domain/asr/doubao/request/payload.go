package request

import (
	"bytes"
	"encoding/binary"

	"github.com/bytedance/sonic"

	"xiaozhi-esp32-server-golang/internal/domain/asr/doubao/common"
)

type UserMeta struct {
	Uid        string `json:"uid,omitempty"`
	Did        string `json:"did,omitempty"`
	Platform   string `json:"platform,omitempty" `
	SDKVersion string `json:"sdk_version,omitempty"`
	APPVersion string `json:"app_version,omitempty"`
}

type AudioMeta struct {
	Format   string `json:"format,omitempty"`
	Codec    string `json:"codec,omitempty"`
	Rate     int    `json:"rate,omitempty"`
	Bits     int    `json:"bits,omitempty"`
	Channel  int    `json:"channel,omitempty"`
	Language string `json:"language,omitempty"`
}

type CorpusMeta struct {
	BoostingTableName string `json:"boosting_table_name,omitempty"`
	CorrectTableName  string `json:"correct_table_name,omitempty"`
	Context           string `json:"context,omitempty"`
}

type RequestMeta struct {
	ModelName         string     `json:"model_name,omitempty"`
	EnableITN         bool       `json:"enable_itn,omitempty"`
	EnablePUNC        bool       `json:"enable_punc,omitempty"`
	EnableDDC         bool       `json:"enable_ddc,omitempty"`
	EndWindowSize     int        `json:"end_window_size,omitempty"`
	ResultType        string     `json:"result_type,omitempty"`
	ShowUtterances    bool       `json:"show_utterances"`
	ForceToSpeechTime int        `json:"force_to_speech_time,omitempty"`
	EnableNonstream   bool       `json:"enable_nonstream"`
	Corpus            CorpusMeta `json:"corpus,omitempty"`
}

type AsrRequestPayload struct {
	User    UserMeta    `json:"user"`
	Audio   AudioMeta   `json:"audio"`
	Request RequestMeta `json:"request"`
}

type FullClientRequestOptions struct {
	Uid               string
	ModelName         string
	EnableITN         bool
	EnablePUNC        bool
	EnableDDC         bool
	EndWindowSize     int
	ResultType        string
	ShowUtterances    bool
	ForceToSpeechTime int
	EnableNonstream   bool
}

func (o FullClientRequestOptions) withDefaults() FullClientRequestOptions {
	if o.Uid == "" {
		o.Uid = "demo_uid"
	}
	if o.ModelName == "" {
		o.ModelName = "bigmodel"
	}
	if o.EndWindowSize <= 0 {
		o.EndWindowSize = 800
	}
	if o.ResultType == "" {
		o.ResultType = "full"
	}
	if o.ForceToSpeechTime <= 0 {
		o.ForceToSpeechTime = 1000
	}
	return o
}

func NewFullClientRequest(opts FullClientRequestOptions) []byte {
	opts = opts.withDefaults()
	var request bytes.Buffer
	request.Write(DefaultHeader().WithMessageTypeSpecificFlags(common.POS_SEQUENCE).toBytes())
	payload := AsrRequestPayload{
		User: UserMeta{
			Uid: opts.Uid,
		},
		Audio: AudioMeta{
			Format:  "pcm",
			Codec:   "raw",
			Rate:    16000,
			Bits:    16,
			Channel: 1,
		},
		Request: RequestMeta{
			ModelName:         opts.ModelName,
			EnableITN:         opts.EnableITN,
			EnablePUNC:        opts.EnablePUNC,
			EnableDDC:         opts.EnableDDC,
			EndWindowSize:     opts.EndWindowSize,
			ResultType:        opts.ResultType,
			ShowUtterances:    opts.ShowUtterances,
			ForceToSpeechTime: opts.ForceToSpeechTime,
			EnableNonstream:   opts.EnableNonstream,
		},
	}
	payloadArr, _ := sonic.Marshal(payload)
	payloadArr = common.GzipCompress(payloadArr)
	payloadSize := len(payloadArr)
	payloadSizeArr := make([]byte, 4)
	binary.BigEndian.PutUint32(payloadSizeArr, uint32(payloadSize))
	_ = binary.Write(&request, binary.BigEndian, int32(1))
	request.Write(payloadSizeArr)
	request.Write(payloadArr)
	return request.Bytes()
}

func NewAudioOnlyRequest(seq int, segment []byte) []byte {
	var request bytes.Buffer
	header := DefaultHeader()
	if seq < 0 {
		header.WithMessageTypeSpecificFlags(common.NEG_WITH_SEQUENCE)
	} else {
		header.WithMessageTypeSpecificFlags(common.POS_SEQUENCE)
	}
	header.WithMessageType(common.CLIENT_AUDIO_ONLY_REQUEST)
	request.Write(header.toBytes())

	// write seq
	_ = binary.Write(&request, binary.BigEndian, int32(seq))
	// write payload size
	payload := common.GzipCompress(segment)
	_ = binary.Write(&request, binary.BigEndian, int32(len(payload)))
	// write payload
	request.Write(payload)
	return request.Bytes()
}
