package audio

const (
	SampleRate    = 16000
	Channels      = 1
	FrameDuration = 60
	Format        = "opus"
)

type AudioFormat struct {
	Format        string `json:"format,omitempty"`
	SampleRate    int    `json:"sample_rate,omitempty"`
	Channels      int    `json:"channels,omitempty"`
	FrameDuration int    `json:"frame_duration,omitempty"`
}
