package aliyun_funasr

// Header WebSocket 事件头
type Header struct {
	Action       string                 `json:"action,omitempty"`
	TaskID       string                 `json:"task_id,omitempty"`
	Streaming    string                 `json:"streaming,omitempty"`
	Event        string                 `json:"event,omitempty"`
	ErrorCode    string                 `json:"error_code,omitempty"`
	ErrorMessage string                 `json:"error_message,omitempty"`
	Attributes   map[string]interface{} `json:"attributes,omitempty"`
}

// Params 识别参数
type Params struct {
	Format                     string   `json:"format,omitempty"`
	SampleRate                 int      `json:"sample_rate,omitempty"`
	LanguageHints              []string `json:"language_hints,omitempty"`
	VocabularyID               string   `json:"vocabulary_id,omitempty"`
	DisfluencyRemovalEnabled   bool     `json:"disfluency_removal_enabled,omitempty"`
	SemanticPunctuationEnabled bool     `json:"semantic_punctuation_enabled,omitempty"`
}

// Output 识别输出
type Output struct {
	Sentence struct {
		BeginTime   int64  `json:"begin_time"`
		EndTime     *int64 `json:"end_time"`
		Text        string `json:"text"`
		Heartbeat   bool   `json:"heartbeat"`
		SentenceEnd bool   `json:"sentence_end"`
		Words       []struct {
			BeginTime   int64  `json:"begin_time"`
			EndTime     *int64 `json:"end_time"`
			Text        string `json:"text"`
			Punctuation string `json:"punctuation"`
		} `json:"words"`
	} `json:"sentence"`
}

// Payload 事件载荷
type Payload struct {
	TaskGroup  string `json:"task_group,omitempty"`
	Task       string `json:"task,omitempty"`
	Function   string `json:"function,omitempty"`
	Model      string `json:"model,omitempty"`
	Parameters Params `json:"parameters,omitempty"`
	Input      Input  `json:"input,omitempty"`
	Output     Output `json:"output,omitempty"`
	Usage      *struct {
		Duration int `json:"duration"`
	} `json:"usage,omitempty"`
}

// Input 事件输入（占位）
type Input struct{}

// Event 事件结构
type Event struct {
	Header  Header  `json:"header"`
	Payload Payload `json:"payload"`
}
