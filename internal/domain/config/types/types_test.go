// package types

// types_test.go
//
// 本文件用于测试 config/types/types.go 中定义的所有数据结构。
// 重点演示：
//   - 各配置结构体的序列化/反序列化（JSON encode/decode）
//   - UConfig 的完整字段填充和默认值行为
//   - 嵌套结构（如 AsrConfig、SpeakerGroupInfo）的行为
//   - 指针字段的 nil 处理

package types

import (
	"encoding/json"
	"testing"
)

// ---------------------------------------------------------------------------

// TestAsrConfigSerialization 测试 ASR 配置的 JSON 序列化与反序列化
func TestAsrConfigSerialization(t *testing.T) {
	original := AsrConfig{
		Provider: "funasr",
		Config: map[string]interface{}{
			"model":       "paraformer-zh",
			"hotword":     "小智",
			"sample_rate": 16000,
		},
	}
	// 序列化为 JSON
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	t.Logf("序列化结果: %s", string(data))
	// 反序列化
	var decoded AsrConfig
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}
	// 验证字段
	if decoded.Provider != original.Provider {
		t.Errorf("Provider 不匹配: 期望 %q, 实际 %q", original.Provider, decoded.Provider)
	}
	if decoded.Config["model"] != original.Config["model"] {
		t.Errorf("model 不匹配")
	}
	t.Log("ASR 配置序列化/反序列化测试通过")
}

// ---------------------------------------------------------------------------

// TestTtsConfigSerialization 测试 TTS 配置的 JSON 序列化
func TestTtsConfigSerialization(t *testing.T) {
	config := TtsConfig{
		Provider: "doubao",
		Config: map[string]interface{}{
			"voice_id": "zh_female_shuangkuaisisi_natural",
			"speed":    1.0,
		},
	}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	var decoded TtsConfig
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}
	if decoded.Provider != "doubao" {
		t.Errorf("Provider 不匹配")
	}
	t.Log("TTS 配置序列化测试通过")
}

// ---------------------------------------------------------------------------

// TestUConfigFullStructure 测试完整的 UConfig 结构
// 验证所有字段的初始化、默认值和 JSON 行为
func TestUConfigFullStructure(t *testing.T) {
	// 创建一个完整的 UConfig
	cfg := UConfig{
		SystemPrompt:    "你是一个智能助手",
		MemoryMode:      "short",
		SpeakerChatMode: "off",
		AgentId:         "agent_001",
		MCPServiceNames: "tool_a,tool_b",
		Asr: AsrConfig{
			Provider: "sherpa_onnx",
			Config:   map[string]interface{}{"model_path": "/models/sherpa.onnx"},
		},
		Tts: TtsConfig{
			Provider: "edge",
			Config:   map[string]interface{}{"voice": "zh-CN-XiaoxiaoNeural"},
		},
		Llm: LlmConfig{
			Provider: "dify",
			Config:   map[string]interface{}{"api_key": "test_key", "url": "http://localhost:5001"},
		},
		Vad: VadConfig{
			Provider: "silero_vad",
			Config:   map[string]interface{}{"threshold": 0.5, "sample_rate": 16000},
		},
		Memory: MemoryConfig{
			Provider: "mem0",
			Config:   map[string]interface{}{"api_key": "mem0_key"},
		},
		OpenClaw: OpenClawConfig{
			Allowed:       true,
			EnterKeywords: []string{"打开龙虾"},
			ExitKeywords:  []string{"关闭龙虾"},
		},
		KnowledgeBases: []KnowledgeBaseRef{
			{ID: 1, Name: "技术文档", Provider: "ragflow", Status: "active"},
		},
		VoiceIdentify: map[string]SpeakerGroupInfo{
			"主人生": {
				ID:          1,
				Name:        "主人",
				Prompt:      "你是主人的助手",
				Description: "主人的声纹",
				Uuids:       []string{"uuid_001"},
			},
		},
	}
	// 序列化并反序列化
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	var decoded UConfig
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}
	// 验证关键字段
	if decoded.SystemPrompt != "你是一个智能助手" {
		t.Error("SystemPrompt 不匹配")
	}
	if decoded.MemoryMode != "short" {
		t.Error("MemoryMode 不匹配")
	}
	if len(decoded.KnowledgeBases) != 1 {
		t.Errorf("KnowledgeBases 长度不匹配: 期望 1, 实际 %d", len(decoded.KnowledgeBases))
	}
	if len(decoded.VoiceIdentify) != 1 {
		t.Errorf("VoiceIdentify 长度不匹配: 期望 1, 实际 %d", len(decoded.VoiceIdentify))
	}
	t.Logf("UConfig 完整结构测试通过，总大小: %d 字节", len(data))
}

// ---------------------------------------------------------------------------

// TestUConfigDefaultValues 测试 UConfig 的零值/默认行为
func TestUConfigDefaultValues(t *testing.T) {
	// 创建一个空的 UConfig（零值）
	var emptyCfg UConfig
	// 空 UConfig 的各字段应该是零值
	if emptyCfg.MemoryMode != "" {
		t.Errorf("空配置的 MemoryMode 应为空，实际: %q", emptyCfg.MemoryMode)
	}
	if emptyCfg.SpeakerChatMode != "" {
		t.Errorf("空配置的 SpeakerChatMode 应为空，实际: %q", emptyCfg.SpeakerChatMode)
	}
	if len(emptyCfg.KnowledgeBases) != 0 {
		t.Errorf("空配置的 KnowledgeBases 应为空，实际长度: %d", len(emptyCfg.KnowledgeBases))
	}
	t.Log("UConfig 零值测试通过")
}

// ---------------------------------------------------------------------------

// TestSpeakerGroupInfoFields 测试声纹组信息的完整字段
func TestSpeakerGroupInfoFields(t *testing.T) {
	ttsConfigID := "tts_config_001"
	voice := "zh_female_smart"
	voiceModelOverride := "cosyvoice-v3"
	group := SpeakerGroupInfo{
		ID:                 1,
		Name:               "主人",
		Prompt:             "你是主人的助手",
		Description:        "主人的声纹标识",
		Uuids:              []string{"uuid_001", "uuid_002"},
		TTSConfigID:        &ttsConfigID,
		Voice:              &voice,
		VoiceModelOverride: &voiceModelOverride,
	}
	if group.TTSConfigID == nil || *group.TTSConfigID != "tts_config_001" {
		t.Error("TTSConfigID 指针值不匹配")
	}
	if group.Voice == nil || *group.Voice != "zh_female_smart" {
		t.Error("Voice 指针值不匹配")
	}
	if group.VoiceModelOverride == nil || *group.VoiceModelOverride != "cosyvoice-v3" {
		t.Error("VoiceModelOverride 指针值不匹配")
	}
	if len(group.Uuids) != 2 {
		t.Errorf("Uuids 长度不匹配: 期望 2, 实际 %d", len(group.Uuids))
	}
	// 测试指针为 nil 的情况
	emptyGroup := SpeakerGroupInfo{Name: "访客"}
	if emptyGroup.TTSConfigID != nil {
		t.Error("未设置的指针应为 nil")
	}
	t.Log("SpeakerGroupInfo 字段测试通过")
}

// ---------------------------------------------------------------------------

// TestKnowledgeBaseRef 测试知识库引用的序列化
func TestKnowledgeBaseRef(t *testing.T) {
	threshold := 0.85
	ref := KnowledgeBaseRef{
		ID:                 42,
		Name:               "产品文档",
		Description:        "产品相关的知识文档",
		Provider:           "ragflow",
		ExternalKBID:       "kb_12345",
		ExternalDocID:      "doc_67890",
		RetrievalThreshold: &threshold,
		Status:             "active",
	}
	data, err := json.Marshal(ref)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	var decoded KnowledgeBaseRef
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}
	if decoded.RetrievalThreshold == nil || *decoded.RetrievalThreshold != 0.85 {
		t.Error("RetrievalThreshold 不匹配")
	}
	if decoded.Status != "active" {
		t.Errorf("Status 不匹配: 期望 active, 实际 %q", decoded.Status)
	}
	// 测试 nil threshold 的序列化
	refNoThreshold := KnowledgeBaseRef{Name: "无阈值知识库"}
	data2, _ := json.Marshal(refNoThreshold)
	t.Logf("nil threshold 序列化结果: %s", string(data2))
	t.Log("KnowledgeBaseRef 测试通过")
}

// ---------------------------------------------------------------------------

// TestOpenClawConfig 测试 OpenClaw 配置的序列化
func TestOpenClawConfig(t *testing.T) {
	config := OpenClawConfig{
		Allowed:       true,
		EnterKeywords: []string{"打开龙虾", "进入龙虾"},
		ExitKeywords:  []string{"关闭龙虾", "退出龙虾"},
	}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	var decoded OpenClawConfig
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}
	if !decoded.Allowed {
		t.Error("Allowed 应为 true")
	}
	if len(decoded.EnterKeywords) != 2 {
		t.Errorf("EnterKeywords 长度不匹配: 期望 2, 实际 %d", len(decoded.EnterKeywords))
	}
	if len(decoded.ExitKeywords) != 2 {
		t.Errorf("ExitKeywords 长度不匹配: 期望 2, 实际 %d", len(decoded.ExitKeywords))
	}
	t.Log("OpenClawConfig 测试通过")
}

// ---------------------------------------------------------------------------

// TestConfigItem 测试 ConfigItem 的基本用法
func TestConfigItem(t *testing.T) {
	item := ConfigItem{
		Provider: "dify",
		JsonData: map[string]interface{}{
			"api_key": "test_dify_key",
			"mode":    "chat",
		},
	}
	data, err := json.Marshal(item)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	var decoded ConfigItem
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}
	if decoded.Provider != "dify" {
		t.Error("Provider 不匹配")
	}
	if decoded.JsonData["api_key"] != "test_dify_key" {
		t.Error("JsonData API key 不匹配")
	}
	t.Log("ConfigItem 测试通过")
}

// ---------------------------------------------------------------------------

// TestTtsConfigItem 测试 TtsConfigItem 的序列化
func TestTtsConfigItem(t *testing.T) {
	item := TtsConfigItem{
		ConfigID:  "tts_001",
		Name:      "豆包女声",
		Provider:  "doubao",
		IsDefault: true,
		Config: map[string]interface{}{
			"voice_id": "female_voice",
		},
	}
	data, err := json.Marshal(item)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	var decoded TtsConfigItem
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}
	if !decoded.IsDefault {
		t.Error("IsDefault 应为 true")
	}
	if decoded.ConfigID != "tts_001" {
		t.Error("ConfigID 不匹配")
	}
	t.Log("TtsConfigItem 测试通过")
}

// ---------------------------------------------------------------------------

// TestKnowledgeSearchHit 测试搜索结果命中结构
func TestKnowledgeSearchHit(t *testing.T) {
	hit := KnowledgeSearchHit{
		Content: "这是一段检索到的文本内容",
		Title:   "产品使用手册",
		Score:   0.92,
	}
	data, err := json.Marshal(hit)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	var decoded KnowledgeSearchHit
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}
	if decoded.Score != 0.92 {
		t.Errorf("Score 不匹配: 期望 0.92, 实际 %f", decoded.Score)
	}
	if decoded.Title != "产品使用手册" {
		t.Error("Title 不匹配")
	}
	// 测试省略 title 时的行为
	hitNoTitle := KnowledgeSearchHit{Content: "无标题内容", Score: 0.5}
	data2, _ := json.Marshal(hitNoTitle)
	t.Logf("无 Title 的序列化: %s", string(data2))
	t.Log("KnowledgeSearchHit 测试通过")
}

// 学习提示：
//  - types.go 定义了配置系统中所有核心数据结构
//  - UConfig 是最核心的结构，聚合了所有子系统配置（ASR/TTS/LLM/VAD/Memory）
//  - 指针字段（*string）允许配置项为可选，JSON 中可省略
//  - 所有结构体都使用 json tag，便于与 Redis/API 交换数据
