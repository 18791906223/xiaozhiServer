package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/eino/schema"

	"xiaozhi-esp32-server-golang/internal/domain/llm"
	llm_common "xiaozhi-esp32-server-golang/internal/domain/llm/common"
	"xiaozhi-esp32-server-golang/internal/pool"
	log "xiaozhi-esp32-server-golang/logger"
)

var openClawWarmupSchedule = []time.Duration{
	1 * time.Second,
	10 * time.Second,
	20 * time.Second,
	30 * time.Second,
	40 * time.Second,
	50 * time.Second,
	60 * time.Second,
	70 * time.Second,
	80 * time.Second,
	90 * time.Second,
	100 * time.Second,
}

const (
	openClawWarmupPlanTimeout = 8 * time.Second
	openClawWarmupPlanSize    = 11
)

const openClawWarmupSystemPrompt = `你是实时语音对话里的暖场助手，不是主回答者。

你的任务是：在主回复返回前，生成 11 条很短的中文接话，让等待过程听起来一直有人在回应。

硬性要求：
1. 只负责暖场，不能直接回答问题，不能给出事实、结论、建议、步骤、分析、解释或推测。
2. 语气要像真人在通话里轻声接话：简短、自然、口语化、有耐心。
3. 不要像客服，不要像系统提示，不要像通知播报，不要像写文案。
4. 禁止复述用户原话，尤其不要把“帮我查一下”“帮我看看”“帮我查询一下”“告诉我”这类用户指令原样拼进回复。
5. 如果需要提到主题，只能提炼成助手视角的名词短语，例如“北京后天的天气”“这个安排”；不要用命令句。
6. 前 1 到 2 条尽量更轻，不一定带主题词，例如“我看一下”“等我一下”；不要一上来就说很重的安慰话。
7. 后面的句子再逐步表达“我还在看”“我还在确认”，但要自然，不要机械重复。
8. 避免使用“正在为您处理”“请稍候”“持续跟进”“调取数据”“连接服务中”这类生硬说法。
9. 每条都必须是单句短中文，适合语音播报，长度控制在 4 到 16 个汉字。
10. 你会拿到实际播报时间点。11 条话术必须严格按这些时间点依次设计：
   - 第 1 秒：像刚接到问题，轻轻接一句。
   - 第 10 秒：自然补一句，语气仍然轻。
   - 第 20、30 秒：开始表达“我还在看”，但不要机械。
   - 第 40、50、60 秒：继续安抚，允许更明确地说“还在确认”。
   - 第 70、80、90、100 秒：承认时间有点久，但仍然自然、平静，不抱怨。
11. 只输出严格 JSON 数组，长度必须为 11。
12. JSON 每项格式必须为：{"text":"暖场语"}。
13. 禁止输出编号、Markdown、解释、代码块或 JSON 之外的任何内容。`

type openClawWarmupTask struct {
	correlationID string
	sessionCtx    context.Context
	warmupCtx     context.Context
	cancelWarmup  context.CancelFunc

	linesMu sync.RWMutex
	lines   []string

	stateMu                  sync.Mutex
	speechStarted            bool
	speechEnded              bool
	nextWarmupSegmentIsStart bool
	planReadyAt              time.Time
	planReadySignaled        bool

	spokeAny    atomic.Bool
	planReadyCh chan struct{}
}

type openClawWarmupLine struct {
	Text string `json:"text"`
}

func (s *ChatSession) startOpenClawWarmup(correlationID string, userText string) {
	correlationID = strings.TrimSpace(correlationID)
	if correlationID == "" || s == nil || s.clientState == nil {
		return
	}

	sessionCtx := s.clientState.SessionCtx.Get(s.clientState.Ctx)
	parentCtx := s.clientState.AfterAsrSessionCtx.Get(sessionCtx)
	warmupCtx, cancelWarmup := context.WithCancel(parentCtx)
	task := &openClawWarmupTask{
		correlationID:            correlationID,
		sessionCtx:               parentCtx,
		warmupCtx:                warmupCtx,
		cancelWarmup:             cancelWarmup,
		lines:                    make([]string, openClawWarmupPlanSize),
		nextWarmupSegmentIsStart: true,
		planReadyCh:              make(chan struct{}),
	}

	s.replaceOpenClawWarmup(task)
	log.Infof("OpenClaw warmup started: device=%s correlation_id=%s", s.clientState.DeviceID, correlationID)

	go s.runOpenClawWarmupTask(task, userText)
}

func (s *ChatSession) replaceOpenClawWarmup(task *openClawWarmupTask) {
	s.openClawWarmupMu.Lock()
	oldTask := s.openClawWarmup
	s.openClawWarmup = task
	s.openClawWarmupMu.Unlock()

	if oldTask != nil {
		oldTask.cancelWarmupOnly()
	}
}

func (task *openClawWarmupTask) cancelWarmupOnly() {
	if task == nil || task.cancelWarmup == nil {
		return
	}
	task.cancelWarmup()
}

func (task *openClawWarmupTask) markSpeechStarted() bool {
	if task == nil {
		return false
	}
	task.stateMu.Lock()
	defer task.stateMu.Unlock()
	if task.speechStarted || task.speechEnded {
		return false
	}
	task.speechStarted = true
	return true
}

func (task *openClawWarmupTask) markSpeechEnded() bool {
	if task == nil {
		return false
	}
	task.stateMu.Lock()
	defer task.stateMu.Unlock()
	if !task.speechStarted || task.speechEnded {
		return false
	}
	task.speechEnded = true
	return true
}

func (task *openClawWarmupTask) takeWarmupSegmentStartFlag() bool {
	if task == nil {
		return true
	}
	task.stateMu.Lock()
	defer task.stateMu.Unlock()
	isStart := task.nextWarmupSegmentIsStart
	task.nextWarmupSegmentIsStart = false
	return isStart
}

func (task *openClawWarmupTask) markPlanReady(readyAt time.Time) {
	if task == nil {
		return
	}
	task.stateMu.Lock()
	if task.planReadySignaled {
		task.stateMu.Unlock()
		return
	}
	task.planReadyAt = readyAt
	task.planReadySignaled = true
	close(task.planReadyCh)
	task.stateMu.Unlock()
}

func (task *openClawWarmupTask) waitPlanReady(ctx context.Context) (time.Time, bool) {
	if task == nil {
		return time.Time{}, false
	}

	select {
	case <-ctx.Done():
		return time.Time{}, false
	case <-task.planReadyCh:
	}

	task.stateMu.Lock()
	defer task.stateMu.Unlock()
	if task.planReadyAt.IsZero() {
		return time.Time{}, false
	}
	return task.planReadyAt, true
}

func (task *openClawWarmupTask) hasSpokenAny() bool {
	if task == nil {
		return false
	}
	return task.spokeAny.Load()
}

func (s *ChatSession) getOpenClawWarmupTask(correlationID string) *openClawWarmupTask {
	if s == nil {
		return nil
	}
	correlationID = strings.TrimSpace(correlationID)
	s.openClawWarmupMu.Lock()
	defer s.openClawWarmupMu.Unlock()
	task := s.openClawWarmup
	if task == nil {
		return nil
	}
	if correlationID != "" && task.correlationID != correlationID {
		return nil
	}
	return task
}

func (s *ChatSession) takeOpenClawWarmupTask(correlationID string) *openClawWarmupTask {
	if s == nil {
		return nil
	}
	correlationID = strings.TrimSpace(correlationID)
	s.openClawWarmupMu.Lock()
	defer s.openClawWarmupMu.Unlock()
	task := s.openClawWarmup
	if task == nil {
		return nil
	}
	if correlationID != "" && task.correlationID != correlationID {
		return nil
	}
	s.openClawWarmup = nil
	return task
}

func (s *ChatSession) cancelOpenClawWarmup(correlationID string, interrupt bool) bool {
	if s == nil {
		return false
	}

	task := s.getOpenClawWarmupTask(correlationID)
	if task == nil {
		return false
	}
	if task.warmupCtx.Err() != nil {
		return false
	}

	task.cancelWarmupOnly()
	if interrupt && task.hasSpokenAny() {
		s.InterruptAndClearTTSQueueWithReason(fmt.Sprintf("OpenClaw warmup canceled correlation_id=%s", correlationID))
	}

	log.Infof(
		"OpenClaw warmup canceled: device=%s correlation_id=%s interrupt=%v spoke_any=%v",
		s.clientState.DeviceID,
		task.correlationID,
		interrupt,
		task.hasSpokenAny(),
	)
	return true
}

func (s *ChatSession) finishOpenClawWarmup(correlationID string, interrupt bool) bool {
	task := s.takeOpenClawWarmupTask(correlationID)
	if task == nil {
		return false
	}

	task.cancelWarmupOnly()
	if interrupt {
		s.InterruptAndClearTTSQueueWithReason(fmt.Sprintf("OpenClaw warmup finished correlation_id=%s interrupt", correlationID))
	}
	s.endOpenClawSpeech(task)

	log.Infof(
		"OpenClaw warmup finished: device=%s correlation_id=%s interrupt=%v spoke_any=%v",
		s.clientState.DeviceID,
		task.correlationID,
		interrupt,
		task.hasSpokenAny(),
	)
	return true
}

func (s *ChatSession) beginOpenClawSpeech(task *openClawWarmupTask) {
	if task == nil {
		return
	}
	if !task.markSpeechStarted() {
		return
	}
	s.ttsManager.ClearAudioHistory()
	s.ttsManager.EnqueueTtsStartWithReason(task.sessionCtx, fmt.Sprintf("OpenClaw warmup start correlation_id=%s", task.correlationID))
}

func (s *ChatSession) endOpenClawSpeech(task *openClawWarmupTask) {
	if task == nil {
		return
	}
	if !task.markSpeechEnded() {
		return
	}
	s.ttsManager.GetAndClearAudioHistory()
}

func (s *ChatSession) runOpenClawWarmupTask(task *openClawWarmupTask, userText string) {
	planCtx, cancel := context.WithTimeout(task.warmupCtx, openClawWarmupPlanTimeout)
	defer cancel()
	defer log.Infof(
		"OpenClaw warmup task stopped: device=%s correlation_id=%s warmup_err=%v session_err=%v spoke_any=%v",
		s.clientState.DeviceID,
		task.correlationID,
		task.warmupCtx.Err(),
		task.sessionCtx.Err(),
		task.hasSpokenAny(),
	)

	go func() {
		lines, err := s.generateOpenClawWarmupPlan(planCtx, task.correlationID, userText)
		if err != nil {
			if planCtx.Err() == nil {
				log.Warnf("OpenClaw warmup plan generation failed: device=%s correlation_id=%s err=%v", s.clientState.DeviceID, task.correlationID, err)
			}
			task.markPlanReady(time.Time{})
			return
		}
		task.setLines(lines)
		task.markPlanReady(time.Now())
		log.Infof("OpenClaw warmup plan ready: device=%s correlation_id=%s line_count=%d", s.clientState.DeviceID, task.correlationID, len(lines))
	}()

	baseAt, ok := task.waitPlanReady(task.warmupCtx)
	if !ok {
		return
	}

	for idx, delay := range openClawWarmupSchedule {
		if !waitOpenClawWarmupUntil(task.warmupCtx, baseAt.Add(delay)) {
			return
		}
		if task.warmupCtx.Err() != nil {
			return
		}

		text := task.lineAt(idx)
		if text == "" {
			continue
		}

		log.Infof(
			"OpenClaw warmup speaking: device=%s correlation_id=%s slot=%d text=%q",
			s.clientState.DeviceID,
			task.correlationID,
			idx,
			text,
		)
		if err := s.speakOpenClawWarmupLine(task, text); err != nil && task.sessionCtx.Err() == nil {
			log.Warnf("OpenClaw warmup speak failed: device=%s correlation_id=%s slot=%d err=%v", s.clientState.DeviceID, task.correlationID, idx, err)
			return
		}
		task.spokeAny.Store(true)
	}

	// 不在这里清理 active task：最后一条暖场音频可能仍在发送/播放中，
	// 需要继续允许 OpenClaw 首句到达时执行抢占打断。
}

func waitOpenClawWarmupUntil(ctx context.Context, deadline time.Time) bool {
	wait := time.Until(deadline)
	if wait <= 0 {
		return ctx.Err() == nil
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (task *openClawWarmupTask) setLines(lines []string) {
	if task == nil || len(lines) == 0 {
		return
	}

	task.linesMu.Lock()
	defer task.linesMu.Unlock()

	if task.lines == nil {
		task.lines = make([]string, openClawWarmupPlanSize)
	}
	for idx := 0; idx < openClawWarmupPlanSize && idx < len(lines); idx++ {
		if text := sanitizeOpenClawWarmupText(lines[idx]); text != "" {
			task.lines[idx] = text
		}
	}
}

func (task *openClawWarmupTask) lineAt(index int) string {
	if task == nil || index < 0 {
		return ""
	}

	task.linesMu.RLock()
	defer task.linesMu.RUnlock()

	if index >= len(task.lines) {
		return ""
	}
	return strings.TrimSpace(task.lines[index])
}

func (s *ChatSession) speakOpenClawWarmupLine(task *openClawWarmupTask, text string) error {
	text = sanitizeOpenClawWarmupText(text)
	if text == "" {
		return nil
	}
	if task == nil {
		return nil
	}
	if task.sessionCtx.Err() != nil {
		return task.sessionCtx.Err()
	}

	s.beginOpenClawSpeech(task)
	if task.sessionCtx.Err() != nil {
		return task.sessionCtx.Err()
	}

	resp := llm_common.LLMResponseStruct{
		Text:    text,
		IsStart: task.takeWarmupSegmentStartFlag(),
		IsEnd:   true,
	}
	// 暖场句需要确保已经进入发送链路，避免被后续正式回复“看起来像没生效”。
	return s.ttsManager.handleTextResponse(task.sessionCtx, resp, true)
}

func (s *ChatSession) generateOpenClawWarmupPlan(ctx context.Context, correlationID string, userText string) ([]string, error) {
	llmWrapper, err := pool.Acquire[llm.LLMProvider](
		"llm",
		s.clientState.DeviceConfig.Llm.Provider,
		s.clientState.DeviceConfig.Llm.Config,
	)
	if err != nil {
		return nil, fmt.Errorf("acquire llm provider: %w", err)
	}
	defer pool.Release(llmWrapper)

	dialogue := []*schema.Message{
		schema.SystemMessage(openClawWarmupSystemPrompt),
		schema.UserMessage(buildOpenClawWarmupUserPrompt(userText)),
	}

	msgChan := llmWrapper.GetProvider().ResponseWithContext(
		ctx,
		buildOpenClawWarmupSessionID(s.clientState.SessionID, correlationID),
		dialogue,
		nil,
	)

	raw, err := collectOpenClawWarmupResponse(ctx, msgChan)
	if err != nil {
		return nil, err
	}
	lines := parseOpenClawWarmupPlan(raw)
	if countOpenClawWarmupLines(lines) == 0 {
		return nil, fmt.Errorf("empty warmup plan")
	}
	return lines, nil
}

func buildOpenClawWarmupUserPrompt(userText string) string {
	trimmed := strings.TrimSpace(userText)
	topic := formatOpenClawWarmupTopic(buildOpenClawWarmupHint(userText))
	topicLine := "不要复述“帮我查一下”这类用户指令。"
	if topic != "" {
		topicLine = fmt.Sprintf("如果需要提到主题，只能提炼成名词短语“%s”，不要复述“帮我查一下”这类用户指令。", topic)
	}
	return fmt.Sprintf(
		"用户本轮任务：\n%s\n\n%s\n\n实际播报时间点依次为：第1秒、第10秒、第20秒、第30秒、第40秒、第50秒、第60秒、第70秒、第80秒、第90秒、第100秒。\n请输出 11 条暖场语，并按上述 11 个时间点一一对应。",
		trimmed,
		topicLine,
	)
}

func buildOpenClawWarmupSessionID(sessionID string, correlationID string) string {
	base := strings.TrimSpace(sessionID)
	if base == "" {
		base = "openclaw"
	}
	correlationID = strings.TrimSpace(correlationID)
	if len(correlationID) > 12 {
		correlationID = correlationID[:12]
	}
	if correlationID == "" {
		return base + ":warmup"
	}
	return base + ":warmup:" + correlationID
}

func collectOpenClawWarmupResponse(ctx context.Context, msgChan chan *schema.Message) (string, error) {
	var builder strings.Builder

	for {
		select {
		case <-ctx.Done():
			return builder.String(), ctx.Err()
		case msg, ok := <-msgChan:
			if !ok {
				return builder.String(), nil
			}
			if msg == nil {
				continue
			}
			if llm.IsLLMErrorMessage(msg) {
				errMsg := strings.TrimSpace(llm.LLMErrorMessage(msg))
				if errMsg == "" {
					errMsg = "unknown llm error"
				}
				return builder.String(), fmt.Errorf("llm returned error: %s", errMsg)
			}
			if msg.Content != "" {
				builder.WriteString(msg.Content)
			}
		}
	}
}

func parseOpenClawWarmupPlan(raw string) []string {
	lines := make([]string, openClawWarmupPlanSize)

	raw = strings.TrimSpace(raw)
	if raw == "" {
		return lines
	}

	candidate := raw
	start := strings.Index(candidate, "[")
	end := strings.LastIndex(candidate, "]")
	if start >= 0 && end > start {
		candidate = candidate[start : end+1]
	}

	var objectItems []openClawWarmupLine
	if err := json.Unmarshal([]byte(candidate), &objectItems); err == nil {
		return buildOpenClawWarmupPlanLines(objectItemsToStrings(objectItems))
	}

	var stringItems []string
	if err := json.Unmarshal([]byte(candidate), &stringItems); err == nil {
		return buildOpenClawWarmupPlanLines(stringItems)
	}

	log.Warnf("OpenClaw warmup plan parse failed, ignored: raw=%q", raw)
	return lines
}

func objectItemsToStrings(items []openClawWarmupLine) []string {
	lines := make([]string, 0, len(items))
	for _, item := range items {
		lines = append(lines, item.Text)
	}
	return lines
}

func buildOpenClawWarmupPlanLines(items []string) []string {
	lines := make([]string, openClawWarmupPlanSize)
	for idx := 0; idx < openClawWarmupPlanSize && idx < len(items); idx++ {
		if text := sanitizeOpenClawWarmupText(items[idx]); text != "" {
			lines[idx] = text
		}
	}
	return lines
}

func countOpenClawWarmupLines(lines []string) int {
	count := 0
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	return count
}

func sanitizeOpenClawWarmupText(text string) string {
	text = strings.ReplaceAll(text, "\n", " ")
	text = strings.TrimSpace(text)
	text = strings.Trim(text, "\"'`[]{}")
	text = strings.TrimLeft(text, "0123456789.、- ")
	text = strings.Join(strings.Fields(text), " ")
	if text == "" {
		return ""
	}
	if isInvalidOpenClawWarmupText(text) {
		return ""
	}

	runes := []rune(text)
	if len(runes) > 16 {
		return ""
	}
	return text
}

func isInvalidOpenClawWarmupText(text string) bool {
	for _, bad := range []string{
		"帮我",
		"给我",
		"告诉我",
		"请帮",
		"麻烦帮",
		"能帮我",
		"可以帮我",
		"帮忙查",
		"帮忙看",
		"帮忙问",
	} {
		if strings.Contains(text, bad) {
			return true
		}
	}
	return false
}

func buildOpenClawWarmupHint(userText string) string {
	trimmed := strings.TrimSpace(userText)
	if trimmed == "" {
		return ""
	}

	normalized := removePunctuation(trimmed)
	if normalized == "" {
		return ""
	}
	normalized = trimOpenClawWarmupCommandPrefix(normalized)
	normalized = trimOpenClawWarmupQuestionSuffix(normalized)
	if normalized == "" {
		return ""
	}

	for _, keyword := range []string{"天气", "气温", "温度", "预报"} {
		if idx := strings.Index(normalized, keyword); idx >= 0 {
			limit := idx + len([]rune(keyword))
			runes := []rune(normalized)
			if limit > len(runes) {
				limit = len(runes)
			}
			normalized = string(runes[:limit])
			break
		}
	}

	runes := []rune(normalized)
	if len(runes) > 10 {
		runes = runes[:10]
	}
	for len(runes) > 0 {
		last := runes[len(runes)-1]
		if last == '的' || last == '了' || last == '呢' {
			runes = runes[:len(runes)-1]
			continue
		}
		break
	}
	return string(runes)
}

func trimOpenClawWarmupCommandPrefix(text string) string {
	trimmed := strings.TrimSpace(text)
	for {
		changed := false
		for _, prefix := range []string{
			"麻烦帮我查询一下",
			"麻烦帮我查一下",
			"麻烦帮我看一下",
			"请帮我查询一下",
			"请帮我查一下",
			"请帮我看一下",
			"帮我查询一下",
			"帮我查一下",
			"帮我看一下",
			"帮我问一下",
			"给我查询一下",
			"给我查一下",
			"给我看一下",
			"可以帮我查一下",
			"可以帮我看一下",
			"能帮我查一下",
			"能帮我看一下",
			"我想知道",
			"我想问一下",
			"我想问",
			"请问一下",
			"请问",
			"查询一下",
			"查一下",
			"看一下",
			"问一下",
			"帮我查询",
			"帮我查",
			"帮我看",
			"帮我问",
			"给我查询",
			"给我查",
			"给我看",
			"查询",
			"查",
			"看",
			"问",
		} {
			if strings.HasPrefix(trimmed, prefix) {
				trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, prefix))
				changed = true
				break
			}
		}
		if !changed {
			break
		}
	}
	return trimmed
}

func trimOpenClawWarmupQuestionSuffix(text string) string {
	trimmed := strings.TrimSpace(text)
	for _, suffix := range []string{
		"怎么样",
		"如何",
		"多少",
		"是什么",
		"是啥",
		"吗",
		"呢",
		"呀",
		"吧",
	} {
		trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, suffix))
	}
	return trimmed
}

func formatOpenClawWarmupTopic(hint string) string {
	hint = strings.TrimSpace(hint)
	if hint == "" {
		return ""
	}
	for _, keyword := range []string{"天气", "气温", "温度", "预报"} {
		if idx := strings.Index(hint, keyword); idx > 0 {
			prefix := strings.TrimSpace(hint[:idx])
			if prefix == "" || strings.HasSuffix(prefix, "的") {
				return hint
			}
			return prefix + "的" + hint[idx:]
		}
	}
	return hint
}
