// 文件说明：LLM 工具调用执行器（工具轮的发动机）。
//
// 职责：把 LLM 返回的一批 ToolCalls 并发执行完，把每个工具的返回值解析成
// 能回喂给模型的 schema.Message，并把工具产出的音频/资源交给媒体播放器播放。
//
// 核心类型：
//   - toolCallExecutor：一轮工具调用的执行器，负责提交、去重、等待、结果排序；
//   - toolCallExecutionResult：单个工具的结构化结果（含若干流程控制标志）；
//   - toolCallResponseSummary：整批工具的汇总结论，返回给 llm.go 决定下一步。
//
// 在流程中的位置（ARCHITECTURE.md 第 4.3 / 4.4 节）：
//
//	LLM 流式响应
//	  -> llm.go: handleLLMResponse 收集 ToolCalls
//	  -> executor.Submit(tools)        // 本文件，立即并发执行
//	  -> llm.go: handleToolCallResponse（本文件）
//	       -> executor.Wait()          // 等所有工具出结果
//	       -> 写历史 / 处理退出 / 等媒体播完
//	       -> 有成功且未被要求停止时再请求一次 LLM（nest+1 轮）
//
// 与其他文件的配合：
//   - llm.go：创建并传入 executor，消费本文件的汇总结果；
//   - media_player.go：媒体产物最终变成 MediaSourceDescriptor 由 SessionMediaPlayer 播放；
//   - mcp_response_types.go：本地工具返回的 JSON 由 ParseMCPResponse 解析；
//   - types.go：通过 ctx 里的 ChatSessionOperator 回调会话能力（退出、清历史、切角色…）。
package chat

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	data_client "xiaozhi-esp32-server-golang/internal/data/client"
	"xiaozhi-esp32-server-golang/internal/domain/eventbus"
	"xiaozhi-esp32-server-golang/internal/domain/mcp"
	log "xiaozhi-esp32-server-golang/logger"

	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	mcp_go "github.com/mark3labs/mcp-go/mcp"
)

// toolCallResponseSummary 一批工具执行完后的“汇总结论”。
//
// llm.go 不关心每个工具的细节，只关心两件事：这轮工具里有没有成功的、
// 有没有产出媒体（音频/资源）。前者决定要不要带着结果再问一次 LLM，
// 后者决定本轮收尾时要不要等媒体播完。
type toolCallResponseSummary struct {
	// invokeToolSuccess 至少有一个工具执行成功（成功才值得带着结果再问 LLM）
	invokeToolSuccess bool
	// hasMediaOutput 至少有一个工具产出了要播放的媒体（音频/资源链接）
	hasMediaOutput bool
}

// [4.2] 工具收口：等工具跑完、把结果落历史/塞进上下文，并处理退出事件。
//
// handleToolCallResponse 处理工具调用响应
//
// 这是“工具轮”的收口函数：等工具跑完 -> 把消息落历史/塞进上下文 -> 处理退出事件
// -> 视情况带着工具结果再请求一次 LLM。返回值交给 llm.go 汇总本轮状态。
func (l *LLMManager) handleToolCallResponse(ctx context.Context, respMsg *schema.Message, tools []schema.ToolCall, executor *toolCallExecutor) (toolCallResponseSummary, error) {
	// 没有工具调用直接返回，省掉后面的执行器创建与空消息写入。
	if len(tools) == 0 {
		return toolCallResponseSummary{}, nil
	}

	// 补全缺失 ID 的 tool_call。必须在这里做：下面要把 assistant 消息（带 tool_calls）
	// 和 tool 结果消息一起交给模型，两侧 ID 必须一一对应，否则模型侧会报 400。
	tools = normalizeToolCallIDs(tools)
	// respMsg 里存着同一批 ToolCalls（要落历史），同样补齐，保证两处 ID 完全一致。
	if respMsg != nil && len(respMsg.ToolCalls) > 0 {
		respMsg.ToolCalls = normalizeToolCallIDs(respMsg.ToolCalls)
	}

	log.Infof("处理 %d 个工具调用", len(tools))
	// executor 允许由外部传入。llm.go 会在流式收文本的同时提前创建并 Submit，
	// 把工具执行与文本接收重叠起来（省掉一整段串行时间）；未传入则在这里就地创建。
	if executor == nil {
		executor = newToolCallExecutor(l, ctx)
		executor.Submit(tools)
	}

	var messageList []*schema.Message

	// 只有当respMsg有内容（Content不为空或ToolCalls不为空）时才添加到messageList
	// 避免保存空的assistant消息导致后续LLM调用出现400错误
	if respMsg != nil && (respMsg.Content != "" || len(respMsg.ToolCalls) > 0) {
		messageList = append(messageList, respMsg)
	}

	// 四个标志在遍历结果时做“或”运算汇总
	invokeToolSuccess := false
	findExitTool := false
	shouldStopLLMProcessing := false
	hasMediaOutput := false

	// [4.2] 等工具执行完：Wait 会阻塞到所有工具 goroutine 结束，并按提交顺序返回结果。
	results := executor.Wait()
	for _, result := range results {
		// 任一工具成功，本轮就算“有成功”，后面才可能再问一次 LLM
		if result.invokeToolSuccess {
			invokeToolSuccess = true
		}
		// 出现退出类工具，需要向上层发事件关闭会话
		if result.findExitTool {
			findExitTool = true
		}
		// 任一工具要求“到此为止/别说话”，本轮就不再请求 LLM
		if result.shouldStopLLMProcessing {
			shouldStopLLMProcessing = true
		}
		// 任一工具产出了媒体，整轮就要等媒体播完（见下方 WaitMedia）
		if result.hasMediaOutput {
			hasMediaOutput = true
		}
		// 顺序即语义：results 已按提交顺序排好，这里依次 append，
		// 保证写进历史/上下文的 tool 消息顺序与 LLM 给出的 ToolCalls 顺序一致。
		messageList = append(messageList, result.message)
	}

	if len(messageList) > 0 {
		for _, msg := range messageList {
			// 过滤掉Content为空的assistant消息，避免保存到历史记录中
			// 空的assistant消息会导致后续LLM调用时出现400错误
			if msg != nil && msg.Role == schema.Assistant && msg.Content == "" && len(msg.ToolCalls) == 0 {
				log.Debugf("跳过保存空的assistant消息")
				continue
			}
			l.AddLlmMessage(ctx, msg)
		}
	}

	// MemoryModeNone（无记忆）模式下历史列表被刻意清空，模型下一轮就看不到
	// “用户问题 -> 工具调用 -> 工具结果”这条链。所以要把本轮消息塞进 context，
	// 由 GetMessages 临时拼进请求，让模型在无记忆模式下也能基于工具结果作答。
	if l.clientState.GetMemoryMode() == data_client.MemoryModeNone {
		ctx = appendToolRoundMessagesToContext(ctx, messageList)
	}

	// 等媒体播放结束再往下走。原因：媒体音频占用同一路音频输出通道，
	// 若不等它播完就继续请求 LLM，LLM 的语音会和媒体音频互相打断。
	executor.WaitMedia()

	if findExitTool {
		// 发布退出聊天事件
		// 用事件而不是直接关会话：让上层（会话/连接管理层）按自己的节奏收尾，
		// 工具层不需要知道会话销毁的细节。
		eventbus.Get().Publish(eventbus.TopicExitChat, &eventbus.ExitChatEvent{
			ClientState: l.clientState,
			Reason:      "工具调用退出",
			TriggerType: "tool_call",
			UserText:    "",
			Timestamp:   time.Now(),
		})

		return toolCallResponseSummary{
			invokeToolSuccess: invokeToolSuccess,
			hasMediaOutput:    hasMediaOutput,
		}, nil
	}

	// 如果工具调用成功且没有被标记为停止处理，则继续LLM调用
	// 这里 userMessage 传 nil 的原因：工具结果已被 AddLlmMessage / context 带入请求，
	// 本轮不再有新的“用户消息”，只是让模型基于工具结果接着说
	// （这就是 ARCHITECTURE.md 里说的 nest+1 嵌套轮）。
	// [4.3] 带结果再问 LLM：这就是「nest+1 嵌套轮」（userMessage 传 nil，结果已进上下文）
	if invokeToolSuccess && !shouldStopLLMProcessing {
		l.DoLLmRequest(ctx, nil, l.einoTools, true, nil)
	}

	return toolCallResponseSummary{
		invokeToolSuccess: invokeToolSuccess,
		hasMediaOutput:    hasMediaOutput,
	}, nil
}

// toolCallExecutionResult 单个工具调用的执行结果。
//
// 为什么不直接返回 schema.Message：执行器还要向上层传递若干“控制标志”，
// 它们决定整轮流程怎么走（是否再问 LLM、是否退出、是否等媒体），
// 这些信息与“给模型看的文本内容”是两回事，所以分开存放。
type toolCallExecutionResult struct {
	// order 提交顺序（由 Submit 分配的递增序号）。goroutine 完成顺序不确定，
	// 用它把结果重新排回 LLM 期望的顺序。
	order int
	// message 回喂给 LLM 的 tool 角色消息（Role=Tool，Content 是文本化后的结果）
	message *schema.Message
	// invokeToolSuccess 该工具是否调用成功（失败时 message.Content 里是错误描述）
	invokeToolSuccess bool
	// findExitTool 该工具是否要求退出会话（如 exit_conversation）
	findExitTool bool
	// shouldStopLLMProcessing 该工具是否要求结束本轮（终止性动作、静音、或产出了媒体）
	shouldStopLLMProcessing bool
	// hasMediaOutput 该工具是否产出了要播放的媒体（音频/资源链接）
	hasMediaOutput bool
}

// toolCallExecutor 一次“工具轮”的执行器：并发跑工具、去重、等待、按序取结果。
//
// 为什么并发执行：一轮里 LLM 可能一次给出多个互不依赖的工具调用（例如同时查天气
// 和查时间）。串行执行会把整轮响应延迟叠加成“各工具耗时之和”，而实际上后面
// 请求 LLM 必须等所有工具出结果——所以并发后总耗时约等于最慢的那个工具，明显更快。
type toolCallExecutor struct {
	// manager 回指 LLMManager：要读 clientState、发起 LLM 请求、解析工具结果
	manager *LLMManager
	// ctx 原始上下文（用于解析工具结果、媒体播放链路等）
	ctx context.Context
	// toolCtx 传给工具 InvokableRun 的上下文。
	// 与 ctx 的差异在于它确保携带 chat_session_operator，本地工具靠 ctx.Value
	// 拿回 ChatSessionOperator 才能回调会话能力（退出、清历史、切角色、控音乐）。
	toolCtx context.Context

	// mu 保护下面几个可变字段：结果是在多个工具 goroutine 里并发写入的。
	mu sync.Mutex
	// submittedCall 已提交过的 tool_call ID 集合，用于幂等去重。
	// 为什么需要：同一批 ToolCalls 可能被提交两次（llm.go 提前 Submit 一次、
	// 收口时又 Submit 一次），而部分工具有副作用（切角色、退出），重复执行会出错。
	submittedCall map[string]struct{}
	// submitSequence 下一个待分配的提交序号，配合 order 实现稳定排序
	submitSequence int
	// results 已完成工具的结果集合，Wait 时排序后返回副本
	results []toolCallExecutionResult

	// wg 工具执行本身的等待组（被 Wait 使用）
	wg sync.WaitGroup
	// mediaWg 媒体播放的等待组（被 WaitMedia 使用）。
	// 之所以与 wg 分开：工具可以“先返回结果、再继续播放媒体”，两者完成时机不同；
	// wg 保证结果收集完毕，mediaWg 保证音频真正播完。
	mediaWg sync.WaitGroup
}

// newToolCallExecutor 创建执行器。
// 关键动作：把 ctx 里的 chat_session_operator 复制到 toolCtx 上，
// 因为工具执行只使用 toolCtx，这一步保证本地工具一定能取到会话回调。
func newToolCallExecutor(manager *LLMManager, ctx context.Context) *toolCallExecutor {
	toolCtx := ctx
	if chatSessionOperator, ok := ctx.Value("chat_session_operator").(ChatSessionOperator); ok {
		toolCtx = context.WithValue(ctx, "chat_session_operator", chatSessionOperator)
	}

	return &toolCallExecutor{
		manager:       manager,
		ctx:           ctx,
		toolCtx:       toolCtx,
		submittedCall: make(map[string]struct{}),
	}
}

// [4.0] 提交工具：去重后为每个工具起一个 goroutine 并发执行（非阻塞，结果由 Wait 收集）。
//
// Submit 提交一批工具调用：去重后为每个工具起一个 goroutine 并发执行。
// 它是非阻塞的（立即返回），执行结果由 Wait 统一收集。
func (e *toolCallExecutor) Submit(toolCalls []schema.ToolCall) {
	for _, tc := range toolCalls {
		// 补全缺失的 ID（LLM/网关偶尔会返回没有 ID 的 tool_call）
		toolCall := ensureToolCallID(tc)
		callID := toolCall.ID

		// 检查去重 + 登记 + 分配 order + wg.Add(1) 全部在同一把锁内完成。
		// Add 必须在锁内、且在起 goroutine 之前：否则 Wait 可能在计数增加前
		// 就先返回，导致结果丢失。
		e.mu.Lock()
		if _, exists := e.submittedCall[callID]; exists {
			e.mu.Unlock()
			continue
		}
		e.submittedCall[callID] = struct{}{}
		order := e.submitSequence
		e.submitSequence++
		e.wg.Add(1)
		e.mu.Unlock()

		// toolCall 在循环体内用 := 声明，每次迭代都是新变量，闭包捕获是安全的；
		// 这里仍显式写成空参闭包，避免读者误以为有共享变量问题。
		go func() {
			defer e.wg.Done()
			result := e.executeToolCall(order, toolCall)

			e.mu.Lock()
			e.results = append(e.results, result)
			e.mu.Unlock()
		}()
	}
}

// ensureToolCallID 给缺失 ID 的 tool_call 补一个稳定、可复现的 ID。
//
// 为什么需要：部分模型/网关返回的 ToolCall 没有 ID，而下游处处要求
// “assistant.tool_calls 与 tool 结果消息的 ToolCallID 一一对应”（消息历史、
// 厂商侧接口校验都会因此报错）。
// 为什么用“工具名 + 参数的 MD5 前 8 字节”派生：
//   - 稳定性：同样的调用永远得到同样的 ID，于是重复提交能被 submittedCall 去重；
//   - 前缀可读：带工具名前缀，日志里一眼能看出是哪个工具的自动 ID；
//   - 碰撞概率极低：8 字节（64bit）对单轮对话场景足够。
func ensureToolCallID(toolCall schema.ToolCall) schema.ToolCall {
	if strings.TrimSpace(toolCall.ID) != "" {
		return toolCall
	}

	toolName := strings.TrimSpace(toolCall.Function.Name)
	if toolName == "" {
		toolName = "tool"
	}

	fingerprint := md5.Sum([]byte(toolName + "\n" + strings.TrimSpace(toolCall.Function.Arguments)))
	toolCall.ID = fmt.Sprintf("auto_call_%s_%s", toolName, hex.EncodeToString(fingerprint[:8]))
	return toolCall
}

// normalizeToolCallIDs 对一批工具调用逐个补全 ID，返回新切片。
// 不去改原切片的元素（Go 值语义下这里其实会复制），保证调用方持有的数据不被意外修改。
func normalizeToolCallIDs(toolCalls []schema.ToolCall) []schema.ToolCall {
	if len(toolCalls) == 0 {
		return toolCalls
	}

	normalized := make([]schema.ToolCall, len(toolCalls))
	for i, tc := range toolCalls {
		normalized[i] = ensureToolCallID(tc)
	}
	return normalized
}

// [4.2] 等工具完成：阻塞到所有工具执行完毕，并按提交顺序（order）排序后返回结果副本。
//
// Wait 阻塞到所有工具执行完毕，并按提交顺序（order）排序后返回结果副本。
//
// 必须排序的原因：goroutine 完成顺序不确定，而 tool 消息的顺序必须与
// assistant.tool_calls 一致，否则模型会读到错乱的多轮工具结果。
// 返回副本（而非内部切片）避免调用方修改影响执行器状态。
func (e *toolCallExecutor) Wait() []toolCallExecutionResult {
	e.wg.Wait()

	e.mu.Lock()
	defer e.mu.Unlock()

	results := append([]toolCallExecutionResult(nil), e.results...)
	sort.Slice(results, func(i, j int) bool {
		return results[i].order < results[j].order
	})
	return results
}

// WaitMedia 等待所有由工具触发的媒体播放结束。
// 与 Wait 分开的意义：Wait 只保证“拿到工具结果”，媒体可能还在播；
// 调用方在写好消息后再调它，实现“媒体播完才继续下一轮”的语义。
func (e *toolCallExecutor) WaitMedia() {
	e.mediaWg.Wait()
}

// [4.1] 执行工具：执行单个工具调用，并把返回值整理成结构化结果。
//
// executeToolCall 执行单个工具调用，并把返回值整理成结构化结果。
// 流程：定位工具 -> InvokableRun -> 解析返回内容 -> 媒体交给播放器 ->
// 结果文本写进 tool 角色消息。
func (e *toolCallExecutor) executeToolCall(order int, toolCall schema.ToolCall) toolCallExecutionResult {
	// 先构造“兜底”的结果消息：无论中途在哪一步失败，都要有一条 Role=Tool、
	// ToolCallID 对应的消息回给模型，否则 assistant.tool_calls 与 tool 结果数量
	// 不匹配会导致模型侧校验失败。
	resultMessage := &schema.Message{
		Role:       schema.Tool,
		ToolCallID: toolCall.ID,
	}

	state := e.manager.clientState
	toolName := toolCall.Function.Name
	// 传输类型会参与工具定位：同一设备在 websocket 与 mqtt_udp 下可能连到不同 MCP 服务实例
	transportType := ""
	if e.manager != nil && e.manager.serverTransport != nil {
		transportType = e.manager.serverTransport.GetTransportType()
	}
	// 按 设备ID + agentID + 传输类型 + 工具名 精确定位工具（本地工具优先，见 mcp 包）
	toolObj, ok := mcp.GetToolByNameWithTransport(state.DeviceID, state.AgentID, transportType, toolName, state.DeviceConfig.MCPServiceNames)
	if !ok || toolObj == nil {
		// 找不到工具也要回一条文本结果：让模型知道“这个工具不可用”，比静默失败友好
		log.Errorf("未找到工具: %s", toolName)
		resultMessage.Content = fmt.Sprintf("未找到工具: %s", toolName)
		return toolCallExecutionResult{order: order, message: resultMessage}
	}

	log.Infof("进行工具调用请求: %s, 参数: %+v", toolName, toolCall.Function.Arguments)
	// 计时用于区分“工具本身慢”还是“网络慢”
	startTs := time.Now().UnixMilli()
	// 传入 e.toolCtx（带 chat_session_operator），本地工具靠它回调会话能力
	fcResult, err := toolObj.InvokableRun(e.toolCtx, toolCall.Function.Arguments)
	if err != nil {
		// 工具报错同样要回一条文本结果，让模型能向用户解释
		log.Errorf("工具调用失败: %v", err)
		resultMessage.Content = fmt.Sprintf("工具 %s 调用失败: %v", toolName, err)
		return toolCallExecutionResult{order: order, message: resultMessage}
	}
	costTs := time.Now().UnixMilli() - startTs
	// 结果很长时只打长度：音频/文档类结果会把日志刷爆
	if len(fcResult) > 2048 {
		log.Infof("工具调用结果 len: %d, 耗时: %dms", len(fcResult), costTs)
	} else {
		log.Infof("工具调用结果 %s, 耗时: %dms", fcResult, costTs)
	}

	execResult := toolCallExecutionResult{
		order:   order,
		message: resultMessage,
		// 能走到这里说明工具没报错，先记为成功（后续“播放失败”只会改写 message 的文本内容）
		invokeToolSuccess: true,
	}
	normalizedContent := fcResult

	// 解析路径二选一，顺序不能颠倒：
	//  1) handleLocalToolResult：本地 MCP 工具（mcp_response_types.go 那套统一协议）；
	//  2) handleToolResult：远程 MCP 的标准 CallToolResult。
	// 先试本地协议的理由：本地响应里的 type/控制标志是流程语义来源，若先按远程结果
	// 解，会把它当成一段普通文本，丢掉“终止/静音/退出”等关键控制信息。
	var contentList []mcp_go.Content
	if mcpResp, ok := e.manager.handleLocalToolResult(fcResult); ok {
		// 本地工具要求退出会话：标记后由 handleToolCallResponse 发 ExitChat 事件
		if mcpResp.GetType() == MCPResponseTypeAction && mcpResp.GetAction() == "exit_conversation" {
			execResult.findExitTool = true
		}
		// 动作类响应的三个开关：任一为真都表示“本轮到此为止，不要再请求 LLM”。
		// （FinalAction=动作闭环、NoFurtherResponse=禁止任何后续回复、SilenceLLM=只操作不说话）
		if actionResp, ok := mcpResp.(*MCPActionResponse); ok {
			if actionResp.FinalAction || actionResp.NoFurtherResponse || actionResp.SilenceLLM {
				execResult.shouldStopLLMProcessing = true
			}
		}
		contentList = mcpResp.GetContent()
	} else if toolCallResult, ok := e.manager.handleToolResult(fcResult); ok {
		// 远程 MCP 标准结果：IsError 只影响日志，内容照样回给模型
		if toolCallResult.IsError {
			log.Errorf("工具调用失败: %s, 错误标记: %t", fcResult, toolCallResult.IsError)
		}
		contentList = toolCallResult.Content
	}

	if len(contentList) > 0 {
		var mcpContent string
		// 遍历内容块：音频/资源链接要“播放”，文本要“拼成给模型的描述”。
		// 两类可播放内容都用 break 提前结束循环——一个工具结果里只要出现可播放内容，
		// 剩下的文本块就没有意义了（而且本轮已被标记为终止）。
		for _, content := range contentList {
			if audioContent, ok := content.(mcp_go.AudioContent); ok {
				log.Debugf("调用工具 %s 返回音频资源长度: %d", toolName, len(audioContent.Data))
				mcpContent = "执行成功"
				// handleAudioContent 内部会做 mediaWg.Add(1)，所以 WaitMedia 能等到它播完
				if err := e.manager.handleAudioContent(e.ctx, toolName, audioContent, &e.mediaWg); err != nil {
					log.Errorf("mcp播放音频资源失败: %v", err)
					mcpContent = "执行失败"
				}
				// 播了音频就不能再让 LLM 说话，否则两路声音会重叠
				execResult.shouldStopLLMProcessing = true
				execResult.hasMediaOutput = true
				break
			}
			if resourceLink, ok := content.(mcp_go.ResourceLink); ok {
				log.Debugf("调用工具 %s 返回资源链接: %+v", toolName, resourceLink)
				mcpContent = "执行成功"
				if err := e.manager.handleResourceLink(e.ctx, resourceLink, toolObj, &e.mediaWg); err != nil {
					log.Errorf("mcp播放资源链接失败: %v", err)
					mcpContent = "执行失败"
				}
				execResult.shouldStopLLMProcessing = true
				execResult.hasMediaOutput = true
				break
			}
			if textContent, ok := content.(mcp_go.TextContent); ok {
				log.Debugf("调用工具 %s 返回文本资源长度: %s", toolName, textContent.Text)
				mcpContent += textContent.Text
			}
		}
		if mcpContent != "" {
			// 用“执行成功/失败（+文本）”替换原始返回值：
			// 原始 JSON 通常很长且含 base64，塞给模型既浪费 token 又没有信息量。
			normalizedContent = mcpContent
		}
	}

	// 真正给模型看的工具结果文本
	execResult.message.Content = normalizedContent
	return execResult
}

// handleResourceLink 把远程 MCP 工具返回的“资源链接”转成媒体描述并交给播放器播放。
//
// 与 handleAudioContent 的差别：资源链接只是“指针”（URI/URL），真正的音频可能在
// 播放时才按 URI 去读（见 media_player.go 的 openMCPResourceAudioStream）；
// 而音频内容本身已是内联的字节。
// 参数 wg 由 executeToolCall 传入，把“播放结束”纳入本轮的等待范围（WaitMedia）。
func (l *LLMManager) handleResourceLink(ctx context.Context, resourceLink mcp_go.ResourceLink, toolCall tool.InvokableTool, wg *sync.WaitGroup) error {
	// 先 Add(1)：必须在起 goroutine 之前计数，否则 WaitMedia 可能提前返回。
	// 注意后面每个提前返回的分支都必须 Done()，否则 WaitMedia 会永久阻塞。
	wg.Add(1)

	source, err := buildMediaSourceFromResourceLink(resourceLink, toolCall)
	if err != nil {
		wg.Done()
		return err
	}

	if l.session == nil || l.session.mediaPlayer == nil {
		wg.Done()
		return fmt.Errorf("session media player 未初始化")
	}

	handle, err := l.session.mediaPlayer.PlaySourceWithHandle(ctx, source)
	if err != nil {
		wg.Done()
		return err
	}

	// PlaySourceWithHandle 只是“开始播放”并返回句柄；再开一个 goroutine 等它真正
	// 播完（handle.Wait）后才 Done，这样 WaitMedia 的语义才是“等媒体播完”。
	go func() {
		defer wg.Done()
		_ = handle.Wait(ctx)
	}()

	return nil
}

// handleAudioContent 把远程 MCP 工具返回的内联音频内容转成媒体描述并播放。
// 参数 realMusicName 实际传的是工具名，会被用作显示标题（见 buildMediaSourceFromAudioContent
// 里的标题兜底逻辑）。其余语义与 handleResourceLink 一致，包括 wg 的 Add/Done 约定。
func (l *LLMManager) handleAudioContent(ctx context.Context, realMusicName string, audioContent mcp_go.AudioContent, wg *sync.WaitGroup) error {
	wg.Add(1)

	source, err := buildMediaSourceFromAudioContent(realMusicName, audioContent)
	if err != nil {
		wg.Done()
		log.Errorf("解码音频数据失败: %v", err)
		return err
	}

	if l.session == nil || l.session.mediaPlayer == nil {
		wg.Done()
		return fmt.Errorf("session media player 未初始化")
	}

	handle, err := l.session.mediaPlayer.PlaySourceWithHandle(ctx, source)
	if err != nil {
		wg.Done()
		return err
	}

	// 同上：等播放真正结束才 Done，保证 WaitMedia 等的是“播完”而不是“已开始”
	go func() {
		defer wg.Done()
		_ = handle.Wait(ctx)
	}()

	return nil
}

// buildMediaSourceFromAudioContent 把 MCP 的 AudioContent 转成播放器能理解的
// MediaSourceDescriptor（内联音频源）。
// 为什么需要转换：MCP 的 AudioContent 是“协议层”格式（base64 文本），
// 而 SessionMediaPlayer 只认识自己的 MediaSourceDescriptor（带原始 []byte），
// 这一层就是协议到播放器的适配点。
func buildMediaSourceFromAudioContent(title string, audioContent mcp_go.AudioContent) (MediaSourceDescriptor, error) {
	// 播放器对 MediaSourceTypeInlineAudio 的约定是传原始字节，所以这里解码 base64
	rawAudioData, err := base64.StdEncoding.DecodeString(audioContent.Data)
	if err != nil {
		return MediaSourceDescriptor{}, fmt.Errorf("解码音频数据失败: %v", err)
	}

	// 标题兜底：调用方传进来的往往是工具名或“执行成功”这类占位文本，
	// 直接当歌名显示很怪，统一换成“工具音频”。
	title = strings.TrimSpace(title)
	if title == "" || title == "执行成功" {
		title = "工具音频"
	}

	return MediaSourceDescriptor{
		Title:      title,
		MIMEType:   audioContent.MIMEType,
		SourceType: MediaSourceTypeInlineAudio,
		Meta: map[string]string{
			"mime_type": audioContent.MIMEType,
			"source":    string(MediaSourceTypeInlineAudio),
		},
		Inline: &InlineAudioSource{
			Data: rawAudioData,
		},
	}, nil
}

// buildMediaSourceFromResourceLink 把资源链接转成 MediaSourceDescriptor（MCP 资源源）。
//
// 为什么要把 server 名、端点快照、分页大小、MCP client 一起放进描述里：
// 真正的“读取资源”动作发生在播放器内部（播放时才去 ReadResource），
// 那里需要这些上下文才能发出合法的 MCP 请求。
//
// 参数 toolCall 是产出该资源的工具实例，用于取出它所属的 MCP server 与 client。
func buildMediaSourceFromResourceLink(resourceLink mcp_go.ResourceLink, toolCall tool.InvokableTool) (MediaSourceDescriptor, error) {
	// 只有远程 MCP 工具持有 client；本地工具没有可读取的资源服务，直接报错
	mcpTool, ok := toolCall.(*mcp.McpTool)
	if !ok || mcpTool == nil {
		return MediaSourceDescriptor{}, fmt.Errorf("resource link 播放仅支持 MCP 远程工具")
	}

	serverName := mcpTool.GetServerName()
	// 记录端点快照：配置刷新/重连后端点可能变化，快照便于排查“当时连的是哪个服务”
	endpointSnapshot := mcp.GetServerEndpointSnapshotByName(serverName)
	// 有些 MCP 工具会把可直连的音频 URL 放在 Description 里；
	// 能直连就免去一次资源读取（更快），所以把它复制进 ReadArgs。
	directAudioURL := strings.TrimSpace(resourceLink.Description)
	readArgs := make(map[string]any)
	if directAudioURL != "" {
		readArgs["url"] = directAudioURL
	}

	// 标题逐级兜底：Name -> Description -> URI，尽量给用户一个可读的名字
	title := strings.TrimSpace(resourceLink.Name)
	if title == "" {
		title = strings.TrimSpace(resourceLink.Description)
	}
	if title == "" {
		title = strings.TrimSpace(resourceLink.URI)
	}

	// 工具名仅用于元信息/日志，取不到不影响播放
	toolName := ""
	if info, err := mcpTool.Info(context.Background()); err == nil && info != nil {
		toolName = info.Name
	}

	return MediaSourceDescriptor{
		Title:      title,
		MIMEType:   resourceLink.MIMEType,
		SourceType: MediaSourceTypeMCPResource,
		Meta: map[string]string{
			"source":       string(MediaSourceTypeMCPResource),
			"server_name":  serverName,
			"endpoint":     endpointSnapshot,
			"resource_uri": resourceLink.URI,
			"audio_url":    directAudioURL,
			"tool_name":    toolName,
		},
		MCP: &MCPMediaSource{
			ServerName:       serverName,
			EndpointSnapshot: endpointSnapshot,
			ToolName:         toolName,
			ResourceURI:      resourceLink.URI,
			DirectAudioURL:   directAudioURL,
			Description:      resourceLink.Description,
			ReadArgs:         readArgs,
			PageSize:         McpReadResourcePageSize,
			Client:           mcpTool.GetClient(),
		},
	}, nil
}

// handleLocalToolResult 尝试按“本地工具统一协议”解析工具返回值。
//
// 返回“响应 + 是否解析成功”：false 表示这串内容不符合本地协议，
// 调用方（executeToolCall）会回退到远程 MCP 的解析路径。
// 之所以用“试解析 + 布尔”而不是先做类型判断：工具返回值统一是字符串，
// 只能靠“能否解析成 MCPResponse”来区分本地协议与远程标准结果。
func (l *LLMManager) handleLocalToolResult(toolResult string) (MCPResponse, bool) {
	// 如果是mcp返回格式, 则解析
	var response MCPResponse
	var err error
	if response, err = ParseMCPResponse(toolResult); err != nil {
		return nil, false
	}
	return response, true
}

// handleToolResult 按远程 MCP 标准结果（CallToolResult）解析工具返回值。
//
// 关键设计：即使 JSON 解析失败也返回 true，并把原始字符串包成一个文本内容。
// 原因——不少远程工具返回的是普通文本/HTML 而不是标准 CallToolResult；
// 若这里返回 false，上层会把内容整体丢弃，用户会得到“工具没结果”的错觉。
// 包成 TextContent 后，至少模型还能看到并转述这些内容。
func (l *LLMManager) handleToolResult(toolResultStr string) (mcp_go.CallToolResult, bool) {
	var toolResult mcp_go.CallToolResult
	if err := json.Unmarshal([]byte(toolResultStr), &toolResult); err != nil {
		log.Debugf("Failed to unmarshal tool result: %v", err)
		toolResult.Content = []mcp_go.Content{
			mcp_go.TextContent{
				Type: "text",
				Text: toolResultStr,
			},
		}
		return toolResult, true
	}

	return toolResult, true
}
