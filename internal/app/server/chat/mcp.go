// 文件：mcp.go
// 职责：IoT over MCP 的收发桥接。
// 把「会话/工具层」与「internal/domain/mcp 的 MCP 客户端」连接起来：
//   - 上行（发给设备）：McpTransport 实现 mcp 包要求的 ConnInterface，
//     让 MCP 客户端能借它把 JSON-RPC 请求发到设备端；
//   - 下行（设备回包）：设备的 mcp 消息经 ServerTransport.HandleMcpMessage 进入缓冲通道，
//     再由 McpTransport.RecvMcpMsg 交给 MCP 客户端。
//
// 核心类型：McpTransport。
//
// 在流程中的位置：连接建立（hello）后，若设备声明支持 mcp，
// 会异步初始化这条运行时（initMcp → ensureDeviceMcpRuntime），
// 之后 LLM 工具调用阶段就能通过 MCP 协议调用设备端能力（如 IoT 控制）。
//
// 与哪些文件配合：
//   - server_transport.go：McpTransport 把收发都转发给它。
//   - chat.go：握手后触发 MCP 运行时初始化的时机判断。
//   - internal/domain/mcp：真正的 MCP 客户端与连接池（EnsureDeviceIotOverMcp 等）。
package chat

import (
	"context"
	"encoding/json"

	. "xiaozhi-esp32-server-golang/internal/data/client"
	"xiaozhi-esp32-server-golang/internal/domain/mcp"
	log "xiaozhi-esp32-server-golang/logger"

	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/spf13/viper"
)

// 下面这三个以 var 声明的函数变量，是 mcp 包那几个函数的「间接引用」。
// 为什么不直接调用 mcp.EnsureDeviceIotOverMcp(...)：
// 通过函数变量可以在测试里替换成假实现（注入 mock、模拟失败、断言调用次数），
// 而不必真的去建 MCP 连接或改动生产代码；直接调用则无法替换，相关分支就测不到。
var (
	// ensureDeviceMcpRuntime 确保设备的 IoT over MCP 运行时已建立（幂等，可重复调用）。
	ensureDeviceMcpRuntime = func(deviceID string, mcpTransport *McpTransport) error {
		return mcp.EnsureDeviceIotOverMcp(deviceID, mcpTransport)
	}
	// closeDeviceMcpRuntime 关闭并清理设备的 MCP 运行时。
	closeDeviceMcpRuntime = func(deviceID string, mcpTransport *McpTransport) {
		mcp.CloseDeviceIotOverMcp(deviceID, mcpTransport)
	}
	// shouldScheduleDeviceMcpRuntimeInit 判断当前传输是否值得去初始化 MCP 运行时。
	// 放成函数变量同样是为了测试可替换：握手路径上只做一次「要不要初始化」的决策。
	shouldScheduleDeviceMcpRuntimeInit = func(deviceID string, mcpTransport *McpTransport) bool {
		return mcp.ShouldScheduleDeviceIotOverMcp(deviceID, mcpTransport)
	}
)

// McpTransport 是 mcp.ConnInterface 的实现，把 MCP 的收发转接到 ChatSession 的连接资源上。
// 它不自己持有连接，收发包都委托给 ServerTransport，因此只要连接在，MCP 就能用。
type McpTransport struct {
	// Client 设备状态（用于取设备 ID 等信息，也便于日志定位是哪个设备）。
	Client *ClientState
	// ServerTransport 协议发送封装，MCP 的收发最终都落到它上面。
	ServerTransport *ServerTransport
}

// SendMcpMsg 发送一条 MCP JSON-RPC 消息给设备。
// 关键特殊处理：如果这是 initialize 请求，则往协商能力（Capabilities）里注入 vision。
//
// 为什么要在 initialize 里注入 vision：
// MCP 的 initialize 是双方交换能力的时机，设备端据此知道服务端能提供什么。
// 在这里带上 vision.url/token，就相当于告诉设备「你可以把图片上传到这个地址做识别」，
// 设备无需额外配置即可启用拍照识图能力（视觉识别实现见 vllm.go）。
//
// 注入方式是「解出 Params → 补 capabilities → 再编回 payload」，
// 因为原始 payload 是字节流，无法直接改其中一个字段。
func (c *McpTransport) SendMcpMsg(payload []byte) error {
	//如果是initialize请求，则注入vision
	var request transport.JSONRPCRequest
	err := json.Unmarshal(payload, &request)
	if err == nil {
		if request.Method == "initialize" {
			if origInitParams, ok := request.Params.(map[string]interface{}); ok {
				b, err := json.Marshal(origInitParams)
				if err != nil {
					return err
				}

				var initParams mcp.InitializeParams
				err = json.Unmarshal(b, &initParams)
				if err != nil {
					return err
				}
				initParams.Capabilities["vision"] = mcp.Vision{
					Url:   viper.GetString("vision.vision_url"),
					Token: "1234567890",
				}
				request.Params = initParams
			}
			payload, _ = json.Marshal(request)
		}
	}

	return c.ServerTransport.SendMcpMsg(payload)
}

// RecvMcpMsg 阻塞等待一条设备回包（超时/取消语义由 ServerTransport 决定）。
func (c *McpTransport) RecvMcpMsg(ctx context.Context, timeOut int) ([]byte, error) {
	return c.ServerTransport.RecvMcpMsg(ctx, timeOut)
}

// HandleMcpMessage 把设备发来的 mcp 消息投递给缓冲通道（由 ServerTransport 完成）。
// 这里做 nil 保护：MCP 运行时可能在会话尚未完全装配时就被回调，
// 提前收到消息时静默忽略比 panic 好（消息本身也会因超时被上层放弃）。
func (c *McpTransport) HandleMcpMessage(payload []byte) error {
	if c == nil || c.ServerTransport == nil {
		return nil
	}
	return c.ServerTransport.HandleMcpMessage(payload)
}

// GetMcpTransportType 返回底层传输类型。
// mcp 包用它区分不同传输（websocket / mqtt_udp）下的 MCP 客户端实例，
// 因此未装配时返回空串表示「尚不可用」。
func (c *McpTransport) GetMcpTransportType() string {
	if c == nil || c.ServerTransport == nil {
		return ""
	}
	return c.ServerTransport.GetTransportType()
}

// initMcp 初始化设备的 IoT over MCP 运行时（握手后异步调用）。
// 只做「确保运行时存在」这一件事并记录失败原因：
// MCP 初始化失败不应阻塞或中断语音对话主链路，因此这里只打日志、向上返回错误供调用方决定。
func initMcp(deviceID string, mcpTransport *McpTransport) error {
	if err := ensureDeviceMcpRuntime(deviceID, mcpTransport); err != nil {
		log.Errorf("确保IotOverMcp客户端失败: %v", err)
		return err
	}
	return nil
}
