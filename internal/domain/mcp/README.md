# MCP Host 实现

基于 [Eino 框架](https://github.com/cloudwego/eino) 实现的 MCP (Model Context Protocol) Host，支持全局和设备维度的工具管理。

## 功能特性

### 🌐 全局 MCP 工具管理
- 通过 SSE 连接到多个 MCP 服务器
- 自动工具发现和注册
- 连接状态监控和自动重连
- 工具调用代理

### 📱 设备维度 MCP 管理  
- 每个设备独立的 MCP 连接
- WebSocket 协议支持
- 设备特定工具注册
- 连接数限制和清理

### 🔧 Eino 框架集成
- 实现 `tool.InvokableTool` 接口
- 支持 Eino 原生工具调用
- 完整的类型安全
- 流式处理支持

## 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                    WebSocket Server                        │
│  /xiaozhi/mcp/{deviceId} - 设备MCP连接                      │
│  /xiaozhi/api/mcp/tools/{deviceId} - 工具列表API            │
└─────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
┌─────────────────────────┐  ┌─────────────────────────┐
│   GlobalMCPManager      │  │   DeviceMCPManager      │
│   • SSE 连接管理        │  │   • WebSocket 连接管理   │
│   • 全局工具注册        │  │   • 设备工具注册         │
│   • 自动重连           │  │   • 连接清理            │
└─────────────────────────┘  └─────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Eino Tool Interface                     │
│  tool.InvokableTool - 统一工具调用接口                      │
└─────────────────────────────────────────────────────────────┘
```

## 配置说明

### config.json 配置

```json
{
  "mcp": {
    "global": {
      "enabled": true,
      "servers": [
        {
          "name": "filesystem",
          "sse_url": "http://localhost:3001/sse",
          "enabled": true
        },
        {
          "name": "memory", 
          "sse_url": "http://localhost:3002/sse",
          "enabled": false
        }
      ],
      "reconnect_interval": 5,
      "max_reconnect_attempts": 10
    },
    "device": {
      "enabled": true,
      "websocket_path": "/xiaozhi/mcp/",
      "max_connections_per_device": 5
    }
  }
}
```

### 配置参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `mcp.global.enabled` | bool | 是否启用全局MCP管理器 |
| `mcp.global.servers` | array | MCP服务器列表 |
| `mcp.global.reconnect_interval` | int | 重连间隔（秒） |
| `mcp.global.max_reconnect_attempts` | int | 最大重连次数 |
| `mcp.device.enabled` | bool | 是否启用设备MCP管理器 |
| `mcp.device.websocket_path` | string | WebSocket路径前缀 |
| `mcp.device.max_connections_per_device` | int | 每设备最大连接数 |

## API 接口

### WebSocket 端点

#### 设备 MCP 连接
```
ws://localhost:8989/xiaozhi/mcp/{deviceId}
```

**连接流程：**
1. 客户端连接到 WebSocket 端点
2. 服务器发送初始化消息
3. 客户端响应工具列表
4. 建立双向通信

**消息格式：**
```json
{
  "jsonrpc": "2.0",
  "method": "tools/list",
  "id": 1,
  "params": {}
}
```

### REST API

#### 获取设备工具列表
```http
GET /xiaozhi/api/mcp/tools/{deviceId}
```

**响应示例：**
```json
{
  "deviceId": "device123",
  "tools": {
    "filesystem_read_file": {
      "name": "read_file",
      "description": "读取文件内容",
      "type": "global"
    },
    "device_sensor_data": {
      "name": "sensor_data", 
      "description": "获取传感器数据",
      "type": "device"
    }
  },
  "globalCount": 5,
  "deviceCount": 3,
  "totalCount": 8,
  "timestamp": 1704067200
}
```

## 使用示例

### 1. 启动服务器

```go
package main

import (
    "xiaozhi-esp32-server-golang/internal/app/server/websocket"
)

func main() {
    server := websocket.NewWebSocketServer(8989)
    server.Start()
}
```

### 2. 连接 MCP 服务器

MCP 服务器需要提供 SSE 端点，支持以下事件：

- `tools` - 工具列表更新
- `status` - 连接状态更新

### 3. 设备连接示例

```javascript
// 设备端 WebSocket 连接
const ws = new WebSocket('ws://localhost:8989/xiaozhi/mcp/device123');

ws.onopen = function() {
    console.log('MCP连接已建立');
};

ws.onmessage = function(event) {
    const message = JSON.parse(event.data);
    if (message.method === 'initialize') {
        // 响应初始化
        ws.send(JSON.stringify({
            jsonrpc: "2.0",
            id: message.id,
            result: {
                protocolVersion: "2024-11-05",
                serverInfo: {
                    name: "device-mcp-server",
                    version: "1.0.0"
                }
            }
        }));
    }
};
```

### 4. 工具调用示例

```go
// 获取全局工具
globalManager := mcp.GetGlobalMCPManager()
tools := globalManager.GetAllTools()

// 调用工具
for name, tool := range tools {
    result, err := tool.InvokableRun(
        context.Background(),
        `{"path": "/tmp/test.txt"}`,
    )
    if err != nil {
        log.Errorf("工具调用失败: %v", err)
        continue
    }
    log.Infof("工具 %s 结果: %s", name, result)
}
```

## 开发指南

### 实现自定义 MCP 工具

```go
type customTool struct {
    name        string
    description string
}

func (t *customTool) Info(ctx context.Context) (*schema.ToolInfo, error) {
    return &schema.ToolInfo{
        Name: t.name,
        Desc: t.description,
        ParamsOneOf: &schema.ParamsOneOf{
            // 参数定义
        },
    }, nil
}

func (t *customTool) InvokableRun(ctx context.Context, argumentsInJSON string, opts ...tool.Option) (string, error) {
    // 工具实现逻辑
    return "result", nil
}
```

### 扩展 MCP 协议

1. 在 `MCPMessage` 结构体中添加新字段
2. 在 `handleMessage` 方法中添加新的消息处理
3. 实现对应的处理函数

## 监控和调试

### 日志级别

- `INFO` - 连接建立、工具注册等关键事件
- `ERROR` - 连接失败、工具调用错误等
- `DEBUG` - 详细的协议交互信息

### 健康检查

```bash
# 检查全局工具
curl http://localhost:8989/xiaozhi/api/mcp/tools/health_check

# 检查特定设备工具  
curl http://localhost:8989/xiaozhi/api/mcp/tools/device123
```

## 故障排除

### 常见问题

1. **SSE 连接失败**
   - 检查 MCP 服务器是否运行
   - 验证 SSE URL 配置
   - 查看网络连接

2. **WebSocket 连接断开**
   - 检查心跳机制
   - 验证设备 ID 格式
   - 查看连接数限制

3. **工具调用失败**
   - 验证工具参数格式
   - 检查工具是否已注册
   - 查看错误日志

### 性能优化

- 调整重连间隔和次数
- 设置合适的连接数限制
- 启用连接池复用
- 定期清理过期连接

## 参考资料

- [Eino 框架文档](https://www.cloudwego.io/docs/eino/)
- [MCP 协议规范](https://github.com/mark3labs/mcp-go)
- [SSE 规范](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events)
- [WebSocket 协议](https://tools.ietf.org/html/rfc6455) 