package websocket

import (
	"net/http"
	"strings"
	"xiaozhi-esp32-server-golang/internal/domain/mcp"
	"xiaozhi-esp32-server-golang/internal/util"
	log "xiaozhi-esp32-server-golang/logger"

	"github.com/golang-jwt/jwt/v4"
)

// MCPClaims JWT claims结构
type MCPClaims struct {
	UserID     uint   `json:"userId"`
	AgentID    string `json:"agentId"`
	EndpointID string `json:"endpointId"`
	Purpose    string `json:"purpose"`
	jwt.RegisteredClaims
}

// handleMCPWebSocket 处理MCP WebSocket连接
func (s *WebSocketServer) handleMCPWebSocket(w http.ResponseWriter, r *http.Request) {
	var agentId string

	// 首先尝试从URL参数中获取token
	token := r.URL.Query().Get("token")
	if token != "" {
		// 从token中解析设备ID
		claims, err := s.parseMCPToken(token)
		if err != nil {
			log.Warnf("解析token失败: %v", err)
			http.Error(w, "无效的token", http.StatusUnauthorized)
			return
		}
		log.Infof("解析token成功: %v", claims)

		agentId = claims.AgentID
	} else {
		log.Errorf("缺少token")
		return
	}

	log.Infof("收到MCP服务器的WebSocket连接请求，Agent ID: %s", agentId)

	// 升级WebSocket连接
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Errorf("升级WebSocket连接失败: %v", err)
		return
	}

	mcpClientSession := mcp.GetDeviceMcpClient(agentId)
	if mcpClientSession == nil {
		mcpClientSession = mcp.NewDeviceMCPSession(agentId)
		mcp.AddDeviceMcpClient(agentId, mcpClientSession)
	}

	// 创建MCP客户端
	mcpClient := mcp.NewWsEndPointMcpClient(mcpClientSession.Ctx, agentId, conn)
	if mcpClient == nil {
		log.Errorf("创建MCP客户端失败")
		conn.Close()
		return
	}
	mcpClientSession.AddWsEndPointMcp(mcpClient)

	// 当 mcp server断开时, 清理 ws endpoint mcp client
	go func() {
		<-mcpClient.Ctx.Done()
		log.Infof("server %s 的MCP连接已断开", mcpClient.GetServerName())
	}()

	log.Infof("server %s 的MCP连接已建立", mcpClient.GetServerName()) // todo
}

// parseMCPToken 解析MCP JWT token
func (s *WebSocketServer) parseMCPToken(tokenString string) (*MCPClaims, error) {
	// 移除 "Bearer " 前缀
	if len(tokenString) > 7 && tokenString[:7] == "Bearer " {
		tokenString = tokenString[7:]
	}

	// 使用与生成token相同的密钥
	jwtSecret := []byte(util.GetManagerEndpointAuthToken())

	token, err := jwt.ParseWithClaims(tokenString, &MCPClaims{}, func(token *jwt.Token) (interface{}, error) {
		return jwtSecret, nil
	})

	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(*MCPClaims); ok && token.Valid {
		return claims, nil
	}

	return nil, jwt.ErrInvalidKey
}

// handleMCPAPI 处理MCP REST API请求
func (s *WebSocketServer) handleMCPAPI(w http.ResponseWriter, r *http.Request) {
	// 从URL路径中提取deviceId
	// URL格式: /xiaozhi/api/mcp/tools/{deviceId}
	path := strings.TrimPrefix(r.URL.Path, "/xiaozhi/api/mcp/tools/")
	if path == "" || path == r.URL.Path {
		http.Error(w, "缺少设备ID参数", http.StatusBadRequest)
		return
	}

	deviceID := strings.TrimSuffix(path, "/")
	if deviceID == "" {
		http.Error(w, "设备ID不能为空", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case "GET":
		s.handleGetDeviceTools(w, r, deviceID)
	default:
		http.Error(w, "不支持的HTTP方法", http.StatusMethodNotAllowed)
	}
}
