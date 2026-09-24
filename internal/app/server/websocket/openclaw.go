package websocket

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	"xiaozhi-esp32-server-golang/internal/domain/openclaw"
	"xiaozhi-esp32-server-golang/internal/util"
	log "xiaozhi-esp32-server-golang/logger"

	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	gws "github.com/gorilla/websocket"
)

type OpenClawClaims struct {
	UserID     uint   `json:"user_id"`
	AgentID    string `json:"agent_id"`
	EndpointID string `json:"endpoint_id"`
	Purpose    string `json:"purpose"`
	jwt.RegisteredClaims
}

func openClawSnippet(text string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return ""
	}
	runes := []rune(trimmed)
	if len(runes) <= maxRunes {
		return string(runes)
	}
	return string(runes[:maxRunes]) + "..."
}

func openClawReadMetadataString(metadata map[string]interface{}, key string) string {
	if metadata == nil {
		return ""
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case json.Number:
		return strings.TrimSpace(v.String())
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", value))
	}
}

func openClawReadMetadataBool(metadata map[string]interface{}, key string) bool {
	if metadata == nil {
		return false
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return false
	}
	switch v := value.(type) {
	case bool:
		return v
	case string:
		s := strings.TrimSpace(strings.ToLower(v))
		return s == "true" || s == "1" || s == "yes" || s == "on"
	case json.Number:
		n, err := v.Int64()
		return err == nil && n != 0
	case float64:
		return v != 0
	case float32:
		return v != 0
	case int:
		return v != 0
	case int32:
		return v != 0
	case int64:
		return v != 0
	default:
		return false
	}
}

func openClawReadMetadataInt64(metadata map[string]interface{}, key string) int64 {
	if metadata == nil {
		return 0
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return 0
	}
	switch v := value.(type) {
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		if v > uint64((1<<63)-1) {
			return 0
		}
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return 0
		}
		return n
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return 0
		}
		return n
	default:
		return 0
	}
}

func (s *WebSocketServer) handleOpenClawWebSocket(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimSpace(r.URL.Query().Get("token"))
	if token == "" {
		http.Error(w, "missing token", http.StatusUnauthorized)
		return
	}

	claims, err := s.parseOpenClawToken(token)
	if err != nil {
		log.Warnf("OpenClaw token parse failed: %v", err)
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}

	agentID := strings.TrimSpace(claims.AgentID)
	if agentID == "" {
		http.Error(w, "invalid token: missing agent_id", http.StatusUnauthorized)
		return
	}
	if claims.Purpose != "" && claims.Purpose != "openclaw-endpoint" {
		http.Error(w, "invalid token purpose", http.StatusUnauthorized)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Errorf("OpenClaw websocket upgrade failed: %v", err)
		return
	}

	manager := openclaw.GetManager()
	session := manager.RegisterAgentConnection(agentID, conn)
	if session == nil {
		_ = conn.Close()
		log.Errorf("failed to init openclaw session, agent=%s", agentID)
		return
	}
	defer manager.UnregisterAgentConnection(agentID, session)

	if err := sendOpenClawHandshakeAck(session); err != nil {
		log.Errorf("Send OpenClaw handshake_ack failed, agent=%s err=%v", agentID, err)
		return
	}
	log.Infof("OpenClaw connected, agent=%s endpoint=%s", agentID, claims.EndpointID)

	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			log.Infof("OpenClaw websocket closed, agent=%s err=%v", agentID, err)
			return
		}
		if msgType != gws.TextMessage {
			continue
		}

		var wsMsg openclaw.WSMessage
		if err := json.Unmarshal(data, &wsMsg); err != nil {
			log.Warnf("OpenClaw message decode failed, agent=%s err=%v", agentID, err)
			continue
		}
		log.Debugf(
			"OpenClaw inbound message: agent=%s type=%s id=%s corr=%s payload_bytes=%d",
			agentID,
			wsMsg.Type,
			wsMsg.ID,
			wsMsg.CorrelationID,
			len(wsMsg.Payload),
		)

		switch wsMsg.Type {
		case "handshake":
			log.Infof("OpenClaw handshake received, agent=%s", agentID)
		case "ping":
			if err := replyOpenClawPong(session, wsMsg); err != nil {
				log.Warnf("Reply OpenClaw pong failed, agent=%s err=%v", agentID, err)
			}
		case "response":
			handleOpenClawResponse(agentID, session, wsMsg, s.onOpenClawResponse)
		case "error":
			log.Warnf("OpenClaw returned error, agent=%s payload=%s", agentID, string(wsMsg.Payload))
		case "close":
			log.Infof("OpenClaw requested close, agent=%s", agentID)
			return
		default:
			log.Warnf("OpenClaw unknown message type, agent=%s type=%s", agentID, wsMsg.Type)
		}
	}
}

func handleOpenClawResponse(agentID string, session *openclaw.AgentSession, wsMsg openclaw.WSMessage, deliver func(event openclaw.ResponseDelivery) bool) {
	var payload openclaw.ResponsePayload
	if err := json.Unmarshal(wsMsg.Payload, &payload); err != nil {
		log.Warnf("OpenClaw response payload decode failed, agent=%s err=%v", agentID, err)
		return
	}
	metadataDeviceID := ""
	if payload.Metadata != nil {
		if rawDeviceID, ok := payload.Metadata["device_id"].(string); ok {
			metadataDeviceID = strings.TrimSpace(rawDeviceID)
		}
	}
	content := strings.TrimSpace(payload.Content)
	streamDone := openClawReadMetadataBool(payload.Metadata, "done")
	streamSeq := openClawReadMetadataInt64(payload.Metadata, "seq")
	streamID := openClawReadMetadataString(payload.Metadata, "stream_id")
	streamPhase := openClawReadMetadataString(payload.Metadata, "phase")
	isStreaming := streamDone || streamSeq > 0 || streamID != "" || streamPhase != ""

	if isStreaming {
		log.Infof(
			"OpenClaw stream response received: agent=%s message_id=%s correlation_id=%s session=%s metadata_device=%s done=%v seq=%d stream_id=%s phase=%s content_len=%d content_snippet=%q",
			agentID,
			wsMsg.ID,
			wsMsg.CorrelationID,
			strings.TrimSpace(payload.SessionID),
			metadataDeviceID,
			streamDone,
			streamSeq,
			streamID,
			streamPhase,
			len(content),
			openClawSnippet(content, 64),
		)
	} else {
		log.Infof(
			"OpenClaw response received: agent=%s message_id=%s correlation_id=%s session=%s metadata_device=%s content_len=%d content_snippet=%q",
			agentID,
			wsMsg.ID,
			wsMsg.CorrelationID,
			strings.TrimSpace(payload.SessionID),
			metadataDeviceID,
			len(content),
			openClawSnippet(content, 64),
		)
	}
	openclaw.GetManager().HandleResponse(agentID, session, wsMsg.CorrelationID, payload, deliver)
}

func sendOpenClawHandshakeAck(session *openclaw.AgentSession) error {
	payloadBytes, err := json.Marshal(map[string]interface{}{
		"version": "1.0.0",
		"server":  "xiaozhi-esp32-server",
	})
	if err != nil {
		return err
	}

	return session.Send(openclaw.WSMessage{
		ID:        uuid.NewString(),
		Timestamp: time.Now().UnixMilli(),
		Type:      "handshake_ack",
		Payload:   payloadBytes,
	})
}

func replyOpenClawPong(session *openclaw.AgentSession, ping openclaw.WSMessage) error {
	type PingPayload struct {
		Seq int `json:"seq"`
	}

	payload := PingPayload{}
	if len(ping.Payload) > 0 {
		_ = json.Unmarshal(ping.Payload, &payload)
	}

	pongPayload, err := json.Marshal(map[string]interface{}{
		"seq": payload.Seq,
	})
	if err != nil {
		return err
	}

	return session.Send(openclaw.WSMessage{
		ID:            uuid.NewString(),
		Timestamp:     time.Now().UnixMilli(),
		Type:          "pong",
		CorrelationID: ping.ID,
		Payload:       pongPayload,
	})
}

func (s *WebSocketServer) parseOpenClawToken(tokenString string) (*OpenClawClaims, error) {
	if strings.HasPrefix(tokenString, "Bearer ") {
		tokenString = strings.TrimSpace(strings.TrimPrefix(tokenString, "Bearer "))
	}

	jwtSecret := []byte(util.GetManagerEndpointAuthToken())
	token, err := jwt.ParseWithClaims(tokenString, &OpenClawClaims{}, func(token *jwt.Token) (interface{}, error) {
		return jwtSecret, nil
	})
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(*OpenClawClaims)
	if !ok || !token.Valid {
		return nil, jwt.ErrInvalidKey
	}
	return claims, nil
}
