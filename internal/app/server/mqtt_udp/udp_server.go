package mqtt_udp

import (
	"crypto/aes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	. "xiaozhi-esp32-server-golang/logger"
)

// UDPServer UDP服务器结构
/*
type UDPServer struct {
	conn       *net.UDPConn
	sessions   map[string]*Session
	mqttServer *MqttServer
	udpPort    int
	sync.RWMutex
}*/

type UdpServer struct {
	conn           *net.UDPConn
	udpPort        int      //udp server listen port
	externalHost   string   //udp server external host
	externalPort   int      //udp server external port
	connId2Session sync.Map //connId => UdpSession
	mqttAdapter    *MqttUdpAdapter
	sync.RWMutex
}

const maxConnIDGenerateAttempts = 16

var udpRandReader io.Reader = rand.Reader

// NewUDPServer 创建新的UDP服务器
func NewUDPServer(udpPort int, externalHost string, externalPort int) *UdpServer {
	return &UdpServer{
		udpPort:        udpPort,
		externalHost:   externalHost,
		externalPort:   externalPort,
		connId2Session: sync.Map{},
	}
}

// Start 启动UDP服务器
func (s *UdpServer) Start() error {
	addr := &net.UDPAddr{
		IP:   net.ParseIP("0.0.0.0"),
		Port: s.udpPort,
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("监听UDP失败: %v", err)
	}

	s.conn = conn
	Infof("UDP服务器启动在 %s:%d", "0.0.0.0", s.udpPort)

	// 启动会话清理
	//go s.cleanupSessions()

	// 启动数据包处理
	go s.handlePackets()

	return nil
}

// Close 关闭 UDP 服务器，使 handlePackets 退出
func (s *UdpServer) Close() error {
	s.Lock()
	conn := s.conn
	s.conn = nil
	s.Unlock()
	if conn == nil {
		return nil
	}
	return conn.Close()
}

// handlePackets 处理接收到的数据包
func (s *UdpServer) handlePackets() {
	buffer := make([]byte, 4096) // 使用默认的缓冲区大小
	for {
		s.RLock()
		conn := s.conn
		s.RUnlock()
		if conn == nil {
			return
		}
		n, addr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			s.RLock()
			closed := s.conn == nil
			s.RUnlock()
			if closed {
				return
			}
			Errorf("读取UDP数据失败: %v", err)
			continue
		}

		// 复制数据，避免并发修改
		data := make([]byte, n)
		copy(data, buffer[:n])

		// 处理数据包
		s.processPacket(addr, data)
	}
}

func (s *UdpServer) getSessionByConnID(connID string) *UdpSession {
	val, ok := s.connId2Session.Load(connID)
	if ok {
		return val.(*UdpSession)
	}
	return nil
}

// processPacket 处理单个数据包
func (s *UdpServer) processPacket(addr *net.UDPAddr, data []byte) {
	// 检查数据包大小
	if len(data) < 16 {
		Warn("数据包太小")
		return
	}

	fullNonce := data[:16]
	connID := fullNonce[4:8] // 取5-8字节作为连接id
	strConnID := hex.EncodeToString(connID)
	udpSession := s.getSessionByConnID(strConnID)
	if udpSession == nil {
		//Warnf("session不存在 addr: %s, connID: %s", addr, strConnID)
		return
	}

	// 更新最后活动时间
	udpSession.LastActive = time.Now()

	decrypted, err := udpSession.Decrypt(data)
	if err != nil {
		Errorf("addr: %s 解密失败: %v", addr, err)
		return
	}
	currentAddr := udpSession.GetRemoteAddr()
	if currentAddr == nil || currentAddr.String() != addr.String() {
		udpSession.SetRemoteAddr(addr)
	}
	Debugf("收到音频数据, addr: %s, 大小: %d 字节", addr, len(decrypted))
	ok, err := udpSession.RecvData(decrypted)
	if err != nil {
		Errorf("addr: %s 接收数据失败: %v", addr, err)
		return
	}
	if !ok {
		Warnf("addr: %s 接收数据失败, 通道已满", addr)
		return
	}
	/*select {
	case udpSession.RecvChannel <- decrypted:
		return
	default:
		Warnf("udpSession.RecvChannel is full, addr: %s", addr)
	}*/
}

// cleanupSessions 清理过期会话
func (s *UdpServer) cleanupSessions() {
	ticker := time.NewTicker(time.Minute)
	for range ticker.C {
		now := time.Now()
		s.connId2Session.Range(func(key, value interface{}) bool {
			session := value.(*UdpSession)
			if now.Sub(session.LastActive) > 5*time.Minute {
				s.connId2Session.Delete(key)
				Infof("清理过期会话: %s", key)
			}
			return true
		})
	}
}

// CreateSession 创建新会话
func (s *UdpServer) CreateSession(deviceId, clientId string) *UdpSession {
	// 生成会话ID
	sessionID, err := generateSessionID()
	if err != nil {
		Errorf("生成会话ID失败: %v", err)
		return nil
	}

	// 生成AES密钥
	key := make([]byte, 16)
	if err := fillRandomBytes(key); err != nil {
		Errorf("生成AES密钥失败: %v", err)
		return nil
	}

	// 创建AES块
	block, err := aes.NewCipher(key)
	if err != nil {
		Errorf("创建AES块失败: %v", err)
		return nil
	}

	// 将key转换为[16]byte
	aesKey := [16]byte{}
	copy(aesKey[:], key)

	for attempt := 0; attempt < maxConnIDGenerateAttempts; attempt++ {
		// 生成4字节连接id
		connID := make([]byte, 4)
		if err := fillRandomBytes(connID); err != nil {
			Errorf("生成连接ID失败: %v", err)
			return nil
		}
		strConnID := hex.EncodeToString(connID)

		// 4字节时间戳
		timestamp := make([]byte, 4)
		binary.BigEndian.PutUint32(timestamp, uint32(time.Now().Unix()))

		// 拼接nonce: 4字节连接id + 4字节时间戳
		nonce := append(connID, timestamp...)

		// 将nonce转换为[8]byte
		nonceBytes := [8]byte{}
		copy(nonceBytes[:], nonce)

		// 创建会话
		session := &UdpSession{
			ID:          sessionID,
			ConnId:      strConnID,
			ClientId:    clientId,
			DeviceId:    deviceId,
			AesKey:      aesKey,
			Nonce:       nonceBytes, // 保存原始nonce模板
			CreatedAt:   time.Now(),
			LastActive:  time.Now(),
			Block:       block,
			RecvChannel: make(chan []byte, 100),
			SendChannel: make(chan []byte, 100),
			Status:      UdpSessionStatusActive,
			Lock:        sync.Mutex{},
		}

		if _, loaded := s.connId2Session.LoadOrStore(strConnID, session); loaded {
			Warnf("UDP connID冲突，重试生成: device=%s, connID=%s, attempt=%d", deviceId, strConnID, attempt+1)
			continue
		}

		s.startSessionSender(session)
		return session
	}

	Errorf("生成唯一UDP connID失败: device=%s", deviceId)
	return nil
}

func (s *UdpServer) startSessionSender(session *UdpSession) {
	go func() {
		for data := range session.SendChannel {
			remoteAddr := session.WaitRemoteAddr(2 * time.Second)
			if remoteAddr == nil {
				dropped := 1 + session.DrainPendingAudio()
				Warnf("UDP远端地址未建立，TTS音频被丢弃: device=%s, connId=%s, dropped=%d", session.DeviceId, session.ConnId, dropped)
				continue
			}
			encrypted, err := session.Encrypt(data)
			if err != nil {
				Errorf("加密失败: %v", err)
				continue
			}
			_, err = s.writeToUDP(encrypted, remoteAddr)
			if err != nil {
				Errorf("发送音频数据失败: %v", err)
				continue
			}
		}
	}()
}

func (s *UdpServer) writeToUDP(data []byte, remoteAddr *net.UDPAddr) (int, error) {
	s.RLock()
	conn := s.conn
	s.RUnlock()
	if conn == nil {
		return 0, fmt.Errorf("udp server is closed")
	}
	return conn.WriteToUDP(data, remoteAddr)
}

// CloseSession 关闭会话
func (s *UdpServer) CloseSession(connID string) {
	session := s.getSessionByConnID(connID)
	s.CloseSessionByRef(session)
}

// ClearSessionAddrBinding 清理 connID 对应会话的 UDP 地址绑定，不销毁会话本身
func (s *UdpServer) ClearSessionAddrBinding(connID string) {
	session := s.getSessionByConnID(connID)
	if session == nil {
		return
	}
	session.SetRemoteAddr(nil)
}

func (s *UdpServer) SetConnId2Session(connID string, session *UdpSession) {
	Debugf("SetConnId2Session, connID: %s, session: %+v", connID, session)
	s.connId2Session.Store(connID, session)
}

// GetSessionByConnID 获取会话信息
func (s *UdpServer) GetSessionByConnID(connID string) *UdpSession {
	val, ok := s.connId2Session.Load(connID)
	if ok {
		return val.(*UdpSession)
	}
	return nil
}

// generateSessionID 生成会话ID
func generateSessionID() (string, error) {
	b := make([]byte, 8)
	if err := fillRandomBytes(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func fillRandomBytes(buffer []byte) error {
	_, err := io.ReadFull(udpRandReader, buffer)
	return err
}

func (s *UdpServer) CloseSessionByRef(session *UdpSession) {
	if session == nil {
		return
	}
	s.connId2Session.Delete(session.ConnId)
	session.SetRemoteAddr(nil)
	session.Destroy()
}
