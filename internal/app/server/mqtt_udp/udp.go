package mqtt_udp

import (
	"crypto/cipher"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	UdpSessionStatusActive = "active"
	UdpSessionStatusClosed = "closed"
)

// Session 表示一个UDP会话
type UdpSession struct {
	ID          string
	Conn        *net.UDPConn //udp conn
	ConnId      string
	ClientId    string
	DeviceId    string
	AesKey      [16]byte // 随机32位
	Nonce       [8]byte  // 存储原始nonce模板 16位
	CreatedAt   time.Time
	LastActive  time.Time
	RemoteAddr  *net.UDPAddr //remote addr
	LocalSeq    uint32
	Block       cipher.Block
	RemoteSeq   uint32
	RecvChannel chan []byte //发送的音频数据
	SendChannel chan []byte //接收的音频数据
	Status      string
	Lock        sync.Mutex
}

func (s *UdpSession) SetRemoteAddr(addr *net.UDPAddr) {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	if addr == nil {
		s.RemoteAddr = nil
		return
	}
	addrCopy := *addr
	s.RemoteAddr = &addrCopy
}

func (s *UdpSession) GetRemoteAddr() *net.UDPAddr {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	if s.RemoteAddr == nil {
		return nil
	}
	addrCopy := *s.RemoteAddr
	return &addrCopy
}

func (s *UdpSession) IsClosed() bool {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	return s.Status == UdpSessionStatusClosed
}

func (s *UdpSession) WaitRemoteAddr(timeout time.Duration) *net.UDPAddr {
	if timeout <= 0 {
		return s.GetRemoteAddr()
	}

	deadline := time.Now().Add(timeout)
	for {
		if addr := s.GetRemoteAddr(); addr != nil {
			return addr
		}
		if s.IsClosed() || !time.Now().Before(deadline) {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (s *UdpSession) DrainPendingAudio() int {
	drained := 0
	for {
		select {
		case <-s.SendChannel:
			drained++
		default:
			return drained
		}
	}
}

// decrypt 解密数据
func (s *UdpSession) Decrypt(data []byte) ([]byte, error) {
	// 分离nonce和密文
	nonce := data[:16] // 使用16字节nonce
	ciphertext := data[16:]

	// 提取序列号
	seqNum := binary.BigEndian.Uint32(data[12:16])

	// 检查序列号
	/*if seqNum < s.RemoteSeq {
		return nil, fmt.Errorf("序列号过期: got %d, expected >= %d", seqNum, s.RemoteSeq)
	}*/
	s.RemoteSeq = seqNum

	// 解密数据
	stream := cipher.NewCTR(s.Block, nonce)
	decrypted := make([]byte, len(ciphertext))
	stream.XORKeyStream(decrypted, ciphertext)

	return decrypted, nil
}

// encrypt 加密数据
func (s *UdpSession) Encrypt(data []byte) ([]byte, error) {
	// 预分配内存，避免扩容
	encrypted := make([]byte, 16+len(data))

	// 构建nonce (16字节)
	encrypted[0] = 0x01                                          // 包类型
	binary.BigEndian.PutUint16(encrypted[2:], uint16(len(data))) // 数据长度
	copy(encrypted[4:12], s.Nonce[:])                            // 8字节nonce
	s.LocalSeq++
	binary.BigEndian.PutUint32(encrypted[12:], s.LocalSeq) // 序列号

	// 加密数据
	stream := cipher.NewCTR(s.Block, encrypted[:16]) // 使用16字节作为IV
	stream.XORKeyStream(encrypted[16:], data)

	return encrypted, nil
}

func (s *UdpSession) GetAesKeyAndNonce() (string, string) {
	//处理
	strAesKey := hex.EncodeToString(s.AesKey[:])

	// 构造 fullNonce: 前缀2字节0100 + 长度2字节0000 + 真实nonce(8字节) + seq(4字节00000000)
	prefix := []byte{0x01, 0x00}
	length := []byte{0x00, 0x00}
	seq := []byte{0x00, 0x00, 0x00, 0x00}
	fullNonce := append(append(append(prefix, length...), s.Nonce[:]...), seq...)
	strFullNonce := hex.EncodeToString(fullNonce)

	return strAesKey, strFullNonce
}

func (s *UdpSession) RecvData(data []byte) (bool, error) {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	if s.Status == UdpSessionStatusClosed {
		return false, nil
	}
	select {
	case s.RecvChannel <- data:
		return true, nil
	default:
		return false, fmt.Errorf("recv channel is full")
	}
}

// SendAudioData 发送音频数据
func (s *UdpSession) SendAudioData(data []byte) (bool, error) {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	if s.Status == UdpSessionStatusClosed {
		return false, nil
	}
	select {
	case s.SendChannel <- data:
		return true, nil
	default:
		return false, fmt.Errorf("send channel is full")
	}
}

func (s *UdpSession) Destroy() {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	if s.Status == UdpSessionStatusClosed {
		return
	}
	s.Status = UdpSessionStatusClosed
	close(s.RecvChannel)
	close(s.SendChannel)
}
