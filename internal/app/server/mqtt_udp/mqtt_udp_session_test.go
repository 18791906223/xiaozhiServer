package mqtt_udp

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRotateDeviceUdpSessionReplacesSessionAndCleansOldIndexes(t *testing.T) {
	udpServer := NewUDPServer(0, "", 0)
	adapter := &MqttUdpAdapter{udpServer: udpServer}

	oldSession := udpServer.CreateSession("device-1", "")
	require.NotNil(t, oldSession)

	oldAddr := testUDPAddr(10001)
	oldSession.SetRemoteAddr(oldAddr)

	conn := NewMqttUdpConn("device-1", "topic/device-1", nil, udpServer, oldSession)
	t.Cleanup(func() {
		conn.ReleaseUdpSession()
	})

	_, oldNonce := oldSession.GetAesKeyAndNonce()
	adapter.bindUdpSessionData(conn, oldSession)

	newSession, err := adapter.rotateDeviceUdpSession(conn, "device-1")
	require.NoError(t, err)
	require.NotNil(t, newSession)

	assert.NotSame(t, oldSession, newSession)
	assert.Same(t, newSession, conn.GetUdpSession())
	assert.Same(t, newSession, udpServer.GetSessionByConnID(newSession.ConnId))
	assert.Nil(t, udpServer.GetSessionByConnID(oldSession.ConnId))
	assert.Nil(t, oldSession.GetRemoteAddr())
	assert.True(t, oldSession.IsClosed())
	assert.NotEqual(t, oldSession.ConnId, newSession.ConnId)

	newKey, newNonce := newSession.GetAesKeyAndNonce()
	assert.NotEqual(t, oldNonce, newNonce)

	boundKey, err := conn.GetData("aes_key")
	require.NoError(t, err)
	assert.Equal(t, newKey, boundKey)

	boundNonce, err := conn.GetData("full_nonce")
	require.NoError(t, err)
	assert.Equal(t, newNonce, boundNonce)
}

func TestProcessPacketUpdatesRemoteAddrByConnID(t *testing.T) {
	udpServer := NewUDPServer(0, "", 0)

	session := udpServer.CreateSession("device-2", "")
	require.NotNil(t, session)
	t.Cleanup(func() {
		udpServer.CloseSessionByRef(session)
	})

	oldAddr := testUDPAddr(10011)
	newAddr := testUDPAddr(10012)
	session.SetRemoteAddr(oldAddr)

	payload := []byte("hello-audio")
	packet, err := session.Encrypt(payload)
	require.NoError(t, err)

	udpServer.processPacket(newAddr, packet)

	remoteAddr := session.GetRemoteAddr()
	require.NotNil(t, remoteAddr)
	assert.Equal(t, newAddr.String(), remoteAddr.String())

	received := requireReceivePayload(t, session.RecvChannel)
	assert.Equal(t, payload, received)
}

func TestOldPacketCannotRebindAfterSessionRotation(t *testing.T) {
	udpServer := NewUDPServer(0, "", 0)
	adapter := &MqttUdpAdapter{udpServer: udpServer}

	oldSession := udpServer.CreateSession("device-3", "")
	require.NotNil(t, oldSession)

	oldAddr := testUDPAddr(10021)
	oldSession.SetRemoteAddr(oldAddr)

	oldPacket, err := oldSession.Encrypt([]byte("stale-packet"))
	require.NoError(t, err)

	conn := NewMqttUdpConn("device-3", "topic/device-3", nil, udpServer, oldSession)
	t.Cleanup(func() {
		conn.ReleaseUdpSession()
	})
	adapter.bindUdpSessionData(conn, oldSession)

	newSession, err := adapter.rotateDeviceUdpSession(conn, "device-3")
	require.NoError(t, err)
	require.NotNil(t, newSession)

	udpServer.processPacket(oldAddr, oldPacket)

	assert.Nil(t, udpServer.GetSessionByConnID(oldSession.ConnId))
	assert.Nil(t, oldSession.GetRemoteAddr())
	assert.Same(t, newSession, conn.GetUdpSession())
}

func TestCloseSessionByRefClearsRemoteAddr(t *testing.T) {
	udpServer := NewUDPServer(0, "", 0)

	session := udpServer.CreateSession("device-4", "")
	require.NotNil(t, session)
	session.SetRemoteAddr(testUDPAddr(10031))

	udpServer.CloseSessionByRef(session)

	assert.Nil(t, udpServer.GetSessionByConnID(session.ConnId))
	assert.Nil(t, session.GetRemoteAddr())
	assert.True(t, session.IsClosed())
}

func TestCreateSessionRetriesConnIDCollision(t *testing.T) {
	originalReader := udpRandReader
	udpRandReader = bytes.NewReader(buildCollisionRandomStream())
	t.Cleanup(func() {
		udpRandReader = originalReader
	})

	udpServer := NewUDPServer(0, "", 0)

	first := udpServer.CreateSession("device-a", "")
	require.NotNil(t, first)
	t.Cleanup(func() {
		udpServer.CloseSessionByRef(first)
	})

	second := udpServer.CreateSession("device-b", "")
	require.NotNil(t, second)
	t.Cleanup(func() {
		udpServer.CloseSessionByRef(second)
	})

	assert.NotEqual(t, first.ConnId, second.ConnId)
	assert.Same(t, first, udpServer.GetSessionByConnID(first.ConnId))
	assert.Same(t, second, udpServer.GetSessionByConnID(second.ConnId))
}

func TestMqttUdpConnSendAudioQueuesUDPDataAndIgnoresClosedSession(t *testing.T) {
	session := newTestUdpSession()
	conn := NewMqttUdpConn("device-a", "topic/device-a", nil, nil, session)
	defer conn.Destroy()

	payload := []byte("tts-frame")
	require.NoError(t, conn.SendAudio(payload))

	select {
	case got := <-session.SendChannel:
		assert.Equal(t, payload, got)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued udp audio")
	}

	session.Destroy()
	require.NoError(t, conn.SendAudio([]byte("after-close")))
}

func TestMqttUdpConnRecvAudioReturnsQueuedUDPData(t *testing.T) {
	session := newTestUdpSession()
	conn := NewMqttUdpConn("device-a", "topic/device-a", nil, nil, session)
	defer conn.Destroy()

	expected := []byte("asr-frame")
	session.RecvChannel <- expected

	got, err := conn.RecvAudio(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, expected, got)
}

func buildCollisionRandomStream() []byte {
	stream := make([]byte, 0, 60)
	stream = append(stream, []byte{0, 1, 2, 3, 4, 5, 6, 7}...)
	stream = append(stream, bytes.Repeat([]byte{0x11}, 16)...)
	stream = append(stream, []byte{0xaa, 0xbb, 0xcc, 0xdd}...)
	stream = append(stream, []byte{8, 9, 10, 11, 12, 13, 14, 15}...)
	stream = append(stream, bytes.Repeat([]byte{0x22}, 16)...)
	stream = append(stream, []byte{0xaa, 0xbb, 0xcc, 0xdd}...)
	stream = append(stream, []byte{0xde, 0xad, 0xbe, 0xef}...)
	return stream
}

func testUDPAddr(port int) *net.UDPAddr {
	return &net.UDPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: port,
	}
}

func requireReceivePayload(t *testing.T, ch <-chan []byte) []byte {
	t.Helper()

	select {
	case payload := <-ch:
		return payload
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for decrypted payload")
		return nil
	}
}
