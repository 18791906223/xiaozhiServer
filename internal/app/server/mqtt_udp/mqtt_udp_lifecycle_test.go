package mqtt_udp

import (
	"encoding/json"
	"testing"
	"time"
)

func newTestUdpSession() *UdpSession {
	return &UdpSession{
		RecvChannel: make(chan []byte, 1),
		SendChannel: make(chan []byte, 1),
		Status:      UdpSessionStatusActive,
	}
}

func TestMqttUdpConnBrokerLifecycleAffectsIsActive(t *testing.T) {
	conn := NewMqttUdpConn("device-1", "/p2p/device_sub/device_1", nil, nil, newTestUdpSession())
	defer conn.Destroy()

	if !conn.IsActive() {
		t.Fatalf("expected new mqtt udp conn to be active")
	}

	conn.MarkBrokerOffline(20 * time.Millisecond)
	if !conn.IsActive() {
		t.Fatalf("expected conn to remain active during offline grace period")
	}

	time.Sleep(35 * time.Millisecond)
	if conn.IsActive() {
		t.Fatalf("expected conn to become inactive after offline grace period")
	}

	conn.MarkBrokerOnline()
	if !conn.IsActive() {
		t.Fatalf("expected conn to become active again after broker reconnect")
	}
}

func TestLifecycleReconnectCancelsOfflineCleanup(t *testing.T) {
	adapter := NewMqttUdpAdapter(&MqttConfig{})
	adapter.offlineGracePeriod = 20 * time.Millisecond
	defer adapter.Stop()

	if notify, accepted := adapter.markDeviceOnline("device-1", 100); !accepted || !notify {
		t.Fatalf("expected first online event to notify")
	}

	notifyOffline, version := adapter.markDeviceOffline("device-1", 200)
	if !notifyOffline {
		t.Fatalf("expected offline transition to notify")
	}
	if version == 0 {
		t.Fatalf("expected offline cleanup version to be assigned")
	}

	time.Sleep(5 * time.Millisecond)

	if notify, accepted := adapter.markDeviceOnline("device-1", 300); !accepted || !notify {
		t.Fatalf("expected reconnect to notify online again")
	}

	time.Sleep(30 * time.Millisecond)

	state := adapter.getLifecycleState("device-1")
	if state == nil {
		t.Fatalf("expected lifecycle state to remain after reconnect")
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.brokerOnline {
		t.Fatalf("expected device to stay online after reconnect")
	}
	if state.cleanupTimer != nil {
		t.Fatalf("expected offline cleanup timer to be cancelled after reconnect")
	}
}

func TestLifecycleOnlineAlwaysTriggersTransportReadyCallback(t *testing.T) {
	readyCalls := 0
	adapter := NewMqttUdpAdapter(&MqttConfig{}, WithOnTransportReady(func(deviceID string) {
		if deviceID != "device-1" {
			t.Fatalf("unexpected device id: %s", deviceID)
		}
		readyCalls++
	}))
	adapter.setUdpServer(NewUDPServer(0, "", 0))
	adapter.SetDeviceSession("device-1", NewMqttUdpConn("device-1", "topic/device-1", nil, nil, newTestUdpSession()))
	defer adapter.Stop()

	onlinePayload := func(ts int64) []byte {
		payload, err := json.Marshal(map[string]interface{}{
			"device_id": "device-1",
			"state":     "online",
			"ts":        ts,
		})
		if err != nil {
			t.Fatalf("marshal lifecycle payload failed: %v", err)
		}
		return payload
	}

	adapter.handleLifecycleMessage(onlinePayload(100))
	adapter.handleLifecycleMessage(onlinePayload(200))

	if readyCalls != 2 {
		t.Fatalf("expected online broadcast to trigger transport ready twice, got %d", readyCalls)
	}
}

func TestLifecycleStaleOnlineDoesNotTriggerTransportReadyCallback(t *testing.T) {
	readyCalls := 0
	adapter := NewMqttUdpAdapter(&MqttConfig{}, WithOnTransportReady(func(deviceID string) {
		if deviceID != "device-1" {
			t.Fatalf("unexpected device id: %s", deviceID)
		}
		readyCalls++
	}))
	adapter.setUdpServer(NewUDPServer(0, "", 0))
	adapter.SetDeviceSession("device-1", NewMqttUdpConn("device-1", "topic/device-1", nil, nil, newTestUdpSession()))
	defer adapter.Stop()

	onlinePayload := func(ts int64) []byte {
		payload, err := json.Marshal(map[string]interface{}{
			"device_id": "device-1",
			"state":     "online",
			"ts":        ts,
		})
		if err != nil {
			t.Fatalf("marshal lifecycle payload failed: %v", err)
		}
		return payload
	}

	adapter.handleLifecycleMessage(onlinePayload(200))
	adapter.handleLifecycleMessage(onlinePayload(100))

	if readyCalls != 1 {
		t.Fatalf("expected stale online broadcast to be ignored, got %d transport ready callbacks", readyCalls)
	}
}

func TestLifecycleStaleOfflineDoesNotMarkFreshConnOffline(t *testing.T) {
	adapter := NewMqttUdpAdapter(&MqttConfig{})
	defer adapter.Stop()

	session := newTestUdpSession()
	conn := NewMqttUdpConn("device-1", "topic/device-1", nil, nil, session)
	conn.MarkBrokerOnline()
	adapter.SetDeviceSession("device-1", conn)

	if notify, accepted := adapter.markDeviceOnline("device-1", 200); !accepted || !notify {
		t.Fatalf("expected fresh online event to be accepted, got notify=%v accepted=%v", notify, accepted)
	}

	payload, err := json.Marshal(map[string]interface{}{
		"device_id": "device-1",
		"state":     "offline",
		"ts":        int64(100),
	})
	if err != nil {
		t.Fatalf("marshal lifecycle payload failed: %v", err)
	}

	adapter.handleLifecycleMessage(payload)

	if !conn.IsBrokerOnline() {
		t.Fatal("expected stale offline event not to mark fresh conn offline")
	}

	state := adapter.getLifecycleState("device-1")
	if state == nil {
		t.Fatal("expected lifecycle state to remain")
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.brokerOnline {
		t.Fatal("expected lifecycle state to stay broker-online after stale offline")
	}
	if state.cleanupTimer != nil {
		t.Fatal("expected stale offline event not to schedule cleanup")
	}
}

func TestHandleDisconnectIgnoresStaleConn(t *testing.T) {
	adapter := NewMqttUdpAdapter(&MqttConfig{})
	defer adapter.Stop()

	udpServer := NewUDPServer(0, "", 0)
	adapter.setUdpServer(udpServer)

	staleSession := udpServer.CreateSession("device-1", "")
	if staleSession == nil {
		t.Fatal("expected stale session to be created")
	}
	freshSession := udpServer.CreateSession("device-1", "")
	if freshSession == nil {
		t.Fatal("expected fresh session to be created")
	}
	t.Cleanup(func() {
		udpServer.CloseSessionByRef(staleSession)
		udpServer.CloseSessionByRef(freshSession)
	})

	staleConn := NewMqttUdpConn("device-1", "topic/device-1", nil, udpServer, staleSession)
	freshConn := NewMqttUdpConn("device-1", "topic/device-1", nil, udpServer, freshSession)

	adapter.SetDeviceSession("device-1", freshConn)
	adapter.handleDisconnect("device-1", staleConn)

	if adapter.getDeviceSession("device-1") != freshConn {
		t.Fatal("expected fresh conn to stay registered after stale disconnect")
	}
	if freshConn.GetUdpSession() != freshSession {
		t.Fatal("expected fresh udp session to remain attached")
	}
	if staleConn.GetUdpSession() != staleSession {
		t.Fatal("expected stale conn to remain untouched by ignored disconnect")
	}
}
