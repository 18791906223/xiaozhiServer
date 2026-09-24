package chat

import (
	"context"
	"testing"
	"time"

	types_conn "xiaozhi-esp32-server-golang/internal/app/server/types"
	. "xiaozhi-esp32-server-golang/internal/data/client"
)

func TestScheduleMcpInitLocked_ReinitializesWhenRuntimeDriftsFromReadyState(t *testing.T) {
	fakeConn := &sessionCloseTestConn{
		deviceID:      "device-1",
		transportType: types_conn.TransportTypeMqttUdp,
	}
	clientState := &ClientState{DeviceID: "device-1"}
	serverTransport := NewServerTransport(fakeConn, clientState)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager := &ChatManager{
		DeviceID:        "device-1",
		clientState:     clientState,
		serverTransport: serverTransport,
		mcpTransport: &McpTransport{
			Client:          clientState,
			ServerTransport: serverTransport,
		},
		ctx:          ctx,
		cancel:       cancel,
		mcpInitState: chatMcpInitStateReady,
	}

	oldShouldSchedule := shouldScheduleDeviceMcpRuntimeInit
	oldEnsureRuntime := ensureDeviceMcpRuntime
	defer func() {
		shouldScheduleDeviceMcpRuntimeInit = oldShouldSchedule
		ensureDeviceMcpRuntime = oldEnsureRuntime
	}()

	shouldScheduleDeviceMcpRuntimeInit = func(deviceID string, mcpTransport *McpTransport) bool {
		return true
	}

	initCalled := make(chan struct{}, 1)
	ensureDeviceMcpRuntime = func(deviceID string, mcpTransport *McpTransport) error {
		if deviceID != "device-1" {
			t.Fatalf("unexpected device id: %s", deviceID)
		}
		initCalled <- struct{}{}
		return nil
	}

	manager.scheduleMcpInitLocked()

	select {
	case <-initCalled:
	case <-time.After(time.Second):
		t.Fatal("expected MCP runtime reinitialization to be triggered")
	}

	waitForMcpInitState(t, manager, chatMcpInitStateReady)
}

func TestScheduleMcpInitLocked_DoesNotReinitializeHealthyRuntime(t *testing.T) {
	fakeConn := &sessionCloseTestConn{
		deviceID:      "device-2",
		transportType: types_conn.TransportTypeMqttUdp,
	}
	clientState := &ClientState{DeviceID: "device-2"}
	serverTransport := NewServerTransport(fakeConn, clientState)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager := &ChatManager{
		DeviceID:        "device-2",
		clientState:     clientState,
		serverTransport: serverTransport,
		mcpTransport: &McpTransport{
			Client:          clientState,
			ServerTransport: serverTransport,
		},
		ctx:          ctx,
		cancel:       cancel,
		mcpInitState: chatMcpInitStateReady,
	}

	oldShouldSchedule := shouldScheduleDeviceMcpRuntimeInit
	oldEnsureRuntime := ensureDeviceMcpRuntime
	defer func() {
		shouldScheduleDeviceMcpRuntimeInit = oldShouldSchedule
		ensureDeviceMcpRuntime = oldEnsureRuntime
	}()

	shouldScheduleDeviceMcpRuntimeInit = func(deviceID string, mcpTransport *McpTransport) bool {
		return false
	}

	initCalled := make(chan struct{}, 1)
	ensureDeviceMcpRuntime = func(deviceID string, mcpTransport *McpTransport) error {
		initCalled <- struct{}{}
		return nil
	}

	manager.scheduleMcpInitLocked()

	select {
	case <-initCalled:
		t.Fatal("expected healthy MCP runtime to skip reinitialization")
	case <-time.After(100 * time.Millisecond):
	}

	if manager.mcpInitState != chatMcpInitStateReady {
		t.Fatalf("expected MCP init state to stay ready, got %v", manager.mcpInitState)
	}
}

func waitForMcpInitState(t *testing.T, manager *ChatManager, want chatMcpInitState) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if manager.mcpInitState == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("expected MCP init state %v, got %v", want, manager.mcpInitState)
}
