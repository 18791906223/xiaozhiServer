package chat

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	types_conn "xiaozhi-esp32-server-golang/internal/app/server/types"
	types_audio "xiaozhi-esp32-server-golang/internal/data/audio"
	. "xiaozhi-esp32-server-golang/internal/data/client"
	. "xiaozhi-esp32-server-golang/internal/data/msg"
	"xiaozhi-esp32-server-golang/internal/domain/speaker"
)

func newGoodbyeRetentionTestManager(t *testing.T, retainedTTL time.Duration) (*ChatManager, *sessionCloseTestConn, *ChatSession) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	clientState := &ClientState{
		Ctx:         ctx,
		Cancel:      cancel,
		Dialogue:    &Dialogue{},
		DeviceID:    "device-1",
		SessionID:   "session-1",
		ListenPhase: ListenPhaseIdle,
		Status:      ClientStatusInit,
		OutputAudioFormat: types_audio.AudioFormat{
			SampleRate:    16000,
			Channels:      1,
			FrameDuration: 20,
			Format:        "opus",
		},
		OpusAudioBuffer: make(chan []byte, 8),
		AsrAudioBuffer: &AsrAudioBuffer{
			PcmData:          make([]float32, 0),
			AudioBufferMutex: sync.RWMutex{},
		},
		VoiceStatus: VoiceStatus{
			SilenceThresholdTime: 400,
		},
	}

	fakeConn := &sessionCloseTestConn{
		deviceID:      clientState.DeviceID,
		transportType: types_conn.TransportTypeMqttUdp,
	}
	serverTransport := NewServerTransport(fakeConn, clientState)
	manager := &ChatManager{
		DeviceID:                   clientState.DeviceID,
		transport:                  fakeConn,
		clientState:                clientState,
		serverTransport:            serverTransport,
		ctx:                        ctx,
		cancel:                     cancel,
		helloInited:                true,
		retainedSessionIdleTimeout: retainedTTL,
	}
	session := NewChatSession(clientState, serverTransport, nil, nil, WithChatSessionCloseHandler(manager.handleSessionClosed))
	manager.session = session
	return manager, fakeConn, session
}

func TestHandleGoodByeMessageRetainsSessionAndResetsSilentState(t *testing.T) {
	manager, fakeConn, session := newGoodbyeRetentionTestManager(t, time.Minute)

	manager.lastSpeakPathWarmAt.Store(time.Now().UnixMilli())
	pending := &pendingSpeakRequest{
		sessionID: manager.clientState.SessionID,
		done:      make(chan struct{}),
		timer:     time.NewTimer(time.Hour),
	}
	manager.pendingSpeakRequest = pending
	manager.clientState.Abort = true
	manager.clientState.IsWelcomeSpeaking = true
	manager.clientState.IsWelcomePlaying = true
	manager.clientState.SetStatus(ClientStatusTTSStart)
	manager.clientState.SetListenPhase(ListenPhaseListening)
	manager.clientState.SessionCtx.Get(manager.clientState.Ctx)
	manager.clientState.AfterAsrSessionCtx.Get(manager.clientState.Ctx)
	session.pendingSpeakerResult = &speaker.IdentifyResult{Identified: true, SpeakerName: "tester"}
	session.speakerResultReady <- struct{}{}

	if err := manager.HandleGoodByeMessage(&ClientMessage{
		Type:     MessageTypeGoodBye,
		DeviceID: manager.clientState.DeviceID,
	}); err != nil {
		t.Fatalf("HandleGoodByeMessage returned error: %v", err)
	}

	if manager.GetSession() != session {
		t.Fatal("expected goodbye to retain current ChatSession")
	}
	if manager.needFreshHello {
		t.Fatal("expected goodbye retention flow to avoid requiring fresh hello")
	}
	if fakeConn.closeAudioCalls != 1 {
		t.Fatalf("expected CloseAudioChannel to be called once, got %d", fakeConn.closeAudioCalls)
	}
	if manager.lastSpeakPathWarmAt.Load() != 0 {
		t.Fatal("expected goodbye to reset warm speak path timestamp")
	}
	if manager.clientState.GetStatus() != ClientStatusInit {
		t.Fatalf("expected client status to reset to init, got %s", manager.clientState.GetStatus())
	}
	if manager.clientState.GetListenPhase() != ListenPhaseIdle {
		t.Fatalf("expected listen phase to reset to idle, got %s", manager.clientState.GetListenPhase())
	}
	if manager.clientState.Abort {
		t.Fatal("expected goodbye to clear abort flag")
	}
	if manager.clientState.IsWelcomeSpeaking || manager.clientState.IsWelcomePlaying {
		t.Fatal("expected goodbye to clear welcome speaking flags")
	}
	if session.pendingSpeakerResult != nil {
		t.Fatal("expected goodbye to clear pending speaker result")
	}
	select {
	case <-session.speakerResultReady:
		t.Fatal("expected goodbye to drain speaker result ready signal")
	default:
	}
	select {
	case <-pending.done:
		if pending.Err() == nil || !strings.Contains(pending.Err().Error(), "goodbye") {
			t.Fatalf("expected pending speak request to be finished with goodbye error, got %v", pending.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("expected pending speak request to be resolved after goodbye")
	}
	if manager.retainedSessionCleanupTimer == nil {
		t.Fatal("expected goodbye to schedule retained-session cleanup timer")
	}
}

func TestShouldSendSpeakRequestAfterGoodbyeRetention(t *testing.T) {
	manager, _, session := newGoodbyeRetentionTestManager(t, time.Minute)
	manager.lastSpeakPathWarmAt.Store(time.Now().UnixMilli())

	if err := manager.HandleGoodByeMessage(&ClientMessage{
		Type:     MessageTypeGoodBye,
		DeviceID: manager.clientState.DeviceID,
	}); err != nil {
		t.Fatalf("HandleGoodByeMessage returned error: %v", err)
	}

	if manager.GetSession() != session {
		t.Fatal("expected goodbye to retain current ChatSession")
	}
	if !manager.shouldSendSpeakRequest(time.Now()) {
		t.Fatal("expected retained silent ChatSession to require speak_request")
	}
}

func TestHandleGoodByeMessageCleansRetainedSessionAfterIdleTimeout(t *testing.T) {
	manager, fakeConn, _ := newGoodbyeRetentionTestManager(t, 20*time.Millisecond)

	if err := manager.HandleGoodByeMessage(&ClientMessage{
		Type:     MessageTypeGoodBye,
		DeviceID: manager.clientState.DeviceID,
	}); err != nil {
		t.Fatalf("HandleGoodByeMessage returned error: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if manager.GetSession() == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if manager.GetSession() != nil {
		t.Fatal("expected retained ChatSession to be fully cleaned after idle timeout")
	}
	if fakeConn.closeAudioCalls != 1 {
		t.Fatalf("expected only the goodbye path to close audio once, got %d", fakeConn.closeAudioCalls)
	}
	if len(fakeConn.sentCmds) != 0 {
		t.Fatalf("expected retained-session idle cleanup to avoid sending protocol commands, got %d", len(fakeConn.sentCmds))
	}
}
