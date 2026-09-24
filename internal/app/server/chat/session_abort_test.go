package chat

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	client "xiaozhi-esp32-server-golang/internal/data/client"
	msgdata "xiaozhi-esp32-server-golang/internal/data/msg"
	"xiaozhi-esp32-server-golang/internal/util"
)

func TestHandleAbortMessageRealtimeKeepsSessionContext(t *testing.T) {
	session, clientState := newAbortTestSession("realtime")

	sessionCtx := clientState.SessionCtx.Get(clientState.Ctx)
	afterAsrCtx := clientState.AfterAsrSessionCtx.Get(sessionCtx)

	require.NoError(t, session.HandleAbortMessage(&client.ClientMessage{DeviceID: "device-realtime"}))
	require.True(t, clientState.Abort)
	require.NoError(t, sessionCtx.Err())
	require.ErrorIs(t, afterAsrCtx.Err(), context.Canceled)
	require.Equal(t, client.ListenPhaseListening, clientState.GetListenPhase())
}

func TestHandleAbortMessageManualCancelsSessionContext(t *testing.T) {
	session, clientState := newAbortTestSession("manual")

	sessionCtx := clientState.SessionCtx.Get(clientState.Ctx)
	afterAsrCtx := clientState.AfterAsrSessionCtx.Get(sessionCtx)

	require.NoError(t, session.HandleAbortMessage(&client.ClientMessage{DeviceID: "device-manual"}))
	require.True(t, clientState.Abort)
	require.ErrorIs(t, sessionCtx.Err(), context.Canceled)
	require.ErrorIs(t, afterAsrCtx.Err(), context.Canceled)
	require.Equal(t, client.ListenPhaseIdle, clientState.GetListenPhase())
}

func TestHandleAbortMessageDuringTTSStopsActiveTTS(t *testing.T) {
	session, conn, cleanup := newStartedTTSControlTestSession(t)
	defer cleanup()

	session.ttsManager.EnqueueTtsStart(context.Background())
	startMsg := waitForServerMessage(t, conn, 0)
	require.Equal(t, msgdata.MessageStateStart, startMsg.State)

	require.NoError(t, session.HandleAbortMessage(&client.ClientMessage{DeviceID: session.clientState.DeviceID}))
	require.True(t, session.clientState.Abort)

	stopMsg := waitForServerMessage(t, conn, 1)
	require.Equal(t, msgdata.MessageStateStop, stopMsg.State)
}

func newAbortTestSession(listenMode string) (*ChatSession, *client.ClientState) {
	clientState := &client.ClientState{
		Ctx:         context.Background(),
		ListenMode:  listenMode,
		ListenPhase: client.ListenPhaseListening,
	}

	ttsManager := NewTTSManager(clientState, nil, nil)
	session := &ChatSession{
		clientState:   clientState,
		ttsManager:    ttsManager,
		llmManager:    NewLLMManager(clientState, nil, ttsManager, nil, nil),
		chatTextQueue: util.NewQueue[AsrResponseChannelItem](1),
	}

	return session, clientState
}
