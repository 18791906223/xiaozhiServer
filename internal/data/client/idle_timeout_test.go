package client

import (
	"testing"
	"time"
)

func TestShouldCountAudioIdleTimeoutRealtimeOutputStates(t *testing.T) {
	state := &ClientState{ListenMode: "realtime"}

	if !state.ShouldCountAudioIdleTimeout() {
		t.Fatal("expected realtime idle timeout to count before assistant output starts")
	}

	state.SetStatus(ClientStatusLLMStart)
	if state.ShouldCountAudioIdleTimeout() {
		t.Fatal("expected realtime idle timeout to pause during LLM output")
	}

	state.SetStatus(ClientStatusTTSStart)
	if state.ShouldCountAudioIdleTimeout() {
		t.Fatal("expected realtime idle timeout to pause during TTS output")
	}

	state.SetStatus(ClientStatusListenStop)
	state.SetTtsStart(true)
	if state.ShouldCountAudioIdleTimeout() {
		t.Fatal("expected realtime idle timeout to pause while TTS start flag is active")
	}

	state.SetTtsStart(false)
	if !state.ShouldCountAudioIdleTimeout() {
		t.Fatal("expected realtime idle timeout to resume after TTS stop")
	}
}

func TestShouldCountAudioIdleTimeoutNonRealtimeKeepsExistingBehavior(t *testing.T) {
	state := &ClientState{
		ListenMode: "auto",
		Status:     ClientStatusTTSStart,
	}
	state.SetTtsStart(true)

	if !state.ShouldCountAudioIdleTimeout() {
		t.Fatal("expected non-realtime idle timeout behavior to stay unchanged")
	}
}

func TestOnManualStopClearsAudioIdleTimeoutPending(t *testing.T) {
	state := &ClientState{}
	if !state.MarkAudioIdleTimeoutPending() {
		t.Fatal("expected timeout pending mark to succeed")
	}

	state.OnManualStop()

	if state.AudioIdleTimeoutPending() {
		t.Fatal("expected manual stop to clear timeout pending state")
	}
}

func TestStartAudioIdleWindowClearsVoiceStop(t *testing.T) {
	state := &ClientState{ListenMode: "auto"}
	state.SetClientVoiceStop(true)

	state.StartAudioIdleWindow(time.Now())

	if state.GetClientVoiceStop() {
		t.Fatal("expected starting idle window to clear voice stop flag")
	}
}

func TestResumeAudioIdleWindowClearsVoiceStop(t *testing.T) {
	state := &ClientState{ListenMode: "realtime"}
	startAt := time.Now().Add(-5 * time.Second)

	state.StartAudioIdleWindow(startAt)
	state.PauseAudioIdleWindow(startAt.Add(time.Second))
	state.SetClientVoiceStop(true)

	state.ResumeAudioIdleWindow(time.Now())

	if state.GetClientVoiceStop() {
		t.Fatal("expected resuming idle window to clear voice stop flag")
	}
}
