package chat

import (
	"context"
	"testing"
	"time"

	"xiaozhi-esp32-server-golang/internal/domain/play_music"
)

func TestDeviceMediaRuntimePlayAgentPlaylistKeepsCurrentIndexWhenAlreadyBound(t *testing.T) {
	coordinator := newMediaPlaybackCoordinator()
	runtime := coordinator.getOrCreateRuntime("device-test")

	appendTestPlaylistItem(t, coordinator, "agent-1", "song-a")
	appendTestPlaylistItem(t, coordinator, "agent-1", "song-b")

	runtime.mu.Lock()
	runtime.mode = mediaPlaybackModeAgentPlaylist
	runtime.playlistAgentID = "agent-1"
	runtime.playbackIndex = 1
	runtime.mu.Unlock()

	if err := runtime.PlayAgentPlaylist(context.Background(), "agent-1", mediaPlaybackAudioConfig{}); err != nil {
		t.Fatalf("PlayAgentPlaylist returned error: %v", err)
	}

	state := runtime.GetState()
	if state.Status != play_music.StatusPlaying {
		t.Fatalf("expected playing status, got %s", state.Status)
	}
	if state.CurrentIndex != 1 {
		t.Fatalf("expected current index 1, got %d", state.CurrentIndex)
	}
	if state.CurrentTitle != "song-b" {
		t.Fatalf("expected current title song-b, got %q", state.CurrentTitle)
	}

	stopRuntimeAndWait(t, runtime)
}

func TestDeviceMediaRuntimePlayAgentPlaylistStartsAtZeroWhenNotBound(t *testing.T) {
	coordinator := newMediaPlaybackCoordinator()
	runtime := coordinator.getOrCreateRuntime("device-test")

	appendTestPlaylistItem(t, coordinator, "agent-1", "song-a")
	appendTestPlaylistItem(t, coordinator, "agent-1", "song-b")

	runtime.mu.Lock()
	runtime.mode = mediaPlaybackModeStandalone
	runtime.playlistAgentID = ""
	runtime.playbackIndex = -1
	runtime.mu.Unlock()

	if err := runtime.PlayAgentPlaylist(context.Background(), "agent-1", mediaPlaybackAudioConfig{}); err != nil {
		t.Fatalf("PlayAgentPlaylist returned error: %v", err)
	}

	state := runtime.GetState()
	if state.Status != play_music.StatusPlaying {
		t.Fatalf("expected playing status, got %s", state.Status)
	}
	if state.CurrentIndex != 0 {
		t.Fatalf("expected current index 0, got %d", state.CurrentIndex)
	}
	if state.CurrentTitle != "song-a" {
		t.Fatalf("expected current title song-a, got %q", state.CurrentTitle)
	}

	stopRuntimeAndWait(t, runtime)
}

func TestNormalizeMusicPlaybackActionSupportsPlayPlaylist(t *testing.T) {
	cases := map[string]string{
		"play_playlist":       "play_playlist",
		"play_agent_playlist": "play_playlist",
		"play_playlist_songs": "play_playlist",
		"playlist":            "play_playlist",
	}

	for input, want := range cases {
		if got := normalizeMusicPlaybackAction(input); got != want {
			t.Fatalf("normalizeMusicPlaybackAction(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestDeviceMediaRuntimeJumpToRelativeWrapsAroundPlaylist(t *testing.T) {
	coordinator := newMediaPlaybackCoordinator()
	runtime := coordinator.getOrCreateRuntime("device-test")

	appendTestPlaylistItem(t, coordinator, "agent-1", "song-a")
	appendTestPlaylistItem(t, coordinator, "agent-1", "song-b")
	appendTestPlaylistItem(t, coordinator, "agent-1", "song-c")

	runtime.mu.Lock()
	runtime.mode = mediaPlaybackModeAgentPlaylist
	runtime.playlistAgentID = "agent-1"
	runtime.playbackIndex = 2
	runtime.mu.Unlock()

	if err := runtime.jumpToRelative(context.Background(), 1, mediaPlaybackAudioConfig{}); err != nil {
		t.Fatalf("jumpToRelative next returned error: %v", err)
	}

	state := runtime.GetState()
	if state.CurrentIndex != 0 {
		t.Fatalf("expected wrapped next index 0, got %d", state.CurrentIndex)
	}
	if state.CurrentTitle != "song-a" {
		t.Fatalf("expected wrapped next title song-a, got %q", state.CurrentTitle)
	}

	stopRuntimeAndWait(t, runtime)

	runtime.mu.Lock()
	runtime.mode = mediaPlaybackModeAgentPlaylist
	runtime.playlistAgentID = "agent-1"
	runtime.playbackIndex = 0
	runtime.mu.Unlock()

	if err := runtime.jumpToRelative(context.Background(), -1, mediaPlaybackAudioConfig{}); err != nil {
		t.Fatalf("jumpToRelative prev returned error: %v", err)
	}

	state = runtime.GetState()
	if state.CurrentIndex != 2 {
		t.Fatalf("expected wrapped prev index 2, got %d", state.CurrentIndex)
	}
	if state.CurrentTitle != "song-c" {
		t.Fatalf("expected wrapped prev title song-c, got %q", state.CurrentTitle)
	}

	stopRuntimeAndWait(t, runtime)
}

func TestAppendCurrentToAgentPlaylistKeepsCurrentPlaybackIndexWhenAlreadyInPlaylist(t *testing.T) {
	coordinator := newMediaPlaybackCoordinator()
	runtime := coordinator.getOrCreateRuntime("device-test")

	appendTestPlaylistItem(t, coordinator, "agent-1", "song-a")
	appendTestPlaylistItem(t, coordinator, "agent-1", "song-b")

	runtime.mu.Lock()
	runtime.mode = mediaPlaybackModeAgentPlaylist
	runtime.playlistAgentID = "agent-1"
	runtime.playbackIndex = 0
	currentSource := MediaSourceDescriptor{
		ID:         "song-a",
		Title:      "song-a",
		SourceType: MediaSourceTypeHTTPURL,
		HTTP: &HTTPMediaSource{
			URL: "https://example.com/song-a.mp3",
		},
	}
	runtime.currentSource = &currentSource
	runtime.state.Status = play_music.StatusPlaying
	runtime.state.CurrentIndex = 0
	runtime.state.CurrentTitle = "song-a"
	runtime.state.CurrentSourceType = MediaSourceTypeHTTPURL
	runtime.state.Playlist = coordinator.snapshotAgentPlaylist("agent-1")
	runtime.mu.Unlock()

	result, err := runtime.AppendCurrentToAgentPlaylist("agent-1")
	if err != nil {
		t.Fatalf("AppendCurrentToAgentPlaylist returned error: %v", err)
	}

	state := runtime.GetState()
	if state.CurrentIndex != 0 {
		t.Fatalf("expected current index to stay at 0, got %d", state.CurrentIndex)
	}
	if state.CurrentTitle != "song-a" {
		t.Fatalf("expected current title song-a, got %q", state.CurrentTitle)
	}
	if len(state.Playlist) != 3 {
		t.Fatalf("expected playlist length 3, got %d", len(state.Playlist))
	}
	if result.CurrentIndex != 0 {
		t.Fatalf("expected result current index 0, got %d", result.CurrentIndex)
	}
	if result.AddedTitle != "song-a" {
		t.Fatalf("expected added title song-a, got %q", result.AddedTitle)
	}
}

func TestDeviceMediaRuntimeResumeIfInterruptedPauseOnlyResumesInterruptPause(t *testing.T) {
	coordinator := newMediaPlaybackCoordinator()
	runtime := coordinator.getOrCreateRuntime("device-test")

	active := newActiveMediaPlayback(context.Background())
	active.setPaused(true)
	go func() {
		<-active.ctx.Done()
		active.closeDone()
	}()

	runtime.mu.Lock()
	runtime.active = active
	runtime.attachment = &mediaSessionAttachment{}
	runtime.state.Status = play_music.StatusPaused
	runtime.pauseReason = mediaPauseReasonInterrupt
	runtime.mu.Unlock()

	resumed, err := runtime.ResumeIfInterruptedPause()
	if err != nil {
		t.Fatalf("ResumeIfInterruptedPause returned error: %v", err)
	}
	if !resumed {
		t.Fatal("expected interrupted pause to be resumed")
	}
	if active.isPaused() {
		t.Fatal("expected active playback to leave paused state")
	}

	active.setPaused(true)
	runtime.mu.Lock()
	runtime.state.Status = play_music.StatusPaused
	runtime.pauseReason = mediaPauseReasonUser
	runtime.mu.Unlock()

	resumed, err = runtime.ResumeIfInterruptedPause()
	if err != nil {
		t.Fatalf("ResumeIfInterruptedPause returned unexpected error for user pause: %v", err)
	}
	if resumed {
		t.Fatal("expected user pause to remain paused")
	}
	if !active.isPaused() {
		t.Fatal("expected active playback to stay paused for user pause")
	}

	if err := runtime.Stop(context.Background()); err != nil {
		t.Fatalf("runtime.Stop failed: %v", err)
	}
	select {
	case <-active.done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for media playback goroutine to exit")
	}
}

func TestDeviceMediaRuntimeRecoverPlaybackAttachKeepsResumeOnAttachWhenRecoverFails(t *testing.T) {
	coordinator := newMediaPlaybackCoordinator()
	runtime := coordinator.getOrCreateRuntime("device-test-attach-failure")

	active := newActiveMediaPlayback(context.Background())
	defer active.cancel()
	defer active.closeDone()
	active.setPaused(true)

	runtime.mu.Lock()
	runtime.active = active
	runtime.resumeOnAttach = true
	runtime.state.Status = play_music.StatusPaused
	runtime.state.CurrentSourceType = MediaSourceTypeMCPResource
	runtime.mu.Unlock()

	err := runtime.RecoverPlayback(context.Background(), mediaPlaybackAudioConfig{}, mediaRecoveryTriggerAttach)
	if err == nil {
		t.Fatal("expected attach recovery without attachment to fail")
	}

	runtime.mu.RLock()
	resumeOnAttach := runtime.resumeOnAttach
	runtime.mu.RUnlock()
	if !resumeOnAttach {
		t.Fatal("expected failed attach recovery to keep resumeOnAttach for next reconnect")
	}
}

func TestDeviceMediaRuntimeRealtimeMcpAudioGateStatus(t *testing.T) {
	coordinator := newMediaPlaybackCoordinator()
	runtime := coordinator.getOrCreateRuntime("device-test-realtime-gate")

	active := newActiveMediaPlayback(context.Background())
	defer active.cancel()
	defer active.closeDone()

	source := MediaSourceDescriptor{SourceType: MediaSourceTypeMCPResource}
	runtime.mu.Lock()
	runtime.active = active
	runtime.attachment = &mediaSessionAttachment{}
	runtime.currentSource = &source
	runtime.state.Status = play_music.StatusPlaying
	runtime.state.CurrentSourceType = MediaSourceTypeMCPResource
	runtime.mu.Unlock()

	if !runtime.hasRealtimeMcpAudioControlContext() {
		t.Fatal("expected active realtime media playback to keep control context")
	}
	if !runtime.shouldGateRealtimeMcpAudioASR() {
		t.Fatal("expected active realtime media playback to gate general ASR")
	}

	active.setPaused(true)
	runtime.mu.Lock()
	runtime.state.Status = play_music.StatusPaused
	runtime.mu.Unlock()

	if !runtime.hasRealtimeMcpAudioControlContext() {
		t.Fatal("expected paused realtime media playback to keep control context")
	}
	if runtime.shouldGateRealtimeMcpAudioASR() {
		t.Fatal("expected paused realtime media playback not to gate general ASR")
	}
}

func appendTestPlaylistItem(t *testing.T, coordinator *mediaPlaybackCoordinator, agentID, title string) {
	t.Helper()

	source := MediaSourceDescriptor{
		ID:         title,
		Title:      title,
		SourceType: MediaSourceTypeHTTPURL,
		HTTP: &HTTPMediaSource{
			URL: "https://example.com/" + title + ".mp3",
		},
	}

	if _, _, _, err := coordinator.appendToAgentPlaylist(agentID, source); err != nil {
		t.Fatalf("appendToAgentPlaylist failed: %v", err)
	}
}

func stopRuntimeAndWait(t *testing.T, runtime *deviceMediaRuntime) {
	t.Helper()

	runtime.mu.RLock()
	active := runtime.active
	runtime.mu.RUnlock()

	if err := runtime.Stop(context.Background()); err != nil {
		t.Fatalf("runtime.Stop failed: %v", err)
	}
	if active == nil {
		return
	}

	select {
	case <-active.done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for media playback goroutine to exit")
	}
}
