package chat

import (
	"context"
	"testing"

	client "xiaozhi-esp32-server-golang/internal/data/client"
	"xiaozhi-esp32-server-golang/internal/domain/play_music"
)

func TestDetectRealtimeMcpAudioControlAction(t *testing.T) {
	cases := []struct {
		text string
		want string
	}{
		{text: "给我继续播放", want: "resume"},
		{text: "先暂停一下。", want: "pause"},
		{text: "停止播放吧", want: "stop"},
		{text: "下一首", want: "next"},
		{text: "上一首", want: "prev"},
		{text: "播放歌单里的歌曲", want: "play_playlist"},
		{text: "把当前播放加入歌单", want: "enqueue_current"},
		{text: "帮我讲个笑话", want: ""},
	}

	for _, tc := range cases {
		got := detectRealtimeMcpAudioControlAction(tc.text)
		if got != tc.want {
			t.Fatalf("detectRealtimeMcpAudioControlAction(%q)=%q, want %q", tc.text, got, tc.want)
		}
	}
}

func TestIsRealtimeMcpAudioExitCommand(t *testing.T) {
	cases := []struct {
		text string
		want bool
	}{
		{text: "再见", want: true},
		{text: "那就退出对话", want: true},
		{text: "拜拜啦", want: true},
		{text: "继续播放", want: false},
		{text: "今天天气怎么样", want: false},
	}

	for _, tc := range cases {
		got := isRealtimeMcpAudioExitCommand(tc.text)
		if got != tc.want {
			t.Fatalf("isRealtimeMcpAudioExitCommand(%q)=%v, want %v", tc.text, got, tc.want)
		}
	}
}

func TestIsRealtimeMcpAudioPlaybackState(t *testing.T) {
	if !isRealtimeMcpAudioPlaybackState(MediaPlayerState{
		Status:            play_music.StatusPlaying,
		CurrentSourceType: MediaSourceTypeMCPResource,
	}) {
		t.Fatal("expected mcp playing state to be gated")
	}

	if isRealtimeMcpAudioPlaybackState(MediaPlayerState{
		Status:            play_music.StatusPaused,
		CurrentSourceType: MediaSourceTypeInlineAudio,
	}) {
		t.Fatal("expected inline paused state not to gate general ASR")
	}

	if isRealtimeMcpAudioPlaybackState(MediaPlayerState{
		Status:            play_music.StatusStopped,
		CurrentSourceType: MediaSourceTypeMCPResource,
	}) {
		t.Fatal("expected stopped state not to be gated")
	}

	if isRealtimeMcpAudioPlaybackState(MediaPlayerState{
		Status:            play_music.StatusPlaying,
		CurrentSourceType: MediaSourceTypeHTTPURL,
	}) {
		t.Fatal("expected non-mcp source not to be gated")
	}
}

func TestTryHandleRealtimeMcpAudioASRAllowsNormalChatWhenPlaybackPaused(t *testing.T) {
	session, runtime := newRealtimeGateTestSession(t)
	active := newActiveMediaPlayback(context.Background())
	defer active.cancel()
	defer active.closeDone()
	active.setPaused(true)

	source := MediaSourceDescriptor{SourceType: MediaSourceTypeMCPResource}
	runtime.mu.Lock()
	runtime.active = active
	runtime.attachment = &mediaSessionAttachment{}
	runtime.currentSource = &source
	runtime.state.Status = play_music.StatusPaused
	runtime.state.CurrentSourceType = MediaSourceTypeMCPResource
	runtime.mu.Unlock()

	handled, err := session.tryHandleRealtimeMcpAudioASR(context.Background(), "你在干什么")
	if err != nil {
		t.Fatalf("tryHandleRealtimeMcpAudioASR returned error: %v", err)
	}
	if handled {
		t.Fatal("expected paused media context to allow normal chat through")
	}
}

func TestTryHandleRealtimeMcpAudioASRSwallowsNormalChatWhenPlaybackActive(t *testing.T) {
	session, runtime := newRealtimeGateTestSession(t)
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

	handled, err := session.tryHandleRealtimeMcpAudioASR(context.Background(), "你在干什么")
	if err != nil {
		t.Fatalf("tryHandleRealtimeMcpAudioASR returned error: %v", err)
	}
	if !handled {
		t.Fatal("expected active media playback to gate normal ASR text")
	}
}

func newRealtimeGateTestSession(t *testing.T) (*ChatSession, *deviceMediaRuntime) {
	t.Helper()

	clientState := &client.ClientState{
		Ctx:        context.Background(),
		DeviceID:   t.Name(),
		ListenMode: "realtime",
	}
	session := &ChatSession{
		clientState: clientState,
	}
	session.mediaPlayer = NewSessionMediaPlayer(session)
	return session, session.mediaPlayer.runtime
}
