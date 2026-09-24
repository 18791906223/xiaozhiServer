package client

import (
	"testing"
	"time"
)

func TestAudioIdleClockStartAndElapsed(t *testing.T) {
	var clock AudioIdleClock
	startAt := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)

	clock.Start(startAt)

	elapsed := clock.Elapsed(startAt.Add(3 * time.Second))
	if elapsed != 3*time.Second {
		t.Fatalf("expected elapsed=3s, got %s", elapsed)
	}
	if !clock.Started() {
		t.Fatal("expected clock to report started")
	}
	if clock.Paused() {
		t.Fatal("expected clock to report running")
	}
}

func TestAudioIdleClockPauseAndResume(t *testing.T) {
	var clock AudioIdleClock
	startAt := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)
	pauseAt := startAt.Add(5 * time.Second)
	resumeAt := pauseAt.Add(4 * time.Second)

	clock.Start(startAt)
	clock.Pause(pauseAt)

	elapsedWhilePaused := clock.Elapsed(resumeAt.Add(2 * time.Second))
	if elapsedWhilePaused != 5*time.Second {
		t.Fatalf("expected paused elapsed=5s, got %s", elapsedWhilePaused)
	}
	if !clock.Paused() {
		t.Fatal("expected clock to stay paused")
	}

	clock.Resume(resumeAt)

	elapsedAfterResume := clock.Elapsed(resumeAt.Add(3 * time.Second))
	if elapsedAfterResume != 8*time.Second {
		t.Fatalf("expected elapsed=8s after resume, got %s", elapsedAfterResume)
	}
	if clock.Paused() {
		t.Fatal("expected clock to resume running")
	}
}

func TestAudioIdleClockResetClearsState(t *testing.T) {
	var clock AudioIdleClock
	startAt := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)

	clock.Start(startAt)
	clock.Pause(startAt.Add(time.Second))
	clock.MarkTimeoutPending()
	clock.Reset()

	if clock.Started() {
		t.Fatal("expected reset clock to report not started")
	}
	if clock.Paused() {
		t.Fatal("expected reset clock to report not paused")
	}
	if clock.TimeoutPending() {
		t.Fatal("expected reset clock to clear timeout pending state")
	}
	if elapsed := clock.Elapsed(startAt.Add(5 * time.Second)); elapsed != 0 {
		t.Fatalf("expected reset clock elapsed=0, got %s", elapsed)
	}
}

func TestAudioIdleClockTimeoutPendingResetsOnStart(t *testing.T) {
	var clock AudioIdleClock
	startAt := time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC)

	clock.Start(startAt)
	if !clock.MarkTimeoutPending() {
		t.Fatal("expected first timeout pending mark to succeed")
	}
	if clock.MarkTimeoutPending() {
		t.Fatal("expected second timeout pending mark to be ignored")
	}

	clock.Start(startAt.Add(2 * time.Second))
	if clock.TimeoutPending() {
		t.Fatal("expected restarting idle window to clear timeout pending state")
	}
}
