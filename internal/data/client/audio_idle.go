package client

import (
	"sync"
	"time"
)

// AudioIdleClock tracks how long the device has been waiting for the next user turn.
type AudioIdleClock struct {
	mu      sync.RWMutex
	startAt time.Time
	pauseAt time.Time
	paused  bool

	timeoutPending bool
}

func (c *AudioIdleClock) Start(now time.Time) {
	if now.IsZero() {
		now = time.Now()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.startAt = now
	c.pauseAt = time.Time{}
	c.paused = false
	c.timeoutPending = false
}

func (c *AudioIdleClock) Pause(now time.Time) {
	if now.IsZero() {
		now = time.Now()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.startAt.IsZero() || c.paused {
		return
	}

	c.pauseAt = now
	c.paused = true
}

func (c *AudioIdleClock) Resume(now time.Time) {
	if now.IsZero() {
		now = time.Now()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.startAt.IsZero() || !c.paused {
		return
	}
	if now.Before(c.pauseAt) {
		now = c.pauseAt
	}

	c.startAt = c.startAt.Add(now.Sub(c.pauseAt))
	c.pauseAt = time.Time{}
	c.paused = false
}

func (c *AudioIdleClock) Elapsed(now time.Time) time.Duration {
	if now.IsZero() {
		now = time.Now()
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.startAt.IsZero() {
		return 0
	}

	endAt := now
	if c.paused {
		endAt = c.pauseAt
	}
	if endAt.Before(c.startAt) {
		return 0
	}
	return endAt.Sub(c.startAt)
}

func (c *AudioIdleClock) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.startAt = time.Time{}
	c.pauseAt = time.Time{}
	c.paused = false
	c.timeoutPending = false
}

func (c *AudioIdleClock) Started() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return !c.startAt.IsZero()
}

func (c *AudioIdleClock) Paused() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.paused
}

func (c *AudioIdleClock) MarkTimeoutPending() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.timeoutPending {
		return false
	}

	c.timeoutPending = true
	return true
}

func (c *AudioIdleClock) ClearTimeoutPending() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.timeoutPending = false
}

func (c *AudioIdleClock) TimeoutPending() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.timeoutPending
}
