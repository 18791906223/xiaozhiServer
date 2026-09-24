package hooks

import (
	"context"
	"testing"
	"time"
)

func TestAsyncHandlersPreserveEmitOrder(t *testing.T) {
	hub := NewHub(context.Background())
	defer hub.Close()

	got := make(chan int, 3)
	hub.RegisterAsync("metric", "collector", 100, func(ctx Context, payload any) {
		got <- payload.(int)
	})

	for i := 0; i < 3; i++ {
		if _, _, err := hub.Emit("metric", Context{}, i); err != nil {
			t.Fatalf("Emit() error = %v", err)
		}
	}

	want := []int{0, 1, 2}
	for i, expected := range want {
		select {
		case actual := <-got:
			if actual != expected {
				t.Fatalf("result[%d] = %d, want %d", i, actual, expected)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for async handler result %d", i)
		}
	}
}

func TestAsyncEmitDoesNotBlockWhenHandlersAreSlow(t *testing.T) {
	hub := NewHub(context.Background())
	defer hub.Close()

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	hub.RegisterAsync("metric", "slow", 100, func(ctx Context, payload any) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 600; i++ {
			if _, _, err := hub.Emit("metric", Context{}, i); err != nil {
				t.Errorf("Emit() error = %v", err)
				return
			}
		}
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for async handler to start")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Emit() blocked while async handler was slow")
	}

	close(release)
}

func TestAsyncQueueDropsWhenFull(t *testing.T) {
	hub := NewHub(context.Background(), WithAsyncConfig(AsyncConfig{
		QueueSize:    1,
		WorkerCount:  1,
		DropWhenFull: true,
	}))
	defer hub.Close()

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	hub.RegisterAsync("metric", "slow", 100, func(ctx Context, payload any) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
	})

	for i := 0; i < 10; i++ {
		if _, _, err := hub.Emit("metric", Context{}, i); err != nil {
			t.Fatalf("Emit() error = %v", err)
		}
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for async handler to start")
	}

	stats := hub.Stats()
	if stats.DroppedAsync == 0 {
		t.Fatalf("expected dropped async tasks > 0, got %+v", stats)
	}
	if stats.Plugins["slow"].DroppedAsync == 0 {
		t.Fatalf("expected plugin dropped async tasks > 0, got %+v", stats.Plugins["slow"])
	}

	close(release)
}

func TestAsyncTimeoutIsAccounted(t *testing.T) {
	hub := NewHub(context.Background(), WithAsyncConfig(AsyncConfig{Timeout: 20 * time.Millisecond}))
	defer hub.Close()

	hub.RegisterAsync("metric", "timeout-observer", 100, func(ctx Context, payload any) {
		<-ctx.Ctx.Done()
	})

	if _, _, err := hub.Emit("metric", Context{Ctx: context.Background()}, 1); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats := hub.Stats()
		if stats.Plugins["timeout-observer"].Timeouts > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected timeout observer stats to be recorded, got %+v", hub.Stats().Plugins["timeout-observer"])
}

func TestSyncPluginStatsAreTracked(t *testing.T) {
	hub := NewHub(context.Background())
	defer hub.Close()

	hub.RegisterSync("llm", "rewrite", 10, func(ctx Context, payload any) (any, bool, error) {
		return payload, true, nil
	})

	if _, stop, err := hub.Emit("llm", Context{}, "hello"); err != nil {
		t.Fatalf("Emit() error = %v", err)
	} else if !stop {
		t.Fatal("expected stop=true")
	}

	stats := hub.Stats().Plugins["rewrite"]
	if stats.Invocations != 1 || stats.Stops != 1 {
		t.Fatalf("unexpected sync plugin stats: %+v", stats)
	}
}

func TestSharedAsyncExecutorAcrossHubs(t *testing.T) {
	exec := NewAsyncExecutor(context.Background(), AsyncConfig{QueueSize: 16, WorkerCount: 1, DropWhenFull: false})
	defer exec.Close()

	hub1 := NewHub(context.Background(), WithAsyncExecutor(exec))
	defer hub1.Close()
	hub2 := NewHub(context.Background(), WithAsyncExecutor(exec))
	defer hub2.Close()

	got := make(chan int, 2)
	hub1.RegisterAsync("metric", "hub1-observer", 100, func(ctx Context, payload any) { got <- payload.(int) })
	hub2.RegisterAsync("metric", "hub2-observer", 100, func(ctx Context, payload any) { got <- payload.(int) })

	if _, _, err := hub1.Emit("metric", Context{}, 1); err != nil {
		t.Fatalf("hub1 Emit() error = %v", err)
	}
	if _, _, err := hub2.Emit("metric", Context{}, 2); err != nil {
		t.Fatalf("hub2 Emit() error = %v", err)
	}

	seen := map[int]bool{}
	for len(seen) < 2 {
		select {
		case v := <-got:
			seen[v] = true
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting shared executor results, seen=%v", seen)
		}
	}
}
