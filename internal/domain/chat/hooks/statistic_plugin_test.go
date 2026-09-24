package hooks

import (
	"context"
	"testing"
)

func testHookContext(sessionID string) Context {
	return Context{
		Ctx:       context.Background(),
		SessionID: sessionID,
		DeviceID:  "device-test",
	}
}

func currentTurnForTest(t *testing.T, plugin *statisticPlugin, sessionID string) *turnMetric {
	t.Helper()

	state, ok := plugin.sessions.Get(sessionID)
	if !ok {
		return nil
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	if state.current == nil {
		return nil
	}
	snapshot := *state.current
	return &snapshot
}

func TestStatisticPluginHandlesLateTurnStart(t *testing.T) {
	plugin := newStatisticPlugin()
	ctx := testHookContext("session-late-start")

	plugin.onMetric(ctx, MetricData{Stage: MetricAsrFirstText, Ts: 20})
	plugin.onMetric(ctx, MetricData{Stage: MetricTurnStart, Ts: 10})

	tm := currentTurnForTest(t, plugin, ctx.SessionID)
	if tm == nil {
		t.Fatalf("expected active turn for session %q", ctx.SessionID)
	}
	if tm.turnID != 1 {
		t.Fatalf("turnID = %d, want 1", tm.turnID)
	}
	if tm.turnStartTs != 10 {
		t.Fatalf("turnStartTs = %d, want 10", tm.turnStartTs)
	}
	if tm.asrFirstTextTs != 20 {
		t.Fatalf("asrFirstTextTs = %d, want 20", tm.asrFirstTextTs)
	}
}

func TestStatisticPluginTracksVoiceSilenceAndFirstSentence(t *testing.T) {
	plugin := newStatisticPlugin()
	ctx := testHookContext("session-latency")

	plugin.onMetric(ctx, MetricData{Stage: MetricTurnStart, Ts: 10})
	plugin.onMetric(ctx, MetricData{Stage: MetricVoiceSilence, Ts: 40})
	plugin.onMetric(ctx, MetricData{Stage: MetricAsrFinalText, Ts: 70})
	plugin.onMetric(ctx, MetricData{Stage: MetricLlmStart, Ts: 80})
	plugin.onMetric(ctx, MetricData{Stage: MetricLlmFirstToken, Ts: 90})
	plugin.onMetric(ctx, MetricData{Stage: MetricLlmFirstSentence, Ts: 120})

	tm := currentTurnForTest(t, plugin, ctx.SessionID)
	if tm == nil {
		t.Fatalf("expected active turn for session %q", ctx.SessionID)
	}
	if got := calcDelta(tm.voiceSilenceTs, tm.asrFinalTextTs); got != 30 {
		t.Fatalf("asr silence-final delta = %d, want 30", got)
	}
	if got := calcDelta(tm.llmStartTs, tm.llmFirstTokenTs); got != 10 {
		t.Fatalf("llm first token delta = %d, want 10", got)
	}
	if got := calcDelta(tm.llmStartTs, tm.llmFirstSentenceTs); got != 40 {
		t.Fatalf("llm first sentence delta = %d, want 40", got)
	}
}

func TestStatisticPluginKeepsOutOfOrderAsyncMetricTimestamps(t *testing.T) {
	plugin := newStatisticPlugin()
	ctx := testHookContext("session-out-of-order")

	plugin.onMetric(ctx, MetricData{Stage: MetricTurnStart, Ts: 10})
	plugin.onMetric(ctx, MetricData{Stage: MetricLlmFirstToken, Ts: 45})
	plugin.onMetric(ctx, MetricData{Stage: MetricLlmStart, Ts: 30})
	plugin.onMetric(ctx, MetricData{Stage: MetricTtsFirstFrame, Ts: 70})
	plugin.onMetric(ctx, MetricData{Stage: MetricTtsStart, Ts: 50})
	plugin.onMetric(ctx, MetricData{Stage: MetricTtsStop, Ts: 90})

	tm := currentTurnForTest(t, plugin, ctx.SessionID)
	if tm == nil {
		t.Fatalf("expected active turn for session %q", ctx.SessionID)
	}
	if got := calcDelta(tm.llmStartTs, tm.llmFirstTokenTs); got != 15 {
		t.Fatalf("llm first token delta = %d, want 15", got)
	}
	if got := calcDelta(tm.ttsStartTs, tm.ttsFirstFrameTs); got != 20 {
		t.Fatalf("tts first frame delta = %d, want 20", got)
	}
	if got := calcDelta(tm.ttsStartTs, tm.ttsStopTs); got != 40 {
		t.Fatalf("tts total delta = %d, want 40", got)
	}
}

func TestStatisticPluginTracksSessionsIndependently(t *testing.T) {
	plugin := newStatisticPlugin()
	ctxA := testHookContext("session-a")
	ctxB := testHookContext("session-b")

	plugin.onMetric(ctxA, MetricData{Stage: MetricTurnStart, Ts: 10})
	plugin.onMetric(ctxB, MetricData{Stage: MetricTurnStart, Ts: 100})
	plugin.onMetric(ctxA, MetricData{Stage: MetricLlmStart, Ts: 20})
	plugin.onMetric(ctxB, MetricData{Stage: MetricLlmStart, Ts: 120})
	plugin.onMetric(ctxA, MetricData{Stage: MetricTurnEnd, Ts: 30})

	if tm := currentTurnForTest(t, plugin, ctxA.SessionID); tm != nil {
		t.Fatalf("expected session A current turn to be cleared")
	}

	tmB := currentTurnForTest(t, plugin, ctxB.SessionID)
	if tmB == nil {
		t.Fatalf("expected session B current turn to remain active")
	}
	if tmB.turnID != 1 {
		t.Fatalf("session B turnID = %d, want 1", tmB.turnID)
	}
	if tmB.turnStartTs != 100 {
		t.Fatalf("session B turnStartTs = %d, want 100", tmB.turnStartTs)
	}
	if tmB.llmStartTs != 120 {
		t.Fatalf("session B llmStartTs = %d, want 120", tmB.llmStartTs)
	}
}

func TestStatisticPluginCleansStaleSessionState(t *testing.T) {
	plugin := newStatisticPlugin()
	ctx := testHookContext("session-stale")

	plugin.onMetric(ctx, MetricData{Stage: MetricTurnStart, Ts: 10})
	state, ok := plugin.sessions.Get(ctx.SessionID)
	if !ok {
		t.Fatalf("expected session state for %q", ctx.SessionID)
	}

	state.mu.Lock()
	state.lastSeen = 1
	state.mu.Unlock()

	plugin.cleanupStale(1 + int64(2*60*1000) + 1)

	if _, ok := plugin.sessions.Get(ctx.SessionID); ok {
		t.Fatalf("expected stale session state to be removed")
	}
}

func TestMetricEventAllowsSynchronousStatisticPlugin(t *testing.T) {
	if err := ValidateEventKind(EventChatMetric, PluginKindInterceptor); err != nil {
		t.Fatalf("metric interceptor validation failed: %v", err)
	}
	if err := ValidateEventKind(EventChatMetric, PluginKindObserver); err != nil {
		t.Fatalf("metric observer validation failed: %v", err)
	}
}

func TestStatisticPluginCompletesOnTurnEndAndIgnoresLateMetrics(t *testing.T) {
	plugin := newStatisticPlugin()
	ctx := testHookContext("session-stop")

	plugin.onMetric(ctx, MetricData{Stage: MetricTurnStart, Ts: 10})
	plugin.onMetric(ctx, MetricData{Stage: MetricLlmStart, Ts: 20})
	plugin.onMetric(ctx, MetricData{Stage: MetricTtsStart, Ts: 30})
	plugin.onMetric(ctx, MetricData{Stage: MetricTtsStop, Ts: 40})
	plugin.onMetric(ctx, MetricData{Stage: MetricTurnEnd, Ts: 50})

	if tm := currentTurnForTest(t, plugin, ctx.SessionID); tm != nil {
		t.Fatalf("expected current turn to be cleared after turn_end")
	}

	plugin.onMetric(ctx, MetricData{Stage: MetricLlmEnd, Ts: 60})
	plugin.onMetric(ctx, MetricData{Stage: MetricTtsStop, Ts: 70})

	if tm := currentTurnForTest(t, plugin, ctx.SessionID); tm != nil {
		t.Fatalf("late metrics should not create a new turn")
	}
}

func TestStatisticPluginCompletesTurnWithoutTts(t *testing.T) {
	plugin := newStatisticPlugin()
	ctx := testHookContext("session-no-tts")

	plugin.onMetric(ctx, MetricData{Stage: MetricTurnStart, Ts: 10})
	plugin.onMetric(ctx, MetricData{Stage: MetricLlmStart, Ts: 20})
	plugin.onMetric(ctx, MetricData{Stage: MetricLlmEnd, Ts: 30})
	plugin.onMetric(ctx, MetricData{Stage: MetricTurnEnd, Ts: 35})

	if tm := currentTurnForTest(t, plugin, ctx.SessionID); tm != nil {
		t.Fatalf("expected current turn to be cleared after turn_end without tts")
	}
}

func TestStatisticPluginKeepsOnlyLatestTurn(t *testing.T) {
	plugin := newStatisticPlugin()
	ctx := testHookContext("session-latest-only")

	plugin.onMetric(ctx, MetricData{Stage: MetricTurnStart, Ts: 10})
	plugin.onMetric(ctx, MetricData{Stage: MetricLlmStart, Ts: 20})
	plugin.onMetric(ctx, MetricData{Stage: MetricTtsStart, Ts: 30})

	plugin.onMetric(ctx, MetricData{Stage: MetricTurnStart, Ts: 40})

	tm := currentTurnForTest(t, plugin, ctx.SessionID)
	if tm == nil {
		t.Fatalf("expected latest turn to exist")
	}
	if tm.turnID != 2 {
		t.Fatalf("turnID = %d, want 2", tm.turnID)
	}
	if tm.turnStartTs != 40 {
		t.Fatalf("turnStartTs = %d, want 40", tm.turnStartTs)
	}
	if tm.llmStartTs != 0 {
		t.Fatalf("llmStartTs = %d, want 0", tm.llmStartTs)
	}
	if tm.ttsStartTs != 0 {
		t.Fatalf("ttsStartTs = %d, want 0", tm.ttsStartTs)
	}
}

func TestStatisticPluginDoesNotCrossOldTtsStopIntoNewTurn(t *testing.T) {
	plugin := newStatisticPlugin()
	ctx := testHookContext("session-no-cross")

	plugin.onMetric(ctx, MetricData{Stage: MetricTurnStart, Ts: 10})
	plugin.onMetric(ctx, MetricData{Stage: MetricTtsStart, Ts: 20})

	plugin.onMetric(ctx, MetricData{Stage: MetricTurnStart, Ts: 30})
	plugin.onMetric(ctx, MetricData{Stage: MetricTtsStop, Ts: 40})

	tm := currentTurnForTest(t, plugin, ctx.SessionID)
	if tm == nil {
		t.Fatalf("expected latest turn to remain active")
	}
	if tm.turnID != 2 {
		t.Fatalf("turnID = %d, want 2", tm.turnID)
	}
	if tm.ttsStopTs != 0 {
		t.Fatalf("ttsStopTs = %d, want 0", tm.ttsStopTs)
	}
}

func TestStatisticPluginDoesNotCrossOldTurnEndIntoNewTurn(t *testing.T) {
	plugin := newStatisticPlugin()
	ctx := testHookContext("session-no-cross-turn-end")

	plugin.onMetric(ctx, MetricData{Stage: MetricTurnStart, Ts: 10})
	plugin.onMetric(ctx, MetricData{Stage: MetricLlmStart, Ts: 20})

	plugin.onMetric(ctx, MetricData{Stage: MetricTurnStart, Ts: 30})
	plugin.onMetric(ctx, MetricData{Stage: MetricTurnEnd, Ts: 25})

	tm := currentTurnForTest(t, plugin, ctx.SessionID)
	if tm == nil {
		t.Fatalf("expected latest turn to remain active")
	}
	if tm.turnID != 2 {
		t.Fatalf("turnID = %d, want 2", tm.turnID)
	}
	if tm.turnEndTs != 0 {
		t.Fatalf("turnEndTs = %d, want 0", tm.turnEndTs)
	}
}
