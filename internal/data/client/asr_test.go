package client

import (
	"context"
	"errors"
	"testing"

	asr_types "xiaozhi-esp32-server-golang/internal/domain/asr/types"
)

func TestRetireAsrResult_RetryableErrorUsesRetryReason(t *testing.T) {
	retryErr := errors.New("provider retryable error")
	a := &Asr{
		AsrResultChannel: make(chan asr_types.StreamingResult, 1),
	}
	a.AsrResultChannel <- asr_types.StreamingResult{
		Error:       retryErr,
		RetryReason: asr_types.RetryReasonXunfeiServiceInstanceInvalid,
		IsFinal:     true,
	}

	result, shouldContinue, err := a.RetireAsrResult(context.Background())
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !shouldContinue {
		t.Fatalf("expected shouldContinue to be true")
	}
	if result.RetryReason != asr_types.RetryReasonXunfeiServiceInstanceInvalid {
		t.Fatalf("expected retry reason %q, got %q", asr_types.RetryReasonXunfeiServiceInstanceInvalid, result.RetryReason)
	}
	if !errors.Is(result.Error, retryErr) {
		t.Fatalf("expected original error to be preserved")
	}
}

func TestRetireAsrResult_NonRetryableErrorReturnsError(t *testing.T) {
	fatalErr := errors.New("fatal provider error")
	a := &Asr{
		AsrResultChannel: make(chan asr_types.StreamingResult, 1),
	}
	a.AsrResultChannel <- asr_types.StreamingResult{
		Error: fatalErr,
	}

	_, shouldContinue, err := a.RetireAsrResult(context.Background())
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, fatalErr) {
		t.Fatalf("expected original error, got %v", err)
	}
	if shouldContinue {
		t.Fatalf("expected shouldContinue to be false")
	}
}

func TestRetireAsrResult_NonFinalResultsOnlyTriggerFirstTextOnce(t *testing.T) {
	var firstTexts []string
	a := &Asr{
		AsrResultChannel: make(chan asr_types.StreamingResult, 4),
		ClientState: &ClientState{
			OnAsrFirstTextCallback: func(text string, isFinal bool) {
				firstTexts = append(firstTexts, text)
			},
		},
	}
	a.AsrResultChannel <- asr_types.StreamingResult{Text: "你在", IsFinal: false}
	a.AsrResultChannel <- asr_types.StreamingResult{Text: "你在干啥呢", IsFinal: false}
	a.AsrResultChannel <- asr_types.StreamingResult{Text: "你在干啥呢", IsFinal: false}
	a.AsrResultChannel <- asr_types.StreamingResult{Text: "你在干啥呢？", IsFinal: true}

	result, shouldContinue, err := a.RetireAsrResult(context.Background())
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !shouldContinue {
		t.Fatalf("expected shouldContinue to be true")
	}
	if result.Text != "你在干啥呢？" {
		t.Fatalf("expected final text %q, got %q", "你在干啥呢？", result.Text)
	}
	if len(firstTexts) != 1 || firstTexts[0] != "你在" {
		t.Fatalf("unexpected first text callbacks: %v", firstTexts)
	}
}

func TestRetireAsrResult_FinalOnlyStillTriggersFirstText(t *testing.T) {
	var firstText string
	var firstIsFinal bool
	a := &Asr{
		AsrResultChannel: make(chan asr_types.StreamingResult, 1),
		ClientState: &ClientState{
			OnAsrFirstTextCallback: func(text string, isFinal bool) {
				firstText = text
				firstIsFinal = isFinal
			},
		},
	}
	a.AsrResultChannel <- asr_types.StreamingResult{Text: "最终文本", IsFinal: true}

	result, shouldContinue, err := a.RetireAsrResult(context.Background())
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !shouldContinue {
		t.Fatalf("expected shouldContinue to be true")
	}
	if result.Text != "最终文本" {
		t.Fatalf("expected final text %q, got %q", "最终文本", result.Text)
	}
	if firstText != "最终文本" || !firstIsFinal {
		t.Fatalf("unexpected first text callback: text=%q, isFinal=%v", firstText, firstIsFinal)
	}
}

func TestRetireAsrResult_Funasr2PassOnlineMarkedFinalStillWaitsForOfflineFinal(t *testing.T) {
	var firstTexts []string
	a := &Asr{
		AsrType:          "funasr",
		Mode:             "2pass",
		AsrResultChannel: make(chan asr_types.StreamingResult, 2),
		ClientState: &ClientState{
			OnAsrFirstTextCallback: func(text string, isFinal bool) {
				firstTexts = append(firstTexts, text)
			},
		},
	}
	a.AsrResultChannel <- asr_types.StreamingResult{Text: "你在", IsFinal: true, Mode: "2pass-online"}
	a.AsrResultChannel <- asr_types.StreamingResult{Text: "你在干啥呢。", IsFinal: true, Mode: "2pass-offline"}

	result, shouldContinue, err := a.RetireAsrResult(context.Background())
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !shouldContinue {
		t.Fatalf("expected shouldContinue to be true")
	}
	if result.Text != "你在干啥呢。" {
		t.Fatalf("expected final text %q, got %q", "你在干啥呢。", result.Text)
	}
	if len(firstTexts) != 1 || firstTexts[0] != "你在" {
		t.Fatalf("unexpected first text callbacks: %v", firstTexts)
	}
}
