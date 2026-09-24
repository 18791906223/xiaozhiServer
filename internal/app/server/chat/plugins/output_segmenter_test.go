package plugins

import (
	"testing"

	"github.com/cloudwego/eino/schema"
	"xiaozhi-esp32-server-golang/internal/domain/chat/streamtransform"
)

func TestOutputSegmenterFlushesOnEnd(t *testing.T) {
	transformer, err := outputSegmenterFactory{}.New(streamtransform.Context{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	out, err := transformer.Transform(streamtransform.Item{
		Kind: streamtransform.ItemKindTextDelta,
		Text: "你好，世界",
	})
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}
	if len(out.Items) != 1 {
		t.Fatalf("len(out.Items) = %d, want 1", len(out.Items))
	}
	if got := out.Items[0].Text; got != "你好，" {
		t.Fatalf("out.Items[0].Text = %q, want %q", got, "你好，")
	}
	if out.Items[0].IsEnd {
		t.Fatalf("out.Items[0].IsEnd = true, want false")
	}

	out, err = transformer.Transform(streamtransform.Item{
		Kind:  streamtransform.ItemKindTextDelta,
		IsEnd: true,
	})
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}
	if len(out.Items) != 1 {
		t.Fatalf("len(out.Items) = %d, want 1", len(out.Items))
	}
	if got := out.Items[0].Text; got != "世界" {
		t.Fatalf("out.Items[0].Text = %q, want %q", got, "世界")
	}
	if !out.Items[0].IsEnd {
		t.Fatalf("out.Items[0].IsEnd = false, want true")
	}
}

func TestOutputSegmenterEmitsEmptyEndWhenNoRemainder(t *testing.T) {
	transformer, err := outputSegmenterFactory{}.New(streamtransform.Context{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	out, err := transformer.Transform(streamtransform.Item{
		Kind: streamtransform.ItemKindTextDelta,
		Text: "你好。",
	})
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}
	if len(out.Items) != 1 {
		t.Fatalf("len(out.Items) = %d, want 1", len(out.Items))
	}
	if got := out.Items[0].Text; got != "你好。" {
		t.Fatalf("out.Items[0].Text = %q, want %q", got, "你好。")
	}

	out, err = transformer.Transform(streamtransform.Item{
		Kind:  streamtransform.ItemKindTextDelta,
		IsEnd: true,
	})
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}
	if len(out.Items) != 1 {
		t.Fatalf("len(out.Items) = %d, want 1", len(out.Items))
	}
	if got := out.Items[0].Text; got != "" {
		t.Fatalf("out.Items[0].Text = %q, want empty", got)
	}
	if !out.Items[0].IsEnd {
		t.Fatalf("out.Items[0].IsEnd = false, want true")
	}
}

func TestOutputSegmenterFlushesBufferedTextBeforeToolCalls(t *testing.T) {
	transformer, err := outputSegmenterFactory{}.New(streamtransform.Context{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	out, err := transformer.Transform(streamtransform.Item{
		Kind: streamtransform.ItemKindTextDelta,
		Text: "半句文本",
	})
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}
	if len(out.Items) != 0 {
		t.Fatalf("len(out.Items) = %d, want 0", len(out.Items))
	}

	out, err = transformer.Transform(streamtransform.Item{
		Kind: streamtransform.ItemKindToolCalls,
		ToolCalls: []schema.ToolCall{
			{ID: "call_1", Type: "function", Function: schema.FunctionCall{Name: "weather", Arguments: `{"city":"shanghai"}`}},
		},
	})
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}
	if len(out.Items) != 1 {
		t.Fatalf("len(out.Items) = %d, want 1", len(out.Items))
	}
	if out.Items[0].Kind != streamtransform.ItemKindTextSegment {
		t.Fatalf("out.Items[0].Kind = %q, want %q", out.Items[0].Kind, streamtransform.ItemKindTextSegment)
	}
	if got := out.Items[0].Text; got != "半句文本" {
		t.Fatalf("out.Items[0].Text = %q, want %q", got, "半句文本")
	}

	out, err = transformer.Transform(streamtransform.Item{
		Kind:  streamtransform.ItemKindTextDelta,
		IsEnd: true,
	})
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}
	if len(out.Items) != 1 {
		t.Fatalf("len(out.Items) = %d, want 1", len(out.Items))
	}
	if out.Items[0].Kind != streamtransform.ItemKindToolCalls {
		t.Fatalf("out.Items[0].Kind = %q, want %q", out.Items[0].Kind, streamtransform.ItemKindToolCalls)
	}
	if len(out.Items[0].ToolCalls) != 1 {
		t.Fatalf("len(out.Items[0].ToolCalls) = %d, want 1", len(out.Items[0].ToolCalls))
	}
	if !out.Items[0].IsEnd {
		t.Fatalf("out.Items[0].IsEnd = false, want true")
	}
}

func TestOutputSegmenterAggregatesToolCallsUntilBoundary(t *testing.T) {
	transformer, err := outputSegmenterFactory{}.New(streamtransform.Context{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	for _, tc := range []schema.ToolCall{
		{ID: "call_1", Type: "function", Function: schema.FunctionCall{Name: "weather", Arguments: `{"city":"shanghai"}`}},
		{ID: "call_2", Type: "function", Function: schema.FunctionCall{Name: "clock", Arguments: `{"timezone":"Asia/Shanghai"}`}},
	} {
		out, err := transformer.Transform(streamtransform.Item{
			Kind:      streamtransform.ItemKindToolCalls,
			ToolCalls: []schema.ToolCall{tc},
		})
		if err != nil {
			t.Fatalf("Transform() error = %v", err)
		}
		if len(out.Items) != 0 {
			t.Fatalf("len(out.Items) = %d, want 0", len(out.Items))
		}
	}

	out, err := transformer.Transform(streamtransform.Item{
		Kind: streamtransform.ItemKindTextDelta,
		Text: "继续回复。",
	})
	if err != nil {
		t.Fatalf("Transform() error = %v", err)
	}
	if len(out.Items) != 2 {
		t.Fatalf("len(out.Items) = %d, want 2", len(out.Items))
	}
	if out.Items[0].Kind != streamtransform.ItemKindToolCalls {
		t.Fatalf("out.Items[0].Kind = %q, want %q", out.Items[0].Kind, streamtransform.ItemKindToolCalls)
	}
	if len(out.Items[0].ToolCalls) != 2 {
		t.Fatalf("len(out.Items[0].ToolCalls) = %d, want 2", len(out.Items[0].ToolCalls))
	}
	if out.Items[1].Kind != streamtransform.ItemKindTextSegment {
		t.Fatalf("out.Items[1].Kind = %q, want %q", out.Items[1].Kind, streamtransform.ItemKindTextSegment)
	}
	if got := out.Items[1].Text; got != "继续回复。" {
		t.Fatalf("out.Items[1].Text = %q, want %q", got, "继续回复。")
	}
}
