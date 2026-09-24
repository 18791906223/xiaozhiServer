package plugins

import (
	"strings"

	"github.com/cloudwego/eino/schema"
	"xiaozhi-esp32-server-golang/internal/domain/chat/streamtransform"
	"xiaozhi-esp32-server-golang/internal/util"
)

const (
	defaultOutputSegmenterMinLen = 2
	defaultOutputSegmenterMaxLen = 100
)

type outputSegmenterFactory struct{}

func (f outputSegmenterFactory) Name() string {
	return "output_segmenter"
}

func (f outputSegmenterFactory) Priority() int {
	return 200
}

func (f outputSegmenterFactory) New(ctx streamtransform.Context) (streamtransform.Transformer, error) {
	return &outputSegmenterTransformer{
		minLen:  defaultOutputSegmenterMinLen,
		maxLen:  defaultOutputSegmenterMaxLen,
		isFirst: true,
	}, nil
}

type outputSegmenterTransformer struct {
	textBuffer       strings.Builder
	pendingToolCalls []schema.ToolCall
	minLen           int
	maxLen           int
	isFirst          bool
}

func (t *outputSegmenterTransformer) Transform(item streamtransform.Item) (streamtransform.Result, error) {
	switch item.Kind {
	case streamtransform.ItemKindTextDelta:
		return t.transformText(item), nil
	case streamtransform.ItemKindToolCalls:
		return t.transformToolCalls(item), nil
	default:
		out := t.flushPendingText(item.Meta, true, false)
		out = append(out, t.flushPendingToolCalls(item.Meta, false)...)
		out = append(out, item)
		return streamtransform.Result{Items: out}, nil
	}
}

func (t *outputSegmenterTransformer) Close() error {
	t.textBuffer.Reset()
	t.pendingToolCalls = nil
	return nil
}

func (t *outputSegmenterTransformer) transformText(item streamtransform.Item) streamtransform.Result {
	out := t.flushPendingToolCalls(item.Meta, false)

	if item.Text != "" {
		t.textBuffer.WriteString(item.Text)
	}

	if item.Text != "" && util.ContainsSentenceSeparator(item.Text, t.isFirst) {
		sentences, remaining := util.ExtractSmartSentences(t.textBuffer.String(), t.minLen, t.maxLen, t.isFirst)
		t.textBuffer.Reset()
		t.textBuffer.WriteString(remaining)
		for _, sentence := range sentences {
			if strings.TrimSpace(sentence) == "" {
				continue
			}
			out = append(out, streamtransform.Item{
				Kind: streamtransform.ItemKindTextSegment,
				Text: sentence,
				Meta: item.Meta,
			})
			t.isFirst = false
		}
	}

	if !item.IsEnd {
		return streamtransform.Result{Items: out}
	}

	out = append(out, t.flushPendingText(item.Meta, true, true)...)
	if len(out) > 0 {
		last := len(out) - 1
		out[last].IsEnd = true
		return streamtransform.Result{Items: out}
	}

	out = append(out, streamtransform.Item{
		Kind:  streamtransform.ItemKindTextSegment,
		IsEnd: true,
		Meta:  item.Meta,
	})
	return streamtransform.Result{Items: out}
}

func (t *outputSegmenterTransformer) transformToolCalls(item streamtransform.Item) streamtransform.Result {
	out := t.flushPendingText(item.Meta, true, false)
	if len(item.ToolCalls) > 0 {
		t.pendingToolCalls = append(t.pendingToolCalls, item.ToolCalls...)
	}
	if item.IsEnd {
		out = append(out, t.flushPendingToolCalls(item.Meta, true)...)
		if len(out) > 0 {
			out[len(out)-1].IsEnd = true
			return streamtransform.Result{Items: out}
		}
	}
	return streamtransform.Result{Items: out}
}

func (t *outputSegmenterTransformer) flushPendingText(meta map[string]any, force bool, isEnd bool) []streamtransform.Item {
	buffered := strings.TrimSpace(t.textBuffer.String())
	if buffered == "" {
		if isEnd {
			t.textBuffer.Reset()
		}
		return nil
	}

	if !force {
		return nil
	}

	t.textBuffer.Reset()
	t.isFirst = false
	return []streamtransform.Item{{
		Kind:  streamtransform.ItemKindTextSegment,
		Text:  buffered,
		IsEnd: isEnd,
		Meta:  meta,
	}}
}

func (t *outputSegmenterTransformer) flushPendingToolCalls(meta map[string]any, isEnd bool) []streamtransform.Item {
	if len(t.pendingToolCalls) == 0 {
		return nil
	}

	toolCalls := append([]schema.ToolCall(nil), t.pendingToolCalls...)
	t.pendingToolCalls = nil
	return []streamtransform.Item{{
		Kind:      streamtransform.ItemKindToolCalls,
		ToolCalls: toolCalls,
		IsEnd:     isEnd,
		Meta:      meta,
	}}
}

func RegisterOutputSegmenter(registry *streamtransform.Registry) {
	if registry == nil {
		return
	}
	registry.Register(outputSegmenterFactory{})
}
