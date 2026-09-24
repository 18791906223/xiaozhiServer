package streamtransform

import (
	"context"

	"github.com/cloudwego/eino/schema"
)

type Context struct {
	Ctx       context.Context
	SessionID string
	DeviceID  string
	RequestID string
}

type ItemKind string

const (
	ItemKindTextDelta   ItemKind = "text_delta"
	ItemKindTextSegment ItemKind = "text_segment"
	ItemKindToolCalls   ItemKind = "tool_calls"
)

type Item struct {
	Kind      ItemKind
	Text      string
	ToolCalls []schema.ToolCall
	IsEnd     bool
	Meta      map[string]any
}

type Result struct {
	Items []Item
	Stop  bool
}

type Factory interface {
	Name() string
	Priority() int
	New(Context) (Transformer, error)
}

type Transformer interface {
	Transform(Item) (Result, error)
	Close() error
}
