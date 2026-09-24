package streamtransform

import (
	"testing"
)

type testFactory struct {
	name     string
	priority int
	newFn    func(Context) (Transformer, error)
}

func (f testFactory) Name() string { return f.name }

func (f testFactory) Priority() int { return f.priority }

func (f testFactory) New(ctx Context) (Transformer, error) { return f.newFn(ctx) }

type testTransformer struct {
	transformFn func(Item) (Result, error)
	closeFn     func() error
}

func (t testTransformer) Transform(item Item) (Result, error) {
	return t.transformFn(item)
}

func (t testTransformer) Close() error {
	if t.closeFn == nil {
		return nil
	}
	return t.closeFn()
}

func TestPipelineCascadesByPriority(t *testing.T) {
	registry := NewRegistry()
	registry.Register(testFactory{
		name:     "append-b",
		priority: 20,
		newFn: func(Context) (Transformer, error) {
			return testTransformer{
				transformFn: func(item Item) (Result, error) {
					item.Text += "B"
					return Result{Items: []Item{item}}, nil
				},
			}, nil
		},
	})
	registry.Register(testFactory{
		name:     "append-a",
		priority: 10,
		newFn: func(Context) (Transformer, error) {
			return testTransformer{
				transformFn: func(item Item) (Result, error) {
					item.Text += "A"
					return Result{Items: []Item{item}}, nil
				},
			}, nil
		},
	})

	pipeline, err := registry.Open(Context{})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	out, stop, err := pipeline.Push(Item{Kind: ItemKindTextDelta, Text: "x"})
	if err != nil {
		t.Fatalf("Push() error = %v", err)
	}
	if stop {
		t.Fatalf("Push() stop = true, want false")
	}
	if len(out) != 1 {
		t.Fatalf("len(out) = %d, want 1", len(out))
	}
	if got := out[0].Text; got != "xAB" {
		t.Fatalf("out[0].Text = %q, want %q", got, "xAB")
	}
}

func TestPipelineSupportsOneToMany(t *testing.T) {
	registry := NewRegistry()
	registry.Register(testFactory{
		name:     "split",
		priority: 10,
		newFn: func(Context) (Transformer, error) {
			return testTransformer{
				transformFn: func(item Item) (Result, error) {
					return Result{
						Items: []Item{
							{Kind: ItemKindTextSegment, Text: item.Text + "-1"},
							{Kind: ItemKindTextSegment, Text: item.Text + "-2", IsEnd: item.IsEnd},
						},
					}, nil
				},
			}, nil
		},
	})

	pipeline, err := registry.Open(Context{})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	out, stop, err := pipeline.Push(Item{Kind: ItemKindTextDelta, Text: "x", IsEnd: true})
	if err != nil {
		t.Fatalf("Push() error = %v", err)
	}
	if stop {
		t.Fatalf("Push() stop = true, want false")
	}
	if len(out) != 2 {
		t.Fatalf("len(out) = %d, want 2", len(out))
	}
	if got := out[0].Text; got != "x-1" {
		t.Fatalf("out[0].Text = %q, want %q", got, "x-1")
	}
	if got := out[1].Text; got != "x-2" {
		t.Fatalf("out[1].Text = %q, want %q", got, "x-2")
	}
	if !out[1].IsEnd {
		t.Fatalf("out[1].IsEnd = false, want true")
	}
}
