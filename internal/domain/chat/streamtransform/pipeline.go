package streamtransform

import (
	"fmt"
	"sort"
	"sync"
)

type Registry struct {
	mu        sync.RWMutex
	factories []Factory
}

func NewRegistry() *Registry {
	return &Registry{}
}

func (r *Registry) Register(factory Factory) {
	if r == nil || factory == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	next := make([]Factory, 0, len(r.factories)+1)
	next = append(next, r.factories...)
	next = append(next, factory)
	sort.SliceStable(next, func(i, j int) bool {
		return next[i].Priority() < next[j].Priority()
	})
	r.factories = next
}

func (r *Registry) Open(ctx Context) (*Pipeline, error) {
	if r == nil {
		return &Pipeline{}, nil
	}

	r.mu.RLock()
	factories := append([]Factory(nil), r.factories...)
	r.mu.RUnlock()

	steps := make([]namedTransformer, 0, len(factories))
	for _, factory := range factories {
		transformer, err := factory.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("open transformer %s failed: %w", factory.Name(), err)
		}
		steps = append(steps, namedTransformer{
			name:        factory.Name(),
			transformer: transformer,
		})
	}

	return &Pipeline{steps: steps}, nil
}

type namedTransformer struct {
	name        string
	transformer Transformer
}

type Pipeline struct {
	steps []namedTransformer
}

func (p *Pipeline) Push(item Item) ([]Item, bool, error) {
	if p == nil || len(p.steps) == 0 {
		return []Item{item}, false, nil
	}

	items := []Item{item}
	for _, step := range p.steps {
		next := make([]Item, 0, len(items))
		for _, in := range items {
			result, err := step.transformer.Transform(in)
			if err != nil {
				return nil, false, fmt.Errorf("transformer %s failed: %w", step.name, err)
			}
			if len(result.Items) > 0 {
				next = append(next, result.Items...)
			}
			if result.Stop {
				return next, true, nil
			}
		}
		items = next
	}

	return items, false, nil
}

func (p *Pipeline) Close() error {
	if p == nil {
		return nil
	}

	var firstErr error
	for _, step := range p.steps {
		if step.transformer == nil {
			continue
		}
		if err := step.transformer.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close transformer %s failed: %w", step.name, err)
		}
	}
	return firstErr
}
