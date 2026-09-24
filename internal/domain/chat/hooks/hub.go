package hooks

import (
	"context"
	"fmt"

	pkghooks "xiaozhi-esp32-server-golang/internal/pkg/hooks"
	log "xiaozhi-esp32-server-golang/logger"
)

type SyncHandler func(Context, any) (any, bool, error)
type AsyncHandler func(Context, any)

type Hub struct {
	hub        *pkghooks.Hub
	lifecycles []Lifecycle
}

func NewHub(parent context.Context, opts ...pkghooks.HubOption) *Hub {
	return &Hub{hub: pkghooks.NewHub(parent, opts...)}
}

func (h *Hub) Close() {
	if h == nil || h.hub == nil {
		return
	}
	h.hub.Close()
	for i := len(h.lifecycles) - 1; i >= 0; i-- {
		if err := h.lifecycles[i].Close(); err != nil {
			log.Warnf("hook lifecycle close failed: %v", err)
		}
	}
}

func (h *Hub) InitLifecycle(ctx context.Context, lc Lifecycle) error {
	if h == nil || lc == nil {
		return nil
	}
	if err := lc.Init(ctx); err != nil {
		return err
	}
	h.lifecycles = append(h.lifecycles, lc)
	return nil
}

func (h *Hub) Stats() pkghooks.Stats {
	if h == nil || h.hub == nil {
		return pkghooks.Stats{}
	}
	return h.hub.Stats()
}

func (h *Hub) PluginMetas() []pkghooks.PluginMeta {
	if h == nil || h.hub == nil {
		return nil
	}
	return h.hub.PluginMetas()
}

func (h *Hub) Emit(event string, ctx Context, payload any) (any, bool, error) {
	if h == nil || h.hub == nil {
		return payload, false, nil
	}
	return h.hub.Emit(event, toPlatformContext(ctx), payload)
}

func EmitTyped[T any](h *Hub, event string, ctx Context, payload T) (T, bool, error) {
	if h == nil || h.hub == nil {
		return payload, false, nil
	}

	out, stop, err := h.Emit(event, ctx, payload)
	if typed, ok := out.(T); ok {
		return typed, stop, err
	}
	return payload, stop, err
}

func (h *Hub) EmitASROutput(ctx Context, data ASROutputData) (ASROutputData, bool, error) {
	return EmitTyped(h, EventChatASROutput, ctx, data)
}

func (h *Hub) EmitLLMInput(ctx Context, data LLMInputData) (LLMInputData, bool, error) {
	return EmitTyped(h, EventChatLLMInput, ctx, data)
}

func (h *Hub) EmitLLMOutputRaw(ctx Context, data LLMOutputRawData) (LLMOutputRawData, bool, error) {
	return EmitTyped(h, EventChatLLMOutputRaw, ctx, data)
}

func (h *Hub) EmitTTSInput(ctx Context, data TTSInputData) (TTSInputData, bool, error) {
	return EmitTyped(h, EventChatTTSInput, ctx, data)
}

func (h *Hub) EmitTTSOutputStart(ctx Context) error {
	_, _, err := h.Emit(EventChatTTSOutputStart, ctx, TTSOutputStartData{})
	return err
}

func (h *Hub) EmitTTSOutputStop(ctx Context, data TTSOutputStopData) error {
	_, _, err := h.Emit(EventChatTTSOutputStop, ctx, data)
	return err
}

func (h *Hub) EmitMetric(ctx Context, data MetricData) error {
	_, _, err := h.Emit(EventChatMetric, ctx, data)
	return err
}

func (h *Hub) RegisterSync(event, name string, priority int, handler SyncHandler) {
	if err := h.RegisterInterceptor(event, PluginMeta{Name: name, Priority: priority}, handler); err != nil {
		panic(err)
	}
}

func (h *Hub) RegisterInterceptor(event string, meta PluginMeta, handler SyncHandler) error {
	if h == nil || h.hub == nil {
		return nil
	}
	if err := ValidateEventKind(event, PluginKindInterceptor); err != nil {
		return err
	}
	h.hub.RegisterSyncMeta(event, toPkgMeta(meta, pkghooks.PluginKindInterceptor, event), func(ctx pkghooks.Context, payload any) (any, bool, error) {
		return handler(fromPkgContext(ctx), payload)
	})
	return nil
}

func (h *Hub) RegisterAsync(event, name string, priority int, handler AsyncHandler) {
	if err := h.RegisterObserver(event, PluginMeta{Name: name, Priority: priority}, handler); err != nil {
		panic(err)
	}
}

func (h *Hub) RegisterObserver(event string, meta PluginMeta, handler AsyncHandler) error {
	if h == nil || h.hub == nil {
		return nil
	}
	if err := ValidateEventKind(event, PluginKindObserver); err != nil {
		return err
	}
	h.hub.RegisterAsyncMeta(event, toPkgMeta(meta, pkghooks.PluginKindObserver, event), func(ctx pkghooks.Context, payload any) {
		handler(fromPkgContext(ctx), payload)
	})
	return nil
}

func RegisterBuiltinPlugins(hub *Hub, overrides map[string]BuiltinPluginConfig) error {
	if hub == nil {
		return nil
	}
	for _, reg := range BuiltinRegistrations() {
		if override, ok := overrides[reg.Meta.Name]; ok {
			if override.Enabled != nil {
				reg.Meta.Enabled = *override.Enabled
			}
			if override.Priority != 0 {
				reg.Meta.Priority = override.Priority
			}
		}
		if !reg.Meta.Enabled {
			continue
		}
		if reg.Lifecycle != nil {
			if err := hub.InitLifecycle(context.Background(), reg.Lifecycle); err != nil {
				return fmt.Errorf("init lifecycle for %s: %w", reg.Meta.Name, err)
			}
		}
		if reg.Register != nil {
			if err := reg.Register(hub, reg.Meta); err != nil {
				return fmt.Errorf("register builtin plugin %s: %w", reg.Meta.Name, err)
			}
		}
	}
	return nil
}

func toPkgMeta(meta PluginMeta, kind pkghooks.PluginKind, event string) pkghooks.PluginMeta {
	return pkghooks.PluginMeta{Name: meta.Name, Priority: meta.Priority, Kind: kind, Stage: event, Enabled: meta.Enabled}
}

func toPlatformContext(ctx Context) pkghooks.Context {
	return pkghooks.Context{Ctx: ctx.Ctx, Meta: ctx}
}

func fromPkgContext(ctx pkghooks.Context) Context {
	chatCtx, ok := ctx.Meta.(Context)
	if !ok {
		return Context{Ctx: ctx.Ctx}
	}
	if chatCtx.Ctx == nil {
		chatCtx.Ctx = ctx.Ctx
	}
	return chatCtx
}
