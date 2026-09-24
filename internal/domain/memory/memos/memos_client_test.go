package memos

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cloudwego/eino/schema"
)

func TestAddMessage(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/add/message" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload failed: %v", err)
		}
		if payload["user_id"] == "" || payload["conversation_id"] == "" {
			t.Fatalf("user_id/conversation_id should not be empty: %#v", payload)
		}
		if payload["agent_id"] == "" {
			t.Fatalf("agent_id should exist when agentID is provided: %#v", payload)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"message":"ok"}`))
	}))
	defer ts.Close()

	c, err := GetWithConfig(map[string]interface{}{"base_url": ts.URL})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AddMessage(context.Background(), "agent1", schema.Message{Role: schema.User, Content: "hello"}); err != nil {
		t.Fatal(err)
	}
}

func TestGetMessages(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/get/messages" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		resp := map[string]interface{}{
			"data": map[string]interface{}{
				"messages": []map[string]interface{}{{"role": "user", "content": "u1"}, {"role": "assistant", "content": "a1"}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	c, err := GetWithConfig(map[string]interface{}{"base_url": ts.URL})
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := c.GetMessages(context.Background(), "agent1", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("want 2 messages, got %d", len(msgs))
	}
}

func TestAddMessage_EmptyAgentID(t *testing.T) {
	c, err := GetWithConfig(map[string]interface{}{"base_url": "http://127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	err = c.AddMessage(context.Background(), "", schema.Message{Role: schema.User, Content: "hello"})
	if err == nil {
		t.Fatal("expected error when agentID is empty")
	}
}

func TestSearchPayload(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/search/memory" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload failed: %v", err)
		}
		if payload["user_id"] != "agentX" || payload["conversation_id"] != "agentX" {
			t.Fatalf("identity fields mismatch: %#v", payload)
		}
		if payload["memory_limit_number"] != float64(7) {
			t.Fatalf("memory_limit_number mismatch: %#v", payload)
		}
		if payload["relativity"] != 0.5 {
			t.Fatalf("relativity mismatch: %#v", payload)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"memory_detail_list":[{"memory_value":"abc"}]}}`))
	}))
	defer ts.Close()

	c, err := GetWithConfig(map[string]interface{}{"base_url": ts.URL, "search_threshold": 0.5})
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := c.Search(context.Background(), "agentX", "hello", 7, 0)
	if err != nil {
		t.Fatal(err)
	}
	if ctx == "" || ctx != "- abc" {
		t.Fatalf("unexpected search context: %s", ctx)
	}
}
