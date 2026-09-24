package auth

import "testing"

func TestEnsureSessionReusesPreferredID(t *testing.T) {
	manager := NewAuthManager()

	session, err := manager.EnsureSession("device-1", "preferred-session")
	if err != nil {
		t.Fatalf("EnsureSession returned error: %v", err)
	}
	if session.ID != "preferred-session" {
		t.Fatalf("expected preferred session id, got %q", session.ID)
	}
	if session.DeviceID != "device-1" {
		t.Fatalf("expected device-1, got %q", session.DeviceID)
	}

	reused, err := manager.EnsureSession("device-2", "preferred-session")
	if err != nil {
		t.Fatalf("EnsureSession returned error on reuse: %v", err)
	}
	if reused != session {
		t.Fatal("expected EnsureSession to reuse the existing session object")
	}
	if reused.DeviceID != "device-2" {
		t.Fatalf("expected reused session device id to be refreshed, got %q", reused.DeviceID)
	}
}

func TestEnsureSessionCreatesNewSessionWhenPreferredIDEmpty(t *testing.T) {
	manager := NewAuthManager()

	session, err := manager.EnsureSession("device-1", "")
	if err != nil {
		t.Fatalf("EnsureSession returned error: %v", err)
	}
	if session.ID == "" {
		t.Fatal("expected generated session id")
	}
	if session.DeviceID != "device-1" {
		t.Fatalf("expected device-1, got %q", session.DeviceID)
	}
}
