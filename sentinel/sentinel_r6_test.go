package sentinel

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestR6AlertCallbackRunsAfterClaim(t *testing.T) {
	now := time.Date(2026, 8, 31, 3, 0, 0, 0, time.UTC)
	settings, err := ParseConfig([]byte(`{
  "version": "r6-fixture",
  "consecutive_observations": 1,
  "nodes": {
    "node-a": {"checks": []},
    "node-b": {"threshold": "30s", "checks": []}
  }
}`), "node-a")
	if err != nil {
		t.Fatal(err)
	}
	remote := &r6Remote{heartbeats: []Heartbeat{{MachineID: "node-b", SeenAt: now.Add(-time.Minute), Checks: map[string]CheckStatus{}, Version: "r6-fixture"}}}
	callback := make(chan Alert, 1)
	service, err := NewService(ServiceConfig{
		MachineID: "node-a", Settings: settings, Remote: remote, Notifier: r6Notifier{}, Now: func() time.Time { return now },
		OnAlert: func(alert Alert) { callback <- alert },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Evaluate(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case alert := <-callback:
		if alert.MachineID != "node-b" || alert.Reason != "stale" {
			t.Fatalf("alert=%+v", alert)
		}
	default:
		t.Fatal("claimed sentinel alert was not relayed to the callback")
	}
}

type r6Remote struct {
	mu         sync.Mutex
	heartbeats []Heartbeat
	claimed    bool
}

func (remote *r6Remote) UpsertHeartbeat(context.Context, Heartbeat) error { return nil }

func (remote *r6Remote) ListHeartbeats(context.Context) ([]Heartbeat, error) {
	return append([]Heartbeat(nil), remote.heartbeats...), nil
}

func (remote *r6Remote) ClaimAlert(context.Context, AlertClaim) (bool, error) {
	remote.mu.Lock()
	defer remote.mu.Unlock()
	if remote.claimed {
		return false, nil
	}
	remote.claimed = true
	return true, nil
}

func (remote *r6Remote) MarkAlertDelivered(context.Context, AlertClaim) error { return nil }

type r6Notifier struct{}

func (r6Notifier) Send(context.Context, Alert) error { return nil }
