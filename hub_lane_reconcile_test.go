package panewire

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/coder/websocket"
	"testing"
	"time"
)

func TestLaneReconcileBackoff(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	calls := 0
	r := laneReconciler{list: func(context.Context) (map[string]hubLaneProjection, error) {
		return map[string]hubLaneProjection{}, nil
	}, register: func(context.Context, HubActiveJob) error { calls++; return errors.New("refused") }}
	jobs := []HubActiveJob{{JobID: "job", AgentLabel: "lane", Pane: "pane"}}
	for i := 0; i < 7; i++ {
		r.pass(context.Background(), jobs, now.Add(time.Duration(i)*5*time.Minute))
	}
	if calls != 3 {
		t.Fatalf("attempts before backoff expires = %d, want 3", calls)
	}
	r.pass(context.Background(), jobs, now.Add(40*time.Minute))
	if calls != 4 {
		t.Fatalf("attempts at backoff expiry = %d, want 4", calls)
	}
}

func TestLaneReconcileHubAnswerAndPendingReceipt(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	available := false
	registered := false
	calls := 0
	r := laneReconciler{
		list: func(context.Context) (map[string]hubLaneProjection, error) {
			if !available {
				return nil, errors.New("unavailable")
			}
			if registered {
				return map[string]hubLaneProjection{"lane": {Pane: "pane"}}, nil
			}
			return map[string]hubLaneProjection{}, nil
		},
		pending:  func(HubActiveJob) bool { return true },
		register: func(context.Context, HubActiveJob) error { calls++; registered = true; return nil },
	}
	job := []HubActiveJob{{JobID: "job", AgentLabel: "lane", Pane: "pane"}}
	r.pass(context.Background(), job, now)
	available = true
	r.pass(context.Background(), job, now.Add(5*time.Minute))
	if calls != 1 {
		t.Fatalf("attempts = %d, want one", calls)
	}
}

func TestLaneReconcileLeavesExistingRoute(t *testing.T) {
	calls := 0
	r := laneReconciler{list: func(context.Context) (map[string]hubLaneProjection, error) {
		return map[string]hubLaneProjection{"lane": {Machine: "node-a", Pane: "new-pane"}}, nil
	}, pending: func(HubActiveJob) bool { return true }, register: func(context.Context, HubActiveJob) error { calls++; return nil }}
	r.pass(context.Background(), []HubActiveJob{{JobID: "old", AgentLabel: "lane", Pane: "old-pane"}, {JobID: "new", AgentLabel: "lane", Pane: "new-pane"}}, time.Now())
	if calls != 0 {
		t.Fatalf("re-registered an existing lane %d times", calls)
	}
}

func TestLaneReconcileRunStartsAndRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ticks := make(chan time.Time, 2)
	var calls atomic.Int32
	client, err := NewHubClient(HubClientConfig{
		URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: t.TempDir(), AllowInsecureForTests: true,
		SpawnConfigPath: filepath.Join(t.TempDir(), "missing.json"),
		Dial: func(ctx context.Context, _ string, _ *websocket.DialOptions) (*websocket.Conn, *http.Response, error) {
			<-ctx.Done()
			return nil, nil, ctx.Err()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client.laneReconcileTicks = ticks
	client.laneReconcileJobs = func(context.Context) []HubActiveJob {
		return []HubActiveJob{{JobID: "job", AgentLabel: "lane", Pane: "pane"}}
	}
	client.laneReconcileList = func(context.Context) (map[string]hubLaneProjection, error) {
		return map[string]hubLaneProjection{}, nil
	}
	client.laneReconcileRegister = func(context.Context, HubActiveJob) error {
		if calls.Add(1) == 1 {
			return errors.New("refused")
		}
		return nil
	}
	done := make(chan struct{})
	go func() { client.Run(ctx); close(done) }()
	ticks <- time.Now()
	eventually(t, time.Second, "first registration refusal", func() bool { return calls.Load() == 1 })
	ticks <- time.Now()
	eventually(t, time.Second, "registration after refusal", func() bool { return calls.Load() == 2 })
	cancel()
	eventually(t, time.Second, "client shutdown", func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
}

func TestLaneReconcileDoesNotFollowRedirect(t *testing.T) {
	var received atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received.Add(1) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer source.Close()
	client, err := NewHubClient(HubClientConfig{URL: "ws" + strings.TrimPrefix(source.URL, "http"), MachineID: "node-a", Token: "fixture", CFAccessClientID: "fixture-id", CFAccessClientSecret: "fixture-secret", AllowInsecureForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.nodeLanes(context.Background()); err == nil {
		t.Fatal("redirect accepted")
	}
	if received.Load() != 0 {
		t.Fatal("followed redirect to another server")
	}
}
