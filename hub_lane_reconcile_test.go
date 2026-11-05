package panewire

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLaneReconcileBackoff(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	calls := 0
	r := laneReconciler{list: func(context.Context) (map[string]bool, error) { return map[string]bool{}, nil }, register: func(context.Context, HubActiveJob) error { calls++; return errors.New("refused") }}
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
		list: func(context.Context) (map[string]bool, error) {
			if !available {
				return nil, errors.New("unavailable")
			}
			if registered {
				return map[string]bool{"lane": true}, nil
			}
			return map[string]bool{}, nil
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
