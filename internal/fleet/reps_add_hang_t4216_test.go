package fleet

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const mergewakeRevisionNotes4216 = `{"source":"mergewake","schema":1,"sample_key":"https://github.com/mgh3326/mergewake/pull/103","outcome_id":"f75f67c51c6f44582bf779027a5d809a5ae5b4067563759d5589dfdeb947a8ff","revision":6,"supersedes":"b08fdd2e2abd485227419a1e2558363fc485fa1dd46d887bdbdb6941d84c6f1e","first_ci_green":true,"findings_by_severity":{"medium":1},"reverted_within_7d":false,"time_to_merge_s":1200,"same_family":true,"experiment_id":"task-4216","arm":"baseline","evidence":[],"outcome_rule":"merged","outcome":{},"observed_findings_by_severity":{"medium":1},"followup":true}`

type stalledRepsHK struct {
	puts    chan []byte
	release chan struct{}
}

func (s *stalledRepsHK) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/bench/reps":
		http.Error(w, `{"error":"unexpected_list_preflight"}`, http.StatusNotImplemented)
	case r.Method == http.MethodPut && r.URL.Path == "/v1/bench/reps":
		body, _ := io.ReadAll(r.Body)
		select {
		case s.puts <- body:
		default:
		}
		select {
		case <-r.Context().Done():
		case <-s.release:
			http.Error(w, `{"error":"released_test_stall"}`, http.StatusServiceUnavailable)
		}
	default:
		http.Error(w, `{"error":"unexpected"}`, http.StatusNotImplemented)
	}
}

func TestRepsAddAndAddOutcomeBoundStalledHKCallsT4216(t *testing.T) {
	priorTimeout := hkCallTimeout
	hkCallTimeout = 75 * time.Millisecond
	t.Cleanup(func() { hkCallTimeout = priorTimeout })

	for _, tc := range []struct {
		name string
		call func(*testing.T, *BenchStore) func() error
	}{
		{
			name: "live-shaped reps add revision superseding an old record",
			call: func(t *testing.T, store *BenchStore) func() error {
				a := testApp(t)
				a.BenchCatalog = func(context.Context) ([]CatalogRow, error) {
					return []CatalogRow{{Profile: "opus", Effort: "high", Grade: "A", ModelID: "opus"}}, nil
				}
				a.BenchStore = store
				return func() error {
					return a.repsAdd(context.Background(), []string{
						"--profile", "opus", "--model", "opus", "--effort", "high",
						"--task", "https://github.com/mgh3326/mergewake/pull/103",
						"--tier", "T2", "--role", "impl", "--grade", "A",
						"--rounds", "1", "--blockers-found", "0", "--completed", "1",
						"--notes", mergewakeRevisionNotes4216,
					}, io.Discard)
				}
			},
		},
		{
			name: "stable automatic outcome origin",
			call: func(_ *testing.T, store *BenchStore) func() error {
				return func() error {
					_, _, err := store.AddOutcome(context.Background(), Rep{
						OriginID: stableRepOriginID("panewire-outcome:old-pr-job"),
						Profile:  "opus", ModelID: "opus", TaskRef: "hk570",
						Tier: "T2", Role: "impl", Grade: "A", Effort: "high",
						RecordedAt: "2026-11-12T03:23:18Z",
					})
					return err
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stall := &stalledRepsHK{puts: make(chan []byte, 1), release: make(chan struct{})}
			srv := httptest.NewServer(stall)
			var stopOnce sync.Once
			stop := func() { stopOnce.Do(func() { close(stall.release); srv.Close() }) }
			t.Cleanup(stop)
			store := &BenchStore{hk: &hkQuota{base: srv.URL, token: "test-token", client: &http.Client{Transport: http.DefaultTransport}}, now: time.Now}
			call := tc.call(t, store)
			done := make(chan error, 1)
			go func() { done <- call() }()

			select {
			case body := <-stall.puts:
				if strings.Contains(tc.name, "live-shaped") {
					var request struct {
						Reps []map[string]any `json:"reps"`
					}
					if err := json.Unmarshal(body, &request); err != nil || len(request.Reps) != 1 {
						t.Fatalf("PUT body = %s (%v)", body, err)
					}
					rep := request.Reps[0]
					if origin, _ := rep["origin_id"].(float64); origin <= 0 {
						t.Fatalf("panewire did not generate an origin id: %v", rep["origin_id"])
					}
					var notes map[string]any
					if err := json.Unmarshal([]byte(rep["notes"].(string)), &notes); err != nil || notes["outcome_id"] != "f75f67c51c6f44582bf779027a5d809a5ae5b4067563759d5589dfdeb947a8ff" || notes["supersedes"] != "b08fdd2e2abd485227419a1e2558363fc485fa1dd46d887bdbdb6941d84c6f1e" {
						t.Fatalf("PUT notes = %v (%v)", notes, err)
					}
				}
			case err := <-done:
				t.Fatalf("call returned before stalled PUT: %v", err)
			case <-time.After(time.Second):
				t.Fatal("test did not reach stalled hk PUT")
			}

			select {
			case err := <-done:
				if err == nil || !strings.Contains(strings.ToLower(err.Error()), "timed out") {
					t.Fatalf("stalled hk call error = %v, want a clear timeout", err)
				}
			case <-time.After(500 * time.Millisecond):
				stop()
				<-done
				t.Fatal("stalled hk call exceeded the 500ms bound")
			}
		})
	}
}

func TestRepAddNewOutcomeStillRecordsWithFreshOriginT4216(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	a := benchFakeApp(t, srv)
	out := &strings.Builder{}
	if err := a.repsAdd(context.Background(), []string{
		"--profile", "opus", "--model", "opus", "--effort", "high",
		"--task", "https://github.com/mgh3326/mergewake/pull/103",
		"--tier", "T2", "--role", "impl", "--grade", "A",
		"--rounds", "1", "--blockers-found", "0", "--completed", "1",
		"--notes", mergewakeRevisionNotes4216,
	}, out); err != nil {
		t.Fatal(err)
	}
	if len(fake.reps) != 1 || !strings.HasPrefix(out.String(), "recorded rep id=srv:") {
		t.Fatalf("new outcome rows=%d receipt=%q", len(fake.reps), out.String())
	}
	if origin, ok := fake.reps[0]["origin_id"].(float64); !ok || origin <= 0 {
		t.Fatalf("new outcome origin_id = %v", fake.reps[0]["origin_id"])
	}
}

func TestRepsAddConflictReturnsClearErrorOnceT4226(t *testing.T) {
	var calls atomic.Int32
	var origin atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/v1/bench/reps" {
			http.Error(w, `{"error":"unexpected"}`, http.StatusNotFound)
			return
		}
		calls.Add(1)
		var input struct {
			Reps []struct {
				OriginID int64 `json:"origin_id"`
			} `json:"reps"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil || len(input.Reps) != 1 {
			http.Error(w, `{"error":"invalid_rep"}`, http.StatusBadRequest)
			return
		}
		origin.Store(input.Reps[0].OriginID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"error":"bench_rep_conflict"}`)
	}))
	defer srv.Close()

	store := &BenchStore{hk: &hkQuota{base: srv.URL, token: "test-token", client: &http.Client{Transport: http.DefaultTransport}}, now: time.Now, pendingPath: filepath.Join(t.TempDir(), "reps.json")}
	a := &App{BenchStore: store, BenchCatalog: func(context.Context) ([]CatalogRow, error) { return nil, nil }}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := a.repsAdd(ctx, []string{
		"--profile", "opus", "--model", "opus", "--effort", "high",
		"--task", "https://github.com/mgh3326/mergewake/pull/103",
		"--tier", "T2", "--role", "impl", "--grade", "A",
		"--rounds", "1", "--blockers-found", "0", "--completed", "1",
		"--notes", mergewakeRevisionNotes4216,
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "bench_rep_conflict") || !strings.Contains(err.Error(), "fresh origin id") {
		t.Fatalf("reps add conflict error = %v, want clear bench_rep_conflict guidance for new content", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("reps add made %d PUTs after immediate conflict, want exactly one", got)
	}
	if got := origin.Load(); got <= 0 {
		t.Fatalf("reps add origin_id = %d, want generated positive id", got)
	}
}
