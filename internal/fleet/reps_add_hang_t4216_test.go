package fleet

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type stalledRepsHK struct {
	puts    chan struct{}
	release chan struct{}
}

func (s *stalledRepsHK) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/bench/reps":
		writeJSONBody(w, http.StatusOK, map[string]any{"reps": []Rep{}})
	case r.Method == http.MethodPut && r.URL.Path == "/v1/bench/reps":
		select {
		case s.puts <- struct{}{}:
		default:
		}
		<-s.release
		http.Error(w, `{"error":"released_test_stall"}`, http.StatusServiceUnavailable)
	default:
		http.Error(w, `{"error":"unexpected"}`, http.StatusNotImplemented)
	}
}

func TestAddOutcomeAlreadyRecordedSkipsSupersedingPutT4216(t *testing.T) {
	const origin int64 = 4216001
	existing := Rep{ID: 77, OriginID: origin, Profile: "codex-terra61", ModelID: "gpt-6.1-terra",
		TaskRef: "hk570", Tier: "T2", Role: "impl", Grade: "B", Effort: "high", RecordedAt: "2026-11-05T00:00:00Z"}
	putStarted := make(chan struct{}, 1)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/bench/reps" {
			writeJSONBody(w, http.StatusOK, map[string]any{"reps": []Rep{existing}})
			return
		}
		if r.Method == http.MethodPut && r.URL.Path == "/v1/bench/reps" {
			putStarted <- struct{}{}
			<-release
			http.Error(w, `{"error":"released_test_stall"}`, http.StatusServiceUnavailable)
			return
		}
		http.Error(w, `{"error":"unexpected"}`, http.StatusNotImplemented)
	}))
	defer func() {
		close(release)
		srv.Close()
	}()

	store := &BenchStore{hk: &hkQuota{base: srv.URL, token: "test-token"}}
	replayed := existing
	replayed.RecordedAt = "2026-11-12T02:59:46Z"
	replayed.Notes = `{"head":"new-revision","pr":570}`
	done := make(chan struct {
		id       int64
		recorded bool
		err      error
	}, 1)
	go func() {
		id, recorded, err := store.AddOutcome(context.Background(), replayed)
		done <- struct {
			id       int64
			recorded bool
			err      error
		}{id, recorded, err}
	}()

	select {
	case result := <-done:
		if result.err != nil || result.recorded || result.id != 0 {
			t.Fatalf("existing outcome result = id %d, recorded %t, err %v; want already recorded", result.id, result.recorded, result.err)
		}
	case <-putStarted:
		t.Fatal("re-emitted existing outcome reached PUT instead of resolving as already recorded")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("re-emitted existing outcome did not finish within 500ms")
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
			name: "reps add with new random origin",
			call: func(t *testing.T, store *BenchStore) func() error {
				a := testApp(t)
				a.BenchCatalog = func(context.Context) ([]CatalogRow, error) {
					return []CatalogRow{{Profile: "codex-terra61", Effort: "high", Grade: "B", ModelID: "gpt-6.1-terra"}}, nil
				}
				a.BenchStore = store
				return func() error {
					return a.repsAdd(context.Background(), []string{
						"--profile", "codex-terra61", "--model", "gpt-6.1-terra", "--effort", "high",
						"--task", "hk570", "--tier", "T2", "--role", "impl", "--grade", "B",
						"--rounds", "1", "--blockers-found", "0", "--completed", "1",
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
						Profile:  "codex-terra61", ModelID: "gpt-6.1-terra", TaskRef: "hk570",
						Tier: "T2", Role: "impl", Grade: "B", Effort: "high",
						RecordedAt: "2026-11-12T02:59:46Z",
					})
					return err
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stall := &stalledRepsHK{puts: make(chan struct{}, 1), release: make(chan struct{})}
			srv := httptest.NewServer(stall)
			store := &BenchStore{hk: &hkQuota{base: srv.URL, token: "test-token", client: &http.Client{Transport: http.DefaultTransport}}, now: time.Now}
			call := tc.call(t, store)
			done := make(chan error, 1)
			go func() { done <- call() }()
			select {
			case <-stall.puts:
			case err := <-done:
				close(stall.release)
				srv.Close()
				t.Fatalf("call returned before stalled PUT: %v", err)
			case <-time.After(time.Second):
				close(stall.release)
				srv.Close()
				t.Fatal("test did not reach stalled hk PUT")
			}
			select {
			case err := <-done:
				if err == nil || !strings.Contains(strings.ToLower(err.Error()), "timed out") {
					close(stall.release)
					srv.Close()
					t.Fatalf("stalled hk call error = %v, want a clear timeout", err)
				}
			case <-time.After(500 * time.Millisecond):
				close(stall.release)
				srv.Close()
				<-done
				t.Fatal("stalled hk call exceeded the 500ms bound")
			}
			close(stall.release)
			srv.Close()
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
		"--profile", "codex-terra61", "--model", "gpt-6.1-terra", "--effort", "high",
		"--task", "hk570", "--tier", "T2", "--role", "impl", "--grade", "B",
		"--rounds", "1", "--blockers-found", "0", "--completed", "1",
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
