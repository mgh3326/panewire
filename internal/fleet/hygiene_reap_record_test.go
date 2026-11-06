package fleet

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

// reapOrderStore is a job store that notes which ledger events exist when the
// hk record is written, so a test can prove the write precedes job.reaped.
type reapOrderStore struct {
	*fakeJobStore
	a          *App
	kindsAtPut [][]string
}

func (s *reapOrderStore) Put(ctx context.Context, id string, fields map[string]any) error {
	evs, _ := s.a.events(id)
	kinds := make([]string, 0, len(evs))
	for _, ev := range evs {
		kinds = append(kinds, ev.Kind)
	}
	s.kindsAtPut = append(s.kindsAtPut, kinds)
	return s.fakeJobStore.Put(ctx, id, fields)
}

// reapEndedJob seeds job j (hk record in hkState) with the given terminal
// events after claim and spawn, lists its pane as present and idle, and reaps
// it. It returns the app, the store and the herdr calls reap made.
func reapEndedJob(t *testing.T, hkState string, putErr error, terminal ...string) (*App, *reapOrderStore, []string) {
	t.Helper()
	a := testApp(t)
	f := ledgerRunner(t, a)
	base := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			return Result{Out: `{"result":{"agents":[{"pane_id":"w:p1","tab_id":"w:t1","name":"agent-a","agent_status":"idle"}]}}`}
		}
		return base(c)
	}
	store := &reapOrderStore{fakeJobStore: a.JobStore.(*fakeJobStore), a: a}
	a.JobStore = store
	if err := store.Claim(t.Context(), "j", "42", "test-host"); err != nil {
		t.Fatal(err)
	}
	store.records["j"]["state"] = hkState
	store.putErr = putErr
	seedJob(t, a, "j", "reviewer")
	for _, kind := range terminal {
		putEvent(t, a, "j", kind, map[string]any{})
	}
	reapJob(t, a)
	var closed []string
	for _, c := range f.Calls {
		if line := strings.Join(c.Args, " "); strings.HasPrefix(line, "pane close") {
			closed = append(closed, line)
		}
	}
	return a, store, closed
}

func TestReapMarksARevokedJobLostBeforeJobReaped(t *testing.T) {
	for _, kind := range []string{"job.revoked", "job.lost"} {
		for _, hkState := range []string{"claimed", "spawned"} {
			t.Run(kind+" "+hkState, func(t *testing.T) {
				a, store, closed := reapEndedJob(t, hkState, nil, kind)
				if state := store.record(t, "j")["state"]; state != "lost" {
					t.Fatalf("hk state = %v, want lost", state)
				}
				if len(store.kindsAtPut) != 1 || slices.Contains(store.kindsAtPut[0], "job.reaped") {
					t.Fatalf("hk write did not precede job.reaped: %v", store.kindsAtPut)
				}
				if !slices.Contains(eventKindsOf(t, a, "j"), "job.reaped") || len(closed) != 1 {
					t.Fatalf("reap incomplete: events %v, closed %v", eventKindsOf(t, a, "j"), closed)
				}
			})
		}
	}
}

func TestReapWritesNoRecordForAJobThatCompleted(t *testing.T) {
	a, store, _ := reapEndedJob(t, "spawned", nil, "job.revoked", "job.completed")
	if len(store.kindsAtPut) != 0 || store.record(t, "j")["state"] != "spawned" {
		t.Fatalf("a completed job's record was written: puts %v, state %v", store.kindsAtPut, store.record(t, "j")["state"])
	}
	if !slices.Contains(eventKindsOf(t, a, "j"), "job.reaped") {
		t.Fatalf("job not reaped: %v", eventKindsOf(t, a, "j"))
	}
}

// A job revoked and then claimed again is live; its later completion wins over
// the older revoke.
func TestReapWritesNoRecordWhenACompletionFollowsTheRevoke(t *testing.T) {
	_, store, _ := reapEndedJob(t, "spawned", nil, "job.revoked", "job.reclaim", "job.completed")
	if len(store.kindsAtPut) != 0 {
		t.Fatalf("record written: %v", store.kindsAtPut)
	}
}

func TestReapLeavesAFinishedRecordAlone(t *testing.T) {
	for _, hkState := range []string{"completed", "lost"} {
		t.Run(hkState, func(t *testing.T) {
			_, store, _ := reapEndedJob(t, hkState, nil, "job.revoked")
			if len(store.kindsAtPut) != 0 || store.record(t, "j")["state"] != hkState {
				t.Fatalf("a %s record was rewritten: puts %v", hkState, store.kindsAtPut)
			}
		})
	}
}

func TestReapContinuesWhenTheRecordWriteFails(t *testing.T) {
	a, _, closed := reapEndedJob(t, "spawned", errors.New("hk down"), "job.revoked")
	kinds := eventKindsOf(t, a, "j")
	if !slices.Contains(kinds, "job.record_failed") || !slices.Contains(kinds, "job.reaped") || len(closed) != 1 {
		t.Fatalf("events %v, closed %v", kinds, closed)
	}
	if !strings.Contains(a.Err.(interface{ String() string }).String(), "job record reap write failed") {
		t.Fatalf("no warning: %q", a.Err.(interface{ String() string }).String())
	}
}

func TestReapContinuesWhenTheRecordCannotBeRead(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	store := a.JobStore.(*fakeJobStore)
	store.getErr = errors.New("hk down")
	seedJob(t, a, "j", "reviewer")
	putEvent(t, a, "j", "job.revoked", map[string]any{})
	reapJob(t, a)
	kinds := eventKindsOf(t, a, "j")
	if !slices.Contains(kinds, "job.record_failed") || !slices.Contains(kinds, "job.reaped") {
		t.Fatalf("events %v", kinds)
	}
}
