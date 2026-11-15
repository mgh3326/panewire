package fleet

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeDecisionLedger writes a job's events in sequence order, each with its own
// created_at, so the attempt boundary is deterministic.
func writeDecisionLedger(t *testing.T, inbox, job string, events ...map[string]any) {
	t.Helper()
	dir := filepath.Join(inbox, "jobs", job, "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for index, event := range events {
		data, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Join(dir, fmt.Sprintf("%05d-%s.json", index+1, event["kind"]))
		if err := os.WriteFile(name, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

// writeLaneEvent writes one lane event file and stamps its mtime, which is the
// time the question was asked.
func writeLaneEvent(t *testing.T, inbox, name string, at time.Time, event map[string]any) {
	t.Helper()
	dir := filepath.Join(inbox, "events-lane")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, at, at); err != nil {
		t.Fatal(err)
	}
}

func laneAsk(at time.Time, id, notifyType, text string) map[string]any {
	return map[string]any{"kind": "lane.event", "pane_id": "w1:p1", "event_id": id, "notify_type": notifyType, "text": text, "created_at": at.UTC().Format(time.RFC3339Nano)}
}

// The node decides an open question from the lane-event file itself, so the
// documented --type decision form (which never starts with ESC) counts, and a
// question the watcher has not copied yet still holds the pane.
func TestOpenLaneQuestion(t *testing.T) {
	now := time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)
	spawned := func(pane string, at time.Time) map[string]any {
		return map[string]any{"kind": "job.spawned", "created_at": at.UTC().Format(time.RFC3339Nano), "payload": map[string]any{"pane_id": pane}}
	}
	cases := []struct {
		name   string
		ledger []map[string]any
		lane   []struct {
			name string
			at   time.Time
			body map[string]any
		}
		wantOpen bool
		wantErr  bool
	}{
		{
			name:   "a --type decision question with no ESC text is open",
			ledger: []map[string]any{spawned("w1:p1", now.Add(-time.Hour))},
			lane: []struct {
				name string
				at   time.Time
				body map[string]any
			}{{"0001.json", now.Add(-time.Minute), laneAsk(now.Add(-time.Minute), "q1", "decision", "Which finding should I keep?")}},
			wantOpen: true,
		},
		{
			name:   "an ESC text question with no notify type is open",
			ledger: []map[string]any{spawned("w1:p1", now.Add(-time.Hour))},
			lane: []struct {
				name string
				at   time.Time
				body map[string]any
			}{{"0001.json", now.Add(-time.Minute), map[string]any{"kind": "lane.event", "pane_id": "w1:p1", "event_id": "q1", "text": "[x] ESC: keep both?"}}},
			wantOpen: true,
		},
		{
			name:   "a blocked question is open",
			ledger: []map[string]any{spawned("w1:p1", now.Add(-time.Hour))},
			lane: []struct {
				name string
				at   time.Time
				body map[string]any
			}{{"0001.json", now.Add(-time.Minute), laneAsk(now.Add(-time.Minute), "q1", "blocked", "stuck on a person")}},
			wantOpen: true,
		},
		{
			name:   "a later lane event from the same pane answers it",
			ledger: []map[string]any{spawned("w1:p1", now.Add(-time.Hour))},
			lane: []struct {
				name string
				at   time.Time
				body map[string]any
			}{
				{"0001.json", now.Add(-2 * time.Minute), laneAsk(now.Add(-2*time.Minute), "q1", "decision", "Which?")},
				{"0002.json", now.Add(-time.Minute), laneAsk(now.Add(-time.Minute), "p2", "progress", "kept both")},
			},
		},
		{
			name: "a question from before a new attempt is closed",
			ledger: []map[string]any{
				spawned("w1:p1", now.Add(-2*time.Hour)),
				map[string]any{"kind": "job.reprompted", "created_at": now.Add(-time.Hour).Format(time.RFC3339Nano)},
			},
			lane: []struct {
				name string
				at   time.Time
				body map[string]any
			}{{"0001.json", now.Add(-90 * time.Minute), laneAsk(now.Add(-90*time.Minute), "q1", "decision", "old question")}},
		},
		{
			name:   "a question older than the decision timeout is closed",
			ledger: []map[string]any{spawned("w1:p1", now.Add(-8*time.Hour))},
			lane: []struct {
				name string
				at   time.Time
				body map[string]any
			}{{"0001.json", now.Add(-7 * time.Hour), laneAsk(now.Add(-7*time.Hour), "q1", "decision", "answered by silence")}},
		},
		{
			name: "a job.decision_resumed naming the question closes it",
			ledger: []map[string]any{
				spawned("w1:p1", now.Add(-time.Hour)),
				map[string]any{"kind": "job.decision_resumed", "created_at": now.Add(-30 * time.Second).Format(time.RFC3339Nano), "payload": map[string]any{"event_id": "q1"}},
			},
			lane: []struct {
				name string
				at   time.Time
				body map[string]any
			}{{"0001.json", now.Add(-time.Minute), laneAsk(now.Add(-time.Minute), "q1", "decision", "Which?")}},
		},
		{
			// The watcher stamps a resume at write time, which can land after a
			// newer question's mtime; it must answer only the question it names.
			name: "a resume for an older question does not close a newer one",
			ledger: []map[string]any{
				spawned("w1:p1", now.Add(-time.Hour)),
				map[string]any{"kind": "job.decision_resumed", "created_at": now.Add(-30 * time.Second).Format(time.RFC3339Nano), "payload": map[string]any{"event_id": "q1"}},
			},
			lane: []struct {
				name string
				at   time.Time
				body map[string]any
			}{
				{"0001.json", now.Add(-2 * time.Minute), laneAsk(now.Add(-2*time.Minute), "q1", "decision", "First?")},
				{"0002.json", now.Add(-time.Minute), laneAsk(now.Add(-time.Minute), "q2", "decision", "Second?")},
			},
			wantOpen: true,
		},
		{
			name:   "a lane event from another pane is not this job's question",
			ledger: []map[string]any{spawned("w1:p1", now.Add(-time.Hour))},
			lane: []struct {
				name string
				at   time.Time
				body map[string]any
			}{{"0001.json", now.Add(-time.Minute), map[string]any{"kind": "lane.event", "pane_id": "w9:p9", "event_id": "q1", "notify_type": "decision", "text": "Which?"}}},
		},
		{
			name:    "a malformed ledger is an error",
			ledger:  []map[string]any{{"kind": "job.spawned", "payload": map[string]any{"pane_id": "w1:p1"}}},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inbox := t.TempDir()
			events := tc.ledger
			if tc.wantErr {
				dir := filepath.Join(inbox, "jobs", "job-a", "events")
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "00001-job.spawned.json"), []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			} else if len(events) > 0 {
				writeDecisionLedger(t, inbox, "job-a", events...)
			}
			for _, lane := range tc.lane {
				writeLaneEvent(t, inbox, lane.name, lane.at, lane.body)
			}
			id, open, err := OpenLaneQuestion(inbox, "job-a", "w1:p1", now, DefaultDecisionTimeout)
			if (err != nil) != tc.wantErr || open != tc.wantOpen {
				t.Fatalf("id=%q open=%v err=%v, want open=%v err=%v", id, open, err, tc.wantOpen, tc.wantErr)
			}
			if open && id == "" {
				t.Fatal("an open question has no id")
			}
		})
	}
}

// A job whose newest spawned pane is another pane cannot answer for this one.
func TestOpenLaneQuestionRefusesAnotherPane(t *testing.T) {
	inbox := t.TempDir()
	writeDecisionLedger(t, inbox, "job-a", map[string]any{"kind": "job.spawned", "created_at": time.Now().UTC().Format(time.RFC3339Nano), "payload": map[string]any{"pane_id": "w1:p2"}})
	if _, _, err := OpenLaneQuestion(inbox, "job-a", "w1:p1", time.Now(), DefaultDecisionTimeout); err != ErrJobPaneChanged {
		t.Fatalf("err=%v, want ErrJobPaneChanged", err)
	}
}

// A node whose inbox never recorded a lane event has no events-lane directory:
// that is "no events", not an error, or every guarded close would fail. A
// record that cannot be parsed is unreadable, and unreadable fails closed.
func TestOpenLaneQuestionLaneDirEdgeCases(t *testing.T) {
	now := time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)
	ledger := map[string]any{"kind": "job.spawned", "created_at": now.Add(-time.Hour).Format(time.RFC3339Nano), "payload": map[string]any{"pane_id": "w1:p1"}}

	inbox := t.TempDir()
	writeDecisionLedger(t, inbox, "job-a", ledger)
	if _, open, err := OpenLaneQuestion(inbox, "job-a", "w1:p1", now, DefaultDecisionTimeout); err != nil || open {
		t.Fatalf("missing events-lane: open=%v err=%v, want no question", open, err)
	}

	// A job with no events folder at all — a launch that never committed —
	// is an empty ledger too, not an error that refuses every close.
	empty := t.TempDir()
	if _, open, err := OpenLaneQuestion(empty, "job-a", "w1:p1", now, DefaultDecisionTimeout); err != nil || open {
		t.Fatalf("no job events: open=%v err=%v, want no question", open, err)
	}

	dir := filepath.Join(inbox, "events-lane")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(dir, "0001.json")
	if err := os.WriteFile(bad, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(bad, now, now); err != nil {
		t.Fatal(err)
	}
	if _, _, err := OpenLaneQuestion(inbox, "job-a", "w1:p1", now, DefaultDecisionTimeout); err == nil {
		t.Fatal("a malformed lane event must fail closed")
	}
}
