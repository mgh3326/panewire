package panewire

// hk 6822, scope decided by panewire-design (result-identity-6822-decision-2):
// a result_id changes nothing on a lane that is not inbox-only. The golden file
// was written by this transcript on the base commit c9018032, where no
// completion carried the field. A change that means to alter how a typed lane
// is delivered to updates the golden in the same commit.
//
// The transcript names only identifiers the base commit has, so it can be
// captured there again.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const t6822TypedGolden = "testdata/result_identity/typed_lanes.golden"

// t6822TypedTranscript drives a READY and its completions through two typed
// lanes, one READY delivered and one still in flight, restarts the hub, and
// returns everything the lanes, the producer and handoffkeep were given and
// what the hub stored. Clock readings are left out. withResultID sends each
// completion's frame with the field naming its lane's READY.
func t6822TypedTranscript(t *testing.T, withResultID bool) string {
	t.Helper()
	fake, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	h, agent := t3426Hub(t, path, hk, t3426Records)
	var out strings.Builder
	line := func(format string, args ...any) { fmt.Fprintf(&out, format+"\n", args...) }
	drain := func(step string, a *hubAgent) []hubRelayInjectEvent {
		injected := t3426Injected(a)
		for _, d := range injected {
			raw, _ := json.Marshal(d)
			line("%s injected %s", step, raw)
		}
		for {
			select {
			case p := <-a.persisted:
				raw, _ := json.Marshal(p)
				line("%s ack %s", step, raw)
			default:
				return injected
			}
		}
	}
	for _, lane := range []string{"director-1", "worker-3404"} {
		ready := "ready-" + lane
		h.relayLaneEvent(t3426Ready(lane, ready), agent)
		injected := drain(lane+" ready", agent)
		if lane == "director-1" && len(injected) == 1 {
			if pending, ok := h.acknowledgeRelayPending("host-a", relayAckPayload{JobID: injected[0].JobID, Pane: injected[0].Pane}); ok {
				h.markRelayEventDelivered(pending)
			}
		}
		for _, step := range []string{"first", "resend", "second"} {
			file := "00001-job.completed.json"
			if step == "second" {
				file = "00002-job.completed.json"
			}
			frame := map[string]any{"job_id": "hk3404-review3-fix", "epoch": 1, "agent_label": "worker-3404", "owner_lane": lane, "label": "worker-3404", "host": "host-b", "report_path": "/jobs/" + lane + "/" + file + ".md", "report_last_line": "READY PR green", "question": "which order?", "cause": "job:hk3404-review3-fix", "event_id": file}
			if withResultID {
				frame["result_id"] = ready
			}
			raw, _ := json.Marshal(frame)
			completion, ok := decodeHubJobCompletionPayload(raw)
			if !ok {
				t.Fatalf("frame refused: %s", raw)
			}
			h.relayJobCompletionFrom("host-a", completion)
			drain(lane+" "+step, agent)
		}
	}
	h.Close()
	h2, agent2 := t3426Hub(t, path, hk, t3426Records)
	later := time.Now().Add(h2.inbox.policy.Relay.wakeWindow + time.Second)
	h2.now = func() time.Time { return later }
	h2.flushInboxWakes()
	drain("restart", agent2)

	dump := func(label, query string) {
		rows, err := h2.inbox.store.db.Query(query)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]any, len(columns))
			into := make([]any, len(columns))
			for i := range values {
				into[i] = &values[i]
			}
			if err := rows.Scan(into...); err != nil {
				t.Fatal(err)
			}
			var row []string
			for i, column := range columns {
				row = append(row, fmt.Sprintf("%s=%q", column, fmt.Sprint(values[i])))
			}
			line("%s %s", label, strings.Join(row, " "))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	}
	dump("item", `SELECT id,event_key,lane,kind,class,payload,must_ack,read_at>0 AS is_read,acked_at>0 AS acked,superseded_at>0 AS superseded,superseded_by,due_at>0 AS due,digest_at>0 AS digest,hold_until>0 AS held,delivered_at>0 AS delivered,delivery_stage,delivery_reason,delivery_machine,delivery_pane,relay_id,digest_batch,escalation_batch,sender,event_id,cause FROM lane_inbox ORDER BY id`)
	dump("batch", `SELECT id,lane,text,sent FROM lane_inbox_batches ORDER BY id`)
	dump("obligation", `SELECT id,event_key,relay_id,kind,lane,machine,pane,state,fault,spent,attempts_seen,settle_reason FROM delivery_obligations ORDER BY id`)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, call := range fake.calls {
		delete(call.Body, "event_time")
		raw, _ := json.Marshal(call.Body)
		line("hk %s %s %s", call.Method, call.Path, raw)
	}
	return out.String()
}

// A typed lane gets the READY and every completion byte for byte as on the base,
// whether or not the completion names its READY: the same injections,
// acknowledgements, stored rows and handoffkeep calls, and nothing more after a
// restart. Mutants: merge on a typed lane; keep result_id on its item.
func TestTypedLaneDeliveryIsAsOnTheBase(t *testing.T) {
	want, err := os.ReadFile(t6822TypedGolden)
	if err != nil {
		t.Fatal(err)
	}
	for name, withResultID := range map[string]bool{"with result_id": true, "without result_id": false} {
		t.Run(name, func(t *testing.T) {
			got := t6822TypedTranscript(t, withResultID)
			wantLines, gotLines := strings.Split(string(want), "\n"), strings.Split(got, "\n")
			for i := 0; i < len(wantLines) || i < len(gotLines); i++ {
				var w, g string
				if i < len(wantLines) {
					w = wantLines[i]
				}
				if i < len(gotLines) {
					g = gotLines[i]
				}
				if w != g {
					t.Fatalf("line %d differs from the base (%d lines, base %d)\nbase: %s\n got: %s", i+1, len(gotLines), len(wantLines), w, g)
				}
			}
		})
	}
}
