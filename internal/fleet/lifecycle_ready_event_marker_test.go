package fleet

// hk 7014: `panewire emit --type ready` inside a job records the READY's
// producer event id in the job dir, and `panewire done` passes it as the
// completion's result_id when --ready-event was not given.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/jobowner"
	"github.com/mgh3326/panewire/internal/laneevent"
)

// recordReady writes the marker the way `panewire emit --type ready` does.
func recordReady(t *testing.T, a *App, job, id string) {
	t.Helper()
	if err := laneevent.WriteReadyEvent(filepath.Join(a.Jobs, job), 1, id); err != nil {
		t.Fatal(err)
	}
}

func resultIDArg(args []string) (string, bool) {
	at := slices.Index(args, "--result-id")
	if at < 0 || at+1 >= len(args) {
		return "", false
	}
	return args[at+1], true
}

// Mutants: drop the marker read, read it for --ready-event too, or send the
// id only on the relay emit and not the job.completed envelope.
func TestDoneUsesRecordedReadyEvent(t *testing.T) {
	for _, watcher := range []bool{false, true} {
		name := "interactive"
		if watcher {
			name = "watcher"
		}
		t.Run(name, func(t *testing.T) {
			r := newDoneRig(t)
			r.write("report body\n")
			recordReady(t, r.a, "j", "j-ready-1")
			var e error
			if watcher {
				e = r.a.lifecycleWithOptions(context.Background(), "done", []string{"j", "--report", r.path}, true, false)
			} else {
				e = r.done()
			}
			if e != nil {
				t.Fatal(e)
			}
			emits := doneRigEmits(r)
			if len(emits) != 1 {
				t.Fatalf("relay emits=%v", emits)
			}
			got, ok := resultIDArg(emits[0])
			if !ok || got != "j-ready-1" {
				t.Fatalf("relay emit=%v, want --result-id j-ready-1", emits[0])
			}
			completed := r.events("job.completed")
			if len(completed) != 1 || completed[0]["result_id"] != "j-ready-1" {
				t.Fatalf("job.completed events=%v", completed)
			}
		})
	}
}

// An explicit --ready-event wins over the recorded one. Mutant: the marker
// overrides the flag.
func TestDoneReadyEventFlagWinsOverRecord(t *testing.T) {
	r := newDoneRig(t)
	r.write("report body\n")
	recordReady(t, r.a, "j", "j-ready-recorded")
	if e := r.done("--ready-event", "j-ready-flag"); e != nil {
		t.Fatal(e)
	}
	emits := doneRigEmits(r)
	if len(emits) != 1 {
		t.Fatalf("relay emits=%v", emits)
	}
	if got, ok := resultIDArg(emits[0]); !ok || got != "j-ready-flag" {
		t.Fatalf("relay emit=%v, want --result-id j-ready-flag", emits[0])
	}
}

// No record means done is exactly as before: no --result-id on the emit and
// no result_id on the envelope. Mutant: send an empty or stale id.
func TestDoneWithoutRecordIsUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		write   bool
	}{
		{name: "no file"},
		{name: "not an event id", content: "bad\nid", write: true},
		{name: "empty", content: "  \n", write: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newDoneRig(t)
			r.write("report body\n")
			if tc.write {
				if err := os.WriteFile(filepath.Join(r.a.Jobs, "j", laneevent.ReadyEventFile), []byte(tc.content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if e := r.done(); e != nil {
				t.Fatal(e)
			}
			emits := doneRigEmits(r)
			if len(emits) != 1 {
				t.Fatalf("relay emits=%v", emits)
			}
			if _, ok := resultIDArg(emits[0]); ok {
				t.Fatalf("relay emit=%v carries --result-id without a record", emits[0])
			}
			completed := r.events("job.completed")
			if len(completed) != 1 {
				t.Fatalf("job.completed events=%v", completed)
			}
			if _, present := completed[0]["result_id"]; present {
				t.Fatalf("job.completed carries result_id without a record: %v", completed[0])
			}
		})
	}
}

// The record pairs one READY with one completion: once a completion claimed
// the recorded id for a different report, a later done goes out on its own
// rather than folding into the READY's inbox item again (hk 7014 review).
// Mutants: reuse the claimed id, or claim it for the same report twice.
func TestDoneRecordedReadyEventIsConsumedOnce(t *testing.T) {
	r := newDoneRig(t)
	r.write("report body\n")
	recordReady(t, r.a, "j", "j-ready-1")
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	r.write("report body v2\n")
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	emits := doneRigEmits(r)
	if len(emits) != 2 {
		t.Fatalf("relay emits=%v", emits)
	}
	if got, ok := resultIDArg(emits[0]); !ok || got != "j-ready-1" {
		t.Fatalf("first emit=%v, want --result-id j-ready-1", emits[0])
	}
	if _, ok := resultIDArg(emits[1]); ok {
		t.Fatalf("second emit=%v reuses the claimed READY id", emits[1])
	}
	completed := r.events("job.completed")
	if len(completed) != 2 || completed[1]["result_id"] != nil {
		t.Fatalf("job.completed events=%v", completed)
	}
}

// A record a superseded or revoked attempt wrote is ignored: the grant moved
// past the epoch the record names. Mutants: skip the epoch check.
func TestDoneIgnoresRecordFromStaleAttempt(t *testing.T) {
	r := newDoneRig(t)
	r.write("report body\n")
	recordReady(t, r.a, "j", "j-ready-stale")
	if err := jobowner.Assign(r.a.Jobs, "j", 2); err != nil {
		t.Fatal(err)
	}
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	emits := doneRigEmits(r)
	if len(emits) != 1 {
		t.Fatalf("relay emits=%v", emits)
	}
	if _, ok := resultIDArg(emits[0]); ok {
		t.Fatalf("relay emit=%v carries a stale attempt's record", emits[0])
	}
}

// putFlatCompleted writes the record `panewire emit --kind job.completed`
// leaves in the job's events dir: the flat emitRecord form, with result_id
// at top level and no payload object.
func putFlatCompleted(t *testing.T, a *App, job, resultID string) {
	t.Helper()
	dir := filepath.Join(a.Jobs, job, "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	record := map[string]any{
		"type": "job.completed", "kind": "job.completed", "job_id": job, "epoch": 1,
		"created_at":  a.Now().Add(time.Duration(len(files)) * time.Nanosecond).Format(time.RFC3339Nano),
		"agent_label": "agent-a", "owner_lane": "owner", "label": "agent-a", "host": "local",
		"report_path": "reports/j/sha256/claimed.md", "pane_id": "w:p1", "result_id": resultID,
	}
	body, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("%04d-job.completed.json", len(files))
	if err := os.WriteFile(filepath.Join(dir, name), body, 0600); err != nil {
		t.Fatal(err)
	}
}

// A completion made through the `emit --kind job.completed` form claims the
// READY too: its record keeps result_id at top level and names the report by
// path, not digest, so the recorded id is consumed and a later done goes out
// on its own (hk 7014 review). Mutant: read only Payload.
func TestDoneRecordedReadyEventClaimedByEmitForm(t *testing.T) {
	r := newDoneRig(t)
	r.write("report body\n")
	recordReady(t, r.a, "j", "j-ready-1")
	putFlatCompleted(t, r.a, "j", "j-ready-1")
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	emits := doneRigEmits(r)
	if len(emits) != 1 {
		t.Fatalf("relay emits=%v", emits)
	}
	if _, ok := resultIDArg(emits[0]); ok {
		t.Fatalf("relay emit=%v reuses an id the emit form already claimed", emits[0])
	}
}

// The record is per job: another job's READY is not this completion's.
// Mutant: read a shared or parent-dir record.
func TestDoneRecordIsPerJob(t *testing.T) {
	r := newDoneRig(t)
	r.write("report body\n")
	if err := os.MkdirAll(filepath.Join(r.a.Jobs, "other-job"), 0700); err != nil {
		t.Fatal(err)
	}
	recordReady(t, r.a, "other-job", "other-ready")
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	emits := doneRigEmits(r)
	if len(emits) != 1 {
		t.Fatalf("relay emits=%v", emits)
	}
	if _, ok := resultIDArg(emits[0]); ok {
		t.Fatalf("relay emit=%v carries another job's record", emits[0])
	}
}
