package fleet

// hk 6822 (hk 4289 T5): `panewire done --ready-event` names the READY the
// completion concludes, and `spawn --wait` reads a completion that the hub
// merged into that READY.

import (
	"context"
	"slices"
	"strings"
	"testing"
)

// doneRigEmits returns the arguments of every relay emit.
func doneRigEmits(r *doneRig) [][]string {
	var out [][]string
	for _, c := range r.f.Calls {
		if c.Name == "panewire" && len(c.Args) > 0 && c.Args[0] == "emit" {
			out = append(out, c.Args)
		}
	}
	return out
}

// Mutants: drop --result-id from the relay emit, or the id from the envelope;
// send an empty one without the flag.
func TestDoneReadyEventNamesTheCompletionsReady(t *testing.T) {
	for _, tc := range []struct {
		name  string
		args  []string
		ready string
	}{
		{"with --ready-event", []string{"--ready-event", "j-ready"}, "j-ready"},
		{"without it", nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newDoneRig(t)
			r.write("report body\n")
			if e := r.done(tc.args...); e != nil {
				t.Fatal(e)
			}
			emits := doneRigEmits(r)
			if len(emits) != 1 {
				t.Fatalf("relay emits=%v", emits)
			}
			at := slices.Index(emits[0], "--result-id")
			if tc.ready == "" && at >= 0 || tc.ready != "" && (at < 0 || emits[0][at+1] != tc.ready) {
				t.Fatalf("relay emit=%v, want result id %q", emits[0], tc.ready)
			}
			completed := r.events("job.completed")
			if len(completed) != 1 {
				t.Fatalf("job.completed events=%v", completed)
			}
			got, present := completed[0]["result_id"]
			if tc.ready == "" && present || tc.ready != "" && got != tc.ready {
				t.Fatalf("job.completed result_id=%v present=%t, want %q", got, present, tc.ready)
			}
		})
	}
}

// A bad id is refused before any step runs, so no completion loses its relay,
// and only `done` takes the flag. Mutant: validate in the relay emit only.
func TestReadyEventRefusedBeforeAnyStep(t *testing.T) {
	for _, bad := range []string{"", "bad\nid", strings.Repeat("r", 513)} {
		r := newDoneRig(t)
		r.write("report body\n")
		seeded := r.hkCalls() // the fixture's own job record writes
		e := r.done("--ready-event", bad)
		if code(e) != exitUsage || len(r.f.Calls) != 0 || r.hkCalls() != seeded || len(r.events("job.completed")) != 0 {
			t.Fatalf("%.20q: error=%v calls=%v hk=%d->%d", bad, e, r.f.Calls, seeded, r.hkCalls())
		}
	}
	r := newDoneRig(t)
	for _, kind := range []string{"escalate", "restore"} {
		e := r.a.lifecycle(context.Background(), kind, []string{"j", "--ready-event", "j-ready"})
		if code(e) != exitUsage || !strings.Contains(e.Error(), "--ready-event") {
			t.Fatalf("%s: %v", kind, e)
		}
	}
}

// A completion merged into a READY has no inbox item of its own, so the wait
// reads it from the READY's merged_completions, even when the READY text does
// not name the job. Mutant: ignore the set.
func TestWaitCompletedFromMergedReady(t *testing.T) {
	item := func(job string) string {
		return `[{"kind":"lane.event","event":{"job_id":"lane-event-x","text":"READY | https://pr.invalid/1","report_path":"hk:doc/reports/j/r.md","merged_completions":["job.completed\n` + job + `\n1\nr.md\n\n00001-job.completed.json"]}}]`
	}
	for _, tc := range []struct {
		name, job string
		completed bool
	}{{"this job's", "j", true}, {"another job's", "j-2", false}} {
		t.Run(tc.name, func(t *testing.T) {
			a, s := waitApp(t)
			a.Config.Hub = map[string]any{"hub_url": "https://hub.invalid"}
			a.Runner = &fakeRunner{Do: func(c Command) Result {
				if c.Name == "panewire" {
					return Result{Out: item(tc.job)}
				}
				return Result{}
			}}
			warned := false
			route := waitInboxViaHub
			got := a.waitOutcome(context.Background(), s, "lane-a", &warned, &warned, &route, false)
			if tc.completed && (got.name != "completed" || got.report != "hk:doc/reports/j/r.md") || !tc.completed && got.name != "" {
				t.Fatalf("outcome=%+v", got)
			}
		})
	}
}
