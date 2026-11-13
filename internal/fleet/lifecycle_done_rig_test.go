package fleet

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// doneRig is a spawned job "j" on task 42 with its hk record spawned, an hk
// document server and the in-memory job store, for tests of `panewire done`
// publication: the report receipts, the record writer, the pin and the relay.
type doneRig struct {
	t     *testing.T
	a     *App
	f     *fakeRunner
	hk    *spawnHK
	store *fakeJobStore
	path  string
}

func newDoneRig(t *testing.T) *doneRig {
	t.Helper()
	a := testApp(t)
	f := ledgerRunner(t, a)
	hk := useFakeHK(t)
	noHKCLI(t, f)
	hk.Handle(hkCommentAnswer)
	seedDoneJob(t, a)
	return &doneRig{t: t, a: a, f: f, hk: hk, store: a.JobStore.(*fakeJobStore), path: filepath.Join(a.Jobs, "j", "report.md")}
}

// write replaces the report file's bytes.
func (r *doneRig) write(body string) {
	r.t.Helper()
	if e := os.WriteFile(r.path, []byte(body), 0600); e != nil {
		r.t.Fatal(e)
	}
}

// done runs `done j --report <path>` plus extra arguments.
func (r *doneRig) done(extra ...string) error {
	r.t.Helper()
	r.a.Out.(*bytes.Buffer).Reset()
	r.a.Err.(*bytes.Buffer).Reset()
	return r.a.lifecycle(context.Background(), "done", append([]string{"j", "--report", r.path}, extra...))
}

// events returns the payloads of the job's events of kind, oldest first.
func (r *doneRig) events(kind string) []map[string]any {
	r.t.Helper()
	evs, e := r.a.events("j")
	if e != nil {
		r.t.Fatal(e)
	}
	var out []map[string]any
	for _, ev := range evs {
		if ev.Kind == kind {
			out = append(out, ev.Payload)
		}
	}
	return out
}

// hkCalls counts every call that reached hk: HTTP requests and job-record
// store calls.
func (r *doneRig) hkCalls() int {
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	return len(r.hk.Requests()) + len(r.store.calls)
}

// relays returns the --report-last-line of every relay emit.
func (r *doneRig) relays() []string {
	var out []string
	for _, c := range r.f.Calls {
		if c.Name != "panewire" || len(c.Args) == 0 || c.Args[0] != "emit" {
			continue
		}
		for i, arg := range c.Args {
			if arg == "--report-last-line" && i+1 < len(c.Args) {
				out = append(out, c.Args[i+1])
			}
		}
	}
	return out
}

// stderr is what `done` warned.
func (r *doneRig) stderr() string { return r.a.Err.(*bytes.Buffer).String() }

func (r *doneRig) stdout() string { return r.a.Out.(*bytes.Buffer).String() }

func (r *doneRig) key(body string) string {
	return reportDocumentKey("j", reportDigest([]byte(body)))
}

// wantNotInHK is the interactive `done` that completed but left its report
// out of hk: it exits non-zero so the caller reruns it.
func wantNotInHK(t *testing.T, e error) {
	t.Helper()
	if code(e) != exitFailure || e == nil || !strings.Contains(e.Error(), "completed; report not in hk; rerun the same command") {
		t.Fatalf("done error=%v, want exit %d: completed; report not in hk; rerun the same command", e, exitFailure)
	}
}
