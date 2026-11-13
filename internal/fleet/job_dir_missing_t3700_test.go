package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// hk 3700 (hk 3685 ticket A): a job dir whose events/ is missing, or present
// but unreadable, must not stop the passes that walk every job dir. The
// artifact sweep and the lanes-missing pass skip such a dir and go on; a dir
// that is named and skipped says so on stderr. The job dir TTL treats a dir
// with neither events/ nor a submission as a leftover only when hk holds a
// terminal record for its job. Every fixture lives under t.TempDir().

// jobWithoutEvents makes a job dir with no events/ directory at all.
func jobWithoutEvents(t *testing.T, a *App, id string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(a.Jobs, id), 0700); err != nil {
		t.Fatal(err)
	}
}

// jobWithCorruptEvent makes a job dir whose events/ holds one unparseable
// record, so the fold fails on the record rather than on the directory.
func jobWithCorruptEvent(t *testing.T, a *App, id string) {
	t.Helper()
	dir := filepath.Join(a.Jobs, id, "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "0001-job.claim.json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
}

// damagedJobDir is the middle of three job dirs: either no events/ at all or
// an unparseable event.
type damagedJobDir struct {
	name    string
	corrupt bool
}

var damagedMiddle = []damagedJobDir{
	{"events missing", false},
	{"events corrupt", true},
}

func (d damagedJobDir) seed(t *testing.T, a *App, id string) {
	t.Helper()
	if d.corrupt {
		jobWithCorruptEvent(t, a, id)
		return
	}
	jobWithoutEvents(t, a, id)
}

// finalBuildJob seeds a completed job with one final build that hk confirms
// as drive-archived, so `artifacts sweep --apply` deletes that build when it
// reaches the job. It returns the hk document key, its body and the build's
// path.
func finalBuildJob(t *testing.T, a *App, id string) (key, body, path string) {
	t.Helper()
	seedJob(t, a, id, "implementer")
	putEvent(t, a, id, "job.completed", map[string]any{})
	src := filepath.Join(a.Jobs, id, "final")
	if e := os.MkdirAll(src, 0700); e != nil {
		t.Fatal(e)
	}
	path = filepath.Join(src, "build.bin")
	if e := os.WriteFile(path, []byte("artifact "+id), 0600); e != nil {
		t.Fatal(e)
	}
	n, h, e := hashFile(path)
	if e != nil {
		t.Fatal(e)
	}
	rec := ArtifactRecord{Schema: "fleet.artifact.v2", Host: "local", Job: id, Task: "42", Archive: Archive{Source: src, State: "drive_confirmed", Contents: []Content{{Path: "build.bin", Bytes: n, SHA: h}}}}
	raw, _ := json.Marshal(rec)
	doc, _ := json.Marshal(map[string]string{"body": string(raw)})
	return "artifacts/Example/42/" + id + "/final", string(doc), path
}

// Artifact reap (artifactSweep) over three job dirs, the middle one damaged:
// the other two are still swept. A dir with events/ missing is skipped
// silently; a dir with unreadable events is skipped and named on stderr.
func TestArtifactReapSkipsDamagedJobDirAndSweepsTheRest(t *testing.T) {
	for _, tt := range damagedMiddle {
		t.Run(tt.name, func(t *testing.T) {
			a := testApp(t)
			f := ledgerRunner(t, a)
			stderr, stdout := &bytes.Buffer{}, &bytes.Buffer{}
			a.Err, a.Out = stderr, stdout
			firstKey, firstDoc, firstPath := finalBuildJob(t, a, "a")
			tt.seed(t, a, "mid")
			lastKey, lastDoc, lastPath := finalBuildJob(t, a, "z")
			noHKCLI(t, f)
			bodies := map[string]string{firstKey: firstDoc, lastKey: lastDoc}
			useFakeHK(t).Handle(func(r hkRequest) (int, string, bool) {
				switch {
				case r.Route() == "GET /v1/tasks/42":
					return http.StatusOK, `{"state":"merged","project":"Example"}`, true
				case r.Route() == "GET /v1/documents":
					return http.StatusOK, `{"documents":[{"key":"` + firstKey + `"},{"key":"` + lastKey + `"}]}`, true
				case r.Method == http.MethodGet && strings.HasPrefix(r.Path, "/v1/documents/"):
					for key, body := range bodies {
						// The key is escaped in the path; accept both spellings.
						if strings.HasSuffix(r.Path, strings.ReplaceAll(key, "/", "%2F")) || strings.HasSuffix(r.Path, key) {
							return http.StatusOK, body, true
						}
					}
				}
				return 0, "", false
			})
			a.Now = func() time.Time { return time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC) }
			if e := a.artifactSweep(context.Background(), []string{"--age", "0s", "--apply"}); e != nil {
				t.Fatalf("sweep over a damaged middle job dir: %v\nstderr=%s", e, stderr.String())
			}
			for _, path := range []string{firstPath, lastPath} {
				if _, e := os.Stat(path); !os.IsNotExist(e) {
					t.Fatalf("%s was not swept past the damaged job dir: %v\nstdout=%s", path, e, stdout.String())
				}
			}
			for _, job := range []string{"a", "z"} {
				if !strings.Contains(stdout.String(), "artifact-sweep job="+job+" ") {
					t.Fatalf("no sweep line for job %s:\n%s", job, stdout.String())
				}
			}
			if tt.corrupt {
				if !strings.Contains(stderr.String(), "skip job mid") {
					t.Fatalf("the corrupt job was not named on stderr: %q", stderr.String())
				}
			} else if stderr.Len() != 0 {
				t.Fatalf("a dir with no events/ warned: %q", stderr.String())
			}
		})
	}
}

// Hygiene (the lanes-missing pass) over three job dirs, the middle one
// damaged: the two other jobs are still audited and reported.
func TestHygieneSkipsDamagedJobDirAndAuditsTheRest(t *testing.T) {
	for _, tt := range damagedMiddle {
		t.Run(tt.name, func(t *testing.T) {
			a := testApp(t)
			a.Config.Hub = map[string]any{"hub_url": "http://hub.invalid"}
			a.Runner = &fakeRunner{Do: func(c Command) Result {
				if len(c.Args) > 1 && c.Args[0] == "lanes" && c.Args[1] == "ls" {
					return Result{Out: `{"lanes":[{"lane":"owner","parent":""}]}`}
				}
				return Result{}
			}}
			seedJob(t, a, "a", "implementer")
			tt.seed(t, a, "mid")
			seedJob(t, a, "z", "implementer")
			stderr, stdout := &bytes.Buffer{}, &bytes.Buffer{}
			a.Err, a.Out = stderr, stdout
			if e := a.diagnostics(context.Background(), "lanes-missing", nil); e != nil {
				t.Fatalf("lanes-missing over a damaged middle job dir: %v\n%s", e, stdout.String())
			}
			want := "missing job=a lane=owner parent=parent\nmissing job=z lane=owner parent=parent\nlanes-missing: audited 2 open job(s), 2 missing\n"
			if stdout.String() != want {
				t.Fatalf("lanes-missing output = %q, want %q", stdout.String(), want)
			}
			if tt.corrupt {
				if !strings.Contains(stderr.String(), "skip job mid") {
					t.Fatalf("the corrupt job was not named on stderr: %q", stderr.String())
				}
			} else if stderr.Len() != 0 {
				t.Fatalf("a dir with no events/ warned: %q", stderr.String())
			}
		})
	}
}

// The job dir TTL's leftover rule: a dir with neither events/ nor a
// submission is removed through the same gate as any expired dir, and only
// when hk holds a terminal record for its job. The dry run lists the verdict;
// --apply removes the dir. Anything else keeps it, and the reason says which
// check failed: not-a-job for a dir hk has no record for, hk-unreadable for a
// read error.
func TestJobDirTTLLeftoverFollowsTheHKRecord(t *testing.T) {
	// The record names the pane the job ran in (the spawned record's pane_id
	// and label), so the pane check can prove it gone (hk 3700 review).
	withPane := func(state string) map[string]any {
		return map[string]any{"state": state, "pane_id": "w:left", "label": "left"}
	}
	const emptyPanes = `{"result":{"panes":[]}}`
	for _, tt := range []struct {
		name   string
		record map[string]any // nil: hk holds no record for the job
		getErr error
		panes  string // herdr pane list; "" is empty
		reason string // "confirmed" removes the dir
	}{
		{"terminal completed with its pane gone is a leftover", withPane("completed"), nil, emptyPanes, "confirmed"},
		{"terminal lost with its pane gone is a leftover", withPane("lost"), nil, emptyPanes, "confirmed"},
		{"terminal with its pane still listed stays", withPane("lost"), nil, `{"result":{"panes":[{"pane_id":"w:left","tab_id":"w:t1","agent_status":"idle","cwd":"/work/a"}]}}`, "pane-live"},
		{"terminal with no pane identity stays", map[string]any{"state": "completed"}, nil, emptyPanes, "pane-unknown"},
		{"no hk record is not a job", nil, nil, emptyPanes, "not-a-job"},
		{"hk read error is kept", nil, errors.New("hk unavailable"), emptyPanes, "hk-unreadable"},
		{"escalated stays, as the TTL confirmation set has it", withPane("escalated"), nil, emptyPanes, "hk-not-terminal"},
		{"live state stays", withPane("spawned"), nil, emptyPanes, "hk-not-terminal"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := newTTLRig(t)
			const id = "left"
			r.write(id, "brief.md", []byte("the brief\n"))
			if tt.record != nil {
				tt.record["job_id"] = id
				r.store.records[id] = tt.record
			}
			if tt.panes != "" {
				r.panes = tt.panes
			}
			r.store.getErr = tt.getErr
			r.age(id)
			action := "keep"
			if tt.reason == "confirmed" {
				action = "delete"
			}
			dry := r.pass(false)
			if got := line(dry, id); !strings.HasPrefix(got, "job="+id+" action="+action+" reason="+tt.reason+" ") {
				t.Fatalf("dry run line = %q, want action=%s reason=%s\n%s", got, action, tt.reason, dry)
			}
			if !r.exists(id) {
				t.Fatal("the dry run removed the dir")
			}
			out := r.pass(true)
			if tt.reason == "confirmed" {
				wantDelete(t, r, out, id, tt.reason)
				return
			}
			wantKeep(t, r, out, id, tt.reason)
		})
	}
}
