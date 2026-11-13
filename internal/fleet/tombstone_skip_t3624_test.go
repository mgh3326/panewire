package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// hk 3624: the job dir TTL renames a dir to <root>/.expiring-<job>-<n> right
// before it deletes it, and leaves such a tombstone behind when it cannot go
// back. `artifacts sweep` and `lanes-missing` walk the jobs root; a
// tombstone is not a job, so each skips it and goes on to the real jobs.

// tombstone leaves a renamed job dir, as an interrupted TTL pass would.
func tombstone(t *testing.T, a *App) {
	t.Helper()
	seedJob(t, a, "x", "implementer")
	if err := os.Rename(filepath.Join(a.Jobs, "x"), filepath.Join(a.Jobs, jobDirTombstone+"x-1")); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactSweepSkipsTombstone(t *testing.T) {
	a := testApp(t)
	f := ledgerRunner(t, a)
	tombstone(t, a)
	seedJob(t, a, "j", "implementer")
	putEvent(t, a, "j", "job.completed", map[string]any{})
	src := filepath.Join(a.Jobs, "j", "final")
	if e := os.MkdirAll(src, 0700); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(src, "build.bin")
	if e := os.WriteFile(path, []byte("artifact"), 0600); e != nil {
		t.Fatal(e)
	}
	n, h, e := hashFile(path)
	if e != nil {
		t.Fatal(e)
	}
	rec := ArtifactRecord{Schema: "fleet.artifact.v2", Host: "local", Job: "j", Task: "42", Archive: Archive{Source: src, State: "drive_confirmed", Contents: []Content{{Path: "build.bin", Bytes: n, SHA: h}}}}
	body, _ := json.Marshal(rec)
	doc, _ := json.Marshal(map[string]string{"body": string(body)})
	noHKCLI(t, f)
	useFakeHK(t).Handle(func(r hkRequest) (int, string, bool) {
		switch {
		case r.Route() == "GET /v1/tasks/42":
			return http.StatusOK, `{"state":"merged","project":"Example"}`, true
		case r.Route() == "GET /v1/documents":
			return http.StatusOK, `{"documents":[{"key":"artifacts/Example/42/j/dataset"}]}`, true
		case r.Method == http.MethodGet && strings.HasPrefix(r.Path, "/v1/documents/"):
			return http.StatusOK, string(doc), true
		}
		return 0, "", false
	})
	a.Now = func() time.Time { return time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC) }
	if e := a.artifactSweep(context.Background(), []string{"--age", "0s", "--apply"}); e != nil {
		t.Fatalf("sweep with a tombstone in the root: %v", e)
	}
	if _, e := os.Stat(path); !os.IsNotExist(e) {
		t.Fatalf("the real job's confirmed build was not swept: %v", e)
	}
	if _, e := os.Stat(filepath.Join(a.Jobs, jobDirTombstone+"x-1", "events")); e != nil {
		t.Fatalf("the tombstone was touched: %v", e)
	}
}

func TestLanesMissingSkipsTombstone(t *testing.T) {
	a := testApp(t)
	a.Config.Hub = map[string]any{"hub_url": "http://hub.invalid"}
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		if len(c.Args) > 1 && c.Args[0] == "lanes" && c.Args[1] == "ls" {
			return Result{Out: `{"lanes":[{"lane":"owner","parent":""}]}`}
		}
		return Result{}
	}}
	tombstone(t, a)
	seedJob(t, a, "j", "implementer")
	out := &bytes.Buffer{}
	a.Out = out
	if e := a.diagnostics(context.Background(), "lanes-missing", nil); e != nil {
		t.Fatalf("lanes-missing with a tombstone in the root: %v\n%s", e, out.String())
	}
	want := "missing job=j lane=owner parent=parent\nlanes-missing: audited 1 open job(s), 1 missing\n"
	if out.String() != want {
		t.Fatalf("lanes-missing output = %q, want %q", out.String(), want)
	}
}
