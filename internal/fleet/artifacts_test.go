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

func TestArtifactsMoveDryRunHasNoSideEffects(t *testing.T) {
	a := testApp(t)
	src := filepath.Join(a.Home, "source")
	drive := filepath.Join(a.Home, "drive")
	if e := os.MkdirAll(src, 0700); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"data.bin", ".env", "private.key"} {
		if e := os.WriteFile(filepath.Join(src, name), []byte("data"), 0600); e != nil {
			t.Fatal(e)
		}
	}
	a.Config.Artifacts = map[string]any{"drive_folder": drive, "exclude": []any{"*.key"}}
	dest := filepath.Join(drive, "host-a", "dataset")
	if e := a.artifactMove(context.Background(), []string{src, "--dest", dest}); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(dest); !os.IsNotExist(e) {
		t.Fatal("dry-run created destination")
	}
	if len(a.Runner.(*fakeRunner).Calls) != 0 {
		t.Fatal("dry-run called external tool")
	}
	out := a.Out.(*bytes.Buffer).String()
	if strings.Contains(out, "private.key") || strings.Contains(out, ".env") {
		t.Fatal("credential-shaped file included")
	}
	if _, e := os.Stat(filepath.Join(src, "data.bin")); e != nil {
		t.Fatal("source deleted")
	}
}
func TestArtifactSweepConfirmationAndHashGates(t *testing.T) {
	for _, tt := range []struct {
		name, state, sha string
		apply, mutate    bool
		wantExist        bool
		wantCode         int
	}{{"preview", "drive_confirmed", "", false, false, true, 0}, {"unconfirmed", "running", "", true, false, true, 0}, {"hash differs", "drive_confirmed", "wrong", true, false, true, 0}, {"confirmed", "drive_confirmed", "", true, false, false, 0}, {"changed during record", "drive_confirmed", "", true, true, true, 75}, {"task reopened", "drive_confirmed", "", true, false, true, 75}} {
		t.Run(tt.name, func(t *testing.T) {
			a := testApp(t)
			f := ledgerRunner(t, a)
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
			if tt.sha != "" {
				h = tt.sha
			}
			rec := ArtifactRecord{Schema: "fleet.artifact.v2", Host: "local", Job: "j", Task: "42", Archive: Archive{Source: src, State: tt.state, Contents: []Content{{Path: "build.bin", Bytes: n, SHA: h}}}}
			body, _ := json.Marshal(rec)
			doc, _ := json.Marshal(map[string]string{"body": string(body)})
			noHKCLI(t, f)
			reopened := false
			useFakeHK(t).Handle(func(r hkRequest) (int, string, bool) {
				switch {
				case r.Route() == "GET /v1/tasks/42":
					if reopened {
						return http.StatusOK, `{"state":"reopened","project":"Example"}`, true
					}
					return http.StatusOK, `{"state":"merged","project":"Example"}`, true
				case r.Route() == "GET /v1/documents":
					return http.StatusOK, `{"documents":[{"key":"artifacts/Example/42/j/dataset"}]}`, true
				case r.Method == http.MethodGet && strings.HasPrefix(r.Path, "/v1/documents/"):
					return http.StatusOK, string(doc), true
				case r.Method == http.MethodPut && strings.HasPrefix(r.Path, "/v1/documents/"):
					if tt.name == "task reopened" {
						reopened = true
					}
					if tt.mutate {
						if e := os.WriteFile(path, []byte("changed"), 0600); e != nil {
							t.Fatal(e)
						}
					}
				}
				return 0, "", false
			})
			a.Now = func() time.Time { return time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC) }
			args := []string{"--job", "j", "--age", "0s"}
			if tt.apply {
				args = append(args, "--apply")
			}
			e = a.artifactSweep(context.Background(), args)
			if code(e) != tt.wantCode {
				t.Fatalf("%v", e)
			}
			_, e = os.Stat(path)
			if (!os.IsNotExist(e)) != tt.wantExist {
				t.Fatalf("exist=%t want=%t", e == nil, tt.wantExist)
			}
		})
	}
}
func TestUploadNeedsTwoFreshReadsSeparatedByTwoMinutes(t *testing.T) {
	a := testApp(t)
	logs := filepath.Join(a.Home, "logs")
	if e := os.MkdirAll(logs, 0700); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(logs, "drive.log")
	if e := os.WriteFile(path, []byte("active"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.Chtimes(path, a.Now(), a.Now()); e != nil {
		t.Fatal(e)
	}
	a.Config.Artifacts = map[string]any{"fpbulk": "fpbulk", "drivefs_log_dir": logs, "confirm_gap_seconds": int64(120), "poll_seconds": int64(30), "confirm_timeout_seconds": int64(180)}
	reads := 0
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		if c.Name == "fpbulk" {
			reads++
			return Result{Out: "11\t/file\n"}
		}
		return Result{}
	}}
	start := a.Now()
	if e := a.confirmUpload(context.Background(), []string{"/file"}); e != nil {
		t.Fatal(e)
	}
	if reads < 2 || a.Now().Sub(start) < 120*time.Second {
		t.Fatal("single/early read accepted")
	}
}
