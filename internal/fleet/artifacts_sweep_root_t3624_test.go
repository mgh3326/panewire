package fleet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// hk 3624: the job dir TTL shares artifactClass and artifactArchived with
// `artifacts sweep`. Sweep behaves as at 81ad1d3: under the policy's own
// root it deletes an uploaded final build under the uploaded hatch and keeps
// mirror-required evidence; under a jobs root the policy root does not match
// (/srv/jobs) no file has a class, so it deletes nothing.
func TestArtifactSweepUploadedHatchByJobsRoot(t *testing.T) {
	for _, tt := range []struct {
		name, jobs string
		deleted    []string
	}{
		{"policy root", "work/herdr-inbox/jobs", []string{"final/app.bin"}},
		{"unmatched root", "srv/jobs", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := testApp(t)
			a.Jobs = filepath.Join(a.Home, tt.jobs)
			f := ledgerRunner(t, a)
			seedJob(t, a, "j", "implementer")
			putEvent(t, a, "j", "job.completed", map[string]any{})
			files := map[string][]byte{"final/app.bin": []byte("bin\n"), "evidence/run.log": []byte("log\n")}
			var docs []string
			bodies := map[string]string{}
			for rel, body := range files {
				path := filepath.Join(a.Jobs, "j", rel)
				if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
					t.Fatal(e)
				}
				if e := os.WriteFile(path, body, 0600); e != nil {
					t.Fatal(e)
				}
				sum := sha256.Sum256(body)
				rec := ArtifactRecord{Schema: "fleet.artifact.v2", Host: a.machine(), Job: "j", Task: "42", Archive: Archive{Source: filepath.Dir(path), State: "uploaded", Contents: []Content{{Path: filepath.Base(rel), Bytes: int64(len(body)), SHA: hex.EncodeToString(sum[:]), UploadedAt: "2026-01-01T00:00:00Z"}}}}
				raw, _ := json.Marshal(rec)
				doc, _ := json.Marshal(map[string]string{"body": string(raw)})
				key := "artifacts/Example/42/j/" + filepath.Dir(rel)
				docs = append(docs, `{"key":"`+key+`"}`)
				bodies[filepath.Dir(rel)] = string(doc)
			}
			noHKCLI(t, f)
			useFakeHK(t).Handle(func(r hkRequest) (int, string, bool) {
				switch {
				case r.Route() == "GET /v1/tasks/42":
					return http.StatusOK, `{"state":"merged","project":"Example"}`, true
				case r.Route() == "GET /v1/documents":
					return http.StatusOK, `{"documents":[` + strings.Join(docs, ",") + `]}`, true
				case r.Method == http.MethodGet && strings.HasPrefix(r.Path, "/v1/documents/"):
					// The key is escaped in the path; its last segment names the record.
					for dir, body := range bodies {
						if strings.HasSuffix(r.Path, "%2F"+dir) || strings.HasSuffix(r.Path, "/"+dir) {
							return http.StatusOK, body, true
						}
					}
				}
				return 0, "", false
			})
			a.Config.Artifacts = map[string]any{"local_delete_at": "uploaded"}
			a.Now = func() time.Time { return time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC) }
			out := &bytes.Buffer{}
			a.Out = out
			if e := a.artifactSweep(context.Background(), []string{"--job", "j", "--age", "0s", "--apply"}); e != nil {
				t.Fatal(e)
			}
			var want strings.Builder
			for _, rel := range tt.deleted {
				want.WriteString("artifact-sweep job=j path=" + filepath.Join(a.Jobs, "j", rel) + " class=final-build apply=true\n")
			}
			if out.String() != want.String() {
				t.Fatalf("sweep output = %q, want %q", out.String(), want.String())
			}
			for rel := range files {
				_, e := os.Stat(filepath.Join(a.Jobs, "j", rel))
				gone := os.IsNotExist(e)
				if gone != (len(tt.deleted) > 0 && tt.deleted[0] == rel) {
					t.Fatalf("%s gone=%t, want deleted %v", rel, gone, tt.deleted)
				}
			}
		})
	}
}
