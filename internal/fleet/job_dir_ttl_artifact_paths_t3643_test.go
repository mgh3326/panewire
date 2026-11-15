package fleet

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// hk 3643: the evidence join matches an artifact record's Archive.Source and
// Contents[].Path against the walked job dir, whose paths are real. A record
// path that names the same file lexically differently (a ./ or .. after a
// symlink, a symlinked component, an alias of the jobs root) did not match,
// so the dir was kept as evidence-unconfirmed. The record paths are now
// resolved before the join (resolveExisting).

// driveEvidence seeds j1 with an unconfirmed evidence/run.log and answers hk
// with an artifact record whose archive is drive-confirmed, listing rowPath
// under source. Every path is a literal: filepath.Join in the fixture would
// erase the defect.
func (r *ttlRig) driveEvidence(source, rowPath string, body []byte) {
	r.t.Helper()
	r.write("j1", "evidence/run.log", body)
	r.age("j1")
	// age cannot reach a symlink's own mtime (the wall clock's), so the pass
	// runs a TTL past the wall clock, as linkDeep does.
	r.now = time.Now().Add(73 * time.Hour)
	rec := ArtifactRecord{Schema: "fleet.artifact.v2", Name: "run", Host: r.a.machine(), Job: "j1", Task: "42", Archive: Archive{Source: source, State: "drive_confirmed", Contents: []Content{{Path: rowPath, Bytes: int64(len(body)), SHA: sha(body)}}}}
	raw, _ := json.Marshal(rec)
	key := "artifacts/proj/42/j1/run"
	r.hk.SetTask("42", hkTaskJSON(42, "merged", "t", "", "proj", "{}"))
	r.hk.SetDoc(key, string(raw))
	r.hk.Handle(func(req hkRequest) (int, string, bool) {
		return http.StatusOK, `{"documents":[{"key":"` + key + `"}]}`, req.Method == http.MethodGet && req.Path == "/v1/documents"
	})
}

// A record path that names evidence/run.log through a ./, a .., a symlinked
// component, or an alias of the jobs root confirms it: the dir goes.
func TestJobDirTTLArtifactRecordPathsResolveBeforeJoin(t *testing.T) {
	body := []byte("log\n")
	cases := []struct {
		name  string
		setup func(r *ttlRig) (source, row string)
	}{
		{"dot segment in the source", func(r *ttlRig) (string, string) {
			return filepath.Join(r.dir("j1"), "evidence") + string(filepath.Separator) + ".", "run.log"
		}},
		{"dot segment in the content path", func(r *ttlRig) (string, string) {
			return filepath.Join(r.dir("j1"), "evidence"), "./run.log"
		}},
		{"symlinked component in the source", func(r *ttlRig) (string, string) {
			if err := os.Symlink(filepath.Join(r.dir("j1"), "evidence"), filepath.Join(r.dir("j1"), "alias")); err != nil {
				r.t.Fatal(err)
			}
			return filepath.Join(r.dir("j1"), "alias"), "run.log"
		}},
		{"dotdot after a symlink in the source", func(r *ttlRig) (string, string) {
			// alias -> evidence/sub (empty, so it adds no pending file),
			// so alias/.. is evidence, not j1.
			if err := os.MkdirAll(filepath.Join(r.dir("j1"), "evidence", "sub"), 0700); err != nil {
				r.t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(r.dir("j1"), "evidence", "sub"), filepath.Join(r.dir("j1"), "alias")); err != nil {
				r.t.Fatal(err)
			}
			return filepath.Join(r.dir("j1"), "alias") + string(filepath.Separator) + "..", "run.log"
		}},
		{"alias of the jobs root", func(r *ttlRig) (string, string) {
			alias := filepath.Join(r.a.Home, "jobs-alias")
			if err := os.Symlink(r.a.Jobs, alias); err != nil {
				r.t.Fatal(err)
			}
			return filepath.Join(alias, "j1", "evidence"), "run.log"
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedJob("j1")
			source, row := c.setup(r)
			r.driveEvidence(source, row, body)
			wantDelete(t, r, r.pass(true), "j1", "confirmed")
		})
	}
}

// A record path whose symlink leaves the job dir does not confirm evidence
// inside it: the resolved file is outside the source, so it is not pending.
func TestJobDirTTLArtifactRecordSymlinkOutsideKeeps(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	outside := filepath.Join(r.a.Home, "elsewhere")
	if err := os.MkdirAll(outside, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(r.dir("j1"), "out")); err != nil {
		t.Fatal(err)
	}
	r.driveEvidence(filepath.Join(r.dir("j1"), "out"), "run.log", []byte("log\n"))
	wantKeep(t, r, r.pass(true), "j1", "evidence-unconfirmed")
}
