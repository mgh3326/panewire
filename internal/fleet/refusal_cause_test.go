package fleet

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const genericUsageFix = "exact flag named in the refusal"

// Every exit-2 refusal the node reaches before its claim stage used to fall
// through to the generic "--help" fix, which names no flag. The refusal text is
// the only thing that separates them.
func TestNodeUsageRefusalsNameTheirCause(t *testing.T) {
	for _, test := range []struct {
		name, want, fix string
		arrange         func(t *testing.T, a *App, s *Spawn, f *fakeRunner)
	}{
		{"jobs root crosses a symlink", "symlink path refused", "symlink", func(t *testing.T, a *App, s *Spawn, _ *fakeRunner) {
			real := filepath.Join(a.Home, "real-jobs")
			if e := os.MkdirAll(real, 0700); e != nil {
				t.Fatal(e)
			}
			a.Jobs = filepath.Join(a.Home, "linked-jobs")
			if e := os.Symlink(real, a.Jobs); e != nil {
				t.Fatal(e)
			}
			s.Report = filepath.Join(a.Jobs, s.Job, "report.md")
		}},
		{"report of another job", "report must be under", "--report", func(_ *testing.T, a *App, s *Spawn, _ *fakeRunner) {
			s.Report = filepath.Join(a.Jobs, "earlier-attempt", "report.md")
		}},
		{"checkout missing", "cwd is not a directory", "preflight", func(_ *testing.T, a *App, s *Spawn, _ *fakeRunner) {
			s.CWD = filepath.Join(a.Home, "gone")
		}},
		{"task unknown to this node", "task does not exist", "handoffkeep tasks show", func(t *testing.T, _ *App, _ *Spawn, _ *fakeRunner) {
			useFakeHK(t).Handle(hkFails("GET /v1/tasks/42", http.StatusNotFound, `{"error":"not_found"}`))
		}},
		{"stale claimant", "another claimant", "stale claim", func(t *testing.T, _ *App, _ *Spawn, _ *fakeRunner) {
			useFakeHK(t).SetTask("42", `{"id":42,"title":"implement (grade B, T1)","state":"claimed","refs":{},"claimed_by":"someone-else"}`)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, s, f, _ := cmdFixture(t)
			s.Delegated = true
			test.arrange(t, a, &s, f)
			err := a.Spawn(context.Background(), s)
			if code(err) != exitUsage || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("refusal: code=%d %v", code(err), err)
			}
			fix := SpawnRefusalFix(err)
			if strings.Contains(fix, genericUsageFix) || !strings.Contains(fix, test.fix) {
				t.Fatalf("fix does not name the cause: %q", fix)
			}
		})
	}
}

// A spawn refused before delivery never claims its task (taskLink runs after
// the brief lands), so a retry on the same task has no binding to trip over.
func TestRefusedSpawnLeavesTaskUnbound(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	s.Delegated = true
	refused := s
	refused.Report = filepath.Join(a.Jobs, "earlier-attempt", "report.md")
	if err := a.Spawn(context.Background(), refused); code(err) != exitUsage {
		t.Fatalf("first attempt: %v", err)
	}
	if hkClaimed(t) {
		t.Fatalf("refused spawn claimed its task: %v", useFakeHK(t).Routes())
	}
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatalf("retry on the same task: %v", err)
	}
}

func TestParentJobNotFoundReason(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	a.JobStore = newFakeHK(t).client()
	s.ParentJob, s.Task = "never-existed", ""
	s.Report = filepath.Join(a.Jobs, "job-a", "report.md")
	err := a.Spawn(context.Background(), s)
	if code(err) != exitStateConflict || AdmissionRefusalReason(err) != "parent_job_not_found" || !strings.Contains(err.Error(), "never-existed") {
		t.Fatalf("code=%d reason=%s err=%v", code(err), AdmissionRefusalReason(err), err)
	}
	if fix := SpawnRefusalFix(err); !strings.Contains(fix, "--parent-job") {
		t.Fatalf("fix=%q", fix)
	}
}

// An unreadable job store is not evidence that the parent does not exist.
func TestParentStoreOutageIsNotNotFound(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	h := newFakeHK(t)
	h.down = true
	a.JobStore = h.client()
	s.ParentJob, s.Task = "pj", ""
	s.Report = filepath.Join(a.Jobs, "job-a", "report.md")
	err := a.Spawn(context.Background(), s)
	if code(err) != exitStateConflict || AdmissionRefusalReason(err) != "admission_refused" {
		t.Fatalf("code=%d reason=%s err=%v", code(err), AdmissionRefusalReason(err), err)
	}
}

func TestQuotaUnconfirmedReasonNamesCause(t *testing.T) {
	const key = "claude:0123456789abcdef"
	for _, test := range []struct {
		name, key, want string
		acc             SharedAccount
	}{
		{"no pool presence", "", "quota_unconfirmed:no_usable_presence", SharedAccount{}},
		{"stale", key, "quota_unconfirmed:stale", SharedAccount{Key: key, Observed: true}},
		{"not observed", key, "quota_unconfirmed:not_observed", SharedAccount{Key: key}},
		{"degraded", key, "quota_unconfirmed:degraded", SharedAccount{Key: key, Degraded: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := testApp(t)
			_, err := a.judgeHost(context.Background(), QuotaRequest{}, "claude", "host-b", false, test.key, test.acc)
			if code(err) != exitEvictionUnconfirmed || AdmissionRefusalReason(err) != test.want {
				t.Fatalf("code=%d reason=%s err=%v", code(err), AdmissionRefusalReason(err), err)
			}
			if !strings.Contains(err.Error(), "host-b") || !strings.Contains(err.Error(), "claude") {
				t.Fatalf("refusal omits host or pool: %v", err)
			}
			if fix := SpawnRefusalFix(err); !strings.Contains(fix, "panewire quota refresh POOL") || strings.Contains(fix, "pacelever") {
				t.Fatalf("fix=%q", fix)
			}
		})
	}
	if got := AdmissionRefusalReason(fail(exitEvictionUnconfirmed, "quota gate unavailable")); got != "quota_unconfirmed" {
		t.Fatalf("untyped rc=4 reason=%q", got)
	}
	wrapped := fmt.Errorf("placement: %w", &quotaHostRefusal{fail(exitEvictionUnconfirmed, "x"), "stale"})
	if got := AdmissionRefusalReason(wrapped); got != "quota_unconfirmed:stale" {
		t.Fatalf("wrapped reason=%q", got)
	}
	if got := AdmissionRefusalReason(errors.New("plain")); got != "admission_refused" {
		t.Fatalf("plain reason=%q", got)
	}
}
