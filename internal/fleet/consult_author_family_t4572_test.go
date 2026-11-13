package fleet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// hk 4572: a consult's other-family rule needs the asker's family. spawn reads
// it from the parent job (--parent-job, or ARBITER_JOB when no task is named);
// resolve must read the same job, or an in-session author gets its own family's
// seat back (opus for an anthropic author). Both paths must pick the same rung.

// consultAuthorRows: the asker's grade A floors at A+; opus and codex-sol both
// sit at A+ on their own pools, so the family rule alone decides the pick.
func consultAuthorRows() []CatalogRow {
	return []CatalogRow{
		consultFloorRung("opus", "high", "A+", "claude"),
		consultFloorRung("codex-sol", "high", "A+", "codex"),
	}
}

// consultAuthorFixture is a resolve/spawn fixture whose author job (job id
// "author-job") was spawned on family. The job carries task 42 so a spawn with
// --parent-job inherits it.
func consultAuthorFixture(t *testing.T, family string) (*App, Spawn, *fakeQuota) {
	t.Helper()
	pools := map[string]PoolView{"claude": riskPool(30, 8), "codex": {Status: "ok", Class: "spend", Blocking: ptrF(40)}}
	a, s, q := selFixture(t, "A", consultAuthorRows(), pools, consultFloorBothPools)
	q.GateDo = gatePools(map[string]string{"opus": "claude", "codex-sol": "codex", "codex-astra": "codex"})
	serveRouting(t, routingDocV1)
	useFakeHK(t).SetTask("42", `{"id":42,"title":"implement (grade A, T1)","state":"ready","refs":{}}`)
	putEvent(t, a, "author-job", "job.claim", map[string]any{"owner_lane": "owner", "task_id": "42", "t_level": "T1", "role": "builder"})
	putEvent(t, a, "author-job", "job.spawned", map[string]any{"family": family, "profile": "opus", "role": "worker", "effort": "high"})
	return a, s, q
}

// TestConsultResolveReadsAuthorFamilyFromJob_t4572: resolve run inside an
// author job (ARBITER_JOB) picks the other family without --author-family,
// matching spawn. The anthropic author must not get opus; the openai author
// must not get codex-sol.
func TestConsultResolveReadsAuthorFamilyFromJob_t4572(t *testing.T) {
	for _, tc := range []struct {
		family, want string
	}{
		{"anthropic", "codex-sol"},
		{"openai", "opus"},
	} {
		t.Run(tc.family, func(t *testing.T) {
			a, _, _ := consultAuthorFixture(t, tc.family)
			t.Setenv("ARBITER_JOB", "author-job")
			out := captureOut(a)
			if e := a.Run(context.Background(), []string{"resolve", "--role", "consult", "--t", "T1", "--grade", "A", "--json"}); e != nil {
				t.Fatalf("resolve: %v", e)
			}
			var r map[string]any
			if err := json.Unmarshal(out.Bytes(), &r); err != nil {
				t.Fatalf("resolve json: %v\n%s", err, out)
			}
			if r["profile"] != tc.want {
				t.Fatalf("resolve for %s author chose %v, want %s (selection %v)", tc.family, r["profile"], tc.want, r["selection"])
			}
			consult, _ := r["selection"].(map[string]any)["consult"].(map[string]any)
			if consult["asker_family"] != tc.family {
				t.Fatalf("consult stamp asker_family = %v, want %s", consult["asker_family"], tc.family)
			}
		})
	}
}

// TestConsultResolveFlagOverridesAuthorJob_t4572: an explicit --author-family
// still wins over the job the caller runs in, as spawn's explicit parent does.
func TestConsultResolveFlagOverridesAuthorJob_t4572(t *testing.T) {
	a, _, _ := consultAuthorFixture(t, "anthropic")
	t.Setenv("ARBITER_JOB", "author-job")
	out := captureOut(a)
	if e := a.Run(context.Background(), []string{"resolve", "--role", "consult", "--t", "T1", "--grade", "A", "--author-family", "openai", "--json"}); e != nil {
		t.Fatalf("resolve: %v", e)
	}
	var r map[string]any
	if err := json.Unmarshal(out.Bytes(), &r); err != nil {
		t.Fatalf("resolve json: %v\n%s", err, out)
	}
	if r["profile"] != "opus" {
		t.Fatalf("--author-family openai chose %v, want opus", r["profile"])
	}
}

// TestConsultResolveUnreadableAuthorJobRefuses_t4572: an ARBITER_JOB that names
// no readable job refuses, as spawn --parent-job does, rather than falling back
// to a family-less selection that may return the author's own family.
func TestConsultResolveUnreadableAuthorJobRefuses_t4572(t *testing.T) {
	a, _, _ := consultAuthorFixture(t, "anthropic")
	t.Setenv("ARBITER_JOB", "no-such-job")
	if e := a.Run(context.Background(), []string{"resolve", "--role", "consult", "--t", "T1", "--grade", "A"}); e == nil {
		t.Fatal("resolve with an unreadable ARBITER_JOB must refuse")
	}
}

// TestConsultSpawnReadsAuthorFamilyFromParentJob_t4572: spawn --role counsel
// --parent-job picks the same rung resolve picks for the same author.
func TestConsultSpawnReadsAuthorFamilyFromParentJob_t4572(t *testing.T) {
	for _, tc := range []struct {
		family, want string
	}{
		{"anthropic", "codex-sol"},
		{"openai", "opus"},
	} {
		t.Run(tc.family, func(t *testing.T) {
			a, _, _ := consultAuthorFixture(t, tc.family)
			prompt := filepath.Join(a.Home, "brief")
			if e := os.WriteFile(prompt, []byte("consult the other family\n"), 0600); e != nil {
				t.Fatal(e)
			}
			report := filepath.Join(a.Jobs, "job-b", "report.md")
			parsed, err := parseSpawn([]string{"-c", a.Home, "-p", prompt, "-w", "workspace", "-l", "consult-a", "--job", "job-b", "--t", "T1", "--role", "consult", "--grade", "A", "--parent-job", "author-job", "--owner", "consult-owner", "--report", report, "--dry-run"})
			if err != nil {
				t.Fatal(err)
			}
			out := captureOut(a)
			if e := a.Spawn(context.Background(), parsed); e != nil {
				t.Fatalf("spawn: %v\n%s", e, out)
			}
			if got := selectionChosen(t, out.String()); got != tc.want {
				t.Fatalf("spawn for %s author chose %s, want %s\n%s", tc.family, got, tc.want, out)
			}
		})
	}
}

// selectionChosen reads the chosen profile out of a printed selection record
// (the `selection=` JSON line of spawn --dry-run).
func selectionChosen(t *testing.T, out string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		raw, ok := strings.CutPrefix(line, "selection=")
		if !ok {
			continue
		}
		var sel struct {
			Chosen struct {
				Canon string `json:"canon"`
			} `json:"chosen"`
		}
		if err := json.Unmarshal([]byte(raw), &sel); err != nil {
			t.Fatalf("selection json: %v\n%s", err, raw)
		}
		return sel.Chosen.Canon
	}
	t.Fatalf("no selection line in output:\n%s", out)
	return ""
}

// TestConsultSpawnTaskReadsAuthorJob_t4572: a consult spawned with --task (no
// --parent-job) still reads the author's family from ARBITER_JOB, the same job
// resolve reads, so the author's own family is excluded.
func TestConsultSpawnTaskReadsAuthorJob_t4572(t *testing.T) {
	for _, tc := range []struct {
		family, want string
	}{
		{"anthropic", "codex-sol"},
		{"openai", "opus"},
	} {
		t.Run(tc.family, func(t *testing.T) {
			a, _, _ := consultAuthorFixture(t, tc.family)
			t.Setenv("ARBITER_JOB", "author-job")
			prompt := filepath.Join(a.Home, "brief")
			if e := os.WriteFile(prompt, []byte("consult the other family\n"), 0600); e != nil {
				t.Fatal(e)
			}
			report := filepath.Join(a.Jobs, "job-b", "report.md")
			parsed, err := parseSpawn([]string{"-c", a.Home, "-p", prompt, "-w", "workspace", "-l", "consult-a", "--job", "job-b", "--t", "T1", "--task", "42", "--owner", "consult-owner", "--role", "consult", "--grade", "A", "--report", report, "--dry-run"})
			if err != nil {
				t.Fatal(err)
			}
			out := captureOut(a)
			if e := a.Spawn(context.Background(), parsed); e != nil {
				t.Fatalf("spawn: %v\n%s", e, out)
			}
			if got := selectionChosen(t, out.String()); got != tc.want {
				t.Fatalf("spawn --task for %s author chose %s, want %s\n%s", tc.family, got, tc.want, out)
			}
		})
	}
}
