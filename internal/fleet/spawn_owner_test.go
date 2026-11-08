package fleet

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ownerHKFixture builds a hub submission whose --owner was omitted and whose
// hk answers name project/effective owner.
func ownerHKFixture(t *testing.T, project, effective string) (*App, Spawn, *fakeRunner) {
	t.Helper()
	a, s, f, _, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	a.Config.Local["machine_id"] = "source-a"
	if err := os.MkdirAll(s.CWD, 0700); err != nil {
		t.Fatal(err)
	}
	brief := filepath.Join(a.Home, "owner-brief.md")
	if err := os.WriteFile(brief, []byte("brief"), 0600); err != nil {
		t.Fatal(err)
	}
	parsed, err := parseSpawn([]string{"-c", s.CWD, "-p", brief, "-l", "job-a", "--job", "job-a", "--t", "T1", "--task", "42", "--host", "h1", "--override-reason", "fixture", "--containment", "hk:12"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(map[string]any{"id": 42, "project": project})
	hk := useFakeHK(t).SetTask("42", string(b))
	if effective != "" {
		b, _ := json.Marshal(map[string]any{"name": project, "design_lane": effective, "effective_owner": effective})
		hk.SetOwner(project, string(b))
	}
	return a, parsed, f
}

func ownerArgValue(args []string, flag string) string {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag {
			return args[i+1]
		}
	}
	return ""
}

func TestSpawnOwnerDefaultsFromHKProject(t *testing.T) {
	for _, tc := range []struct{ name, project, effective string }{
		{"design lane", "panewire", "panewire-design"},
		{"operator desk from hk", "other", "operator-desk"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, _ := ownerHKFixture(t, tc.project, tc.effective)
			request, err := a.PrepareSubmission(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			if got := ownerArgValue(request.Args, "--owner"); got != tc.effective {
				t.Fatalf("owner=%q want %q (args %v)", got, tc.effective, request.Args)
			}
			want := "owner=" + tc.effective + " source=hk project=" + tc.project + "\n"
			if out := a.Out.(interface{ String() string }).String(); !strings.Contains(out, want) {
				t.Fatalf("owner line missing: %q want %q", out, want)
			}
		})
	}
}

func TestSpawnOwnerRefusedWhenHKCannotAnswer(t *testing.T) {
	tests := []struct {
		name   string
		answer func(hkRequest) (int, string, bool)
	}{
		{"owner endpoint 500", hkFails("GET /v1/tasks/projects/", http.StatusInternalServerError, `{"error":"internal"}`)},
		{"owner endpoint 404", hkFails("GET /v1/tasks/projects/", http.StatusNotFound, `{"error":"not_found"}`)},
		{"hk unreachable", hkFails("GET /v1/tasks/42", http.StatusBadGateway, `{"error":"bad_gateway"}`)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, s, _ := ownerHKFixture(t, "panewire", "panewire-design")
			useFakeHK(t).Handle(tc.answer)
			_, err := a.PrepareSubmission(context.Background(), s)
			if exitCode(err) != exitUsage || !strings.Contains(err.Error(), "--owner") {
				t.Fatalf("hk failure must refuse with exitUsage naming --owner: %v", err)
			}
			if out := a.Out.(interface{ String() string }).String(); strings.Contains(out, "source=hk") {
				t.Fatalf("a refused spawn printed a resolved owner: %q", out)
			}
		})
	}
}

func TestSpawnExplicitOwnerWinsWhenHKIsDown(t *testing.T) {
	a, s, f, _, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	a.Config.Local["machine_id"] = "source-a"
	if err := os.MkdirAll(s.CWD, 0700); err != nil {
		t.Fatal(err)
	}
	brief := filepath.Join(a.Home, "owner-brief.md")
	if err := os.WriteFile(brief, []byte("brief"), 0600); err != nil {
		t.Fatal(err)
	}
	parsed, err := parseSpawn([]string{"-c", s.CWD, "-p", brief, "-l", "job-a", "--job", "job-a", "--t", "T1", "--task", "42", "--host", "h1", "--owner", "explicit-lane", "--override-reason", "fixture", "--containment", "hk:12"})
	if err != nil {
		t.Fatal(err)
	}
	hk := useFakeHK(t)
	hk.Close()
	request, err := a.PrepareSubmission(context.Background(), parsed)
	if err != nil {
		t.Fatalf("explicit owner must not need hk: %v", err)
	}
	if got := ownerArgValue(request.Args, "--owner"); got != "explicit-lane" {
		t.Fatalf("owner=%q args=%v", got, request.Args)
	}
	for _, r := range hk.Requests() {
		if strings.Contains(r.Path, "/owner") {
			t.Fatalf("explicit owner must not read hk: %v", r.Route())
		}
	}
	for _, c := range f.Calls {
		if c.Name == "handoffkeep" {
			t.Fatalf("explicit owner must not run the hk CLI: %v", c.Args)
		}
	}
}

func TestSpawnParentJobOwnerFromHKProject(t *testing.T) {
	a, s, _, _, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	a.Config.Local["machine_id"] = "source-a"
	if err := os.MkdirAll(s.CWD, 0700); err != nil {
		t.Fatal(err)
	}
	brief := filepath.Join(a.Home, "owner-brief.md")
	if err := os.WriteFile(brief, []byte("brief"), 0600); err != nil {
		t.Fatal(err)
	}
	store := a.JobStore.(*fakeJobStore)
	store.records["parent-job"] = map[string]any{"job_id": "parent-job", "task_id": "77"}
	parsed, err := parseSpawn([]string{"-c", s.CWD, "-p", brief, "-l", "job-a", "--job", "job-a", "--t", "T1", "--parent-job", "parent-job", "--host", "h1", "--override-reason", "fixture", "--containment", "hk:12"})
	if err != nil {
		t.Fatal(err)
	}
	useFakeHK(t).SetTask("77", `{"id":77,"project":"mergewake"}`).
		SetOwner("mergewake", `{"name":"mergewake","design_lane":"mergewake-design","effective_owner":"mergewake-design"}`)
	request, err := a.PrepareSubmission(context.Background(), parsed)
	if err != nil {
		t.Fatal(err)
	}
	if got := ownerArgValue(request.Args, "--owner"); got != "mergewake-design" {
		t.Fatalf("owner=%q args=%v", got, request.Args)
	}
	if got := ownerArgValue(request.Args, "--parent-job"); got != "parent-job" {
		t.Fatalf("parent-job=%q args=%v", got, request.Args)
	}
	if out := a.Out.(interface{ String() string }).String(); !strings.Contains(out, "owner=mergewake-design source=hk project=mergewake") {
		t.Fatalf("owner line missing: %q", out)
	}
}

func TestSpawnDryRunOwnerResolvedFromHK(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	parsed, err := parseSpawn([]string{"-c", s.CWD, "-p", s.Prompt, "-m", "cmd-dsflash41", "-w", "workspace", "-l", "worker-a", "--job", "job-a", "--t", "T1", "--task", "42", "--host", "local", "--override-reason", "fixture", "--containment", "hk:12", "--dry-run"})
	if err != nil {
		t.Fatal(err)
	}
	useFakeHK(t).SetTask("42", `{"id":42,"title":"implement (grade B, T1)","state":"ready","refs":{},"project":"panewire"}`).
		SetOwner("panewire", `{"name":"panewire","design_lane":"panewire-design","effective_owner":"panewire-design"}`)
	if err := a.Spawn(context.Background(), parsed); err != nil {
		t.Fatal(err)
	}
	if out := a.Out.(interface{ String() string }).String(); !strings.Contains(out, "owner=panewire-design source=hk project=panewire") {
		t.Fatalf("owner line missing: %q", out)
	}
}
