package fleet

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestPresetFlagSets(t *testing.T) {
	a := testApp(t)
	// The pane's own lane is not the owner any more: an omitted --owner is
	// resolved from the task's hk project by the spawn path.
	t.Setenv("PANEWIRE_LANE", "caller-lane")
	useFakeHK(t).SetTask("42", `{"id":42,"title":"change (grade A, T1)","state":"ready","refs":{"worktree":{"path":"/work/project.feature"}}}`)
	job := "task42-work"
	want, err := parseSpawn([]string{"-c", "/work/project.feature", "-m", "auto", "-p", filepath.Join(a.Jobs, job, "brief.md"), "-w", "workers", "-l", job, "--job", job, "--t", "T1", "--task", "42", "--host", "auto", "--report", filepath.Join(a.Jobs, job, "report.md"), "--role", "implementer", "--dry-run"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := a.workPreset(context.Background(), []string{"42", "--dry-run"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		g, _ := json.Marshal(got)
		w, _ := json.Marshal(want)
		t.Fatalf("preset=%s\nlong=%s", g, w)
	}
	if got.Owner != "" || !got.ownerOmitted {
		t.Fatalf("pane lane must not become the owner: %+v", got)
	}
}

func TestPresetOverrides(t *testing.T) {
	a := testApp(t)
	useFakeHK(t).SetTask("42", `{"id":42,"title":"change (grade A, T1)","state":"ready","refs":{"job_id":"existing-work","cwd":"/work/project"}}`)
	s, err := a.workPreset(context.Background(), []string{"42", "--owner", "caller", "--job", "custom", "-l", "custom", "-p", "/brief", "--report", "/report", "-c", "/work/other", "-w", "wF"})
	if err != nil {
		t.Fatal(err)
	}
	if s.Job != "custom" || s.Label != "custom" || s.Owner != "caller" || s.CWD != "/work/other" || s.Workspace != "wF" || s.Prompt != "/brief" || s.Report != "/report" {
		t.Fatalf("%+v", s)
	}
}

// The delegated lead seat is retired: `panewire lead` and `--role builder`
// (or its legacy alias captain) are usage refusals that point to the new lead
// definition, before any task, host or hub is read.
func TestRetiredLeadSeatRefused(t *testing.T) {
	spawnArgs := func(role string) []string {
		return []string{"spawn", "-c", "/work/p", "-p", "/brief", "-l", "job", "--t", "T1", "--task", "42", "--role", role, "--lane", "job", "--parent", "caller"}
	}
	for name, args := range map[string][]string{
		"lead command": {"lead", "42", "--parent", "caller"},
		"role builder": spawnArgs("builder"),
	} {
		t.Run(name, func(t *testing.T) {
			a := testApp(t)
			a.Runner = &fakeRunner{Do: func(c Command) Result {
				t.Errorf("refusal must not run %v", c.Args)
				return Result{}
			}}
			err := a.Run(context.Background(), args)
			if exitCode(err) != exitUsage || err == nil || !strings.Contains(err.Error(), retiredLeadSeatPointer) {
				t.Fatalf("exit=%d err=%v", exitCode(err), err)
			}
			if strings.Contains(err.Error(), "\n") {
				t.Fatalf("pointer must be one line: %q", err)
			}
		})
	}
}

func TestHostAliasResolution(t *testing.T) {
	a := testApp(t)
	a.Config.Hosts = map[string]map[string]any{"machine-a": {"aliases": []any{"m1", "m1b", "mbp", "desktop", "pi"}}}
	for _, alias := range []string{"m1", "m1b", "mbp", "desktop", "pi", "machine-a"} {
		got, err := a.resolveHost(alias)
		if err != nil || got != "machine-a" {
			t.Fatalf("%s=%s %v", alias, got, err)
		}
	}
	a.Config.Hosts["machine-b"] = map[string]any{"aliases": []any{"m1"}}
	if _, err := a.resolveHost("m1"); err == nil || !strings.Contains(err.Error(), "canonical") {
		t.Fatalf("ambiguous=%v", err)
	}
	if _, err := a.resolveHost("missing"); err == nil || !strings.Contains(err.Error(), "aliases") {
		t.Fatalf("missing=%v", err)
	}
}

func TestWorkspaceLabelAndConfiguredDefault(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.Workspace = ""
	a.Config.Local["workspace"] = "workers"
	previous := f.Do
	f.Do = func(c Command) Result {
		if strings.Join(c.Args, " ") == "workspace list" {
			return Result{Out: `{"result":{"workspaces":[{"workspace_id":"wF","label":"workers"}]}}`}
		}
		if len(c.Args) > 1 && c.Args[0] == "tab" && c.Args[1] == "create" {
			if !strings.Contains(strings.Join(c.Args, " "), "--workspace wF ") {
				t.Errorf("label not resolved: %v", c.Args)
			}
		}
		return previous(c)
	}
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
}

func TestSpawnRetryAfterPartialRemoteFailure(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.Delegated = true // the same execution path used by a remote receiver
	previous := f.Do
	failed := false
	f.Do = func(c Command) Result {
		if !failed && len(c.Args) > 1 && c.Args[0] == "tab" && c.Args[1] == "create" {
			failed = true
			return Result{Code: 70, Err: "fixture tab failure"}
		}
		return previous(c)
	}
	if err := a.Spawn(context.Background(), s); err == nil {
		t.Fatal("first attempt must fail")
	}
	store := a.JobStore.(*fakeJobStore)
	if str(store.record(t, s.Job), "state") != "claimed" {
		t.Fatal("retryable attempt consumed job id")
	}
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatalf("same id retry: %v", err)
	}
	if err := a.Spawn(context.Background(), s); code(err) != 74 {
		t.Fatalf("running job duplicate=%v", err)
	}
}
