package fleet

import (
	"context"
	"slices"
	"strings"
	"testing"
)

func TestSpawnContainment(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"pin missing", []string{"-m", "codex-x", "--override-reason", "r"}, "containment_required"},
		{"pin forged hub reason", []string{"-m", "codex-x", "--override-reason", "hub-directed"}, "containment_required"},
		{"exclude with forged hub reason", []string{"--exclude-host", "h1", "--override-reason", "hub-directed"}, ""},
		{"pin containment", []string{"-m", "codex-x", "--override-reason", "r", "--containment", "hk:12"}, ""},
		{"pin operator", []string{"-m", "codex-x", "--override-reason", "r", "--operator-request", "op-1"}, ""},
		// A standing exclusion is policy, not containment (operator 2026-11-05).
		{"exclude alone", []string{"--exclude-host", "h1"}, ""},
		{"exclude containment", []string{"--exclude-host", "h1", "--containment", "hk:12"}, ""},
		{"mergewake reviewer shape", []string{"-m", "auto", "--host", "auto", "--exclude-host", "oci"}, ""},
		{"auto", []string{"-m", "auto", "--host", "auto"}, ""},
		{"local", []string{"-m", "auto", "--host", "local"}, ""},
		{"bad letters", []string{"--containment", "hk:abc"}, "usage"},
		{"bad prefix", []string{"--containment", "12"}, "usage"},
		{"zero", []string{"--containment", "hk:0"}, "usage"},
		{"too long", []string{"--containment", "hk:1234567890"}, "usage"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := parseSpawn(tt.args)
			if err != nil {
				t.Fatal(err)
			}
			err = validateContainment(s)
			if tt.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || exitCode(err) != exitUsage {
				t.Fatalf("expected usage refusal, got %v", err)
			}
			if got := SpawnRefusalCode(err.Error(), exitCode(err)); got != tt.want {
				t.Fatalf("code = %q, want %q", got, tt.want)
			}
			if tt.want == "containment_required" {
				hint := spawnRefusalFix(err)
				if !strings.Contains(hint, "--containment") || !strings.Contains(hint, "--operator-request") {
					t.Fatalf("hint: %s", hint)
				}
			}
		})
	}
	for _, s := range []Spawn{{Profile: "codex-x", Delegated: true}} {
		if err := validateContainment(s); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSpawnContainmentBeforeAdmission(t *testing.T) {
	for _, args := range [][]string{{"-m", "codex-x", "--override-reason", "r"}, {"--host", "h1", "--override-reason", "r"}} {
		a, base, _, _ := cmdFixture(t)
		s, err := parseSpawn(args)
		if err != nil {
			t.Fatal(err)
		}
		s.CWD, s.Prompt, s.Label, s.Job, s.T, s.Task, s.Role = base.CWD, base.Prompt, base.Label, base.Job, base.T, base.Task, base.Role
		err = a.Spawn(context.Background(), s)
		if err == nil || exitCode(err) != exitUsage || SpawnRefusalCode(err.Error(), exitCode(err)) != "containment_required" {
			t.Fatalf("args %v: %v", args, err)
		}
	}
	if _, err := parseSpawn([]string{"--containment", ""}); err == nil || exitCode(err) != exitUsage {
		t.Fatalf("empty containment: %v", err)
	}
}

func TestSpawnContainmentRecorded(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	s.Containment = "hk:12"
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	events, err := a.events(s.Job)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if ev.Kind == "job.placement" {
			if ev.Payload["containment"] != "hk:12" || ev.Payload["override_reason"] != s.OverrideReason {
				t.Fatalf("placement: %v", ev.Payload)
			}
			return
		}
	}
	t.Fatal("job.placement missing")
}

func TestContainmentHubArgs(t *testing.T) {
	s := Spawn{Containment: "hk:12"}
	args, err := hubArgs("/jobs", s)
	if err != nil {
		t.Fatal(err)
	}
	i := slices.Index(args, "--containment")
	if i < 0 || i+1 == len(args) || args[i+1] != s.Containment {
		t.Fatalf("node args: %v", args)
	}
	args = submissionArgs(s)
	i = slices.Index(args, "--containment")
	if i < 0 || i+1 == len(args) || args[i+1] != s.Containment {
		t.Fatalf("submission args: %v", args)
	}
}

func TestContainmentSubmissionLocalHost(t *testing.T) {
	request := Submission{RequestID: "00000000-0000-4000-8000-000000000001", Args: []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", "job", "--job", "job", "--t", "T1", "--task", "42", "--owner", "parent", "--role", "implementer", "--host", "local", "--override-reason", "r"}, Brief: "task", Repository: Repository{Name: "project", Origin: "https://github.com/example/project.git", LocalRoot: "/work/project"}, OriginMachine: "machine-a", DeadlineSeconds: 600}
	s, err := ValidateSubmission(request)
	if err != nil || s.Host != "machine-a" {
		t.Fatalf("local submission: %+v %v", s, err)
	}
	if err := validateContainment(s); err != nil {
		t.Fatalf("resolved local host: %v", err)
	}
}
