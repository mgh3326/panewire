package fleet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// hubRetrySubmission is a valid hub submission whose host is pinned to desktop
// and carries no containment or operator reference.
func hubRetrySubmission(host string, extra ...string) Submission {
	args := []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", "test-job", "--job", "test-job", "--t", "T1", "--task", "42", "--owner", "parent", "--role", "implementer", "--host", host, "--override-reason", "verified-cleanup-retry"}
	return Submission{
		RequestID: "d676220b-e763-4af1-aefd-000000000001", Args: append(args, extra...), Brief: "Implement the task", OriginMachine: "machine-a", DeadlineSeconds: 600,
		Repository: Repository{Name: "project", Origin: "https://github.com/example/project.git", LocalRoot: "/work/project", Worktree: "project.test-job"},
	}
}

func requireContainmentRefusal(t *testing.T, err error) {
	t.Helper()
	if err == nil || SpawnRefusalCode(err.Error(), exitCode(err)) != "containment_required" {
		t.Fatalf("want containment_required, got %v", err)
	}
}

// The hub's own verified-cleanup retry re-pins the host its stored outcome
// named. That is the hub's act, not a submitter's placement pin.
func TestSpawnContainmentHubRetryHost(t *testing.T) {
	t.Run("marker accepts exactly the rewritten host", func(t *testing.T) {
		request := hubRetrySubmission("desktop")
		request.RetryHost = "desktop"
		if _, err := ValidateSubmission(request); err != nil {
			t.Fatalf("hub retry refused: %v", err)
		}
	})
	t.Run("marker for another host does not cover the pin", func(t *testing.T) {
		request := hubRetrySubmission("desktop")
		request.RetryHost = "oci"
		_, err := ValidateSubmission(request)
		requireContainmentRefusal(t, err)
	})
	t.Run("marker does not cover a profile pin", func(t *testing.T) {
		request := hubRetrySubmission("desktop")
		request.Args[5] = "codex-luna"
		request.RetryHost = "desktop"
		_, err := ValidateSubmission(request)
		requireContainmentRefusal(t, err)
	})
	t.Run("submitter pin without marker is refused", func(t *testing.T) {
		_, err := ValidateSubmission(hubRetrySubmission("desktop"))
		requireContainmentRefusal(t, err)
	})
	t.Run("the marker is not a wire field", func(t *testing.T) {
		for _, key := range []string{"RetryHost", "retry_host", "hub_retry_host"} {
			raw := `{"request_id":"d676220b-e763-4af1-aefd-000000000001","` + key + `":"desktop"}`
			var decoded Submission
			if err := json.Unmarshal([]byte(raw), &decoded); err != nil || decoded.RetryHost != "" {
				t.Fatalf("%s decoded into the marker: %+v %v", key, decoded, err)
			}
		}
		request := hubRetrySubmission("desktop")
		request.RetryHost = "desktop"
		raw, err := json.Marshal(request)
		if err != nil || strings.Contains(strings.ToLower(string(raw)), "retry_host") || strings.Contains(string(raw), "RetryHost") {
			t.Fatalf("marker serialized: %s %v", raw, err)
		}
	})
	t.Run("no flag sets the marker", func(t *testing.T) {
		for _, flag := range []string{"--retry-host", "--hub-retry-host", "--retryhost"} {
			s, err := parseSpawn([]string{flag, "desktop", "--host", "desktop"})
			if err == nil {
				t.Fatalf("%s parsed: %+v", flag, s)
			}
			request := hubRetrySubmission("desktop", flag, "desktop")
			if _, err := ValidateSubmission(request); err == nil {
				t.Fatalf("%s accepted through a submission", flag)
			}
		}
		s, err := parseSpawn([]string{"--host", "desktop"})
		if err != nil || s.hubRetryHost != "" {
			t.Fatalf("parseSpawn set the marker: %+v %v", s, err)
		}
	})
}

// Spawn checks containment again on the Spawn the validated submission
// produced, so the marker must survive into that second check. The fixture
// has no quota catalog, so a marked retry stops later, at selection; an
// unmarked pin stops at containment, before any hub call.
func TestSpawnHubRetryPassesContainmentInSpawn(t *testing.T) {
	run := func(retryHost string) (*fakeHub, error) {
		a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
		s.Host, s.Containment, s.OperatorRequest, s.hubRetryHost, s.Profile = "h1", "", "", retryHost, "auto"
		s.OverrideReason, s.Label, s.T, s.Workspace = "verified-cleanup-retry", "label-a", "T1", "workspace"
		s.Prompt = filepath.Join(a.Home, "brief.md")
		if err := os.WriteFile(s.Prompt, []byte("Implement the task\n"), 0600); err != nil {
			t.Fatal(err)
		}
		return hub, a.Spawn(context.Background(), s)
	}
	_, err := run("h1")
	if err == nil || SpawnRefusalCode(err.Error(), exitCode(err)) == "containment_required" {
		t.Fatalf("marked retry must get past containment, got %v", err)
	}
	hub, err := run("")
	requireContainmentRefusal(t, err)
	if len(hub.spawns) != 0 {
		t.Fatalf("unmarked pin reached the node: %+v", hub.spawns)
	}
}
