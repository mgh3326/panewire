package fleet

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSubmissionReceiptReusesKeyAfterLostHTTPResponse(t *testing.T) {
	a := &App{Jobs: t.TempDir()}
	req := Submission{RequestID: "00000000-0000-4000-8000-000000000001", Args: []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", "job", "--job", "job", "--t", "T1", "--task", "42", "--owner", "parent", "--role", "implementer"}, Brief: "marker-do-the-task", Repository: Repository{Name: "project", Origin: "https://github.com/example/project.git", LocalRoot: "/work/project"}, OriginMachine: "machine-a", DeadlineSeconds: 600}
	saved, err := a.saveSubmission(req)
	if err != nil {
		t.Fatal(err)
	}
	req.RequestID = "00000000-0000-4000-8000-000000000002"
	replay, err := a.saveSubmission(req)
	if err != nil || replay.RequestID != saved.RequestID {
		t.Fatalf("lost response generated a fresh key: %+v %v", replay, err)
	}
	req.Brief = "different task"
	if _, err = a.saveSubmission(req); err == nil || !strings.Contains(err.Error(), "replay the original") {
		t.Fatalf("changed payload was not actionable: %v", err)
	}
	path := filepath.Join(a.Jobs, "job", "submission.json")
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("receipt permissions: %v %v", info, err)
	}
}

func TestWaitSpawnPreservesRefusalAndUnknownOwnership(t *testing.T) {
	for _, test := range []struct {
		status   string
		rc, want int
	}{{"succeeded", 0, 0}, {"failed", 77, 77}, {"retryable", 70, 70}, {"deadline_exceeded", 124, exitStateConflict}} {
		t.Run(test.status, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{"status": test.status, "result": map[string]any{"rc": test.rc, "reason": "fixture"}})
			}))
			defer server.Close()
			c := hubHTTP{base: server.URL, token: "fixture", client: server.Client()}
			_, err := c.WaitSpawn(t.Context(), "00000000-0000-4000-8000-000000000001")
			if got := ErrorCode(err); got != test.want {
				t.Fatalf("rc=%d want=%d", got, test.want)
			}
		})
	}
}

func TestNodeSpawnWorkspaceOverrideAndExecutingDefault(t *testing.T) {
	for _, test := range []struct{ requested, want string }{{"", "wDEFAULT"}, {"workers", "workers"}, {"wOTHER", "wOTHER"}} {
		bound := bindNodeSpawn(Spawn{Workspace: test.requested, Host: "untrusted-host", CWD: "/wrong", Prompt: "/wrong"}, "/work/project", "/work/brief.md", "wDEFAULT")
		if bound.Workspace != test.want || bound.CWD != "/work/project" || bound.Host != "local" || !bound.Delegated {
			t.Fatalf("node binding %+v", bound)
		}
		args, err := hubArgs("/jobs", Spawn{Workspace: test.requested})
		if err != nil {
			t.Fatal(err)
		}
		has := false
		for i := range args {
			if args[i] == "-w" {
				has = true
				if i+1 >= len(args) || args[i+1] != test.requested {
					t.Fatal(args)
				}
			}
		}
		if has != (test.requested != "") {
			t.Fatalf("workspace override dropped: %v", args)
		}
	}
}

func TestPrepareSubmissionPinsPushedCheckoutAndResolvesAlias(t *testing.T) {
	for _, pushed := range []bool{true, false} {
		t.Run(fmt.Sprint(pushed), func(t *testing.T) {
			hosts := hubHosts()
			hosts["h1"]["aliases"] = []string{"m1b"}
			a, s, runner, hub, _ := hubFixture(t, gitState{pushed: pushed}, "wk/feat", hosts)
			a.Config.Local["machine_id"] = "source-a"
			if err := os.MkdirAll(s.CWD, 0700); err != nil {
				t.Fatal(err)
			}
			s.Prompt = filepath.Join(a.Home, "submission-brief.md")
			if err := os.WriteFile(s.Prompt, []byte("brief marker"), 0600); err != nil {
				t.Fatal(err)
			}
			s.Label, s.T, s.Owner, s.Host, s.OverrideReason, s.Containment = "job-a", "T1", "parent", "m1b", "fixture", "hk:12"
			request, err := a.PrepareSubmission(t.Context(), s)
			if !pushed {
				if ErrorCode(err) != exitPlacement || !strings.Contains(err.Error(), "push") {
					t.Fatalf("unpushed branch accepted: %+v %v", request, err)
				}
				return
			}
			if err != nil || request.Repository.Checkout == nil || request.Repository.Checkout.Sha != hubHead || request.Repository.Checkout.Branch != "wk/feat" || request.Brief != "brief marker" {
				t.Fatalf("pinned request %+v %v", request, err)
			}
			parsed, err := ValidateSubmission(request)
			if err != nil || parsed.Host != "h1" {
				t.Fatalf("alias %+v %v", parsed, err)
			}
			if !strings.Contains(a.Out.(interface{ String() string }).String(), "host_alias=m1b machine=h1") {
				t.Fatal("resolved machine not printed")
			}
			if sshCalls(runner) != 0 || len(hub.spawns) != 0 {
				t.Fatal("preparation executed a spawn")
			}
		})
	}
}
