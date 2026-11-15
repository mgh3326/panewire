package fleet

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

// hk 3706: a wait reads the owner lane inbox through the local node daemon,
// the route `panewire inbox list` takes by default, so a design session
// without the hub token in its config still sees the lane's signals. The
// direct hub connection this session's config names is dead in these
// fixtures, so a revert to it fails every case below.

// inboxDaemonAnswer is the daemon's answer to one inbox read: the list body
// the hub would have served, rendered the way the CLI prints it.
func inboxDaemonAnswer(t *testing.T, body string) func(map[string]any) string {
	t.Helper()
	return func(map[string]any) string {
		answer, err := json.Marshal(map[string]any{
			"ok":   true,
			"code": 0,
			"result": map[string]any{
				"code":   0,
				"stdout": body + "\n",
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return string(answer)
	}
}

// waitInboxFixture is a wait whose own hub connection cannot read the lane: a
// missing token env file, and a runner that refuses the direct CLI call.
func waitInboxFixture(t *testing.T) (*App, Spawn, *fakeRunner) {
	t.Helper()
	a, s := waitApp(t)
	a.Config.Hub = map[string]any{"hub_url": "https://hub.invalid", "hub_token_env": filepath.Join(t.TempDir(), "absent.env")}
	f := &fakeRunner{Do: func(Command) Result { return Result{Code: 5, Err: "inbox: invalid token env"} }}
	a.Runner = f
	return a, s, f
}

func waitErrText(a *App) string { return a.Err.(interface{ String() string }).String() }

// Acceptance 1: with no hub credential, the daemon's inbox read ends the wait
// with the documented exit status for the signal it carries.
func TestWaitReadsOwnerInboxThroughLocalDaemon(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
		wantCode         int
	}{
		{
			name: "ready",
			body: `[{"kind":"lane.event","event":{"job_id":"j","text":"[j] READY | https://pr.invalid/1 | /jobs/j/report.md"}}]`,
			want: "outcome=ready", wantCode: 0,
		},
		{
			name: "escalate",
			body: `[{"kind":"job.escalate","event":{"job_id":"j","report":"/jobs/j/report.md","question":"which branch?"}}]`,
			want: "outcome=escalate", wantCode: 83,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, f := waitInboxFixture(t)
			stub := newLaneDaemonStub(t, inboxDaemonAnswer(t, tc.body))
			t.Setenv("PANEWIRE_SOCKET", stub.path)
			if got := code(a.waitJob(context.Background(), s)); got != tc.wantCode {
				t.Fatalf("exit=%d want %d (out=%q err=%q)", got, tc.wantCode, waitOut(a), waitErrText(a))
			}
			if out := waitOut(a); !strings.Contains(out, tc.want) {
				t.Fatalf("out=%q, want %q", out, tc.want)
			}
			if len(f.Calls) != 0 {
				t.Fatalf("the daemon served the read, yet the hub route ran: %v", f.Calls)
			}
			request := <-stub.requests
			if request["op"] != "inbox" {
				t.Fatalf("op=%v, want the inbox op", request["op"])
			}
			inbox, _ := request["inbox"].(map[string]any)
			if inbox["command"] != "list" || inbox["lane"] != "default" || inbox["json"] != true {
				t.Fatalf("inbox request=%v, want a list for the owner lane", inbox)
			}
		})
	}
}

// Acceptance 2: no daemon answers, so the wait says so once — not once per
// poll — falls back to the configured hub connection, and still ends on the
// local event.
func TestWaitDaemonDownWarnsOnceAndKeepsLocalEvents(t *testing.T) {
	a, s, _ := waitInboxFixture(t)
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		if strings.HasPrefix(strings.Join(c.Args, " "), "inbox list") {
			// The local event that ends the wait arrives with this poll.
			putEvent(t, a, "j", "job.completed", map[string]any{"report": "/jobs/j/report.md", "pane_id": "w:p1"})
			return Result{Code: 5, Err: "inbox: invalid token env"}
		}
		return Result{}
	}}
	t.Setenv("PANEWIRE_SOCKET", filepath.Join(t.TempDir(), "absent.sock"))
	if e := a.waitJob(context.Background(), s); e != nil {
		t.Fatalf("local events must still end the wait: %v (err=%q)", e, waitErrText(a))
	}
	if out := waitOut(a); !strings.Contains(out, "outcome=completed") {
		t.Fatalf("out=%q", out)
	}
	errs := waitErrText(a)
	if n := strings.Count(errs, "panewire: warning: "); n != 1 {
		t.Fatalf("warnings=%d, want one: %q", n, errs)
	}
	if !strings.Contains(errs, "local node daemon unavailable") || !strings.Contains(errs, "lane inbox read failed") {
		t.Fatalf("warning does not name the daemon and the failed read: %q", errs)
	}
}

// A daemon that answers the read and fails is an answer: the wait warns once
// and does not read again with this session's own credential.
func TestWaitDaemonFailureDoesNotUseHubRoute(t *testing.T) {
	a, s, f := waitInboxFixture(t)
	stub := newLaneDaemonStub(t, func(map[string]any) string {
		return `{"ok":false,"code":6,"error":"inbox: hub returned HTTP 503"}`
	})
	t.Setenv("PANEWIRE_SOCKET", stub.path)
	if got := code(a.waitJob(context.Background(), s)); got != 85 {
		t.Fatalf("exit=%d want 85 (out=%q err=%q)", got, waitOut(a), waitErrText(a))
	}
	if len(f.Calls) != 0 {
		t.Fatalf("a failed daemon read must not fall back to the hub route: %v", f.Calls)
	}
	errs := waitErrText(a)
	if n := strings.Count(errs, "panewire: warning: "); n != 1 || !strings.Contains(errs, "through the local node daemon") {
		t.Fatalf("warnings=%d: %q", n, errs)
	}
}

// A daemon that answers it cannot serve the op — no hub client behind it — is
// as absent as a missing socket, so the wait takes the configured hub
// connection and reports the daemon once.
func TestWaitDaemonWithoutHubClientFallsBack(t *testing.T) {
	a, s, _ := waitInboxFixture(t)
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		if strings.HasPrefix(strings.Join(c.Args, " "), "inbox list") {
			return Result{Out: `[{"kind":"lane.event","event":{"job_id":"j","text":"[j] READY | https://pr.invalid/1 | /jobs/j/report.md"}}]`}
		}
		return Result{}
	}}
	stub := newLaneDaemonStub(t, func(map[string]any) string {
		return `{"ok":false,"code":4,"error":"inbox: daemon has no hub client"}`
	})
	t.Setenv("PANEWIRE_SOCKET", stub.path)
	if e := a.waitJob(context.Background(), s); e != nil {
		t.Fatalf("the hub route must end the wait: %v (err=%q)", e, waitErrText(a))
	}
	if out := waitOut(a); !strings.Contains(out, "outcome=ready") {
		t.Fatalf("out=%q", out)
	}
	errs := waitErrText(a)
	if n := strings.Count(errs, "panewire: warning: "); n != 1 || !strings.Contains(errs, "local node daemon unavailable") {
		t.Fatalf("warnings=%d: %q", n, errs)
	}
}

// A session with its own lane credential reads through that credential, the way
// lane registration does: the daemon's node identity may not serve its lane, so
// its answer is not this wait's answer. The hub read must name the session's
// lane credential file, not the configured hub token file.
func TestWaitWithOwnLaneCredentialSkipsTheDaemon(t *testing.T) {
	a, s, f := waitInboxFixture(t)
	a.HerdrSession = "fleet"
	laneEnv := filepath.Join(t.TempDir(), "lane.env")
	hubEnv := filepath.Join(t.TempDir(), "absent.env")
	a.Config.Hub = map[string]any{
		"hub_url":                  "https://hub.invalid",
		"hub_token_env":            hubEnv,
		"session_lanes_token_envs": map[string]any{"fleet": laneEnv},
	}
	f.Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		if !strings.HasPrefix(args, "inbox list") {
			return Result{}
		}
		if !strings.Contains(args, "--hub-token-env "+laneEnv) {
			t.Fatalf("the hub read must name the session's lane credential: %s", args)
		}
		if strings.Contains(args, "--hub-token-env "+hubEnv) {
			t.Fatalf("the hub read used the configured hub token, not the session's: %s", args)
		}
		return Result{Out: `[{"kind":"lane.event","event":{"job_id":"j","text":"[j] READY | https://pr.invalid/1 | /jobs/j/report.md"}}]`}
	}
	stub := newLaneDaemonStub(t, func(map[string]any) string {
		return `{"ok":false,"code":6,"error":"inbox: hub returned HTTP 403"}`
	})
	t.Setenv("PANEWIRE_SOCKET", stub.path)
	if e := a.waitJob(context.Background(), s); e != nil {
		t.Fatalf("the session's own connection must end the wait: %v (err=%q)", e, waitErrText(a))
	}
	if out := waitOut(a); !strings.Contains(out, "outcome=ready") {
		t.Fatalf("out=%q", out)
	}
	select {
	case request := <-stub.requests:
		t.Fatalf("a session with its own lane credential asked the daemon: %v", request)
	default:
	}
	if errs := waitErrText(a); errs != "" {
		t.Fatalf("the own-credential route is not a fallback and must not warn: %q", errs)
	}
}
