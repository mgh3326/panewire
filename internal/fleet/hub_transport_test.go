package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// fakeHub is the in-memory hub: it records every request and answers from
// node, per machine, so a test scripts the node's reply or the hub's failure.
type fakeHub struct {
	t         *testing.T
	events    *[]string
	spawns    []HubSpawnRequest
	refreshes [][2]string
	node      func(HubSpawnRequest) (HubSpawnReply, error)
	refresh   func(machine, pool string) error
}

func (h *fakeHub) Spawn(_ context.Context, r HubSpawnRequest) (HubSpawnReply, error) {
	h.spawns = append(h.spawns, r)
	*h.events = append(*h.events, "hub.spawn")
	if h.node != nil {
		return h.node(r)
	}
	return HubSpawnReply{RequestID: r.RequestID, Machine: r.Machine, JobID: r.Args[1], Pane: "w:p1", StdoutTail: "OK pane=w:p1 job=job-a\n"}, nil
}

func (h *fakeHub) Refresh(_ context.Context, machine, pool string) error {
	h.refreshes = append(h.refreshes, [2]string{machine, pool})
	*h.events = append(*h.events, "hub.refresh")
	if h.refresh != nil {
		return h.refresh(machine, pool)
	}
	return nil
}

// gitState is what the fake local checkout answers for the hub's ref probes.
type gitState struct {
	pushed    bool     // HEAD is an ancestor of refs/remotes/origin/<branch>
	onOrigin  bool     // refs/remotes/origin/<branch> exists; pushed implies it
	originTip string   // sha of refs/remotes/origin/<branch>; hubHead when empty
	pointsAt  []string // refs whose tip is HEAD
	contained []string // refs that contain HEAD
}

const hubHead = "0123456789abcdef0123456789abcdef01234567"

// hubFixture places a job from a linked worktree onto hub hosts h1 and h2.
// Any ssh use fails the test: a hub host must never reach ssh.
func hubFixture(t *testing.T, g gitState, branch string, hosts map[string]map[string]any) (*App, Spawn, *fakeRunner, *fakeHub, *[]string) {
	t.Helper()
	remote := &remoteWT{}
	a, s, f := worktreeFixture(t, remote, branch, hosts)
	events := &[]string{}
	hub := &fakeHub{t: t, events: events}
	a.Hub = hub
	useFakeHK(t).Handle(func(r hkRequest) (int, string, bool) {
		if r.Method != http.MethodPut || !strings.HasPrefix(r.Path, "/v1/documents/") {
			return 0, "", false
		}
		var doc struct{ Body string }
		_ = json.Unmarshal(r.Body, &doc)
		*events = append(*events, "doc.put", "doc.body="+doc.Body)
		return 0, "", false
	})
	base := f.Do
	f.Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch {
		case c.Name == "ssh":
			t.Errorf("ssh reached for a hub host: %s", args)
		case c.Name == "git" && strings.Contains(args, "merge-base --is-ancestor"):
			if !strings.Contains(args, "refs/remotes/origin/"+branch) {
				t.Errorf("branch probe names the wrong ref: %s", args)
			}
			if g.pushed {
				return Result{}
			}
			return Result{Code: 1}
		case c.Name == "git" && branch != "" && strings.Contains(args, "rev-parse --verify") && strings.Contains(args, "refs/remotes/origin/"+branch):
			if g.pushed || g.onOrigin {
				return Result{Out: firstNonempty(g.originTip, hubHead) + "\n"}
			}
			return Result{Code: 1}
		case c.Name == "git" && strings.Contains(args, "for-each-ref") && strings.Contains(args, "--points-at"):
			return Result{Out: strings.Join(g.pointsAt, "\n")}
		case c.Name == "git" && strings.Contains(args, "for-each-ref") && strings.Contains(args, "--contains"):
			return Result{Out: strings.Join(g.contained, "\n")}
		}
		return base(c)
	}
	return a, s, f, hub, events
}

func indexArg(args []string, flag string) int {
	for i, a := range args {
		if a == flag && i+1 < len(args) {
			return i
		}
	}
	return -1
}

func hubHosts() map[string]map[string]any {
	return map[string]map[string]any{"h1": {"via": "hub"}, "h2": {"via": "hub"}}
}

func sshCalls(f *fakeRunner) int {
	n := 0
	for _, c := range f.Calls {
		if c.Name == "ssh" {
			n++
		}
	}
	return n
}

// The panewire#18 remote-worktree table, through the hub path.
func TestHubWorktreeCheckouts(t *testing.T) {
	tests := []struct {
		name   string
		git    gitState
		branch string
		want   HubCheckout
	}{
		{"pushed branch", gitState{pushed: true}, "wk/feat",
			HubCheckout{Origin: "https://git.invalid/org/repo-a.git", Mode: "branch", Branch: "wk/feat", Dir: "repo-a.wk"}},
		{"PR head, detached HEAD", gitState{pointsAt: []string{"refs/remotes/origin/pull/7/head", "refs/remotes/origin/wk/other"}}, "",
			HubCheckout{Origin: "https://git.invalid/org/repo-a.git", Mode: "detached", Sha: hubHead, Fetch: "pull/7/head", Dir: "repo-a.wk"}},
		{"detached HEAD at a branch tip", gitState{pointsAt: []string{"refs/remotes/origin/HEAD", "refs/remotes/origin/main"}}, "",
			HubCheckout{Origin: "https://git.invalid/org/repo-a.git", Mode: "detached", Sha: hubHead, Fetch: "refs/heads/main", Dir: "repo-a.wk"}},
		{"detached HEAD inside a branch", gitState{contained: []string{"refs/remotes/origin/HEAD", "refs/remotes/origin/wk/feat"}}, "",
			HubCheckout{Origin: "https://git.invalid/org/repo-a.git", Mode: "detached", Sha: hubHead, Fetch: "refs/heads/wk/feat", Dir: "repo-a.wk"}},
		{"branch whose commits sit on another origin ref", gitState{contained: []string{"refs/remotes/origin/main"}}, "wk/feat",
			HubCheckout{Origin: "https://git.invalid/org/repo-a.git", Mode: "detached", Sha: hubHead, Fetch: "refs/heads/main", Dir: "repo-a.wk"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, s, f, hub, events := hubFixture(t, tc.git, tc.branch, hubHosts())
			handled, rec, e := a.placeAuto(context.Background(), s, "the brief")
			if e != nil || !handled || rec["chose"] != "h1" {
				t.Fatalf("handled=%v rec=%v err=%v", handled, rec, e)
			}
			if len(hub.spawns) != 1 || hub.spawns[0].Checkout == nil || *hub.spawns[0].Checkout != tc.want {
				t.Fatalf("checkout: %+v", hub.spawns)
			}
			got := hub.spawns[0]
			if got.Machine != "h1" || got.CWDKey != "repo-a" || got.Brief.Doc != "briefs/job-a.md" || got.WaitSeconds != hubDefaultWait {
				t.Fatalf("request: %+v", got)
			}
			if sshCalls(f) != 0 {
				t.Fatal("ssh was used")
			}
			// The brief is uploaded first, its text intact, then referenced.
			if !reflect.DeepEqual(*events, []string{"doc.put", "doc.body=the brief", "hub.spawn"}) {
				t.Fatalf("order: %v", *events)
			}
			if out := a.Out.(interface{ String() string }).String(); !strings.Contains(out, "OK pane=w:p1 job=job-a host=h1") {
				t.Fatalf("receipt: %q", out)
			}
			var put []hkRequest
			for _, r := range useFakeHK(t).Requests() {
				if r.Method == http.MethodPut {
					put = append(put, r)
				}
			}
			var doc struct{ Key, Kind, Job string }
			if len(put) != 1 || put[0].Path != "/v1/documents/briefs%2Fjob-a.md" || json.Unmarshal(put[0].Body, &doc) != nil || doc != (struct{ Key, Kind, Job string }{"briefs/job-a.md", "brief", "job-a"}) {
				t.Fatalf("doc put: %v %+v", put, doc)
			}
		})
	}
}

func TestHubCanonicalCheckoutSendsNoCheckout(t *testing.T) {
	src := &fakePlacementSource{hosts: []HostView{{Name: "h1", Connected: true, LoadRatio: ptrF(0.1)}}}
	a, s, f := remoteFixture(t, src, &fakeWaker{}, "2026-01-01T00:00:00Z", func(string) Result {
		t.Error("ssh reached for a hub host")
		return Result{}
	})
	events := &[]string{}
	hub := &fakeHub{t: t, events: events}
	a.Hub = hub
	a.Config.Hosts = map[string]map[string]any{"h1": {"via": "hub"}}
	a.Config.HostOrder = []string{"h1"}
	if _, _, e := a.placeAuto(context.Background(), s, "brief"); e != nil {
		t.Fatal(e)
	}
	if len(hub.spawns) != 1 || hub.spawns[0].Checkout != nil || hub.spawns[0].CWDKey != "repo-a" {
		t.Fatalf("%+v", hub.spawns)
	}
	_ = f
}

func TestHubSpawnArgs(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	s.Label, s.T, s.Effort, s.Owner, s.Lane, s.Parent, s.Urgency = "label-a", "T2", "high", "lane-a", "lane-b", "lane-c", "urgent"
	s.Role, s.AuthorFamily, s.BuilderJob = "reviewer", "anthropic", "job-b"
	if _, _, e := a.placeAuto(context.Background(), s, "brief"); e != nil {
		t.Fatal(e)
	}
	want := []string{"-m", "sonnet", "-l", "label-a", "--t", "T2", "--job", "job-a", "--owner", "lane-a", "--effort", "high", "--role", "reviewer", "--lane", "lane-b", "--parent", "lane-c", "--task", "42", "--urgency", "urgent", "--author-family", "anthropic", "--builder-job", "job-b"}
	if !reflect.DeepEqual(hub.spawns[0].Args, want) {
		t.Fatalf("args:\n got %v\nwant %v", hub.spawns[0].Args, want)
	}
	// Node-side flags are never sent: the node supplies workspace, host and
	// the override reason itself.
	for _, bad := range []string{"--host", "-p", "-c", "--wait"} {
		if slices.Contains(hub.spawns[0].Args, bad) {
			t.Fatalf("%s must not be sent", bad)
		}
	}
}

func TestHubArgsParentJobReplacesTask(t *testing.T) {
	args, e := hubArgs("/r", Spawn{Profile: "sonnet", Job: "j", Task: "42", ParentJob: "job-b", Report: "/r/report.md"})
	if e != nil {
		t.Fatal(e)
	}
	if slices.Contains(args, "--task") || !reflect.DeepEqual(args[len(args)-4:], []string{"--parent-job", "job-b", "--report-relative", "report.md"}) {
		t.Fatalf("%v", args)
	}
}

func TestHubArgsPreserveOptionsAcrossInProcessHub(t *testing.T) {
	for flag, s := range map[string]Spawn{"--keep": {Keep: true}, "--job-dup-ok": {DupOK: true}, "--task-hk-bypass": {Bypass: true}, "--allow-escalation": {AllowEscalation: true}, "--operator-request": {OperatorRequest: "x"}, "--requested-by": {RequestedBy: "x"}, "--purpose": {Purpose: "x"}} {
		args, err := hubArgs("/r", s)
		if err != nil || !slices.Contains(args, flag) {
			t.Errorf("%s dropped: %v %v", flag, args, err)
		}
	}
}

func TestHubCheckoutRefusalsExit81(t *testing.T) {
	for _, outcome := range []string{"branch-elsewhere", "sha-missing", "origin-mismatch", "dirty", "locked", "git-failed", "git_too_old"} {
		t.Run(outcome, func(t *testing.T) {
			a, s, f, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
			a.Config.Local = map[string]any{"spawn": true}
			hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
				return HubSpawnReply{RequestID: r.RequestID, RC: 81, Error: "checkout_refused:" + outcome}, nil
			}
			handled, _, e := a.placeAuto(context.Background(), s, "brief")
			if !handled || e == nil || !strings.Contains(e.Error(), outcome) || exitCode(e) != exitPlacement {
				t.Fatalf("handled=%v err=%v", handled, e)
			}
			if len(hub.spawns) != 1 {
				t.Fatalf("a refused checkout must not try the next host: %d spawns", len(hub.spawns))
			}
			if sshCalls(f) != 0 {
				t.Fatal("ssh was used")
			}
		})
	}
}

func TestHubCheckoutBusyRetriesOnce(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	n := 0
	hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
		n++
		if n == 1 {
			return HubSpawnReply{RequestID: r.RequestID, RC: 81, Error: "checkout_refused:busy"}, nil
		}
		return HubSpawnReply{RequestID: r.RequestID, RC: 0, Pane: "w:p2", JobID: "job-a"}, nil
	}
	if _, _, e := a.placeAuto(context.Background(), s, "brief"); e != nil {
		t.Fatal(e)
	}
	if len(hub.spawns) != 2 || hub.spawns[0].RequestID == hub.spawns[1].RequestID {
		t.Fatalf("a busy clone retries once under a fresh request id: %+v", hub.spawns)
	}
	if out := a.Out.(interface{ String() string }).String(); !strings.Contains(out, "OK pane=w:p2 job=job-a host=h1") {
		t.Fatalf("receipt synthesised from pane and job: %q", out)
	}

	a, s, _, hub, _ = hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
		return HubSpawnReply{RequestID: r.RequestID, RC: 81, Error: "checkout_refused:busy"}, nil
	}
	if _, _, e := a.placeAuto(context.Background(), s, "brief"); e == nil || exitCode(e) != exitPlacement || len(hub.spawns) != 2 {
		t.Fatalf("a second busy refuses: spawns=%d err=%v", len(hub.spawns), e)
	}
}

func TestHubUnpushedCommitsRefuseBeforeAnyUpload(t *testing.T) {
	for _, branch := range []string{"wk/feat", ""} {
		a, s, f, hub, events := hubFixture(t, gitState{}, branch, hubHosts())
		a.Config.Local = map[string]any{"spawn": true}
		handled, _, e := a.placeAuto(context.Background(), s, "brief")
		if !handled || e == nil || exitCode(e) != exitPlacement || !strings.Contains(e.Error(), "push the branch") {
			t.Fatalf("branch %q: handled=%v err=%v", branch, handled, e)
		}
		if len(hub.spawns) != 0 || len(*events) != 0 || sshCalls(f) != 0 {
			t.Fatalf("nothing may be uploaded or sent: %v %v", hub.spawns, *events)
		}
	}
}

func TestHubDownRefusesTheHostWithoutSSH(t *testing.T) {
	for name, err := range map[string]error{
		"unreachable":               &HubError{Kind: hubDown},
		"spawn_capability_required": &HubError{Kind: hubNoSpawnV2, Status: 503},
	} {
		t.Run(name, func(t *testing.T) {
			a, s, f, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
			hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
				if r.Machine == "h1" {
					return HubSpawnReply{}, err
				}
				return HubSpawnReply{RequestID: r.RequestID, Pane: "w:p3", JobID: "job-a"}, nil
			}
			handled, rec, e := a.placeAuto(context.Background(), s, "brief")
			if e != nil || !handled || rec["chose"] != "h2" {
				t.Fatalf("the other candidate continues: handled=%v rec=%v err=%v", handled, rec, e)
			}
			var reason string
			for _, c := range rec["candidates"].([]any) {
				if row := c.(map[string]any); row["host"] == "h1" {
					reason, _ = row["outcome"].(string)
				}
			}
			want := "hub_unavailable"
			if name != "unreachable" {
				want = "hub_spawn_capability_required"
			}
			if reason != want {
				t.Fatalf("recorded reason %q, want %q", reason, want)
			}
			if sshCalls(f) != 0 {
				t.Fatal("no ssh fallback")
			}
		})
	}
}

func TestHubDownEverywhereRefusesEveryRemoteHost(t *testing.T) {
	a, s, f, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	a.Config.Local = map[string]any{"spawn": false}
	hub.node = func(HubSpawnRequest) (HubSpawnReply, error) { return HubSpawnReply{}, &HubError{Kind: hubDown} }
	handled, _, e := a.placeAuto(context.Background(), s, "brief")
	// Only a local spawn is left, and this host has it disabled (80); with it
	// enabled the job would run locally (D4).
	if !handled || e == nil || exitCode(e) != exitLocalDisabled {
		t.Fatalf("handled=%v err=%v", handled, e)
	}
	if len(hub.spawns) != 2 || sshCalls(f) != 0 {
		t.Fatalf("both hosts tried through the hub only: spawns=%d", len(hub.spawns))
	}
}

func TestHubDownFallsBackToLocalSpawnOnly(t *testing.T) {
	a, s, f, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	a.Config.Local = map[string]any{"spawn": true}
	hub.node = func(HubSpawnRequest) (HubSpawnReply, error) { return HubSpawnReply{}, &HubError{Kind: hubDown} }
	handled, rec, e := a.placeAuto(context.Background(), s, "brief")
	if e != nil || handled || rec == nil {
		t.Fatalf("local spawn stays allowed: handled=%v err=%v", handled, e)
	}
	if sshCalls(f) != 0 {
		t.Fatal("no ssh fallback")
	}
}

func TestHubDownForcedHostExits81(t *testing.T) {
	a, s, f, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	s.Host = "h1"
	hub.node = func(HubSpawnRequest) (HubSpawnReply, error) { return HubSpawnReply{}, &HubError{Kind: hubDown} }
	_, _, e := a.placeForced(context.Background(), s, "brief")
	if e == nil || exitCode(e) != exitPlacement || !strings.Contains(e.Error(), "hub unavailable") || !strings.Contains(e.Error(), "no ssh fallback") {
		t.Fatalf("err=%v", e)
	}
	if sshCalls(f) != 0 {
		t.Fatal("no ssh fallback")
	}
}

func TestHubSpawnThatMayStillRunStops(t *testing.T) {
	for name, reply := range map[string]struct {
		r HubSpawnReply
		e error
	}{
		"504 pending":  {e: &HubError{Kind: hubPending, Status: 504}},
		"lost":         {r: HubSpawnReply{Status: "lost"}},
		"node timeout": {r: HubSpawnReply{RC: 1, Error: "timeout"}},
	} {
		t.Run(name, func(t *testing.T) {
			a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
			hub.node = func(HubSpawnRequest) (HubSpawnReply, error) { return reply.r, reply.e }
			_, _, e := a.placeAuto(context.Background(), s, "brief")
			if e == nil || exitCode(e) != exitStateConflict || len(hub.spawns) != 1 {
				t.Fatalf("an unknown outcome must not spawn elsewhere: spawns=%d err=%v", len(hub.spawns), e)
			}
		})
	}
}

func TestHubNodeSideRefusalsMoveOn(t *testing.T) {
	for _, code := range []string{"spawn_disabled", "cwd_unmapped", "panewire_missing", "brief_doc_unavailable"} {
		a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
		hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
			if r.Machine == "h1" {
				return HubSpawnReply{RC: 2, Error: code}, nil
			}
			return HubSpawnReply{Pane: "w:p1", JobID: "job-a"}, nil
		}
		_, rec, e := a.placeAuto(context.Background(), s, "brief")
		if e != nil || rec["chose"] != "h2" {
			t.Fatalf("%s: rec=%v err=%v", code, rec, e)
		}
	}
}

func TestHubSpawnRCsFollowTheSSHRules(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
		if r.Machine == "h1" {
			return HubSpawnReply{RC: 77, StdoutTail: "seat refused"}, nil
		}
		return HubSpawnReply{Pane: "w:p1"}, nil
	}
	if _, rec, e := a.placeAuto(context.Background(), s, "brief"); e != nil || rec["chose"] != "h2" {
		t.Fatalf("an unlisted rc refuses the host and moves on: %v %v", rec, e)
	}
	a, s, _, hub, _ = hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
		return HubSpawnReply{RC: 74, StdoutTail: "duplicate"}, nil
	}
	if _, _, e := a.placeAuto(context.Background(), s, "brief"); e == nil || exitCode(e) != exitDuplicate || len(hub.spawns) != 1 {
		t.Fatalf("a duplicate stops the round: %v", e)
	}
}

// A node without a configured workspace refuses that host like the ssh path
// does; only a forced host stops on it.
func TestHubUnconfiguredWorkspaceMovesOnUnlessForced(t *testing.T) {
	reply := func(r HubSpawnRequest) (HubSpawnReply, error) {
		if r.Machine == "h1" {
			return HubSpawnReply{RC: exitConfig, StdoutTail: "panewire: WORKSPACE_UNCONFIGURED: set target workspace"}, nil
		}
		return HubSpawnReply{Pane: "w:p1"}, nil
	}
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	hub.node = reply
	if _, rec, e := a.placeAuto(context.Background(), s, "brief"); e != nil || rec["chose"] != "h2" {
		t.Fatalf("auto placement did not move on: %v %v", rec, e)
	}
	a, s, _, hub, _ = hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	hub.node = reply
	s.Host = "h1"
	if _, _, e := a.place(context.Background(), s, "brief"); e == nil || exitCode(e) != exitConfig {
		t.Fatalf("forced host did not stop: %v", e)
	}
}

// quota.refresh runs through the hub; pacelever over ssh is never invoked.
func hubRefreshFixture(t *testing.T, host string) (*App, Spawn, *fakeRunner, *fakeHub) {
	t.Helper()
	a, s, f := remoteFixture(t, &fakePlacementSource{}, &fakeWaker{}, "2026-01-01T00:00:00Z", func(args string) Result {
		t.Errorf("ssh reached for a hub host: %s", args)
		return Result{}
	})
	const key = "claude:0123456789abcdef"
	src := &fakePlacementSource{hosts: []HostView{{Name: "h1", Connected: true}}, presence: []PresenceRecord{{HostID: "h1", AccountKey: key, LastSeen: a.Now()}}, accounts: map[string]SharedAccount{key: {Key: key, Observed: true}}}
	a.Source = src
	a.Config.Hosts = map[string]map[string]any{"h1": {"via": "hub"}}
	a.Config.HostOrder = []string{"h1"}
	s.Host = host
	events := &[]string{}
	hub := &fakeHub{t: t, events: events}
	a.Hub = hub
	hub.refresh = func(string, string) error {
		src.accounts[key] = SharedAccount{Key: key, Observed: true, Fresh: true, MeasuredAt: a.Now()}
		return nil
	}
	return a, s, f, hub
}

func TestHubQuotaRefreshGoesThroughTheHub(t *testing.T) {
	for _, host := range []string{"auto", "h1"} {
		t.Run(host, func(t *testing.T) {
			a, s, f, hub := hubRefreshFixture(t, host)
			handled, _, e := a.place(context.Background(), s, "brief")
			if e != nil || !handled {
				t.Fatalf("handled=%v err=%v", handled, e)
			}
			if !reflect.DeepEqual(hub.refreshes, [][2]string{{"h1", "claude"}}) || len(hub.spawns) != 1 {
				t.Fatalf("refreshes=%v spawns=%d", hub.refreshes, len(hub.spawns))
			}
			if sshCalls(f) != 0 {
				t.Fatal("pacelever refresh must not run over ssh")
			}
		})
	}
}

func TestHubQuotaRefreshHubDown(t *testing.T) {
	for _, host := range []string{"auto", "h1"} {
		t.Run(host, func(t *testing.T) {
			a, s, f, hub := hubRefreshFixture(t, host)
			a.Config.Local = map[string]any{"spawn": false}
			hub.refresh = func(string, string) error { return &HubError{Kind: hubDown} }
			_, _, e := a.place(context.Background(), s, "brief")
			want := exitPlacement
			if host == "auto" {
				want = exitLocalDisabled // no remote candidate completed, local is off
			}
			if e == nil || exitCode(e) != want {
				t.Fatalf("hub outage must refuse the host (want %d): %v", want, e)
			}
			if len(hub.spawns) != 0 || sshCalls(f) != 0 {
				t.Fatalf("spawns=%d ssh=%d", len(hub.spawns), sshCalls(f))
			}
		})
	}
}

func TestHubRefreshFailureIsAnUnconfirmedMeasurement(t *testing.T) {
	a, s, _, hub := hubRefreshFixture(t, "h1")
	hub.refresh = func(string, string) error { return &HubError{Kind: hubRefreshFail, Status: 502} }
	_, _, e := a.place(context.Background(), s, "brief")
	if e == nil || exitCode(e) != exitEvictionUnconfirmed {
		t.Fatalf("%v", e)
	}
}

// `via` unset (or ssh) keeps the ssh path and never touches the hub.
func TestSSHHostsNeverTouchTheHub(t *testing.T) {
	for _, via := range []map[string]any{{}, {"via": "ssh"}} {
		r := &remoteWT{cloneExists: true, wtExists: true, wtBranch: "wk/feat"}
		a, s, f := worktreeFixture(t, r, "wk/feat", map[string]map[string]any{"h1": via, "h2": {}})
		a.Hub = failingHub{t}
		_, rec, e := a.placeAuto(context.Background(), s, "brief")
		if e != nil || rec["chose"] != "h1" {
			t.Fatalf("%v %v", rec, e)
		}
		if !r.saw("_spawn-stdin") || !strings.Contains(spawnInput(t, f), `"~/work/repo-a.wk"`) {
			t.Fatalf("the ssh path must run unchanged: %v", r.cmds)
		}
	}
}

type failingHub struct{ t *testing.T }

func (h failingHub) Spawn(context.Context, HubSpawnRequest) (HubSpawnReply, error) {
	h.t.Error("hub used for an ssh host")
	return HubSpawnReply{}, nil
}
func (h failingHub) Refresh(context.Context, string, string) error {
	h.t.Error("hub used for an ssh host")
	return nil
}

func TestHubDryRunTouchesNothing(t *testing.T) {
	a, s, f, hub, events := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	s.DryRun = true
	a.Quota = &fakeQuota{LaunchValue: Launch{Profile: "sonnet", Effort: "high", Grade: "A", ModelID: "sonnet-m", Gate: "default", Pool: "claude"}, GateDo: gatePools(map[string]string{"sonnet": "claude"})}
	if _, _, e := a.placeAuto(context.Background(), s, "brief"); e != nil {
		t.Fatal(e)
	}
	if len(hub.spawns) != 0 || len(*events) != 0 || sshCalls(f) != 0 {
		t.Fatalf("dry run must not upload or spawn: %v %v", hub.spawns, *events)
	}
	if !strings.Contains(a.Out.(interface{ String() string }).String(), "dry-run host=h1") {
		t.Fatal("dry-run line missing")
	}
}

func TestHubRefusesWhatItCannotMap(t *testing.T) {
	// A subdirectory start and a mock lane are refused, not approximated.
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	a.Config.Local = map[string]any{"spawn": false}
	base := a.Runner.(*fakeRunner).Do
	a.Runner.(*fakeRunner).Do = func(c Command) Result {
		if c.Name == "git" && strings.Contains(strings.Join(c.Args, " "), "--show-prefix") {
			return Result{Out: "sub/dir/\n"}
		}
		return base(c)
	}
	s.Host = "h1"
	if _, _, e := a.placeForced(context.Background(), s, "brief"); e == nil || exitCode(e) != exitPlacement || len(hub.spawns) != 0 {
		t.Fatalf("subdirectory: %v", e)
	}
	a, s, _, hub, _ = hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	s.ExecutionLane = "mock"
	a.Config.Local = map[string]any{"spawn": false}
	if _, _, e := a.placeAuto(context.Background(), s, "brief"); e == nil || len(hub.spawns) != 0 {
		t.Fatalf("mock lane: %v", e)
	}
}

func TestHubReportMustStayUnderJobsRoot(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	s.Report = "/elsewhere/report.md"
	if _, _, e := a.placeAuto(context.Background(), s, "brief"); e == nil || exitCode(e) != exitUsage || len(hub.spawns) != 0 {
		t.Fatalf("report outside the jobs root: %v", e)
	}
	a, s, _, hub, _ = hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	s.Report = filepath.Join(a.Jobs, "job-a", "report.md")
	if _, _, e := a.placeAuto(context.Background(), s, "brief"); e != nil {
		t.Fatal(e)
	}
	args := hub.spawns[0].Args
	if i := indexArg(args, "--report-relative"); i < 0 || args[i+1] != "job-a/report.md" || indexArg(args, "--report") >= 0 {
		t.Fatalf("a report under the jobs root must cross as --report-relative only: %v", args)
	}
}

func TestHubCallerReportRelativeReachesTheHub(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	s.ReportRelative = "reviews/x/report.md"
	if _, _, e := a.placeAuto(context.Background(), s, "brief"); e != nil {
		t.Fatal(e)
	}
	args := hub.spawns[0].Args
	if i := indexArg(args, "--report-relative"); i < 0 || args[i+1] != "reviews/x/report.md" || indexArg(args, "--report") >= 0 {
		t.Fatalf("%v", args)
	}
	for _, bad := range []string{"/abs/report.md", "../x", "a/../../b"} {
		a, s, _, hub, _ = hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
		s.ReportRelative = bad
		if _, _, e := a.placeAuto(context.Background(), s, "brief"); e == nil || exitCode(e) != exitUsage || len(hub.spawns) != 0 {
			t.Errorf("--report-relative %q: %v", bad, e)
		}
	}
}

func TestHubWithoutReportRelativeNamesTheMissingCapability(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	s.Report = filepath.Join(a.Jobs, "job-a", "report.md")
	hub.node = func(HubSpawnRequest) (HubSpawnReply, error) {
		return HubSpawnReply{}, &HubError{Kind: hubRejected, Status: http.StatusBadRequest, Code: "invalid_request"}
	}
	_, _, e := a.placeAuto(context.Background(), s, "brief")
	if e == nil || !strings.Contains(e.Error(), "--report-relative") || !strings.Contains(e.Error(), "capability") {
		t.Fatalf("error must name the missing capability: %v", e)
	}
	if len(hub.spawns) != 1 {
		t.Fatalf("no retry with an absolute --report: %d hub requests", len(hub.spawns))
	}
}

func TestHubOrigin(t *testing.T) {
	for in, want := range map[string]string{
		"https://github.com/o/r.git":   "https://github.com/o/r.git",
		"git@github.com:o/r.git":       "https://github.com/o/r.git",
		"ssh://git@github.com/o/r.git": "https://github.com/o/r.git",
		"ssh://git@github.com:22/o/r":  "https://github.com/o/r",
		"git://github.com/o/r.git":     "https://github.com/o/r.git",
		"https://tok@github.com/o/r":   "",
		"https://github.com/o/r?x=1":   "",
		"/abs/path/o/r":                "",
		"":                             "",
		"host:/abs":                    "",
		"file:///x/y":                  "",
	} {
		got, ok := hubOrigin(in)
		if (want == "") == ok || got != want {
			t.Errorf("hubOrigin(%q) = %q, %v; want %q", in, got, ok, want)
		}
	}
}

func hubEnv(t *testing.T, content string, mode os.FileMode) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "hub.env")
	if e := os.WriteFile(p, []byte(content), mode); e != nil {
		t.Fatal(e)
	}
	if e := os.Chmod(p, mode); e != nil {
		t.Fatal(e)
	}
	return p
}

func TestHubHTTPClient(t *testing.T) {
	var gotAuth, gotCF, gotPath string
	var gotBody map[string]any
	status, reply := 200, `{"request_id":"r","machine":"h1","rc":0,"job_id":"job-a","pane":"w:p1","stdout_tail":"OK pane=w:p1 job=job-a\n"}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotCF, gotPath = r.Header.Get("Authorization"), r.Header.Get("CF-Access-Client-Id"), r.URL.Path
		gotBody = nil
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(reply))
	}))
	defer srv.Close()
	cfg := map[string]any{
		"hub_url":            srv.URL,
		"operator_token_env": hubEnv(t, "HUB_MACHINE_ID=operator\nHUB_TOKEN=tok-1\n", 0o600),
		"hub_cf_env":         hubEnv(t, "CF_ACCESS_CLIENT_ID=cf-id\nCF_ACCESS_CLIENT_SECRET=cf-secret\n", 0o600),
	}
	c, e := newHubHTTP(cfg)
	if e != nil {
		t.Fatal(e)
	}
	req := HubSpawnRequest{RequestID: "r", Machine: "h1", CWDKey: "repo-a", Brief: HubBrief{Doc: "briefs/job-a.md"}, Args: []string{"--job", "job-a"}, WaitSeconds: 30,
		Checkout: &HubCheckout{Origin: "https://git.invalid/o/r.git", Mode: "branch", Branch: "b", Dir: "r.b"}}
	r, e := c.Spawn(context.Background(), req)
	if e != nil || r.JobID != "job-a" || r.Pane != "w:p1" {
		t.Fatalf("%+v %v", r, e)
	}
	if gotPath != "/v1/spawn" || gotAuth != "Bearer tok-1" || gotCF != "cf-id" {
		t.Fatalf("path=%s auth=%q cf=%q", gotPath, gotAuth, gotCF)
	}
	brief, _ := gotBody["brief"].(map[string]any)
	co, _ := gotBody["checkout"].(map[string]any)
	if brief["doc"] != "briefs/job-a.md" || co["mode"] != "branch" || co["dir"] != "r.b" || gotBody["cwd_key"] != "repo-a" || gotBody["wait_seconds"] != float64(30) {
		t.Fatalf("body: %v", gotBody)
	}
	if _, has := co["sha"]; has {
		t.Fatalf("empty checkout fields must be omitted: %v", co)
	}

	for _, tc := range []struct {
		status int
		body   string
		kind   string
	}{
		{503, `{"error":"spawn_capability_required","capability":"spawn_v2"}`, hubNoSpawnV2},
		{503, `{"error":"node_unavailable","state":"disconnected"}`, hubNoCapacity},
		{504, `{"request_id":"r","status":"pending"}`, hubPending},
		{504, `{"error":"node_timeout"}`, hubPending},
		{504, `<html>gateway timeout</html>`, hubPending},
		{401, `{}`, hubAuth},
		{403, `{}`, hubAuth},
		{400, `{"error":"invalid_request"}`, hubRejected},
		{409, `{}`, hubRejected},
		{500, `oops`, hubDown},
		{502, ``, hubDown},
	} {
		status, reply = tc.status, tc.body
		_, e := c.Spawn(context.Background(), req)
		var he *HubError
		if !errors.As(e, &he) || he.Kind != tc.kind {
			t.Errorf("%d %s: got %v, want kind %s", tc.status, tc.body, e, tc.kind)
		}
	}

	status, reply = 200, `{"request_id":"c","machine":"h1","op":"quota.refresh","result":{"outcome":"refreshed"}}`
	if e := c.Refresh(context.Background(), "h1", "claude"); e != nil {
		t.Fatal(e)
	}
	args, _ := gotBody["args"].(map[string]any)
	if gotPath != "/v1/nodes/h1/command" || gotBody["op"] != "quota.refresh" || args["pool"] != "claude" {
		t.Fatalf("path=%s body=%v", gotPath, gotBody)
	}
	for _, tc := range []struct {
		status int
		body   string
		kind   string
	}{
		{502, `{"request_id":"c","machine":"h1","op":"quota.refresh","error":"refresh_failed"}`, hubRefreshFail},
		{504, `{"error":"node_timeout"}`, hubRefreshFail},
		{503, `{"error":"node_lost"}`, hubRefreshFail},
		{503, `{"error":"node_unavailable","state":"disconnected"}`, hubNoCapacity},
		{200, `{"result":{"outcome":"other"}}`, hubRefreshFail},
		{500, `oops`, hubDown},
		{403, `{}`, hubAuth},
	} {
		status, reply = tc.status, tc.body
		var he *HubError
		if e := c.Refresh(context.Background(), "h1", "claude"); !errors.As(e, &he) || he.Kind != tc.kind {
			t.Errorf("%d %s: got %v, want kind %s", tc.status, tc.body, e, tc.kind)
		}
	}
}

func TestHubHTTPDownIsHubUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	c, e := newHubHTTP(map[string]any{"hub_url": url, "operator_token_env": hubEnv(t, "HUB_MACHINE_ID=operator\nHUB_TOKEN=t\n", 0o600)})
	if e != nil {
		t.Fatal(e)
	}
	_, e = c.Spawn(context.Background(), HubSpawnRequest{WaitSeconds: 1})
	var he *HubError
	if !errors.As(e, &he) || he.Kind != hubDown {
		t.Fatalf("%v", e)
	}
	if e := c.Refresh(context.Background(), "h1", "claude"); !errors.As(e, &he) || he.Kind != hubDown {
		t.Fatalf("%v", e)
	}
}

func TestHubHTTPConfigRefusals(t *testing.T) {
	good := hubEnv(t, "HUB_MACHINE_ID=operator\nHUB_TOKEN=t\n", 0o600)
	for name, cfg := range map[string]map[string]any{
		"no url":              {"operator_token_env": good},
		"no token file":       {"hub_url": "https://hub.example.invalid"},
		"plain http remote":   {"hub_url": "http://hub.example.invalid", "operator_token_env": good},
		"userinfo":            {"hub_url": "https://u:p@hub.example.invalid", "operator_token_env": good},
		"group-readable file": {"hub_url": "https://hub.example.invalid", "operator_token_env": hubEnv(t, "HUB_MACHINE_ID=operator\nHUB_TOKEN=t\n", 0o640)},
		"node credential":     {"hub_url": "https://hub.example.invalid", "operator_token_env": hubEnv(t, "HUB_MACHINE_ID=node-a\nHUB_TOKEN=t\n", 0o600)},
		"missing token":       {"hub_url": "https://hub.example.invalid", "operator_token_env": hubEnv(t, "HUB_MACHINE_ID=operator\n", 0o600)},
		"half a CF pair":      {"hub_url": "https://hub.example.invalid", "operator_token_env": good, "hub_cf_env": hubEnv(t, "CF_ACCESS_CLIENT_ID=x\n", 0o600)},
	} {
		if _, e := newHubHTTP(cfg); e == nil || exitCode(e) != exitConfig {
			t.Errorf("%s: %v", name, e)
		} else if strings.Contains(e.Error(), "tok-1") || strings.Contains(e.Error(), good) {
			t.Errorf("%s: error leaks a credential or path: %v", name, e)
		}
	}
	// hub_token_env is the fallback when no operator file is set.
	if _, e := newHubHTTP(map[string]any{"hub_url": "https://hub.example.invalid", "hub_token_env": good}); e != nil {
		t.Fatal(e)
	}
}

func TestHubConfigKeys(t *testing.T) {
	for _, bad := range []string{"hub_wait = 0", "hub_wait = 301", `hub_wait = "x"`, `brief_doc_prefix = "briefs"`, `brief_doc_prefix = "../x/"`} {
		p := filepath.Join(t.TempDir(), "hosts.toml")
		if e := os.WriteFile(p, []byte("[hosts.h1]\nvia = \"hub\"\n"+bad+"\n"), 0o600); e != nil {
			t.Fatal(e)
		}
		if _, e := LoadConfig(p); e == nil || exitCode(e) != exitConfig {
			t.Errorf("%s accepted: %v", bad, e)
		}
	}
	p := filepath.Join(t.TempDir(), "hosts.toml")
	if e := os.WriteFile(p, []byte("[hosts.h1]\nvia = \"hub\"\nhub_wait = 200\nbrief_doc_prefix = \"briefs/\"\n"), 0o600); e != nil {
		t.Fatal(e)
	}
	if _, e := LoadConfig(p); e != nil {
		t.Fatal(e)
	}
}

// A 400 with another hub code keeps its own cause instead of blaming the
// --report-relative capability.
func TestHubOtherBadRequestIsNotBlamedOnReportRelative(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	s.Report = filepath.Join(a.Jobs, "job-a", "report.md")
	hub.node = func(HubSpawnRequest) (HubSpawnReply, error) {
		return HubSpawnReply{}, &HubError{Kind: hubRejected, Status: http.StatusBadRequest, Code: "machine_unknown"}
	}
	_, _, e := a.placeAuto(context.Background(), s, "brief")
	if e != nil && strings.Contains(e.Error(), "--report-relative capability") {
		t.Fatalf("a non-invalid_request 400 was blamed on --report-relative: %v", e)
	}
}

// The relative report path follows panewire's validHubSpawnDocKey: ':' and
// '@' and dot segments are refused before the hub, "report..md" is a name.
func TestHubReportRelativeMatchesPanewireRule(t *testing.T) {
	for rel, ok := range map[string]bool{
		"job-a/report.md":   true,
		"job-a/report..md":  true,
		"job-a/rep:ort.md":  false,
		"job-a/r@port.md":   false,
		"job-a/../x.md":     false,
		"job-a//report.md":  false,
		"/job-a/report.md":  false,
		"job-a/report.md/":  false,
		"./job-a/report.md": false,
	} {
		if got := hubReportRelativeOK(rel); got != ok {
			t.Errorf("%q: got %v want %v", rel, got, ok)
		}
	}
}
