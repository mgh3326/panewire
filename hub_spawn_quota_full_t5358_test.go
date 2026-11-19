package panewire

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A node's quota gate refusing a full quota moves an automatic request on to
// the next candidate inside the same request, or holds it until the limit's
// end when that fits the request's time limit (hk 5358, operator decisions 433,
// 434 and 435). These tests drive the transport the way a placement pass does.

const quotaFullBudget = 25 * time.Minute

func quotaFullTail(reset time.Time) string {
	return "stderr: quota gate: free limit exceeded until " + reset.UTC().Format(time.RFC3339) + "\nFix: x"
}

func quotaFullAnswer(reset time.Time) map[string]any {
	return map[string]any{"rc": 3, "job_id": "", "pane": "", "stdout_tail": quotaFullTail(reset), "error": "spawn_failed", "stage": "placement", "retryable": true}
}

// quotaFullRequest is an automatic placement whose time limit is the budget.
func quotaFullRequest() fleet.Submission {
	request := autoSpawnRequest()
	request.DeadlineSeconds = 3600
	request.ExpiresSeconds = int(quotaFullBudget / time.Second)
	return request
}

// quotaFullNodeRequest is the placement's node request for the SWE-2 free rung.
func quotaFullNodeRequest(request fleet.Submission, id int, machine string) fleet.HubSpawnRequest {
	node := refusalNodeRequest(request, id, machine)
	node.Args = []string{"--job", "test-job", "-m", "devin-swe2-flash", "-l", "test-job", "--role", "implementer"}
	return node
}

type quotaFullFixture struct {
	h         *HubServer
	transport *directSpawnTransport
	request   fleet.Submission
	clock     *time.Time
	node      func(id int, answer map[string]any) (fleet.HubSpawnReply, error)
	t         *testing.T
	conn      *websocket.Conn
}

func newQuotaFullFixture(t *testing.T, request fleet.Submission) *quotaFullFixture {
	t.Helper()
	h := expiringRefusalHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	// Whole seconds: a gate reason names its reset to the second.
	clock := time.Now().UTC().Truncate(time.Second)
	h.now = func() time.Time { return clock }
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	conn := t14Node(t, h, server, true)
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", clock)
	if err != nil {
		t.Fatal(err)
	}
	f := &quotaFullFixture{h: h, transport: &directSpawnTransport{h: h, task: task}, request: request, clock: &clock, t: t, conn: conn}
	f.node = func(id int, answer map[string]any) (fleet.HubSpawnReply, error) {
		return placeOnNodeRequest(t, f.transport, conn, quotaFullNodeRequest(request, id, "machine-a"), id, answer)
	}
	return f
}

func (f *quotaFullFixture) node2(node fleet.HubSpawnRequest, id int, answer map[string]any) (fleet.HubSpawnReply, error) {
	return placeOnNodeRequest(f.t, f.transport, f.conn, node, id, answer)
}

func (f *quotaFullFixture) stored(t *testing.T) hubSpawnTask {
	t.Helper()
	task, err := f.h.inbox.store.spawnTask(t.Context(), f.request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

// A limit that ends after the request's time limit moves the request on at
// once: same request, same job, the free band excluded from the next selection,
// no back-off, and the band cooled for every other request until the reset.
func TestQuotaFullBeyondTheBudgetMovesOnAtOnce_T5358(t *testing.T) {
	f := newQuotaFullFixture(t, quotaFullRequest())
	before := f.stored(t)
	reset := f.clock.Add(40 * time.Minute)
	reply, err := f.node(1100, quotaFullAnswer(reset))
	if err != nil || !reply.NextHost || reply.Error != "spawn_failed:quota_full" {
		t.Fatalf("the refusal must go back to placement, not to the requester: reply=%+v err=%v", reply, err)
	}
	after := f.stored(t)
	if after.Request.RequestID != before.Request.RequestID || after.Job != before.Job || after.Attempt != before.Attempt {
		t.Fatalf("the continuation must stay the same request: before=%s/%s/%d after=%s/%s/%d", before.Request.RequestID, before.Job, before.Attempt, after.Request.RequestID, after.Job, after.Attempt)
	}
	if after.Status != "accepted" || after.Node != nil || after.ParkReason != "" || !after.NextCheck.IsZero() && after.NextCheck.After(time.UnixMilli(1)) {
		t.Fatalf("the request must be placed again at once: status=%s node=%v park=%q next=%v", after.Status, after.Node, after.ParkReason, after.NextCheck)
	}
	if len(after.Exclusions) != 1 || after.Exclusions[0].Pool != "devin-free" || after.Exclusions[0].Profile != "devin-swe2-flash" || after.Exclusions[0].ExpiresMS != reset.UnixMilli() {
		t.Fatalf("exclusions %+v, want the devin-free band until the reset", after.Exclusions)
	}
	// The next selection leaves the band out.
	if args := withQuotaExclusions(after.Request.Args, liveExclusions(after.Exclusions, *f.clock)); hubSpawnArg(args, "--exclude-pool") != "devin-free" {
		t.Fatalf("next selection args %v do not exclude devin-free", args)
	}
	// The reset is used at once, fleet-wide: the hub cools the band until it.
	snap := f.h.fleetSnapshot(*f.clock)
	if got := snap.Cooling["devin-free"]; !got.Equal(reset.Truncate(time.Second)) {
		t.Fatalf("band cooling until %v, want the refusal's reset %v", got, reset.Truncate(time.Second))
	}
	if len(snap.Cooling) != 1 {
		t.Fatalf("only the free band is cooled, the paid band and the pool stay placeable: %v", snap.Cooling)
	}
	// The refused seat is skipped on this host, not the whole host.
	placementSkipsRequest(t, f, 1101, "devin-swe2-flash")
}

// A limit that ends inside the request's time limit is waited out: the request
// is held until the end, timer-only, then starts on the same pool.
func TestQuotaFullInsideTheBudgetWaitsForTheReset_T5358(t *testing.T) {
	f := newQuotaFullFixture(t, quotaFullRequest())
	before := f.stored(t)
	reset := f.clock.Add(2 * time.Minute)
	reply, err := f.node(1110, quotaFullAnswer(reset))
	if err != nil || !reply.NextHost {
		t.Fatalf("reply=%+v err=%v", reply, err)
	}
	after := f.stored(t)
	if after.Request.RequestID != before.Request.RequestID || after.Attempt != before.Attempt {
		t.Fatalf("the wait must stay the same request")
	}
	if after.ParkReason != spawnParkQuotaReset || !after.NextCheck.Equal(time.UnixMilli(reset.Add(time.Second).UnixMilli())) {
		t.Fatalf("park %q until %v, want %s until reset+1s %v", after.ParkReason, after.NextCheck, spawnParkQuotaReset, reset.Add(time.Second))
	}
	if len(after.Exclusions) != 1 || after.Exclusions[0].Pool != "" {
		t.Fatalf("a wait excludes no pool, the same pool is retried at the end: %+v", after.Exclusions)
	}
	// No event wakes it before the end.
	f.h.noteSpawnWake(spawnParkQuota, spawnParkHostFull, spawnParkNoEligibleHost)
	f.h.wakeParkedSpawns(t.Context(), true)
	if still := f.stored(t); !still.NextCheck.Equal(after.NextCheck) {
		t.Fatalf("an event woke the quota_reset park early: %v", still.NextCheck)
	}
	// At the end the exclusion has lapsed: the seat is tried again.
	*f.clock = reset.Add(2 * time.Second)
	if got := liveExclusions(f.stored(t).Exclusions, *f.clock); len(got) != 0 {
		t.Fatalf("exclusions still live at the reset: %+v", got)
	}
	// Other requests skip the band for the wait as well.
	if snap := f.h.fleetSnapshot(reset.Add(-time.Second)); snap.Cooling["devin-free"].IsZero() {
		t.Fatal("the band is not cooled for other requests during the wait")
	}
}

// The wait never runs past the request's time limit: a limit that ends so late
// that the start would not fit moves on, at the margin and beyond.
func TestQuotaFullWaitNeverPassesTheBudget_T5358(t *testing.T) {
	for _, test := range []struct {
		name  string
		reset time.Duration
		wait  bool
	}{
		{"2 minutes of a 25 minute budget", 2 * time.Minute, true},
		{"fits with the start margin", quotaFullBudget - spawnQuotaStartMargin, true},
		{"ends inside the budget but leaves no time to start", quotaFullBudget - spawnQuotaStartMargin + time.Second, false},
		{"40 minutes", 40 * time.Minute, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newQuotaFullFixture(t, quotaFullRequest())
			if _, err := f.node(1120, quotaFullAnswer(f.clock.Add(test.reset))); err != nil {
				t.Fatal(err)
			}
			got := f.stored(t)
			if waited := got.ParkReason == spawnParkQuotaReset; waited != test.wait {
				t.Fatalf("waited=%v, want %v (park %q, exclusions %+v)", waited, test.wait, got.ParkReason, got.Exclusions)
			}
			if got.ParkReason == spawnParkQuotaReset && got.NextCheck.After(got.Expires) {
				t.Fatalf("the wait runs to %v, past the request's limit %v", got.NextCheck, got.Expires)
			}
		})
	}
}

// A pinned profile keeps today's behaviour: the refusal is the request's answer.
func TestQuotaFullOnAPinnedProfileIsNotMovedOn_T5358(t *testing.T) {
	request := quotaFullRequest()
	for i := 0; i+1 < len(request.Args); i++ {
		if request.Args[i] == "-m" {
			request.Args[i+1] = "devin-swe2-flash"
		}
	}
	request.Args = append(request.Args, "--containment", "hk:5358", "--override-reason", "pinned profile")
	f := newQuotaFullFixture(t, request)
	reply, err := f.node(1130, quotaFullAnswer(f.clock.Add(40*time.Minute)))
	if err != nil || reply.NextHost {
		t.Fatalf("a pinned -m must not move on: reply=%+v err=%v", reply, err)
	}
	got := f.stored(t)
	if len(got.Exclusions) != 0 || got.Status != "retryable" {
		t.Fatalf("status=%s exclusions=%+v, want today's retryable refusal", got.Status, got.Exclusions)
	}
	if snap := f.h.fleetSnapshot(*f.clock); len(snap.Cooling) != 0 {
		t.Fatalf("a refusal that moved nothing on cooled %v", snap.Cooling)
	}
}

// What is not a full quota keeps its own handling, and a refusal after the job
// claim never moves on (hk holds the claim on that host).
func TestQuotaFullBoundaries_T5358(t *testing.T) {
	for _, test := range []struct {
		name   string
		answer func(reset time.Time) map[string]any
	}{
		{"unmeasured reading", func(time.Time) map[string]any {
			a := quotaFullAnswer(time.Time{})
			a["stdout_tail"] = "stderr: quota gate: measurement unavailable\nFix: x"
			return a
		}},
		{"claim stage", func(reset time.Time) map[string]any {
			a := quotaFullAnswer(reset)
			a["stage"] = "claim"
			return a
		}},
		{"not retryable", func(reset time.Time) map[string]any {
			a := quotaFullAnswer(reset)
			a["retryable"] = false
			return a
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newQuotaFullFixture(t, quotaFullRequest())
			reply, _ := f.node(1140, test.answer(f.clock.Add(40*time.Minute)))
			if reply.NextHost {
				t.Fatalf("moved on: %+v", reply)
			}
			if got := f.stored(t); len(got.Exclusions) != 0 {
				t.Fatalf("exclusions %+v", got.Exclusions)
			}
		})
	}
}

func TestWithQuotaExclusionsMergesAndNeverEditsInPlace_T5358(t *testing.T) {
	args := []string{"-m", "auto", "--exclude-pool", "claude"}
	live := []hubSpawnExclusion{{Machine: "a", Reason: "spawn_failed:quota_full", Pool: "devin-free"}, {Machine: "b", Reason: "spawn_failed:quota_full", Pool: "devin-free"}, {Machine: "c", Reason: "cwd_unmapped", Pool: "ignored"}, {Machine: "d", Reason: "spawn_failed:quota_full"}}
	got := withQuotaExclusions(args, live)
	if hubSpawnArg(got, "--exclude-pool") != "claude,devin-free" || args[3] != "claude" {
		t.Fatalf("merged=%v original=%v", got, args)
	}
	if got := withQuotaExclusions([]string{"-m", "auto"}, live[:1]); strings.Join(got, " ") != "-m auto --exclude-pool devin-free" {
		t.Fatalf("appended=%v", got)
	}
	if got := withQuotaExclusions(args, live[2:]); len(got) != len(args) {
		t.Fatalf("no band, no change: %v", got)
	}
}

// placeOnNodeRequest is placeOnNode for a node request the caller built.
func placeOnNodeRequest(t *testing.T, transport *directSpawnTransport, node *websocket.Conn, nodeRequest fleet.HubSpawnRequest, id int, answer map[string]any) (fleet.HubSpawnReply, error) {
	t.Helper()
	type placed struct {
		reply fleet.HubSpawnReply
		err   error
	}
	done := make(chan placed, 1)
	go func() {
		reply, err := dispatchPlacedSpawn(boundedContext(t), transport, nodeRequest)
		done <- placed{reply, err}
	}()
	sent := t14ReadNodeSpawn(t, node)
	if sent.RequestID != t14RequestID(id) {
		t.Fatalf("node was sent request %s, want %s", sent.RequestID, t14RequestID(id))
	}
	answer["type"], answer["request_id"] = "job.spawn.result", sent.RequestID
	if err := wsjson.Write(context.Background(), node, answer); err != nil {
		t.Fatal(err)
	}
	got := <-done
	return got.reply, got.err
}

// placementSkipsRequest asks the transport for the same seat again: the
// exclusion answers NextHost without binding, so the node is not asked.
func placementSkipsRequest(t *testing.T, f *quotaFullFixture, id int, profile string) {
	t.Helper()
	node := quotaFullNodeRequest(f.request, id, "machine-a")
	node.Args = []string{"--job", "test-job", "-m", profile, "-l", "test-job", "--role", "implementer"}
	reply, err := f.transport.Spawn(boundedContext(t), node)
	if err != nil || !reply.NextHost {
		t.Fatalf("the refused seat was not skipped: reply=%+v err=%v", reply, err)
	}
}

// A refusal of an account-scoped pool excludes the seat and, for CommandCode
// credit, the account: never the pool, whose other accounts may be funded
// (mergewake finding on #731). Only the shared Devin free band is a pool.
func TestQuotaFullOfAnAccountScopedPoolExcludesNoPool_T5358(t *testing.T) {
	for _, test := range []struct {
		name, pool, profile, tail string
		marked                    bool
	}{
		{"commandcode credit", "commandcode", "omp-commandcode", "quota gate: credit_exhausted balance=0", true},
		{"claude window", "claude", "sonnet", "quota gate: quota exhausted window=5h cutoff=90", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newQuotaFullFixture(t, quotaFullRequest())
			account := test.pool + ":0123456789abcdef"
			if err := f.h.notePlacementTrace(f.stored(t), fleet.PlacementTrace{Chosen: "machine-a", Profile: test.profile, Pool: test.pool, AccountKey: account}, ""); err != nil {
				t.Fatal(err)
			}
			node := quotaFullNodeRequest(f.request, 1150, "machine-a")
			node.Args = []string{"--job", "test-job", "-m", test.profile, "-l", "test-job", "--role", "implementer"}
			answer := quotaFullAnswer(time.Time{})
			answer["stdout_tail"] = "stderr: " + test.tail + "\nFix: x"
			reply, err := f.node2(node, 1150, answer)
			if err != nil || !reply.NextHost {
				t.Fatalf("reply=%+v err=%v", reply, err)
			}
			got := f.stored(t).Exclusions
			if len(got) != 1 || got[0].Pool != "" || got[0].Account != account || got[0].Profile != test.profile {
				t.Fatalf("exclusions %+v, want the seat and its account only", got)
			}
			if args := withQuotaExclusions(f.stored(t).Request.Args, liveExclusions(got, *f.clock)); hubSpawnArg(args, "--exclude-pool") != "" {
				t.Fatalf("next selection excludes pools %q", hubSpawnArg(args, "--exclude-pool"))
			}
			if held := f.h.accountCreditMarkHeld(account, *f.clock); held != test.marked {
				t.Fatalf("account mark held=%v, want %v", held, test.marked)
			}
		})
	}
}

// When this request's own full-quota refusals leave no candidate, the request
// ends with the refusals named; it is not parked as a lack of hosts until its
// limit (mergewake finding on #731).
func TestQuotaFullExcludedEndsTheRequestInsteadOfParkingIt_T5358(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	live := hubSpawnExclusion{Machine: "mac-personal", Profile: "devin-swe2-flash", Reason: "spawn_failed:quota_full", Pool: "devin-free", ExpiresMS: now.Add(time.Hour).UnixMilli()}
	seat := hubSpawnExclusion{Machine: "mac-dev", Profile: "sonnet", Reason: "spawn_failed:quota_full", Account: "claude:0123456789abcdef"}
	expired := hubSpawnExclusion{Machine: "old", Profile: "sonnet", Reason: "spawn_failed:quota_full", ExpiresMS: now.Add(-time.Minute).UnixMilli()}
	other := hubSpawnExclusion{Machine: "x", Reason: "cwd_unmapped"}
	task := hubSpawnTask{Exclusions: []hubSpawnExclusion{live, seat, expired, other}}
	for _, text := range []string{"role_no_candidate: every candidate was excluded", "no placement candidate accepted spawn; no local fallback; tried: mac-dev hub_quota_full"} {
		got := quotaFullExcluded(task, now, errors.New(text))
		if len(got) != 2 {
			t.Fatalf("%q: refused %+v, want the two live quota refusals", text, got)
		}
	}
	for _, err := range []error{nil, errors.New("role_no_candidate: pool at concurrency cap"), errors.New("QUOTA_EXHAUSTED: claude")} {
		if got := quotaFullExcluded(task, now, err); len(got) != 0 {
			t.Fatalf("%v: %+v, want none", err, got)
		}
	}
	if got := quotaFullExcluded(hubSpawnTask{Exclusions: []hubSpawnExclusion{other, expired}}, now, errors.New("role_no_candidate: x")); len(got) != 0 {
		t.Fatalf("no live quota refusal, nothing to answer with: %+v", got)
	}
	notes := quotaFullRefusedNotes(quotaFullExcluded(task, now, errors.New("role_no_candidate: x")))
	for _, want := range []string{"mac-personal/devin-swe2-flash quota_full pool=devin-free until", "mac-dev/sonnet quota_full account=claude:0123456789abcdef"} {
		if !strings.Contains(notes, want) {
			t.Fatalf("notes %q lack %q", notes, want)
		}
	}
}
