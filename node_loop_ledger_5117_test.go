package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// errFixtureLoops fails the heartbeat's optional collectors in the gate test,
// so the payload stays small and local.
var errFixtureLoops = errors.New("fixture loops collector unavailable")

// TestNodeLoopKinds pins each node mechanism's kind beside docs/loops.md. The
// pane sweeper is a scan: classifying it as flow must turn this test red.
func TestNodeLoopKinds(t *testing.T) {
	want := map[nodeLoopMechanism]nodeLoopKind{
		nodeTaskPaneSweep:      nodeLoopKindScan,
		nodeWorktreeFlow:       nodeLoopKindFlow,
		nodeQuotaMeasure:       nodeLoopKindScan,
		nodeJobDirTTL:          nodeLoopKindScan,
		nodeQuotaPoolReconcile: nodeLoopKindFlow,
		nodeLaneReconcile:      nodeLoopKindScan,
		nodeSpawnSettle:        nodeLoopKindFlow,
		nodeCreditRearm:        nodeLoopKindScan,
		nodeHubCloseSettle:     nodeLoopKindFlow,
	}
	if len(nodeLoopMechanisms) != len(want) {
		t.Fatalf("node mechanisms=%d, want %d", len(nodeLoopMechanisms), len(want))
	}
	for _, m := range nodeLoopMechanisms {
		if got := nodeLoopKindOf(m); got != want[m] {
			t.Fatalf("kind of %s=%d, want %d", m, got, want[m])
		}
	}
}

// TestNodeLoopScanIgnoresMadeDone enforces the scan discipline: a pass has no
// item lifecycle, so a made or done edge on a scan mechanism counts nothing.
func TestNodeLoopScanIgnoresMadeDone(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	client := &HubClient{now: func() time.Time { return now }}
	for _, m := range nodeLoopMechanisms {
		if nodeLoopKindOf(m) != nodeLoopKindScan {
			continue
		}
		client.nodeLoopTick(m)
		client.nodeLoopNoteMade(m, "item-1")
		client.nodeLoopNoteDone(m, "item-1")
	}
	for _, m := range nodeLoopMechanisms {
		snap := client.nodeLoopSnapshot()[m]
		if nodeLoopKindOf(m) == nodeLoopKindScan && (snap.Made != 0 || snap.Done != 0) {
			t.Fatalf("scan %s made=%d done=%d, want 0 and 0", m, snap.Made, snap.Done)
		}
	}
}

// TestNodeLoopSteadyDayNoNotice is the node's healthy-day proof: ticks on
// every mechanism with balanced flow counts raise no notice.
func TestNodeLoopSteadyDayNoNotice(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	client := &HubClient{now: func() time.Time { return now }}
	for _, m := range nodeLoopMechanisms {
		client.nodeLoopTick(m)
		if nodeLoopKindOf(m) == nodeLoopKindFlow {
			client.nodeLoopNoteMade(m, "item-1")
			client.nodeLoopNoteDone(m, "item-1")
		}
	}
	hub := loopLedgerTestHub(t, &now, nil)
	hub.observeNodeLoops("machine-a", client.nodeLoopsForHeartbeat(), now)
	if hits := hub.nodeLoopNoticeHits(now); len(hits) != 0 {
		t.Fatalf("healthy day node hits=%v, want none", hits)
	}
}

// TestNodeSpawnSettleSilentErrorReachesHubRowAndNotice is the track-A2
// acceptance: a node with a store error in spawn settle shows silent_errors
// >= 1 on the hub's daily row for that machine, and the notice names the
// machine and the mechanism.
func TestNodeSpawnSettleSilentErrorReachesHubRowAndNotice(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	// A legacy journal beside spawn.json makes the acquire fail deterministically.
	if err := os.WriteFile(filepath.Join(dir, "spawn.json"), []byte(`{"work_root": "/tmp"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "spawn-inbox.sqlite3"), []byte("legacy"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{now: func() time.Time { return now }, spawnConfigPath: filepath.Join(dir, "spawn.json")}
	client.runSpawnSettle(context.Background())
	snap := client.nodeLoopSnapshot()[nodeSpawnSettle]
	if snap.SilentErrors < 1 {
		t.Fatalf("spawn settle silent_errors=%d, want >= 1", snap.SilentErrors)
	}

	hub := loopLedgerTestHub(t, &now, nil)
	ctx := context.Background()
	for _, m := range loopMechanisms {
		hub.loopTick(m)
	}
	hub.observeNodeLoops("machine-a", client.nodeLoopsForHeartbeat(), now)

	published := map[string]int{}
	bodies := map[string]string{}
	existing := map[string]bool{}
	hub.loopsDocPublish = func(_ context.Context, key, body string) error {
		published[key]++
		existing[key] = true
		bodies[key] = body
		return nil
	}
	hub.loopsDocExists = func(_ context.Context, key string) (bool, error) { return existing[key], nil }

	// The notice fires during the day, before the midnight write prunes the
	// merged report.
	named := false
	for _, hit := range hub.nodeLoopNoticeHits(now) {
		if hit.Machine == "machine-a" && string(hit.Mechanism) == string(nodeSpawnSettle) && strings.Contains(hit.Reason, "silent") {
			named = true
		}
	}
	if !named {
		t.Fatalf("no silent notice hit for machine-a spawn_settle: %+v", hub.nodeLoopNoticeHits(now))
	}

	now = time.Date(2026, 11, 19, 0, 1, 0, 0, time.UTC)
	hub.maybeWriteLoopsRow(ctx)
	if got := published[loopsRowKey("2026-11-18")]; got != 1 {
		t.Fatalf("previous-day publishes=%d, want 1", got)
	}
	var doc loopsRowDoc
	if err := json.Unmarshal([]byte(bodies[loopsRowKey("2026-11-18")]), &doc); err != nil {
		t.Fatalf("row body is not JSON: %v", err)
	}
	var found *loopsRowEntry
	for i, row := range doc.Rows {
		if row.Machine == "machine-a" && row.Mechanism == string(nodeSpawnSettle) {
			found = &doc.Rows[i]
		}
	}
	if found == nil {
		t.Fatalf("no machine-a spawn_settle row in %+v", doc.Rows)
	}
	if found.SilentErrors < 1 {
		t.Fatalf("row silent_errors=%d, want >= 1", found.SilentErrors)
	}

	text := loopNoticeText("machine-a", string(nodeSpawnSettle), []string{"previously silent error path counted >= 1"}, "2026-11-18")
	if !strings.Contains(text, "machine-a") || !strings.Contains(text, string(nodeSpawnSettle)) {
		t.Fatalf("notice text does not name machine and mechanism: %q", text)
	}
}

// TestNodeLoopsUnknownRowNoNotice is the old-node acceptance: a heartbeat
// with no loops field writes an unknown row, with no counts and no notice.
func TestNodeLoopsUnknownRowNoNotice(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	hub := loopLedgerTestHub(t, &now, nil)
	ctx := context.Background()
	for _, m := range loopMechanisms {
		hub.loopTick(m)
	}
	hub.observeNodeLoops("old-node", nil, now)

	var body string
	hub.loopsDocPublish = func(_ context.Context, _, b string) error { body = b; return nil }
	hub.loopsDocExists = func(_ context.Context, _ string) (bool, error) { return false, nil }
	now = time.Date(2026, 11, 19, 0, 1, 0, 0, time.UTC)
	hub.maybeWriteLoopsRow(ctx)

	var doc loopsRowDoc
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("row body is not JSON: %v", err)
	}
	var found *loopsRowEntry
	for i, row := range doc.Rows {
		if row.Machine == "old-node" {
			if found != nil {
				t.Fatalf("old-node has more than one row: %+v", doc.Rows)
			}
			found = &doc.Rows[i]
		}
	}
	if found == nil {
		t.Fatalf("no old-node row in %+v", doc.Rows)
	}
	// A missing field must not read as zero activity: the row is unknown.
	if found.Mechanism != "unknown" {
		t.Fatalf("old-node mechanism=%q, want unknown", found.Mechanism)
	}
	if found.Ticks != 0 || found.Made != 0 || found.Done != 0 || found.Errors != 0 || found.SilentErrors != 0 {
		t.Fatalf("old-node row carries counts: %+v", found)
	}
	for _, hit := range hub.nodeLoopNoticeHits(now) {
		if hit.Machine == "old-node" {
			t.Fatalf("old node raised a notice: %+v", hit)
		}
	}
}

// TestNodeLoopsHeartbeatGatedOnHubFeature keeps an older hub's heartbeat
// clean: the ledger goes out only after hub.features names node_loops.
func TestNodeLoopsHeartbeatGatedOnHubFeature(t *testing.T) {
	client, err := NewHubClient(HubClientConfig{
		URL:                   "ws://fixture.invalid",
		MachineID:             "node-a",
		Token:                 r6NodeAToken,
		JobsInboxRoot:         t.TempDir(),
		AllowInsecureForTests: true,
		hostLoadCollector:     func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, errFixtureLoops },
		hostMemoryCollector:   func(context.Context) (*HubHostMemory, error) { return nil, errFixtureLoops },
	})
	if err != nil {
		t.Fatal(err)
	}
	pinned := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	client.now = func() time.Time { return pinned }
	client.nodeLoopTick(nodeTaskPaneSweep)
	client.signalsPeer.Store(&hubClientConnection{})
	if got := client.nodeLoopsForPeer(); got != nil {
		t.Fatalf("loops sent to a hub without the feature: %+v", got)
	}
	peer := &hubClientConnection{}
	peer.acceptsNodeLoops.Store(true)
	client.signalsPeer.Store(peer)
	got := client.nodeLoopsForPeer()
	if got == nil || len(got.Rows) != len(nodeLoopMechanisms) {
		t.Fatalf("loops missing for a hub with the feature: %+v", got)
	}
	event := client.heartbeatEvent(context.Background())
	heartbeat, ok := decodeHubHeartbeatPayload(event.Payload)
	if !ok || heartbeat.Loops == nil {
		t.Fatalf("heartbeat carries no decodable loops: %v", ok)
	}
}

// TestNodeLoopsLateRowsSingleDoc keeps one document per date when node rows
// arrive late in the day: the midnight write merges them into the same body.
func TestNodeLoopsLateRowsSingleDoc(t *testing.T) {
	now := time.Date(2026, 11, 18, 23, 50, 0, 0, time.UTC)
	hub := loopLedgerTestHub(t, &now, nil)
	ctx := context.Background()
	for _, m := range loopMechanisms {
		hub.loopTick(m)
	}
	client := &HubClient{now: func() time.Time { return now }}
	client.nodeLoopTick(nodeTaskPaneSweep)
	// Late in the day the node's bucket arrives at the hub.
	now = time.Date(2026, 11, 18, 23, 59, 0, 0, time.UTC)
	hub.observeNodeLoops("machine-a", client.nodeLoopsForHeartbeat(), now)

	published := map[string]int{}
	var body string
	hub.loopsDocPublish = func(_ context.Context, key, b string) error {
		published[key]++
		body = b
		return nil
	}
	hub.loopsDocExists = func(_ context.Context, _ string) (bool, error) { return false, nil }
	now = time.Date(2026, 11, 19, 0, 1, 0, 0, time.UTC)
	hub.maybeWriteLoopsRow(ctx)
	if got := published[loopsRowKey("2026-11-18")]; got != 1 {
		t.Fatalf("previous-day publishes=%d, want exactly 1", got)
	}
	var doc loopsRowDoc
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("row body is not JSON: %v", err)
	}
	hubRows, nodeRows := 0, 0
	for _, row := range doc.Rows {
		switch row.Machine {
		case "hub":
			hubRows++
		case "machine-a":
			nodeRows++
		default:
			t.Fatalf("unexpected machine %q", row.Machine)
		}
	}
	if hubRows != len(loopMechanisms) || nodeRows != len(nodeLoopMechanisms) {
		t.Fatalf("hub rows=%d node rows=%d, want %d and %d", hubRows, nodeRows, len(loopMechanisms), len(nodeLoopMechanisms))
	}
}

// TestNodeLoopsNoReportNoticeOnce names a machine that was disconnected for
// the whole day exactly once, and writes no row for it.
func TestNodeLoopsNoReportNoticeOnce(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	hub := loopLedgerTestHub(t, &now, nil)
	ctx := context.Background()
	hub.observeNodeLoops("gone-node", nil, now)

	for _, m := range loopMechanisms {
		hub.loopTick(m)
	}
	published := 0
	hub.loopsDocPublish = func(_ context.Context, _, _ string) error { published++; return nil }
	hub.loopsDocExists = func(_ context.Context, _ string) (bool, error) { return false, nil }
	// Day two passes with no heartbeat from gone-node; the hub still ticks,
	// so the closed day's row writes at the first tick of day three.
	now = time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	for _, m := range loopMechanisms {
		hub.loopTick(m)
	}
	now = time.Date(2026, 11, 20, 0, 1, 0, 0, time.UTC)
	hub.maybeWriteLoopsRow(ctx)
	if published != 2 {
		t.Fatalf("publishes=%d, want 2 (one per closed date)", published)
	}
	if !hub.loopNoticeAlreadySent("gone-node", "no_report", "2026-11-19") {
		t.Fatalf("disconnected machine was not named once for 2026-11-19")
	}
	hub.maybeWriteLoopsRow(ctx)
	if published != 2 {
		t.Fatalf("repeat publishes=%d, want 2: the notice must go out once", published)
	}
}

// TestDecodeHubHeartbeatLoops keeps the heartbeat's loops field closed: a
// valid bucket decodes, an unknown mechanism or null rejects the heartbeat.
func TestDecodeHubHeartbeatLoops(t *testing.T) {
	payload, err := json.Marshal(map[string]any{
		"status": "alive",
		"loops": map[string]any{
			"date": "2026-11-18",
			"rows": []any{map[string]any{
				"mechanism": "spawn_settle", "interval_s": 0, "ticks": 1,
				"made": 0, "done": 0, "errors": 0, "silent_errors": 1,
				"last_tick": "2026-11-18T12:00:00Z", "oldest_pending_s": 0,
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	heartbeat, ok := decodeHubHeartbeatPayload(payload)
	if !ok || heartbeat.Loops == nil || heartbeat.Loops.Date != "2026-11-18" || len(heartbeat.Loops.Rows) != 1 {
		t.Fatalf("valid loops field rejected: %+v %v", heartbeat, ok)
	}
	for _, raw := range []string{
		`{"status":"alive","loops":null}`,
		`{"status":"alive","loops":{"date":"2026-11-18","rows":[]}}`,
		`{"status":"alive","loops":{"date":"18.11.2026","rows":[]}}`,
		`{"status":"alive","loops":{"date":"2026-11-18","rows":[{"mechanism":"nope","interval_s":0,"ticks":0,"made":0,"done":0,"errors":0,"silent_errors":0,"last_tick":"","oldest_pending_s":0}]}}`,
		`{"status":"alive","loops":{"date":"2026-11-18"}}`,
	} {
		if _, ok := decodeHubHeartbeatPayload([]byte(raw)); ok {
			t.Fatalf("bad loops field accepted: %s", raw)
		}
	}
}

// nodeLoopTestGit runs real git in dir for the revisit test: the brief
// requires proof against a real worktree, not the fleet git fake.
func nodeLoopTestGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t",
		"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(out)
}

func nodeLoopTestJobEvent(t *testing.T, jobsDir, id, name, kind, at string, payload map[string]any) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"kind": kind, "created_at": at, "payload": payload})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(jobsDir, id, "events"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(jobsDir, id, "events", name), body, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestNodeCheckoutRevisitRealGit is the track-A2 revisit acceptance: over
// real git in a temp dir, the daily revisit removes a kept, unused worktree
// and keeps a worktree in use, and counts both in the worktree flow.
func TestNodeCheckoutRevisitRealGit(t *testing.T) {
	root := t.TempDir()
	nodeLoopTestGit(t, root, "init", "-b", "main", "clone")
	if err := os.WriteFile(filepath.Join(root, "clone", "notes.txt"), []byte("seed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	nodeLoopTestGit(t, filepath.Join(root, "clone"), "add", "notes.txt")
	nodeLoopTestGit(t, filepath.Join(root, "clone"), "commit", "-m", "seed")
	goneDir := filepath.Join(root, "job-gone-checkout")
	heldDir := filepath.Join(root, "job-held-checkout")
	nodeLoopTestGit(t, filepath.Join(root, "clone"), "worktree", "add", "--detach", goneDir)
	nodeLoopTestGit(t, filepath.Join(root, "clone"), "worktree", "add", "--detach", heldDir)

	jobsDir := filepath.Join(root, "jobs")
	nodeLoopTestJobEvent(t, jobsDir, "job-gone", "01.json", "job.spawned", "2026-11-18T10:00:00Z", map[string]any{"cwd": goneDir, "role": "implementer"})
	nodeLoopTestJobEvent(t, jobsDir, "job-gone", "02.json", "job.completed", "2026-11-18T11:00:00Z", map[string]any{})
	nodeLoopTestJobEvent(t, jobsDir, "job-held", "01.json", "job.spawned", "2026-11-18T10:00:00Z", map[string]any{"cwd": heldDir, "role": "implementer"})
	nodeLoopTestJobEvent(t, jobsDir, "job-held", "02.json", "job.completed", "2026-11-18T11:00:00Z", map[string]any{})
	nodeLoopTestJobEvent(t, jobsDir, "job-live", "01.json", "job.spawned", "2026-11-18T10:00:00Z", map[string]any{"cwd": heldDir, "role": "implementer"})

	spawnCfg := filepath.Join(root, "spawn.json")
	if err := os.WriteFile(spawnCfg, []byte(`{"work_root": "`+root+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	client := &HubClient{now: func() time.Time { return now }, jobsInboxRoot: root, spawnConfigPath: spawnCfg}

	client.revisitKeptWorktrees(context.Background())

	if _, err := os.Lstat(goneDir); !os.IsNotExist(err) {
		t.Fatalf("kept unused worktree was not removed: %v", err)
	}
	if _, err := os.Stat(heldDir); err != nil {
		t.Fatalf("in-use worktree was removed: %v", err)
	}
	snap := client.nodeLoopSnapshot()[nodeWorktreeFlow]
	if snap.Ticks != 1 {
		t.Fatalf("revisit ticks=%d, want 1", snap.Ticks)
	}
	// Both judged checkouts enter the flow; only the removed one exits. The
	// in-use guard keeps the held checkout: dropping it would remove heldDir
	// and turn this test red.
	if snap.Made != 2 || snap.Done != 1 {
		t.Fatalf("revisit made=%d done=%d, want 2 and 1", snap.Made, snap.Done)
	}
	rows, code := client.nodeCheckoutSweep(context.Background(), nodeCheckoutSweepArgs{DryRun: true})
	if code != "" {
		t.Fatalf("dry run after revisit failed: %s", code)
	}
	result, ok := rows.(nodeCheckoutSweepResult)
	if !ok || len(result.Checkouts) != 1 || result.Checkouts[0].Outcome != "kept" {
		t.Fatalf("dry run after revisit = %+v, want the one kept held checkout", rows)
	}
}

// TestNodeLoopsNextDayHeartbeatKeepsPreviousDay is the fix for review finding
// 1 (high): a next-day heartbeat must not replace the previous day's bucket
// before its row is written. The old code kept one report per machine and the
// 2026-11-18 row read unknown with the silent error lost.
func TestNodeLoopsNextDayHeartbeatKeepsPreviousDay(t *testing.T) {
	now := time.Date(2026, 11, 18, 23, 59, 0, 0, time.UTC)
	hub := loopLedgerTestHub(t, &now, nil)
	ctx := context.Background()
	for _, m := range loopMechanisms {
		hub.loopTick(m)
	}
	dayOne := &hubNodeLoopsPayload{Date: "2026-11-18", Rows: []hubNodeLoopRow{
		{Mechanism: string(nodeSpawnSettle), IntervalS: 0, Ticks: 1, SilentErrors: 1},
	}}
	hub.observeNodeLoops("machine-a", dayOne, now)
	now = time.Date(2026, 11, 19, 0, 0, 20, 0, time.UTC)
	hub.observeNodeLoops("machine-a", &hubNodeLoopsPayload{Date: "2026-11-19"}, now)

	var bodies []string
	hub.loopsDocPublish = func(_ context.Context, _, b string) error { bodies = append(bodies, b); return nil }
	hub.loopsDocExists = func(_ context.Context, _ string) (bool, error) { return false, nil }
	now = time.Date(2026, 11, 19, 0, 1, 0, 0, time.UTC)
	hub.maybeWriteLoopsRow(ctx)
	if len(bodies) != 1 {
		t.Fatalf("publishes=%d, want 1", len(bodies))
	}
	var doc loopsRowDoc
	if err := json.Unmarshal([]byte(bodies[0]), &doc); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range doc.Rows {
		if row.Machine == "machine-a" {
			if row.Mechanism == "unknown" {
				t.Fatalf("previous-day row read unknown after a next-day heartbeat")
			}
			if row.Mechanism == string(nodeSpawnSettle) && row.SilentErrors == 1 {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("previous-day spawn_settle row lost: %+v", doc.Rows)
	}
}

// TestRunCheckoutRevisitHoldsCommandMu is the fix for review finding 2
// (high): every revisit pass holds nodeCommandMu, so a git removal never
// interleaves with a pane close. The pass seam observes the same lock the
// real sweep takes.
func TestRunCheckoutRevisitHoldsCommandMu(t *testing.T) {
	client := &HubClient{jobsInboxRoot: t.TempDir()}
	ticks := make(chan time.Time, 1)
	client.checkoutRevisitTicks = ticks
	var held atomic.Bool
	done := make(chan struct{})
	client.checkoutRevisitPass = func(context.Context) {
		held.Store(!client.nodeCommandMu.TryLock())
		close(done)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go client.runCheckoutRevisit(ctx)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("revisit pass did not run")
	}
	if !held.Load() {
		t.Fatal("revisit pass ran without nodeCommandMu held")
	}
}

// TestNodeLoopsNoReportRetriesAfterPersistFailure is the fix for review
// finding 3 (medium): a failed no-report persist keeps the date pending
// instead of pruning it, so the next sweep retries instead of losing the
// notice.
func TestNodeLoopsNoReportRetriesAfterPersistFailure(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	hub := loopLedgerTestHub(t, &now, nil)
	ctx := context.Background()
	hub.observeNodeLoops("gone-node", nil, now)
	for _, m := range loopMechanisms {
		hub.loopTick(m)
	}
	hub.inbox.store.abort()
	// Day two passes with no heartbeat from gone-node but the hub still
	// ticking, so its row is due at the first tick of day three.
	now = time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	for _, m := range loopMechanisms {
		hub.loopTick(m)
	}
	now = time.Date(2026, 11, 20, 0, 1, 0, 0, time.UTC)
	hub.loopsDocPublish = func(context.Context, string, string) error { return nil }
	hub.loopsDocExists = func(context.Context, string) (bool, error) { return false, nil }
	hub.maybeWriteLoopsRow(ctx)
	if hub.loopNoticeAlreadySent("gone-node", "no_report", "2026-11-19") {
		t.Fatal("failed no-report persist was marked sent")
	}
	st := hub.loopLedger()
	st.mu.Lock()
	_, pending := st.days["2026-11-19"]
	st.mu.Unlock()
	if !pending {
		t.Fatal("failed no-report send pruned the date instead of keeping it pending")
	}
}

// TestSweepTaskPanesTicksWorktreeFlow is the fix for review finding 3
// (medium): the sweep pass ticks the worktree tail's flow, so the 5m
// interval on the heartbeat row matches the tail's real cadence.
func TestSweepTaskPanesTicksWorktreeFlow(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	client := &HubClient{now: func() time.Time { return now }, taskPaneSweepInterval: 5 * time.Minute}
	client.sweepTaskPanes(context.Background(), &fleet.TaskPaneSweeper{})
	snap := client.nodeLoopSnapshot()
	if snap[nodeTaskPaneSweep].Ticks != 1 || snap[nodeWorktreeFlow].Ticks != 1 {
		t.Fatalf("sweep ticks=%d worktree ticks=%d, want 1 and 1",
			snap[nodeTaskPaneSweep].Ticks, snap[nodeWorktreeFlow].Ticks)
	}
	var found *hubNodeLoopRow
	for _, row := range client.nodeLoopsForHeartbeat().Rows {
		if row.Mechanism == string(nodeWorktreeFlow) {
			found = &row
			break
		}
	}
	if found == nil {
		t.Fatal("worktree_flow row missing from heartbeat")
	}
	if found.IntervalS != int64(5*time.Minute/time.Second) {
		t.Fatalf("worktree_flow interval_s=%d, want %d", found.IntervalS, int64(5*time.Minute/time.Second))
	}
}

// TestNodeLoopsHeartbeatCarriesLastTickAcrossMidnight is the fix for review
// finding 5 (medium): a row whose new-day bucket is still empty carries the
// mechanism's most recent tick, instead of reading stale from midnight until
// its first pass.
func TestNodeLoopsHeartbeatCarriesLastTickAcrossMidnight(t *testing.T) {
	now := time.Date(2026, 11, 18, 23, 0, 0, 0, time.UTC)
	client := &HubClient{now: func() time.Time { return now }}
	client.nodeLoopTick(nodeLaneReconcile)
	now = time.Date(2026, 11, 19, 0, 30, 0, 0, time.UTC)
	rows := client.nodeLoopsForHeartbeat().Rows
	for _, row := range rows {
		if row.Mechanism == string(nodeLaneReconcile) && row.LastTick != "2026-11-18T23:00:00Z" {
			t.Fatalf("lane_reconcile last_tick=%q, want the previous day's tick", row.LastTick)
		}
	}
}

// pendingCloseLedger is a nodeHubCloseLedger whose Settle never finishes: it
// returns HubClosePending with a nil error, like an unreadable close
// operation does.
type pendingCloseLedger struct{ nodeFleetHubClose }

func (pendingCloseLedger) LockEnd(context.Context, string) (func(), error) {
	return func() {}, nil
}
func (pendingCloseLedger) FenceState(string) (bool, bool, error) { return false, false, nil }
func (pendingCloseLedger) Settle(context.Context, string) (fleet.HubCloseState, error) {
	return fleet.HubClosePending, nil
}

// TestSettleHubClosePendingNilErrorCountsNoDone is the fix for review finding
// 6 (low): ensure terminal-state predicate is used and pending states are not
// counted as done.
func TestSettleHubClosePendingNilErrorCountsNoDone(t *testing.T) {
	client := &HubClient{jobsInboxRoot: t.TempDir(), nodeHubClose: pendingCloseLedger{}}
	state := client.settleHubClose(context.Background(), "job-1")
	if hubCloseStateIsTerminal(state) {
		t.Fatalf("state %v is terminal, want non-terminal", state)
	}
	if !hubCloseStateIsTerminal(fleet.HubCloseSettled) || !hubCloseStateIsTerminal(fleet.HubCloseCancelled) || !hubCloseStateIsTerminal(fleet.HubCloseNone) {
		t.Fatal("terminal states reported non-terminal")
	}
	if hubCloseStateIsTerminal(fleet.HubClosePending) {
		t.Fatal("HubClosePending reported terminal")
	}
	if snap := client.nodeLoopSnapshot()[nodeHubCloseSettle]; snap.Done != 0 {
		t.Fatalf("done=%d, want 0", snap.Done)
	}
}

type errorCloseLedger struct {
	nodeFleetHubClose
	reconciled atomic.Int32
}

func (e *errorCloseLedger) LockEnd(context.Context, string) (func(), error) {
	return func() {}, nil
}
func (e *errorCloseLedger) FenceState(string) (bool, bool, error) {
	e.reconciled.Add(1)
	return false, false, nil
}
func (e *errorCloseLedger) Settle(context.Context, string) (fleet.HubCloseState, error) {
	return fleet.HubClosePending, errors.New("settle simulated error")
}

// TestSettleHubCloseErrorReconcilesWatcherFence is the fix for review finding
// 2 (medium): reconcileWatcherFence must still run when Settle returns an error.
func TestSettleHubCloseErrorReconcilesWatcherFence(t *testing.T) {
	ledger := &errorCloseLedger{}
	client := &HubClient{jobsInboxRoot: t.TempDir(), nodeHubClose: ledger}
	state := client.settleHubClose(context.Background(), "job-1")
	if state != fleet.HubClosePending {
		t.Fatalf("state=%v, want HubClosePending", state)
	}
	if count := ledger.reconciled.Load(); count != 2 {
		t.Fatalf("FenceState called %d times, want 2 (fence reconcile skipped on Settle error)", count)
	}
}

// TestNodeLoopsRestartAccumulatesDayCounts is the fix for review finding 1
// (medium): a node restart accumulates earlier counts within the day across
// epochs instead of dropping them.
func TestNodeLoopsRestartAccumulatesDayCounts(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	hub := loopLedgerTestHub(t, &now, nil)
	ctx := context.Background()
	for _, m := range loopMechanisms {
		hub.loopTick(m)
	}
	epoch1 := &hubNodeLoopsPayload{Date: "2026-11-18", Epoch: "epoch-1", Rows: []hubNodeLoopRow{
		{Mechanism: string(nodeSpawnSettle), Ticks: 5, SilentErrors: 1},
	}}
	hub.observeNodeLoops("machine-a", epoch1, now)

	now = time.Date(2026, 11, 18, 14, 0, 0, 0, time.UTC)
	epoch2 := &hubNodeLoopsPayload{Date: "2026-11-18", Epoch: "epoch-2", Rows: []hubNodeLoopRow{
		{Mechanism: string(nodeSpawnSettle), Ticks: 3, SilentErrors: 0},
	}}
	hub.observeNodeLoops("machine-a", epoch2, now)

	var body string
	hub.loopsDocPublish = func(_ context.Context, _, b string) error { body = b; return nil }
	hub.loopsDocExists = func(_ context.Context, _ string) (bool, error) { return false, nil }
	now = time.Date(2026, 11, 19, 0, 1, 0, 0, time.UTC)
	hub.maybeWriteLoopsRow(ctx)

	var doc loopsRowDoc
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range doc.Rows {
		if row.Machine == "machine-a" && row.Mechanism == string(nodeSpawnSettle) {
			found = true
			if row.Ticks != 8 || row.SilentErrors != 1 {
				t.Fatalf("row ticks=%d silent_errors=%d, want 8 and 1", row.Ticks, row.SilentErrors)
			}
		}
	}
	if !found {
		t.Fatal("spawn_settle row missing")
	}
}

// TestNodeLoopsNoReportRetiresAfterSevenDays is the fix for review finding 4
// (low): machines silent for 7 or more consecutive days stop generating
// daily no_report notices.
func TestNodeLoopsNoReportRetiresAfterSevenDays(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	hub := loopLedgerTestHub(t, &now, nil)
	ctx := context.Background()
	hub.observeNodeLoops("retiring-node", nil, now)
	for _, m := range loopMechanisms {
		hub.loopTick(m)
	}

	hub.loopsDocPublish = func(context.Context, string, string) error { return nil }
	hub.loopsDocExists = func(context.Context, string) (bool, error) { return false, nil }

	for day := 2; day <= 10; day++ {
		now = time.Date(2026, 11, day, 0, 1, 0, 0, time.UTC)
		for _, m := range loopMechanisms {
			hub.loopTick(m)
		}
		hub.maybeWriteLoopsRow(ctx)
		if day == 2 {
			continue // day 1 had a heartbeat, so no no_report notice
		}
		dateStr := fmt.Sprintf("2026-11-%02d", day-1)
		sent := hub.loopNoticeAlreadySent("retiring-node", "no_report", dateStr)
		if day <= 9 {
			if !sent {
				t.Fatalf("no_report notice not sent for day %s", dateStr)
			}
		} else {
			if sent {
				t.Fatalf("no_report notice sent after 7 consecutive days for day %s", dateStr)
			}
		}
	}
}

// TestNodeLoopsClockSkewDoesNotTripStaleNotice is the fix for review finding
// 5 (low): node clock skew does not trip false stale notices because age is
// computed on receipt time plus node AgeS.
func TestNodeLoopsClockSkewDoesNotTripStaleNotice(t *testing.T) {
	hubNow := time.Date(2026, 11, 18, 12, 30, 0, 0, time.UTC)
	hub := loopLedgerTestHub(t, &hubNow, nil)

	// Node clock is 20 minutes behind hub (12:10), but just ticked 10 seconds ago (AgeS = 10).
	nodePayload := &hubNodeLoopsPayload{
		Date:  "2026-11-18",
		Epoch: "epoch-1",
		Rows: []hubNodeLoopRow{
			{
				Mechanism: string(nodeTaskPaneSweep),
				IntervalS: 300,
				LastTick:  "2026-11-18T12:09:50Z",
				AgeS:      10,
				Ticks:     1,
			},
		},
	}
	hub.observeNodeLoops("skewed-node", nodePayload, hubNow)

	hits := hub.nodeLoopNoticeHits(hubNow.Add(15 * time.Second))
	for _, hit := range hits {
		if hit.Machine == "skewed-node" && strings.Contains(hit.Reason, "last_tick older") {
			t.Fatalf("false stale notice tripped by clock skew: %+v", hit)
		}
	}
}

// TestNodeCheckoutRevisitSkipsLiveCheckouts is the fix for review finding 7
// (low): twenty live checkouts enter no flow item, so the progress rule
// cannot fire on a healthy node. The old code counted every judged row.
func TestNodeCheckoutRevisitSkipsLiveCheckouts(t *testing.T) {
	root := t.TempDir()
	nodeLoopTestGit(t, root, "init", "-b", "main", "clone")
	if err := os.WriteFile(filepath.Join(root, "clone", "notes.txt"), []byte("seed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	nodeLoopTestGit(t, filepath.Join(root, "clone"), "add", "notes.txt")
	nodeLoopTestGit(t, filepath.Join(root, "clone"), "commit", "-m", "seed")
	jobsDir := filepath.Join(root, "jobs")
	for i := range 20 {
		id := "job-live"
		if i > 0 {
			id = "job-live" + string(rune('a'+i-1))
		}
		dir := filepath.Join(root, id+"-checkout")
		nodeLoopTestGit(t, filepath.Join(root, "clone"), "worktree", "add", "--detach", dir)
		nodeLoopTestJobEvent(t, jobsDir, id, "01.json", "job.spawned", "2026-11-18T10:00:00Z", map[string]any{"cwd": dir, "role": "implementer"})
	}
	spawnCfg := filepath.Join(root, "spawn.json")
	if err := os.WriteFile(spawnCfg, []byte(`{"work_root": "`+root+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	client := &HubClient{now: func() time.Time { return now }, jobsInboxRoot: root, spawnConfigPath: spawnCfg}
	client.nodeCommandMu.Lock()
	client.revisitKeptWorktrees(context.Background())
	client.nodeCommandMu.Unlock()
	if snap := client.nodeLoopSnapshot()[nodeWorktreeFlow]; snap.Made != 0 {
		t.Fatalf("live checkouts made=%d, want 0", snap.Made)
	}
}

// TestNodeLoopsMultiEpochDeterministicNewestProcessState verifies that two
// epochs with different oldest_pending_s / interval_s produce the newest
// epoch's values deterministically.
func TestNodeLoopsMultiEpochDeterministicNewestProcessState(t *testing.T) {
	hubNow := time.Date(2026, 11, 18, 14, 0, 0, 0, time.UTC)
	hub := loopLedgerTestHub(t, &hubNow, nil)

	p1 := &hubNodeLoopsPayload{
		Date:  "2026-11-18",
		Epoch: "epoch-1",
		Rows: []hubNodeLoopRow{
			{
				Mechanism:      string(nodeTaskPaneSweep),
				IntervalS:      300,
				OldestPendingS: 120,
				Ticks:          5,
			},
		},
	}
	p2 := &hubNodeLoopsPayload{
		Date:  "2026-11-18",
		Epoch: "epoch-2",
		Rows: []hubNodeLoopRow{
			{
				Mechanism:      string(nodeTaskPaneSweep),
				IntervalS:      600,
				OldestPendingS: 30,
				Ticks:          2,
			},
		},
	}
	// Deliver epoch-1 at 12:00, epoch-2 at 13:00.
	hub.observeNodeLoops("node-a", p1, time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC))
	hub.observeNodeLoops("node-a", p2, time.Date(2026, 11, 18, 13, 0, 0, 0, time.UTC))

	st := hub.loopLedger()
	st.mu.Lock()
	rows := hub.nodeLoopsRowsLocked(st, "2026-11-18")
	st.mu.Unlock()

	if len(rows) != 1 {
		t.Fatalf("rows count = %d, want 1", len(rows))
	}
	row := rows[0]
	if row.IntervalS != 600 || row.OldestPendingS != 30 {
		t.Fatalf("row interval=%d oldest_pending=%d, want 600 and 30 (from newest epoch)", row.IntervalS, row.OldestPendingS)
	}
	if row.Ticks != 7 {
		t.Fatalf("row ticks=%d, want 7 (summed across epochs)", row.Ticks)
	}
}

// TestNodeLoopsMultiEpochWedgedNewestFiresStaleNotice verifies that a wedged
// newest epoch (age 3600, interval 300) fires the stale rule even though an
// older epoch reported fresh age 0.
func TestNodeLoopsMultiEpochWedgedNewestFiresStaleNotice(t *testing.T) {
	hubNow := time.Date(2026, 11, 18, 14, 0, 0, 0, time.UTC)
	hub := loopLedgerTestHub(t, &hubNow, nil)

	p1 := &hubNodeLoopsPayload{
		Date:  "2026-11-18",
		Epoch: "epoch-1",
		Rows: []hubNodeLoopRow{
			{
				Mechanism: string(nodeTaskPaneSweep),
				IntervalS: 300,
				AgeS:      0,
				Ticks:     10,
			},
		},
	}
	// Epoch 2 wedged: last ticked 3600s ago.
	p2 := &hubNodeLoopsPayload{
		Date:  "2026-11-18",
		Epoch: "epoch-2",
		Rows: []hubNodeLoopRow{
			{
				Mechanism: string(nodeTaskPaneSweep),
				IntervalS: 300,
				AgeS:      3600,
				Ticks:     1,
			},
		},
	}
	hub.observeNodeLoops("node-a", p1, time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC))
	hub.observeNodeLoops("node-a", p2, time.Date(2026, 11, 18, 14, 0, 0, 0, time.UTC))

	hits := hub.nodeLoopNoticeHits(hubNow)
	staleFound := false
	for _, hit := range hits {
		if hit.Machine == "node-a" && hit.Mechanism == loopMechanism(nodeTaskPaneSweep) && strings.Contains(hit.Reason, "last_tick older") {
			staleFound = true
		}
	}
	if !staleFound {
		t.Fatalf("wedged newest epoch did not fire stale notice: hits=%+v", hits)
	}
}

// TestNodeLoopsMultiEpochMadeDoneFromNewestNotSummed verifies that two epochs
// each with made=20, done=1 produce made=20, not 40, preventing false progress notices.
func TestNodeLoopsMultiEpochMadeDoneFromNewestNotSummed(t *testing.T) {
	hubNow := time.Date(2026, 11, 18, 14, 0, 0, 0, time.UTC)
	hub := loopLedgerTestHub(t, &hubNow, nil)

	p1 := &hubNodeLoopsPayload{
		Date:  "2026-11-18",
		Epoch: "epoch-1",
		Rows: []hubNodeLoopRow{
			{
				Mechanism: string(nodeWorktreeFlow),
				IntervalS: 86400,
				Made:      20,
				Done:      1,
				Ticks:     1,
			},
		},
	}
	p2 := &hubNodeLoopsPayload{
		Date:  "2026-11-18",
		Epoch: "epoch-2",
		Rows: []hubNodeLoopRow{
			{
				Mechanism: string(nodeWorktreeFlow),
				IntervalS: 86400,
				Made:      20,
				Done:      1,
				Ticks:     1,
			},
		},
	}
	hub.observeNodeLoops("node-a", p1, time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC))
	hub.observeNodeLoops("node-a", p2, time.Date(2026, 11, 18, 13, 0, 0, 0, time.UTC))

	st := hub.loopLedger()
	st.mu.Lock()
	rows := hub.nodeLoopsRowsLocked(st, "2026-11-18")
	st.mu.Unlock()

	if len(rows) != 1 {
		t.Fatalf("rows count = %d, want 1", len(rows))
	}
	if rows[0].Made != 20 || rows[0].Done != 1 {
		t.Fatalf("made=%d done=%d, want 20 and 1 (from newest epoch, not summed)", rows[0].Made, rows[0].Done)
	}
}

// TestNodeLoopsReportAfterDateWrittenDropped verifies that a report arriving
// for date D after date D has been written is dropped and not left behind in nodeReports.
func TestNodeLoopsReportAfterDateWrittenDropped(t *testing.T) {
	now := time.Date(2026, 11, 19, 0, 1, 0, 0, time.UTC)
	hub := loopLedgerTestHub(t, &now, nil)
	hub.loopsDocPublish = func(context.Context, string, string) error { return nil }
	hub.loopsDocExists = func(context.Context, string) (bool, error) { return false, nil }
	ctx := context.Background()

	// Initial report for D (2026-11-18) arrives before write.
	p1 := &hubNodeLoopsPayload{
		Date:  "2026-11-18",
		Epoch: "epoch-1",
		Rows: []hubNodeLoopRow{
			{Mechanism: string(nodeTaskPaneSweep), IntervalS: 300, Ticks: 1},
		},
	}
	hub.observeNodeLoops("node-a", p1, time.Date(2026, 11, 18, 23, 50, 0, 0, time.UTC))

	// Write date D at midnight.
	for _, m := range loopMechanisms {
		hub.loopTick(m)
	}
	hub.maybeWriteLoopsRow(ctx)

	// Late report for D arrives after D was written.
	p2 := &hubNodeLoopsPayload{
		Date:  "2026-11-18",
		Epoch: "epoch-2",
		Rows: []hubNodeLoopRow{
			{Mechanism: string(nodeTaskPaneSweep), IntervalS: 300, Ticks: 2},
		},
	}
	hub.observeNodeLoops("node-a", p2, time.Date(2026, 11, 19, 0, 5, 0, 0, time.UTC))

	st := hub.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	if _, exists := st.nodeReports["node-a"]["2026-11-18"]; exists {
		t.Fatalf("report for already-written date 2026-11-18 was stored in nodeReports")
	}
}

// TestNodeLoopsPrunesPastDaysOnRollover verifies that when date rolls over,
// bucketFor deletes earlier dates from s.days so only today's bucket remains.
func TestNodeLoopsPrunesPastDaysOnRollover(t *testing.T) {
	d1 := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	client := &HubClient{now: func() time.Time { return d1 }}
	client.nodeLoopTick(nodeTaskPaneSweep)

	st := client.nodeLoopLedger()
	st.mu.Lock()
	if len(st.days) != 1 || st.days["2026-11-18"] == nil {
		st.mu.Unlock()
		t.Fatalf("expected day 2026-11-18 in days: %+v", st.days)
	}
	st.mu.Unlock()

	// Roll over to next day.
	d2 := time.Date(2026, 11, 19, 0, 1, 0, 0, time.UTC)
	client.now = func() time.Time { return d2 }
	client.nodeLoopTick(nodeTaskPaneSweep)

	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.days) != 1 || st.days["2026-11-19"] == nil {
		t.Fatalf("expected only day 2026-11-19 in days after rollover: %+v", st.days)
	}
	if _, exists := st.days["2026-11-18"]; exists {
		t.Fatalf("earlier day 2026-11-18 was not pruned on rollover")
	}
}
