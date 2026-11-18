package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
