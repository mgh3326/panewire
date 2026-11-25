package panewire

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

// launchDirNode is a node whose prepare answers with the worktree w, beside a
// sibling job-b, and which records what it hands the launcher.
type launchDirNode struct {
	client   *HubClient
	w, other string
	handed   []fleet.LaunchDir
	// atLaunch runs inside the launcher call, after the node resolved.
	atLaunch func()
}

func newLaunchDirNode(t *testing.T) *launchDirNode {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	node := &launchDirNode{w: filepath.Join(root, "job-a"), other: filepath.Join(root, "job-b")}
	for _, dir := range []string{node.w, node.other} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	node.client = &HubClient{spawnConfigPath: t14SpawnConfig(t, root), fleetEvents: (&fleetEventRecorder{}).write}
	node.client.spawnPrepare = func(context.Context, nodeWorktreePrepareArgs) (any, string) {
		return nodeWorktreePrepareResult{Outcome: nodePreparePrepared, Worktree: &nodeWorktree{Path: node.w}}, ""
	}
	node.client.spawnEngine = func(_ context.Context, dir fleet.LaunchDir, _, _, _, _ string, _ time.Time, _ []string) (*hubSpawnOutput, fleet.Execution) {
		node.handed = append(node.handed, dir)
		if node.atLaunch != nil {
			node.atLaunch()
		}
		return t14EngineOK()
	}
	return node
}

func (node *launchDirNode) run(t *testing.T) hubSpawnResult {
	t.Helper()
	message := concurrencyMessage(1, "job-a")
	message.Args = []string{"--job", "job-a", "-L", "live"}
	return node.client.runHubSpawn(t.Context(), message)
}

func (node *launchDirNode) link(t *testing.T, target string) {
	t.Helper()
	if err := os.Symlink(target, filepath.Join(node.w, "live")); err != nil {
		t.Fatal(err)
	}
}

// bound checks the node handed the launcher one bound directory at path: a
// bound LaunchDir never equals the plain one a mapped directory gets.
func (node *launchDirNode) bound(t *testing.T, result hubSpawnResult, path string) {
	t.Helper()
	if result.RC != 0 || len(node.handed) != 1 {
		t.Fatalf("result=%+v launches=%d", result, len(node.handed))
	}
	if got := node.handed[0]; got.Path != path || got == (fleet.LaunchDir{Path: path}) {
		t.Fatalf("launcher got %+v, want %q bound", got, path)
	}
}

func TestFinalCWDKeepsAbsentLaneFallback_t6367(t *testing.T) {
	node := newLaunchDirNode(t)
	if slots := node.client.nodeSpawnSlots(); slots != 1 {
		t.Fatalf("slots=%d, want the default 1", slots)
	}
	node.bound(t, node.run(t), node.w)
}

// The node hands the launcher the resolved target, so a lane link repointed
// afterwards moves nothing.
func TestNodeBindsLaneTargetOnce_t6404(t *testing.T) {
	node := newLaunchDirNode(t)
	target := filepath.Join(node.w, "sub", "live-impl")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	node.link(t, "sub/live-impl")
	node.atLaunch = func() {
		if err := os.Remove(filepath.Join(node.w, "live")); err != nil {
			t.Error(err)
		}
		node.link(t, "../job-b")
		// What the launcher was handed still leads to the bound target.
		if resolved, err := filepath.EvalSymlinks(node.handed[0].Path); err != nil || resolved != target {
			t.Errorf("handed %q now resolves to %q (%v), want %q", node.handed[0].Path, resolved, err, target)
		}
	}
	node.bound(t, node.run(t), target)
}

func testLaneLinkRefused(t *testing.T, target string) {
	t.Helper()
	node := newLaunchDirNode(t)
	node.link(t, target)
	result := node.run(t)
	if result.RC != 81 || result.Error != "checkout_refused:lane_outside_worktree" || result.Stage != "checkout" || len(node.handed) != 0 {
		t.Fatalf("result=%+v launches=%d, want lane_outside_worktree rc 81 and no launch", result, len(node.handed))
	}
}

func TestEscapingLaneLinkRefused_t6404(t *testing.T) { testLaneLinkRefused(t, "../job-b") }

func TestDanglingLaneLinkRefused_t6404(t *testing.T) { testLaneLinkRefused(t, "missing") }

// A spawn without a checkout binds nothing: the launcher resolves its lane.
func TestNodeMappedSpawnBindsNothing_t6404(t *testing.T) {
	node := newLaunchDirNode(t)
	message := admissionMessage(640401, true)
	message.Args = []string{"--job", "job-a", "-L", "live"}
	result := node.client.runHubSpawn(t.Context(), message)
	if result.RC != 0 || len(node.handed) != 1 || node.handed[0] != (fleet.LaunchDir{Path: node.handed[0].Path}) || node.handed[0].Path == "" {
		t.Fatalf("result=%+v handed=%+v, want one unbound directory", result, node.handed)
	}
}

func TestSpawnReceiptCheckoutPathLaneTarget_t6404(t *testing.T) {
	for _, tc := range []struct {
		cwd, lane string
		want      bool
	}{
		{"/w/job-a", "", true},
		{"/w/job-a", "live", true},
		{"/w/job-a/live", "live", true},
		{"/w/job-a/sub/live-impl", "live", true},
		{"/w/job-a/impl", "mock", true},
		{"/w/job-a/sub/live-impl", "", false},
		{"/w/job-a/sub/live-impl", "other", false},
		{"/w/job-b", "live", false},
		{"/w/job-b/live", "live", false},
		{"/w/job-a/../job-b", "live", false},
		{"/w/job-a-copy/impl", "live", false},
		{"", "live", false},
	} {
		if got := spawnReceiptCheckoutPath(tc.cwd, "job-a", tc.lane); got != tc.want {
			t.Errorf("cwd=%q lane=%q: %v, want %v", tc.cwd, tc.lane, got, tc.want)
		}
	}
}

// The hub settles a landed checkout spawn whose in-tree lane link the node
// resolved: the receipt names the target, not <job dir>/live.
func TestSpawnLandedInLaneLinkTargetSucceeds_t6404(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14Node(t, h, server, true)
	request := autoSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	transport := &directSpawnTransport{h: h, task: task}
	placed := refusalNodeRequest(request, 640402, "machine-a")
	placed.Checkout = &fleet.HubCheckout{Origin: "https://github.com/mgh3326/panewire.git", Mode: "branch", Branch: "feature/lane", Dir: "job-dir"}
	placed.Args = append(append([]string(nil), placed.Args...), "-L", "live")
	type answer struct {
		reply fleet.HubSpawnReply
		err   error
	}
	done := make(chan answer, 1)
	go func() {
		reply, err := dispatchPlacedSpawn(boundedContext(t), transport, placed)
		done <- answer{reply, err}
	}()
	sent := t14ReadNodeSpawn(t, node)
	landed := map[string]any{
		"type": "job.spawn.result", "request_id": sent.RequestID, "rc": 0, "job_id": "test-job", "pane": "w1:p9",
		"stdout_tail": "OK pane=w1:p9 job=test-job", "stage": "landing", "retryable": false,
		"receipt": map[string]any{"job": "test-job", "host": "machine-a", "pane": "w1:p9", "profile": "sonnet", "model": "m", "role": "worker", "cwd": "/work/job-dir/sub/live-impl", "label": "test-job", "kind": "claude"},
	}
	if err = wsjson.Write(t.Context(), node, landed); err != nil {
		t.Fatal(err)
	}
	command, asked := readNodeCommand(t, node)
	if !asked || command["op"] != "pane.read" {
		t.Fatalf("hub did not read the landed pane: asked=%v command=%v", asked, command)
	}
	if args, _ := command["args"].(map[string]any); args["cwd"] != "/work/job-dir/sub/live-impl" {
		t.Fatalf("pane.read args %v, want the receipt's directory", command["args"])
	}
	if err = wsjson.Write(t.Context(), node, map[string]any{"type": "node.command.result", "request_id": command["request_id"], "op": "pane.read", "result": map[string]any{"outcome": "read", "agent_status": "working", "focused": false, "composer": "empty"}}); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if got.err != nil || got.reply.RC != 0 {
		t.Fatalf("submitter saw %+v err=%v, want rc 0", got.reply, got.err)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "succeeded" {
		t.Fatalf("saved status=%s err=%v, want succeeded", saved.Status, err)
	}
}
