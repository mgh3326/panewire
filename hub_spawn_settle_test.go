package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

// settleFixture is a node that restarted while it was executing request
// "orphan": the journal row exists, nothing in this process runs it.
type settleFixture struct {
	client  *HubClient
	journal *nodeSpawnJournal
	orphan  hubOutboundMessage
	started *[]string // watchers re-armed
	jobs    string
}

func newSettleFixture(t *testing.T, herdr relayCommandRunner) *settleFixture {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", t.TempDir())
	started := &[]string{}
	journalRoot := t.TempDir()
	client := &HubClient{
		jobsInboxRoot: root, nodeHerdr: herdr, spawnConfigPath: t14SpawnConfig(t, t.TempDir()), spawnJournalDir: journalRoot,
		rearmStart:  func(id string) error { *started = append(*started, id); return nil },
		fleetEvents: (&fleetEventRecorder{}).write,
	}
	journal, err := client.acquireSpawnJournal()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.releaseSpawnJournal)
	orphan := hubOutboundMessage{OperatorAuthorized: true, RequestID: t14RequestID(7001), CWDKey: "repo-a", BriefInline: "brief", Args: []string{"-m", "codex-terra", "-l", "label-a", "--job", "job-a"}, WaitSeconds: 60}
	if claimed, _, err := journal.claim(t.Context(), orphan); err != nil || !claimed {
		t.Fatalf("claim %v %v", claimed, err)
	}
	return &settleFixture{client: client, journal: journal, orphan: orphan, started: started, jobs: root}
}

// recordSpawn writes the job.spawned event the dead process left behind.
func (f *settleFixture) recordSpawn(t *testing.T, pane string) {
	t.Helper()
	dir := filepath.Join(f.jobs, "jobs", "job-a", "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	event, _ := json.Marshal(map[string]any{"kind": "job.spawned", "payload": map[string]any{"pane_id": pane, "herdr_session": "worker", "host": "machine-a", "profile": "codex-terra", "model_id": "m", "role": "implementer", "cwd": "/work/a"}})
	if err := os.WriteFile(filepath.Join(dir, "01.json"), event, 0600); err != nil {
		t.Fatal(err)
	}
}

func (f *settleFixture) stored(t *testing.T) *hubSpawnResult {
	t.Helper()
	result, err := f.journal.resultOf(t.Context(), f.orphan.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// A restart with an executing row whose pane landed: the row completes with
// the job's real result, and the watcher that records the job's outcome is
// armed again (the watcher itself is TestWatcherRearmCatchUp's subject).
func TestSpawnSettleLandedPaneKeepsWatchingAndReportsResult(t *testing.T) {
	herdr := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "label-a", "working", "/work/a"))}, panes: settlePanes("w1:p1", "label-a")}
	f := newSettleFixture(t, herdr.run)
	f.recordSpawn(t, "w1:p1")
	f.client.runSpawnSettle(t.Context())
	result := f.stored(t)
	if result == nil || result.RC != 0 || result.Error != "" || result.Pane != "w1:p1" || result.JobID != "job-a" {
		t.Fatalf("landed spawn settled as %+v", result)
	}
	want := fleet.ExecutionReceipt{Job: "job-a", Host: "machine-a", Pane: "w1:p1", Profile: "codex-terra", Model: "m", Role: "implementer", CWD: "/work/a", Label: "label-a", Kind: firstNonempty(fleet.ProfileKind("codex-terra"), "claude")}
	if result.Receipt == nil || *result.Receipt != want {
		t.Fatalf("receipt %+v, want %+v", result.Receipt, want)
	}
	if !reflect.DeepEqual(*f.started, []string{"job-a"}) {
		t.Fatalf("watchers armed: %v", *f.started)
	}
}

// The replay the hub sends after the restart gets the settled result at once,
// not "unknown" until the request deadline.
func TestSpawnSettleAnswersReplayFromPane(t *testing.T) {
	herdr := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "label-a", "working", "/work/a"))}, panes: settlePanes("w1:p1", "label-a")}
	f := newSettleFixture(t, herdr.run)
	f.recordSpawn(t, "w1:p1")
	peer, next := settlePeer(t)
	f.client.handleHubSpawn(t.Context(), peer, f.orphan)
	if got := next(); got.RC != 0 || got.Pane != "w1:p1" || got.Error != "" {
		t.Fatalf("replay answered %+v", got)
	}
}

func TestSpawnSettleGonePaneEndsLostWithReason(t *testing.T) {
	cases := []struct {
		name        string
		recordPane  string
		agents      string
		reason      string
		retryable   bool
		wantStarted int
	}{
		{"pane recorded then gone", "w1:p1", nodeAgentList(), spawnLostPaneGone, false, 0},
		{"another agent now holds the pane id", "w1:p1", nodeAgentList(nodeAgentJSON("w1:p1", "other", "idle", "/x")), spawnLostPaneGone, false, 0},
		{"nothing was launched", "", nodeAgentList(), spawnLostNoPane, false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			herdr := &nodeFakeHerdr{agents: []string{tc.agents}, panes: settlePanes()}
			f := newSettleFixture(t, herdr.run)
			if tc.recordPane != "" {
				f.recordSpawn(t, tc.recordPane)
			}
			f.client.runSpawnSettle(t.Context())
			result := f.stored(t)
			if result == nil || result.RC == 0 || result.Error != "spawn_failed" || result.Retryable != tc.retryable || !strings.Contains(result.StdoutTail, "lost: "+tc.reason) {
				t.Fatalf("settled as %+v", result)
			}
			// The hub drops a job.spawn.result whose error it does not know, so the
			// settled answer must parse on the hub's side too.
			wire, _ := json.Marshal(spawnResultWire(*result, true))
			if parsed, ok := parseHubSpawnResult(wire); !ok || parsed.RequestID != f.orphan.RequestID || parsed.RC == 0 {
				t.Fatalf("hub rejects the settled result %s", wire)
			}
			if len(*f.started) != tc.wantStarted {
				t.Fatalf("watchers armed for a lost spawn: %v", *f.started)
			}
			if rows, err := f.journal.executing(t.Context()); err != nil || len(rows) != 0 {
				t.Fatalf("row still executing: %v %v", rows, err)
			}
		})
	}
}

// A row written before the journal named its job cannot be matched to a pane;
// it ends lost rather than staying executing for good.
func TestSpawnSettleRowWithoutJobEndsLost(t *testing.T) {
	f := newSettleFixture(t, (&nodeFakeHerdr{agents: []string{nodeAgentList()}, panes: settlePanes()}).run)
	if _, err := f.journal.db.ExecContext(t.Context(), `UPDATE spawn_inbox SET job_id='',label=''`); err != nil {
		t.Fatal(err)
	}
	f.client.runSpawnSettle(t.Context())
	if result := f.stored(t); result == nil || result.Error != "spawn_failed" || !strings.Contains(result.StdoutTail, spawnLostNoJobRecorded) {
		t.Fatalf("settled as %+v", result)
	}
}

// herdr not answering decides nothing: the row stays executing and a later
// pass settles it, so no row stays executing forever.
func TestSpawnSettleRetriesWhileHerdrIsDown(t *testing.T) {
	var down atomic.Bool
	down.Store(true)
	herdr := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "label-a", "idle", "/work/a"))}, panes: settlePanes("w1:p1", "label-a")}
	f := newSettleFixture(t, func(ctx context.Context, args ...string) ([]byte, error) {
		if down.Load() {
			return nil, errors.New("herdr down")
		}
		return herdr.run(ctx, args...)
	})
	f.recordSpawn(t, "w1:p1")
	if settled, left := f.client.settleExecutingSpawns(t.Context(), f.journal); settled != 0 || left != 1 || f.stored(t) != nil {
		t.Fatalf("settled=%d left=%d stored=%+v with herdr down", settled, left, f.stored(t))
	}
	down.Store(false)
	if settled, left := f.client.settleExecutingSpawns(t.Context(), f.journal); settled != 1 || left != 0 {
		t.Fatalf("settled=%d left=%d after herdr returned", settled, left)
	}
}

// A request this process is running is never settled from under it.
func TestSpawnSettleLeavesRunningRequestAlone(t *testing.T) {
	f := newSettleFixture(t, (&nodeFakeHerdr{agents: []string{nodeAgentList()}, panes: settlePanes()}).run)
	f.client.spawnActive = map[string]bool{f.orphan.RequestID: true}
	if settled, left := f.client.settleExecutingSpawns(t.Context(), f.journal); settled != 0 || left != 0 || f.stored(t) != nil {
		t.Fatalf("settled=%d left=%d stored=%+v", settled, left, f.stored(t))
	}
}

// A journal created before job_id and label existed gains the columns and
// keeps its rows.
func TestSpawnJournalMigratesOldTable(t *testing.T) {
	root := t.TempDir()
	old, err := openNodeSpawnJournal(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{`DROP TABLE spawn_inbox`, `CREATE TABLE spawn_inbox (request_id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,status TEXT NOT NULL,result TEXT NOT NULL DEFAULT '')`, `INSERT INTO spawn_inbox(request_id,fingerprint,status) VALUES('r','f','executing')`} {
		if _, err := old.db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	old.db.Close()
	journal, err := openNodeSpawnJournal(root)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.db.Close()
	rows, err := journal.executing(t.Context())
	if err != nil || len(rows) != 1 || rows[0].RequestID != "r" || rows[0].Job != "" {
		t.Fatalf("rows %+v err %v", rows, err)
	}
}

// settlePeer is a websocket peer whose received spawn results come out of next.
func settlePeer(t *testing.T) (*hubClientConnection, func() hubSpawnResult) {
	t.Helper()
	results := make(chan hubSpawnResult, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			var result hubSpawnResult
			if wsjson.Read(r.Context(), conn, &result) != nil {
				return
			}
			results <- result
		}
	}))
	t.Cleanup(server.Close)
	conn, _, err := websocket.Dial(t.Context(), r6WSURL(server.URL, ""), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	return &hubClientConnection{connection: conn}, func() hubSpawnResult {
		t.Helper()
		select {
		case result := <-results:
			return result
		case <-time.After(5 * time.Second):
			t.Fatal("node sent no job.spawn.result")
		}
		return hubSpawnResult{}
	}
}

// Two agents bear the label and no record names the pane: absence is not
// proven, so the row stays executing instead of ending lost and retryable.
func TestSpawnSettleAmbiguousLabelStaysExecuting(t *testing.T) {
	herdr := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "label-a", "idle", "/a"), nodeAgentJSON("w1:p2", "label-a", "idle", "/b"))}, panes: settlePanes("w1:p1", "label-a", "w1:p2", "label-a")}
	f := newSettleFixture(t, herdr.run)
	if settled, left := f.client.settleExecutingSpawns(t.Context(), f.journal); settled != 0 || left != 1 || f.stored(t) != nil {
		t.Fatalf("settled=%d left=%d stored=%+v", settled, left, f.stored(t))
	}
}

// settleNamedPanes is a pane list whose rows carry the label under "name".
func settleNamedPanes(idLabel ...string) string {
	return strings.ReplaceAll(settlePanes(idLabel...), `"label"`, `"name"`)
}

// settlePanes is a `herdr pane list` answer: pairs of pane id and label.
func settlePanes(idLabel ...string) string {
	rows := make([]string, 0, len(idLabel)/2)
	for i := 0; i+1 < len(idLabel); i += 2 {
		rows = append(rows, `{"pane_id":"`+idLabel[i]+`","label":"`+idLabel[i+1]+`","agent_status":"","cwd":"/x"}`)
	}
	return `{"result":{"panes":[` + strings.Join(rows, ",") + `]}}`
}

// What herdr shows can fail to prove anything, and then the row stays
// executing for the hub's own pane read instead of ending in a terminal answer.
func TestSpawnSettleUnprovenCasesStayExecuting(t *testing.T) {
	cases := []struct {
		name       string
		recordPane string
		agents     string
		panes      string
	}{
		{"recorded pane is only in the pane list", "w1:p1", nodeAgentList(), settlePanes("w1:p1", "label-a")},
		{"no record, one agent bears the label (a foreign job may own it)", "", nodeAgentList(nodeAgentJSON("w1:p9", "label-a", "idle", "/other")), settlePanes("w1:p9", "label-a")},
		{"no record, a pane bears the label but no agent registered", "", nodeAgentList(), settlePanes("w1:p9", "label-a")},
		{"no record, the pane list carries the label as name", "", nodeAgentList(), settleNamedPanes("w1:p9", "label-a")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newSettleFixture(t, (&nodeFakeHerdr{agents: []string{tc.agents}, panes: tc.panes}).run)
			if tc.recordPane != "" {
				f.recordSpawn(t, tc.recordPane)
			}
			if settled, left := f.client.settleExecutingSpawns(t.Context(), f.journal); settled != 0 || left != 1 || f.stored(t) != nil {
				t.Fatalf("settled=%d left=%d stored=%+v", settled, left, f.stored(t))
			}
		})
	}
}

// An unreadable pane list is as unproven as an unreadable agent list: not
// JSON, no panes array, a null array, or an error envelope is never "no panes".
func TestSpawnSettlePaneListFailureStaysExecuting(t *testing.T) {
	for _, panes := range []string{`not json`, `{"result":{}}`, `{"result":{"panes":null}}`, `{"error":{"code":"x"}}`} {
		f := newSettleFixture(t, (&nodeFakeHerdr{agents: []string{nodeAgentList()}, panes: panes}).run)
		f.recordSpawn(t, "w1:p1")
		if settled, left := f.client.settleExecutingSpawns(t.Context(), f.journal); settled != 0 || left != 1 {
			t.Fatalf("pane list %s: settled=%d left=%d", panes, settled, left)
		}
	}
}

// A landing whose lane registration failed is still reported as one, so the
// owner is told the pane's findings are not routed.
func TestSpawnSettleKeepsLaneUnregistered(t *testing.T) {
	herdr := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "label-a", "working", "/work/a"))}, panes: settlePanes("w1:p1", "label-a")}
	f := newSettleFixture(t, herdr.run)
	dir := filepath.Join(f.jobs, "jobs", "job-a", "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	event, _ := json.Marshal(map[string]any{"kind": "job.spawned", "payload": map[string]any{"pane_id": "w1:p1", "herdr_session": "worker", "host": "machine-a", "profile": "codex-terra", "cwd": "/work/a", "lane_unregistered": true}})
	if err := os.WriteFile(filepath.Join(dir, "01.json"), event, 0600); err != nil {
		t.Fatal(err)
	}
	f.client.runSpawnSettle(t.Context())
	if result := f.stored(t); result == nil || result.Receipt == nil || !result.Receipt.LaneUnregistered {
		t.Fatalf("settled as %+v", result)
	}
}
