package panewire

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// hk 2887: the hub releases a node's own lane rows when its fresh session
// snapshot no longer lists their panes, reuses those rows at the node cap, and
// warns (rate-limited) about relay that has no route.

type laneReleaseClock struct{ at time.Time }

func (c *laneReleaseClock) now() time.Time { return c.at }

// laneReleaseHub builds a hub whose clock the test moves and whose log lines
// land in the returned buffer.
func laneReleaseHub(t *testing.T, lanes string, mutate func(*HubServerConfig)) (*HubServer, *laneReleaseClock, string, *bytes.Buffer) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, lanes)
	clock := &laneReleaseClock{at: time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)}
	logs := &bytes.Buffer{}
	hub := lanesNodeHub(t, path, func(config *HubServerConfig) {
		config.Now = clock.now
		config.Logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
		if mutate != nil {
			mutate(config)
		}
	})
	return hub, clock, path, logs
}

// laneReleaseNode installs a connected node whose latest heartbeat lists panes.
func laneReleaseNode(h *HubServer, machine string, receivedAt time.Time, panes ...string) {
	sessions := make([]HubSession, 0, len(panes))
	for _, pane := range panes {
		sessions = append(sessions, HubSession{PaneID: pane, Status: "idle"})
	}
	h.mu.Lock()
	h.nodes[machine] = &hubNodeRecord{machineID: machine, state: "connected", sessionSnapshot: &hubSessionSnapshotRecord{sessions: sessions, snapshotStatus: hubSnapshotStatusOK, receivedAt: receivedAt}}
	h.mu.Unlock()
}

func laneReleaseLanes(t *testing.T, path string) []string {
	t.Helper()
	var names []string
	for lane := range lanesNodeFile(t, path) {
		names = append(names, lane)
	}
	sort.Strings(names)
	return names
}

const laneReleaseCapFile = `{"lanes":{
	"dead-1":{"machine":"machine-a","pane":"w1:p1"},
	"dead-2":{"machine":"machine-a","pane":"w1:p2"},
	"live-1":{"machine":"machine-a","pane":"w1:p3"},
	"other-1":{"machine":"machine-b","pane":"w9:p1"}}}`

func TestLaneReleaseSweepDropsDeadRowsAfterGraceAndKeepsLiveOnes(t *testing.T) {
	hub, clock, path, _ := laneReleaseHub(t, laneReleaseCapFile, nil)
	// machine-b has no snapshot at all, so its row must survive every sweep.
	step := func(advance time.Duration) []string {
		clock.at = clock.at.Add(advance)
		laneReleaseNode(hub, "machine-a", clock.at, "w1:p3")
		return hub.releaseDeadLanes()
	}

	if got := step(0); got != nil {
		t.Fatalf("first sighting released %v; absence must outlast the grace", got)
	}
	if got := step(laneReleaseGrace - time.Second); got != nil {
		t.Fatalf("released %v before the grace elapsed", got)
	}
	got := step(time.Second)
	if want := []string{"dead-1", "dead-2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released %v after the grace, want %v", got, want)
	}
	if want := []string{"live-1", "other-1"}; !reflect.DeepEqual(laneReleaseLanes(t, path), want) {
		t.Fatalf("lanes file keeps %v, want %v", laneReleaseLanes(t, path), want)
	}
}

func TestLaneReleaseSweepResetsTheGraceWhenThePaneReturns(t *testing.T) {
	hub, clock, path, _ := laneReleaseHub(t, `{"lanes":{"flap":{"machine":"machine-a","pane":"w1:p1"}}}`, nil)
	sweep := func(advance time.Duration, panes ...string) []string {
		clock.at = clock.at.Add(advance)
		laneReleaseNode(hub, "machine-a", clock.at, panes...)
		return hub.releaseDeadLanes()
	}
	sweep(0)
	sweep(laneReleaseGrace - time.Second)
	sweep(time.Second, "w1:p1")
	sweep(time.Second) // absent again: a new clock starts here
	if got := sweep(laneReleaseGrace - time.Second); got != nil {
		t.Fatalf("a pane that came back still released %v on the old clock", got)
	}
	if got := sweep(time.Second); !reflect.DeepEqual(got, []string{"flap"}) {
		t.Fatalf("released %v after a full new grace, want [flap]", got)
	}
	if len(laneReleaseLanes(t, path)) != 0 {
		t.Fatalf("lanes file still has %v", laneReleaseLanes(t, path))
	}
}

func TestLaneReleaseSweepDropsAFinishedJobsRowWithoutWaiting(t *testing.T) {
	hub, clock, _, _ := laneReleaseHub(t, `{"lanes":{
		"done-job":{"machine":"machine-a","pane":"w1:p1"},
		"busy-job":{"machine":"machine-a","pane":"w1:p2"},
		"shared":{"machine":"machine-a","pane":"w1:p3"}}}`, nil)
	hub.mu.Lock()
	hub.jobs["done-job"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "done-job", Pane: "w1:p1"}, Node: "machine-a", Completed: true}
	hub.jobs["busy-job"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "busy-job", Pane: "w1:p2"}, Node: "machine-a"}
	hub.jobs["old-job"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "old-job", Pane: "w1:p3"}, Node: "machine-a", Completed: true}
	hub.jobs["new-job"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "new-job", Pane: "w1:p3"}, Node: "machine-a"}
	hub.mu.Unlock()
	laneReleaseNode(hub, "machine-a", clock.at)

	if got := hub.releaseDeadLanes(); !reflect.DeepEqual(got, []string{"done-job"}) {
		t.Fatalf("released %v, want only the lane whose pane holds nothing but finished jobs", got)
	}
}

func TestLaneReleaseSweepKeepsProtectedAndForeignReferencedRows(t *testing.T) {
	hub, clock, path, _ := laneReleaseHub(t, `{"lanes":{
		"guarded":{"machine":"machine-a","pane":"w1:p1","protected":true},
		"parent-1":{"machine":"machine-a","pane":"w1:p2"},
		"child-1":{"machine":"machine-b","pane":"w9:p1","parent":"parent-1"}}}`, nil)
	for _, advance := range []time.Duration{0, laneReleaseGrace} {
		clock.at = clock.at.Add(advance)
		laneReleaseNode(hub, "machine-a", clock.at)
		if got := hub.releaseDeadLanes(); got != nil {
			t.Fatalf("released %v; protected rows and rows another machine routes through stay", got)
		}
	}
	if len(laneReleaseLanes(t, path)) != 3 {
		t.Fatalf("lanes file changed: %v", laneReleaseLanes(t, path))
	}
}

// Unknown is never dead: no sweep or eviction may read a snapshot the hub
// cannot vouch for as the node's complete, current pane list. Each setup
// leaves machine-a with the given state at time now.
var laneReleaseUntrusted = []struct {
	name  string
	setup func(h *HubServer, now time.Time)
}{
	{"no node record", func(h *HubServer, now time.Time) {}},
	{"no snapshot yet", laneReleaseBroken(func(n *hubNodeRecord, now time.Time) { n.sessionSnapshot = nil })},
	{"older than the freshness bound", laneReleaseBroken(func(n *hubNodeRecord, now time.Time) {
		n.sessionSnapshot.receivedAt = now.Add(-sessionsFindSnapshotMaxAge - time.Second)
	})},
	{"from the future", laneReleaseBroken(func(n *hubNodeRecord, now time.Time) { n.sessionSnapshot.receivedAt = now.Add(time.Second) })},
	{"snapshot unavailable", laneReleaseBroken(func(n *hubNodeRecord, now time.Time) { n.sessionSnapshot.snapshotStatus = hubSnapshotStatusUnavailable })},
	{"truncated", laneReleaseBroken(func(n *hubNodeRecord, now time.Time) { n.sessionSnapshot.truncated = true })},
	{"node stale", laneReleaseBroken(func(n *hubNodeRecord, now time.Time) { n.state = "stale" })},
	{"node disconnected", laneReleaseBroken(func(n *hubNodeRecord, now time.Time) { n.state = "disconnected" })},
}

func laneReleaseBroken(mutate func(*hubNodeRecord, time.Time)) func(*HubServer, time.Time) {
	return func(h *HubServer, now time.Time) {
		laneReleaseNode(h, "machine-a", now, "w1:p3")
		h.mu.Lock()
		mutate(h.nodes["machine-a"], now)
		h.mu.Unlock()
	}
}

func TestLaneReleaseStaleOrUnknownSnapshotReleasesNothing(t *testing.T) {
	for _, untrusted := range laneReleaseUntrusted {
		t.Run("sweep/"+untrusted.name, func(t *testing.T) {
			hub, clock, path, _ := laneReleaseHub(t, laneReleaseCapFile, nil)
			before, _ := os.ReadFile(path)
			// Several graces' worth of sweeps in an untrusted state must not
			// release anything or start an absence clock.
			for _, advance := range []time.Duration{0, laneReleaseGrace, laneReleaseGrace} {
				clock.at = clock.at.Add(advance)
				untrusted.setup(hub, clock.at)
				if got := hub.releaseDeadLanes(); got != nil {
					t.Fatalf("released %v from an untrusted snapshot", got)
				}
			}
			if after, _ := os.ReadFile(path); !bytes.Equal(before, after) {
				t.Fatalf("lanes file changed: %s", after)
			}
		})
		t.Run("cap/"+untrusted.name, func(t *testing.T) {
			hub, clock, path, _ := laneReleaseHub(t, laneReleaseCapFile, func(config *HubServerConfig) { config.LanesNodeRowCap = 3 })
			untrusted.setup(hub, clock.at)
			before, _ := os.ReadFile(path)
			writer := lanesNodePut(t, hub, "fresh-1", `{"machine":"machine-a","pane":"w1:p4"}`)
			if writer.Code != http.StatusTooManyRequests {
				t.Fatalf("status=%d body=%s; an untrusted snapshot must evict nothing", writer.Code, writer.Body.String())
			}
			if after, _ := os.ReadFile(path); !bytes.Equal(before, after) {
				t.Fatalf("refused put changed the lanes file: %s", after)
			}
		})
	}
}

func TestLaneReleasePutAtTheCapEvictsOnlyTheCallersDeadRows(t *testing.T) {
	hub, clock, path, logs := laneReleaseHub(t, laneReleaseCapFile, func(config *HubServerConfig) { config.LanesNodeRowCap = 3 })
	// machine-b's pane is absent from machine-b's own fresh snapshot too: a
	// foreign node's dead-looking row is still not the caller's to evict.
	laneReleaseNode(hub, "machine-a", clock.at, "w1:p3")
	laneReleaseNode(hub, "machine-b", clock.at)
	subscriber := lanesWriteSubscriber(t, hub)

	writer := lanesNodePut(t, hub, "fresh-1", `{"machine":"machine-a","pane":"w1:p4"}`)
	if writer.Code != http.StatusCreated {
		t.Fatalf("put at the cap status=%d body=%s; a dead row should have been reused", writer.Code, writer.Body.String())
	}
	if want := []string{"dead-2", "fresh-1", "live-1", "other-1"}; !reflect.DeepEqual(laneReleaseLanes(t, path), want) {
		t.Fatalf("lanes after put = %v, want %v (exactly one dead row evicted, lowest name first)", laneReleaseLanes(t, path), want)
	}
	if event := lanesWriteEvent(t, subscriber); event.Kind != "lanes.changed" || !strings.Contains(string(event.Payload), `"dead-1"`) || !strings.Contains(string(event.Payload), `"remove"`) {
		t.Fatalf("first event = %s %s, want a remove of dead-1", event.Kind, event.Payload)
	}
	if !strings.Contains(logs.String(), "lane released") || !strings.Contains(logs.String(), "dead-1") {
		t.Fatalf("eviction not logged: %s", logs.String())
	}

	// The node is back at the cap with one dead row left: one more fits, the
	// next finds nothing to reuse and is refused.
	if writer := lanesNodePut(t, hub, "fresh-2", `{"machine":"machine-a","pane":"w1:p5"}`); writer.Code != http.StatusCreated {
		t.Fatalf("second put status=%d body=%s", writer.Code, writer.Body.String())
	}
	before, _ := os.ReadFile(path)
	if writer := lanesNodePut(t, hub, "fresh-3", `{"machine":"machine-a","pane":"w1:p6"}`); writer.Code != http.StatusTooManyRequests {
		t.Fatalf("put with no dead rows status=%d body=%s, want 429", writer.Code, writer.Body.String())
	}
	if after, _ := os.ReadFile(path); !bytes.Equal(before, after) {
		t.Fatalf("refused put changed the lanes file: %s", after)
	}
}

func TestLaneReleasePutLeavesAnotherNodesRowsAloneWhenOnlyTheyLookDead(t *testing.T) {
	hub, clock, path, _ := laneReleaseHub(t, `{"lanes":{
		"a-live-1":{"machine":"machine-a","pane":"w1:p1"},
		"a-live-2":{"machine":"machine-a","pane":"w1:p2"},
		"b-gone":{"machine":"machine-b","pane":"w9:p9"}}}`, func(config *HubServerConfig) { config.LanesNodeRowCap = 2 })
	laneReleaseNode(hub, "machine-a", clock.at, "w1:p1", "w1:p2")
	laneReleaseNode(hub, "machine-b", clock.at)
	before, _ := os.ReadFile(path)

	if writer := lanesNodePut(t, hub, "a-new", `{"machine":"machine-a","pane":"w1:p3"}`); writer.Code != http.StatusTooManyRequests {
		t.Fatalf("status=%d body=%s; the caller has no dead rows, so machine-b's must not be evicted", writer.Code, writer.Body.String())
	}
	if after, _ := os.ReadFile(path); !bytes.Equal(before, after) {
		t.Fatalf("lanes file changed: %s", after)
	}
}

func TestLaneReleasePutNeedsEnoughDeadRowsToGetUnderAnOverfullCap(t *testing.T) {
	hub, clock, path, _ := laneReleaseHub(t, laneReleaseCapFile, func(config *HubServerConfig) { config.LanesNodeRowCap = 2 })
	// Three rows against a cap of two: a create needs two evictions. With
	// one dead row it is refused and nothing changes.
	laneReleaseNode(hub, "machine-a", clock.at, "w1:p2", "w1:p3")
	before, _ := os.ReadFile(path)
	if writer := lanesNodePut(t, hub, "fresh-1", `{"machine":"machine-a","pane":"w1:p4"}`); writer.Code != http.StatusTooManyRequests {
		t.Fatalf("status=%d body=%s; one dead row cannot bring a node two over the cap back under it", writer.Code, writer.Body.String())
	}
	if after, _ := os.ReadFile(path); !bytes.Equal(before, after) {
		t.Fatalf("partial eviction changed the lanes file: %s", after)
	}
	laneReleaseNode(hub, "machine-a", clock.at, "w1:p3")
	if writer := lanesNodePut(t, hub, "fresh-1", `{"machine":"machine-a","pane":"w1:p4"}`); writer.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s; two dead rows should be enough", writer.Code, writer.Body.String())
	}
	if want := []string{"fresh-1", "live-1", "other-1"}; !reflect.DeepEqual(laneReleaseLanes(t, path), want) {
		t.Fatalf("lanes after put = %v, want %v", laneReleaseLanes(t, path), want)
	}
}

// A snapshot collected just before a PUT cannot list the pane that PUT
// registered, so that row is not dead until a later snapshot also omits it.
func TestLaneReleasePutDoesNotEvictARowTheHubJustWrote(t *testing.T) {
	hub, clock, path, _ := laneReleaseHub(t, `{"lanes":{"seed":{"machine":"machine-a","pane":"w1:p1"}}}`, func(config *HubServerConfig) { config.LanesNodeRowCap = 2 })
	laneReleaseNode(hub, "machine-a", clock.at, "w1:p1")
	if writer := lanesNodePut(t, hub, "newborn", `{"machine":"machine-a","pane":"w1:p2"}`); writer.Code != http.StatusCreated {
		t.Fatalf("seed put status=%d body=%s", writer.Code, writer.Body.String())
	}
	// seed's pane is gone and newborn's is not yet listed; the snapshot is
	// older than newborn's write plus the settle.
	clock.at = clock.at.Add(time.Second)
	laneReleaseNode(hub, "machine-a", clock.at)
	if writer := lanesNodePut(t, hub, "third", `{"machine":"machine-a","pane":"w1:p3"}`); writer.Code != http.StatusCreated {
		t.Fatalf("put status=%d body=%s; the dead seed row should be reused", writer.Code, writer.Body.String())
	}
	if want := []string{"newborn", "third"}; !reflect.DeepEqual(laneReleaseLanes(t, path), want) {
		t.Fatalf("lanes after put = %v, want %v", laneReleaseLanes(t, path), want)
	}
	// Once a snapshot received well after the write still omits newborn's
	// pane, it is dead like any other.
	clock.at = clock.at.Add(laneWriteSettle + time.Second)
	laneReleaseNode(hub, "machine-a", clock.at)
	if writer := lanesNodePut(t, hub, "fourth", `{"machine":"machine-a","pane":"w1:p4"}`); writer.Code != http.StatusCreated {
		t.Fatalf("put status=%d body=%s", writer.Code, writer.Body.String())
	}
}

func TestLaneReleaseClosedPaneRemovesItsRowAndNoOneElses(t *testing.T) {
	const lanes = `{"lanes":{
		"job-a":{"machine":"machine-a","pane":"w1:p1"},
		"job-a2":{"machine":"machine-a","pane":"w1:p2"},
		"job-b":{"machine":"machine-b","pane":"w1:p1"}}}`
	closeArgs := nodePaneCloseArgs{PaneID: "w1:p1", Name: "job-a"}
	result := func(outcome string) hubNodeCommandResult {
		raw, _ := json.Marshal(nodePaneCloseResult{Outcome: outcome})
		return hubNodeCommandResult{Op: nodeCommandPaneClose, Result: raw}
	}
	for outcome, want := range map[string][]string{
		nodePaneClosed:  {"job-a2", "job-b"},
		nodePaneGone:    {"job-a2", "job-b"},
		nodePaneChanged: {"job-a", "job-a2", "job-b"},
	} {
		t.Run(outcome, func(t *testing.T) {
			hub, clock, path, _ := laneReleaseHub(t, lanes, nil)
			hub.releaseClosedPaneLanes("machine-a", closeArgs, result(outcome), clock.at)
			if got := laneReleaseLanes(t, path); !reflect.DeepEqual(got, want) {
				t.Fatalf("lanes after %s = %v, want %v", outcome, got, want)
			}
		})
	}
	t.Run("failed command", func(t *testing.T) {
		hub, clock, path, _ := laneReleaseHub(t, lanes, nil)
		failed := result(nodePaneClosed)
		failed.Error = "node_timeout"
		hub.releaseClosedPaneLanes("machine-a", closeArgs, failed, clock.at)
		if got := laneReleaseLanes(t, path); len(got) != 3 {
			t.Fatalf("lanes after a failed close = %v", got)
		}
	})
	t.Run("other op", func(t *testing.T) {
		hub, clock, path, _ := laneReleaseHub(t, lanes, nil)
		hub.releaseClosedPaneLanes("machine-a", nodePaneReadArgs{PaneID: "w1:p1"}, result(nodePaneClosed), clock.at)
		if got := laneReleaseLanes(t, path); len(got) != 3 {
			t.Fatalf("lanes after a non-close op = %v", got)
		}
	})
	t.Run("row registered after the close began", func(t *testing.T) {
		hub, clock, path, _ := laneReleaseHub(t, lanes, nil)
		started := clock.at
		clock.at = clock.at.Add(time.Second)
		hub.laneLife.noteWrite("job-a", clock.at)
		hub.releaseClosedPaneLanes("machine-a", closeArgs, result(nodePaneClosed), started)
		if got := laneReleaseLanes(t, path); len(got) != 3 {
			t.Fatalf("a row written after the close began was released: %v", got)
		}
	})
}

func TestLaneReleaseUnroutedRelayWarnsOncePerIntervalPerLane(t *testing.T) {
	hub, clock, _, logs := laneReleaseHub(t, `{"lanes":{}}`, nil)
	warns := func() []string {
		var lines []string
		for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
			if strings.Contains(line, "level=WARN") && strings.Contains(line, "relay unrouted") {
				lines = append(lines, line)
			}
		}
		return lines
	}
	event := func(lane, id string) hubJobEventPayload { return hubJobEventPayload{OwnerLane: lane, EventID: id} }

	hub.broadcastRelayUnrouted(event("pw-orphan", "evt-1"))
	for _, id := range []string{"evt-2", "evt-3"} {
		clock.at = clock.at.Add(time.Second)
		hub.broadcastRelayUnrouted(event("pw-orphan", id))
	}
	hub.broadcastRelayUnrouted(event("pw-other", "evt-9"))
	got := warns()
	if len(got) != 2 {
		t.Fatalf("%d warnings for one burst on two lanes, want 2:\n%s", len(got), strings.Join(got, "\n"))
	}
	if !strings.Contains(got[0], "lane=pw-orphan") || !strings.Contains(got[0], "event_id=evt-1") || !strings.Contains(got[1], "lane=pw-other") {
		t.Fatalf("warnings lack lane or event id:\n%s", strings.Join(got, "\n"))
	}

	clock.at = clock.at.Add(unroutedWarnInterval)
	hub.broadcastRelayUnrouted(event("pw-orphan", "evt-4"))
	got = warns()
	if len(got) != 3 || !strings.Contains(got[2], "event_id=evt-4") || !strings.Contains(got[2], "suppressed=2") {
		t.Fatalf("after the interval want a third warning for evt-4 reporting suppressed=2, got:\n%s", strings.Join(got, "\n"))
	}
}
