package panewire

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// hk 5351: a node's final job.lost ends the job on the hub, and the hub releases
// the task role the job held. The incident was a final job.lost followed by a
// respawn on the same task, refused task_role_claimed.

// t5351Settle is a holder that succeeded on machine-a with its hk job already
// lost (what a node's final job.lost means), a connected node, and the hub's
// job record for test-job owned by machine-a at epoch 1.
func t5351Settle(t *testing.T, hkState string) (*HubServer, *httptest.Server, *websocket.Conn, *roleJobs, hubSpawnTask) {
	t.Helper()
	hub, server, jobs, held := roleHolderHub(t, hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}, hkState, "succeeded")
	node := t14Node(t, hub, server, true)
	hub.mu.Lock()
	hub.jobs["test-job"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "test-job", OwnerLane: "lane-a", Epoch: 1}, Node: "machine-a", FencedNodes: map[string]uint64{}}
	hub.mu.Unlock()
	return hub, server, node, jobs, held
}

// t5351SendJobLost sends one job.lost frame over the node's websocket.
func t5351SendJobLost(t *testing.T, node *websocket.Conn, payload map[string]any) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := wsjson.Write(t.Context(), node, hubClientWireEvent(hubClientEvent{Kind: "job.lost", Payload: raw})); err != nil {
		t.Fatal(err)
	}
}

func t5351FinalLost(epoch uint64) map[string]any {
	return map[string]any{"job_id": "test-job", "epoch": epoch, "owner_lane": "lane-a", "reason": "hub_close", "final": true, "pane_id": roleHeldPane}
}

// t5351JobSettled reports whether the hub settled test-job as lost.
func t5351JobSettled(hub *HubServer) bool {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	job := hub.jobs["test-job"]
	return job != nil && job.Completed && job.LostSettled
}

// t5351Held reports whether a reservation still holds the (task, role) for request.
func t5351Held(t *testing.T, hub *HubServer, request string) bool {
	t.Helper()
	return t5351HeldBy(t, hub, request)
}

// t5351HeldBy reports whether the (task, role) reservation belongs to request.
func t5351HeldBy(t *testing.T, hub *HubServer, request string) bool {
	t.Helper()
	owner, _, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
	return err == nil && owner == request
}

// t5351Reserved reports whether any reservation holds the (task, role).
func t5351Reserved(t *testing.T, hub *HubServer) bool {
	t.Helper()
	_, _, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
	return err == nil
}

// The incident: a node's final job.lost, then a respawn on the same task. The
// settle releases the role, so the respawn is admitted.
func TestJobLostFinalSettlesTheJobAndReleasesItsRole(t *testing.T) {
	hub, server, node, _, _ := t5351Settle(t, "lost")
	t5351SendJobLost(t, node, t5351FinalLost(1))
	eventually(t, 2*time.Second, "the final job.lost settles the job", func() bool { return t5351JobSettled(hub) })
	eventually(t, 2*time.Second, "the settle releases the task role", func() bool { return !t5351Reserved(t, hub) })
	response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	if result := t14Result(t, response); result.status != http.StatusAccepted {
		t.Fatalf("respawn status=%d body=%s", result.status, result.body)
	}
	if !t5351HeldBy(t, hub, roleSecondRequest().RequestID) {
		t.Fatal("the respawn did not take the role")
	}
}

// Admission takes the settle when hk cannot be read: the node's final job.lost
// already proves the record lost, so the hold is not kept on a read error.
func TestJobLostAdmissionTrustsTheSettleWhenHKIsUnreadable(t *testing.T) {
	hub, server, node, jobs, held := t5351Settle(t, "lost")
	store := hub.inbox.store
	if _, err := store.db.Exec(store.q(`INSERT INTO pool_actions(id,job,node,epoch,phase,data,final_ms) VALUES(?,?,?,?,?,?,0)`), "pa-5351-read", "test-job", "machine-a", 1, poolActionPending, "{}"); err != nil {
		t.Fatal(err)
	}
	t5351SendJobLost(t, node, t5351FinalLost(1))
	eventually(t, 2*time.Second, "the final job.lost settles the job", func() bool { return t5351JobSettled(hub) })
	if !t5351HeldBy(t, hub, held.Request.RequestID) {
		t.Fatal("a non-final pool action released the role")
	}
	jobs.getErr = map[string]error{"test-job": errors.New("hk unavailable")}
	if _, err := store.db.Exec(store.q(`UPDATE pool_actions SET phase=? WHERE id=?`), poolActionDone, "pa-5351-read"); err != nil {
		t.Fatal(err)
	}
	response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	if result := t14Result(t, response); result.status != http.StatusAccepted {
		t.Fatalf("respawn with hk unreadable: status=%d body=%s", result.status, result.body)
	}
}

// A pool action that owns the job keeps the role after the settle (I1/I2,
// #669). Admission still refuses until the action is final, then releases the
// role on the settle the hub already holds.
func TestJobLostSettleKeepsRoleWhilePoolActionOwnsJob(t *testing.T) {
	hub, server, node, _, held := t5351Settle(t, "lost")
	store := hub.inbox.store
	if _, err := store.db.Exec(store.q(`INSERT INTO pool_actions(id,job,node,epoch,phase,data,final_ms) VALUES(?,?,?,?,?,?,0)`), "pa-5351", "test-job", "machine-a", 1, poolActionPending, "{}"); err != nil {
		t.Fatal(err)
	}
	t5351SendJobLost(t, node, t5351FinalLost(1))
	eventually(t, 2*time.Second, "the final job.lost settles the job", func() bool { return t5351JobSettled(hub) })
	if !t5351Held(t, hub, held.Request.RequestID) {
		t.Fatal("a non-final pool action released the role")
	}
	response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	if result := t14Result(t, response); result.status != http.StatusConflict || !strings.Contains(string(result.body), "task_role_claimed") {
		t.Fatalf("status=%d body=%s", result.status, result.body)
	}
	if _, err := store.db.Exec(store.q(`UPDATE pool_actions SET phase=? WHERE id=?`), poolActionDone, "pa-5351"); err != nil {
		t.Fatal(err)
	}
	// Admission finds the settle once the action is final and takes the role.
	response = t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	if result := t14Result(t, response); result.status != http.StatusAccepted {
		t.Fatalf("respawn after the pool action ended: status=%d body=%s", result.status, result.body)
	}
	if !t5351HeldBy(t, hub, roleSecondRequest().RequestID) {
		t.Fatal("the respawn did not take the role")
	}
}

// An announced quota stop that is not final (hk still claimed) does not settle:
// the successor pointer may still come (hk 3334).
func TestJobLostAnnouncementDoesNotSettle(t *testing.T) {
	hub, _, node, _, held := t5351Settle(t, "claimed")
	payload := t5351FinalLost(1)
	delete(payload, "final")
	t5351SendJobLost(t, node, payload)
	// Proves nothing more happens: the hub ingests the frame asynchronously, so
	// this window lets a settle that would happen land before the check.
	time.Sleep(300 * time.Millisecond)
	if t5351JobSettled(hub) {
		t.Fatal("a non-final job.lost settled the job")
	}
	if !t5351Held(t, hub, held.Request.RequestID) {
		t.Fatal("a non-final job.lost released the role")
	}
}

// A final job.lost from an older epoch than the hub's current one is fenced.
func TestJobLostFromStaleEpochDoesNotSettle(t *testing.T) {
	hub, _, node, _, held := t5351Settle(t, "lost")
	hub.mu.Lock()
	hub.jobs["test-job"].Epoch = 2
	hub.mu.Unlock()
	t5351SendJobLost(t, node, t5351FinalLost(1))
	// Proves nothing more happens: the stale frame must not settle the job.
	time.Sleep(300 * time.Millisecond)
	if t5351JobSettled(hub) {
		t.Fatal("a stale-epoch final job.lost settled the job")
	}
	if !t5351Held(t, hub, held.Request.RequestID) {
		t.Fatal("a stale-epoch final job.lost released the role")
	}
}

// The node's final flag reaches the hub through the relay, and an older node's
// job.lost (no field) reads as not final.
func TestJobLostFinalFlagCrossesTheRelay(t *testing.T) {
	relay := func(t *testing.T, payload string) hubJobEventPayload {
		t.Helper()
		root := t.TempDir()
		dir := filepath.Join(root, "jobs", "test-job", "events")
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "00001-job.lost.json"), []byte(payload), 0600); err != nil {
			t.Fatal(err)
		}
		events := scanHubRelayEvents(root)
		if len(events) != 1 {
			t.Fatalf("events: %+v", events)
		}
		wire, ok := (&HubClient{}).relayEventForSend(events[0])
		if !ok {
			t.Fatal("job.lost was not queued")
		}
		decoded, valid := decodeHubJobEscalationPayload(wire.Payload)
		if !valid {
			t.Fatalf("invalid hub payload: %s", wire.Payload)
		}
		return decoded
	}
	final := relay(t, `{"kind":"job.lost","payload":{"reason":"hub_close","final":true,"owner_lane":"lane-a","pane_id":"w2:p3"}}`)
	if !final.Final {
		t.Fatal("the node's final flag did not reach the hub")
	}
	older := relay(t, `{"kind":"job.lost","payload":{"reason":"hub_close","owner_lane":"lane-a","pane_id":"w2:p3"}}`)
	if older.Final {
		t.Fatal("an older node's job.lost read as final")
	}
}

// A final job.lost that reaches a hub which has not registered the job yet
// (a restart before the first heartbeat) is late-registered and settled, not
// discarded (mergewake: late registration of final losses).
func TestJobLostBeforeTheHubKnowsTheJobSettlesIt(t *testing.T) {
	hub, server, node, _, _ := t5351Settle(t, "lost")
	hub.mu.Lock()
	delete(hub.jobs, "test-job")
	hub.mu.Unlock()
	payload := t5351FinalLost(1)
	payload["agent_label"] = "test-job"
	t5351SendJobLost(t, node, payload)
	eventually(t, 2*time.Second, "the unregistered final job.lost is settled", func() bool { return t5351JobSettled(hub) })
	eventually(t, 2*time.Second, "the settle releases the task role", func() bool { return !t5351Reserved(t, hub) })
	response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	if result := t14Result(t, response); result.status != http.StatusAccepted {
		t.Fatalf("respawn status=%d body=%s", result.status, result.body)
	}
}

// The lost end is durable: a hub that restarted (the in-memory record is gone)
// still releases the role once the pool action ends, even with hk unreadable
// (mergewake: LostSettled must survive a restart).
func TestJobLostSettleSurvivesRestartWhileActionPending(t *testing.T) {
	for _, seeded := range []bool{true, false} {
		t.Run(map[bool]string{true: "seeded", false: "unseeded"}[seeded], func(t *testing.T) {
			t5351RestartSettle(t, seeded)
		})
	}
}

// t5351RestartSettle settles a job with a pending pool action, restarts the hub
// (the in-memory record is dropped), and checks the respawn is admitted with hk
// unreadable. seeded says whether the owner assignment was on disk before the
// loss; an unseeded job still records its lost end (mergewake: no assignment).
func t5351RestartSettle(t *testing.T, seeded bool) {
	hub, server, node, jobs, _ := t5351Settle(t, "lost")
	hub.ownerTransferDir = t.TempDir()
	if seeded {
		if err := hub.saveOwnerAssignment(hubOwnerAssignment{Job: "test-job", Node: "machine-a", Epoch: 1}, false); err != nil {
			t.Fatal(err)
		}
	}
	store := hub.inbox.store
	if _, err := store.db.Exec(store.q(`INSERT INTO pool_actions(id,job,node,epoch,phase,data,final_ms) VALUES(?,?,?,?,?,?,0)`), "pa-5351-restart", "test-job", "machine-a", 1, poolActionPending, "{}"); err != nil {
		t.Fatal(err)
	}
	t5351SendJobLost(t, node, t5351FinalLost(1))
	eventually(t, 2*time.Second, "the final job.lost settles the job", func() bool { return t5351JobSettled(hub) })
	assignment, found, err := hub.ownerAssignment("test-job")
	if err != nil || !found || !assignment.Ended || !assignment.Lost {
		t.Fatalf("durable assignment %+v found=%v err=%v", assignment, found, err)
	}
	// Restart: the in-memory record is gone, and hk cannot be read.
	hub.mu.Lock()
	delete(hub.jobs, "test-job")
	hub.mu.Unlock()
	jobs.getErr = map[string]error{"test-job": errors.New("hk unavailable")}
	if _, err := store.db.Exec(store.q(`UPDATE pool_actions SET phase=? WHERE id=?`), poolActionDone, "pa-5351-restart"); err != nil {
		t.Fatal(err)
	}
	response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	if result := t14Result(t, response); result.status != http.StatusAccepted {
		t.Fatalf("respawn after restart: status=%d body=%s", result.status, result.body)
	}
}

// A final job.lost is held until the hub's features are known, like a
// lane.event, so it is never written before the hub says whether it takes the
// final field. The scan burst and the immediate emit path both use the one
// predicate (mergewake: the immediate path must hold too).
func TestFinalLostIsHeldUntilTheHubFeaturesAreKnown(t *testing.T) {
	final := hubClientEvent{Kind: "job.lost", Payload: json.RawMessage(`{"job_id":"test-job","final":true}`), finalLost: true}
	if !relayHeldForFeatures(final) || relayHeldForFeatures(hubClientEvent{Kind: "job.completed"}) || !relayHeldForFeatures(hubClientEvent{Kind: "lane.event"}) {
		t.Fatal("the held set is not a final job.lost and a lane.event")
	}
	plain := hubClientEvent{Kind: "job.completed", Payload: json.RawMessage(`{"job_id":"other"}`)}
	write, held := splitRelayBurst([]hubClientEvent{final, plain}, false)
	if len(held) != 1 || held[0].Kind != "job.lost" || len(write) != 1 || write[0].Kind != "job.completed" {
		t.Fatalf("unknown features: write=%+v held=%+v", write, held)
	}
	write, held = splitRelayBurst([]hubClientEvent{final, plain}, true)
	if len(held) != 0 || len(write) != 2 {
		t.Fatalf("known features: write=%d held=%d", len(write), len(held))
	}
}

// The final field goes only to a hub that advertised
// hubFeatureFinalLost; any other hub takes the pre-PR payload.
func TestFinalLostKeepsItsFieldsOnlyForAHubThatAdvertisesIt(t *testing.T) {
	event := hubClientEvent{Kind: "job.lost", Payload: json.RawMessage(`{"job_id":"test-job","epoch":1,"reason":"hub_close","final":true}`), finalLost: true}
	wire := func(peer *hubClientConnection) string {
		t.Helper()
		raw, err := json.Marshal(peer.relayWire(event))
		if err != nil {
			t.Fatal(err)
		}
		var envelope struct {
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			t.Fatal(err)
		}
		return string(envelope.Payload)
	}
	old := wire(&hubClientConnection{})
	if strings.Contains(old, `"final"`) {
		t.Fatalf("a hub without final_lost got the final fields: %s", old)
	}
	if _, valid := decodeHubJobEscalationPayload([]byte(old)); !valid {
		t.Fatalf("the pre-PR payload is not accepted by the decoder: %s", old)
	}
	peer := &hubClientConnection{}
	peer.acceptsFinalLost.Store(true)
	if got := wire(peer); !strings.Contains(got, `"final":true`) {
		t.Fatalf("a hub with final_lost lost the final fields: %s", got)
	}
}

// The hub names final_lost in hub.features, so a node sends the final fields.
func TestHubAdvertisesFinalLost(t *testing.T) {
	if !slices.Contains(hubFeaturesFrame().Features, hubFeatureFinalLost) {
		t.Fatalf("hub.features=%v lacks %s", hubFeaturesFrame().Features, hubFeatureFinalLost)
	}
}

// A final job.lost does not delete routes on its pane: a successor's route
// written on the reused pane survives the loss (hk 5351, the eager deletion is
// cut from this PR).
func TestJobLostLeavesTheLaneRouteOnItsPane(t *testing.T) {
	hub, _, node, _, _ := t5351Settle(t, "lost")
	hub.reportRelayPath = r20LanesFile(t, `{"lanes":{"lane-new":{"machine":"machine-a","pane":"`+roleHeldPane+`"}}}`)
	t5351SendJobLost(t, node, t5351FinalLost(1))
	eventually(t, 2*time.Second, "the final loss settles the job", func() bool { return t5351JobSettled(hub) })
	// Proves nothing more happens: a deletion would follow the settle in the same
	// handler, so this window lets it land before the check.
	time.Sleep(300 * time.Millisecond)
	contents, err := os.ReadFile(hub.reportRelayPath)
	if err != nil || !strings.Contains(string(contents), `"lane-new"`) {
		t.Fatalf("a final loss deleted a route on its pane: %s", contents)
	}
}

// The immediate emit path reads finality from the durable event file it names,
// and never from the request, so a plain emit cannot retire a final row.
func TestLedgerFinalLostReadsTheDurableEventFile(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "jobs", "test-job", "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "00001-job.lost.json"), []byte(`{"kind":"job.lost","payload":{"reason":"hub_close","final":true}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "00002-job.lost.json"), []byte(`{"kind":"job.lost","payload":{"reason":"quota_stop"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	job := func(id string) hubScannedRelayEvent {
		return hubScannedRelayEvent{Kind: "job.lost", HubActiveJob: HubActiveJob{JobID: "test-job", Epoch: 1}, EventID: id}
	}
	if !ledgerFinalLost(root, job("00001-job.lost.json")) {
		t.Fatal("the final file was not read as final")
	}
	if ledgerFinalLost(root, job("00002-job.lost.json")) {
		t.Fatal("a plain file was read as final")
	}
	if ledgerFinalLost(root, job("../00001-job.lost.json")) {
		t.Fatal("an event id with a path was accepted")
	}
}
