package panewire

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type hubInbox struct {
	store   *laneInboxStore
	policy  *deliveryPolicy
	// flushMu serializes one flush pass and owns every field below it;
	// Close takes it to wait out an in-flight pass before closing the store.
	flushMu sync.Mutex
	// failures counts consecutive failed flush passes and nextAttempt holds
	// the backoff gate they set, so a broken sink costs one attempt per
	// growing window instead of one per tick. persistDisabled latches a
	// contract-level rejection (a 400/404 from handoffkeep): digest
	// delivery cannot succeed again this process, so the flush stops
	// entirely — the items themselves stay durable in the store.
	failures        int
	nextAttempt     time.Time
	persistDisabled bool
	// flushFailures counts failed flush passes for monitoring and tests.
	flushFailures uint64
}

const (
	// inboxFlushBackoffBase and Max bound the digest flush's retry rhythm:
	// 1s, 2s, 4s … doubling to a 60s cap. At the cap a permanently broken
	// sink costs one attempt per minute, not several per second.
	inboxFlushBackoffBase = time.Second
	inboxFlushBackoffMax  = time.Minute
)

func defaultHubInboxPath() string {
	root, err := os.UserConfigDir()
	if err != nil {
		root = "."
	}
	return filepath.Join(root, "panewire", "hub-inbox.sqlite3")
}

func newHubInbox(policyPath, storePath string) (*hubInbox, error) {
	if policyPath == "" && storePath == "" {
		return nil, nil
	} // embedded callers may retain the legacy contract
	if policyPath == "" {
		policyPath = "default"
	}
	if storePath == "" {
		return nil, fmt.Errorf("inbox store path is required")
	}
	policy, err := loadDeliveryPolicy(policyPath)
	if err != nil {
		return nil, err
	}
	store, err := openLaneInboxStore(storePath)
	if err != nil {
		return nil, err
	}
	return &hubInbox{store: store, policy: policy}, nil
}

func (h *HubServer) receivingLane(kind string, e hubJobEventPayload) string {
	lane := e.OwnerLane
	if kind == "job.escalate" || kind == "job.joined" || kind == "job.revoked" {
		if r, ok := loadReportRelayRoutes(h.reportRelayPath)[lane]; ok && r.Parent != "" {
			lane = r.Parent
		}
	}
	return lane
}

// receiveInbox returns handled=true only after durable ownership, or on a
// storage error (which must never fall through to pane injection). Direct
// interrupts retain the existing handoffkeep and node receipt machinery.
func (h *HubServer) receiveInbox(kind string, event *hubJobEventPayload, sender *hubAgent, replay bool) (result relayLaneEventResult, handled bool) {
	if h.inbox == nil || event.sinkOnly {
		return result, false
	}
	lane := h.receivingLane(kind, *event)
	class := h.inbox.policy.classify(lane, kind, *event, replay)
	deadline, window := h.inbox.policy.timing(lane, class)
	if event.noDigest || chatRelayEventPattern.MatchString(event.EventID) {
		window = 0
	}
	item, err := h.inbox.store.put(relayEventDedupeKey(kind, *event), lane, kind, class, *event, h.now().UTC(), deadline, window)
	if err != nil {
		h.logger.Error("inbox persistence failed; event remains unacknowledged", "kind", kind, "lane", lane)
		return relayLaneEventResult{PersistFailed: true}, true
	}
	// The durable decision wins over changed config or a changed retry body.
	event.DeliveryClass = item.Class
	if item.Class != "inbox" && item.DigestAt == 0 {
		return result, false
	}
	if sender != nil {
		if kind == "lane.event" {
			h.queueLaneRelayPersisted(*event, sender, item.ID)
		} else {
			h.queueRelayPersisted(kind, *event, sender, item.ID)
		}
	}
	return relayLaneEventResult{ID: item.ID, Routed: true, Machine: "inbox", Duplicate: !item.Fresh}, true
}

// replayInbox runs before either the ordinary replay or the once-only
// unconfirmed replay can claim an injection. Old handoffkeep rows are migrated
// to the inbox before being retired; a failed retire is harmless and retried.
func (h *HubServer) replayInbox(record handoffkeepRelayEvent) bool {
	if h.inbox == nil || record.DeliveredAt != "" || isUnconfirmedMarkRecord(record) {
		return false
	}
	event := relayEventFromRecord(record)
	result, handled := h.receiveInbox(record.Kind, &event, nil, true)
	if !handled {
		return false
	}
	if !result.PersistFailed && h.handoffkeep != nil {
		if err := h.handoffkeep.markDelivered(context.Background(), record.ID, "hub", "inbox"); err != nil {
			h.logger.Warn("inbox replay row retirement deferred", "event_id", record.ID)
		}
	}
	return true
}

func (h *HubServer) runInbox(ctx context.Context) {
	if h.inbox == nil {
		return
	}
	h.probeInboxRelayContract(ctx)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.closeCtx.Done():
			return
		case <-ticker.C:
			h.flushInbox()
		}
	}
}

// probeInboxRelayContract checks once, before the first flush, whether the
// configured handoffkeep can serve lane.event relay traffic (its schema v7
// contract). A contract-level rejection latches digest delivery off for this
// process with a single loud line: the hub then delivers normally and the
// stored items wait for a fixed handoffkeep plus a restart. A transient
// failure leaves the flush to discover capability on its own bounded
// schedule, so a handoffkeep that is merely down at boot is not condemned.
func (h *HubServer) probeInboxRelayContract(ctx context.Context) {
	if h.handoffkeep == nil {
		return
	}
	status, err := h.handoffkeep.checkLaneEventContract(ctx)
	if err != nil || !handoffkeepContractError(status) {
		return
	}
	h.inbox.flushMu.Lock()
	h.inbox.persistDisabled = true
	h.inbox.flushMu.Unlock()
	h.logger.Error("inbox digest delivery disabled: handoffkeep lacks the lane.event relay contract (schema v7); upgrade handoffkeep and restart the hub to enable digests", "status", status)
}

// noteInboxFlushResult applies the bounded retry schedule at the end of a
// flush pass: the caller holds flushMu. A clean pass reopens the gate at
// once; a failed one logs once, doubles the delay to the cap, and sets the
// earliest time another pass may start.
func (h *HubServer) noteInboxFlushResult(failed bool) {
	if !failed {
		h.inbox.failures = 0
		h.inbox.nextAttempt = time.Time{}
		return
	}
	h.inbox.flushFailures++
	h.inbox.failures++
	delay := inboxFlushBackoffBase << min(h.inbox.failures-1, 6)
	if delay > inboxFlushBackoffMax {
		delay = inboxFlushBackoffMax
	}
	h.inbox.nextAttempt = h.now().Add(delay)
}

// InboxFlushFailureCount exists for local monitoring and tests.
func (h *HubServer) InboxFlushFailureCount() uint64 {
	if h.inbox == nil {
		return 0
	}
	h.inbox.flushMu.Lock()
	defer h.inbox.flushMu.Unlock()
	return h.inbox.flushFailures
}

// inboxDigestDisabled reports whether a contract rejection has latched digest
// delivery off for this process.
func (h *HubServer) inboxDigestDisabled() bool {
	if h.inbox == nil {
		return false
	}
	h.inbox.flushMu.Lock()
	defer h.inbox.flushMu.Unlock()
	return h.inbox.persistDisabled
}

func (h *HubServer) flushInbox() {
	if h.inbox == nil || !h.inbox.flushMu.TryLock() {
		return
	}
	defer h.inbox.flushMu.Unlock()
	if h.inbox.persistDisabled || h.closeCtx.Err() != nil || h.now().Before(h.inbox.nextAttempt) {
		return
	}
	failed := false
	defer func() { h.noteInboxFlushResult(failed) }()
	if err := h.inbox.store.makeBatches(h.now().UTC()); err != nil {
		h.logger.Error("inbox batching failed")
		failed = true
		return
	}
	batches, err := h.inbox.store.pendingBatches()
	if err != nil {
		h.logger.Error("inbox batch listing failed")
		failed = true
		return
	}
	for _, b := range batches {
		if h.closeCtx.Err() != nil {
			return
		}
		event := hubJobEventPayload{JobID: laneEventTransportID(b.Lane, b.ID), Epoch: 1, OwnerLane: b.Lane, EventID: b.ID, Text: b.Text, DeliveryClass: "interrupt", DeliverPolicy: "now", noDigest: true}
		result := h.relayLaneEventContext(h.closeCtx, event, nil)
		// Handoffkeep now owns the stable batch identity, including an unrouted
		// batch. Its usual replay owns retries from here, without re-escalating.
		if result.ID > 0 && !result.PersistFailed {
			if _, err := h.inbox.store.db.Exec(`UPDATE lane_inbox_batches SET sent=1 WHERE id=?`, b.ID); err != nil {
				h.logger.Error("inbox batch handoff recording failed")
			}
			continue
		}
		if !result.PersistFailed {
			continue
		}
		failed = true
		if handoffkeepContractError(result.PersistStatus) {
			h.inbox.persistDisabled = true
			h.logger.Error("inbox digest delivery disabled: handoffkeep rejected the lane.event relay contract (schema v7); upgrade handoffkeep and restart the hub to enable digests", "status", result.PersistStatus)
		}
		// One failed persistence attempt ends the pass: whatever made this
		// batch fail applies to the rest, so the pass — and its one error —
		// happens at most once per backoff window.
		return
	}
}

func (h *HubServer) authorizeInbox(w http.ResponseWriter, r *http.Request) bool {
	if !h.authorizeOperator(r) {
		machine, ok := h.authorizeAgent(r)
		lane := r.URL.Query().Get("lane")
		route, exists := loadReportRelayRoutes(h.reportRelayPath)[lane]
		if !ok || lane == "" || !exists || route.Machine != machine {
			hubUnauthorized(w)
			return false
		}
	}
	if h.inbox == nil {
		writeHubJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "inbox_disabled"})
		return false
	}
	return true
}

func (h *HubServer) handleInboxList(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeInbox(w, r) {
		return
	}
	lane := r.URL.Query().Get("lane")
	items, err := h.inbox.store.list(lane, r.URL.Query().Get("unread") == "true")
	if err != nil {
		writeHubJSON(w, 503, map[string]string{"error": "inbox_unavailable"})
		return
	}
	writeHubJSON(w, 200, items)
}
func (h *HubServer) handleInboxCount(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeInbox(w, r) {
		return
	}
	counts, err := h.inbox.store.counts(r.URL.Query().Get("lane"))
	if err != nil {
		writeHubJSON(w, 503, map[string]string{"error": "inbox_unavailable"})
		return
	}
	writeHubJSON(w, 200, counts)
}
func (h *HubServer) handleInboxRead(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeInbox(w, r) {
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeHubJSON(w, 400, map[string]string{"error": "invalid_id"})
		return
	}
	item, err := h.inbox.store.read(id, r.URL.Query().Get("lane"), h.now().UTC())
	if errors.Is(err, sql.ErrNoRows) {
		writeHubJSON(w, 404, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		writeHubJSON(w, 503, map[string]string{"error": "inbox_unavailable"})
		return
	}
	writeHubJSON(w, 200, item)
}
func (h *HubServer) handleInboxAck(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeInbox(w, r) {
		return
	}
	var b struct {
		ID   int64  `json:"id"`
		All  bool   `json:"all"`
		Lane string `json:"lane"`
	}
	if !decodeHubJSON(w, r, 4096, &b) || (!b.All && b.ID < 1) || (b.All && b.ID != 0) {
		writeHubJSON(w, 400, map[string]string{"error": "invalid_request"})
		return
	}
	if lane := r.URL.Query().Get("lane"); lane != "" {
		if b.Lane != "" && b.Lane != lane {
			writeHubJSON(w, 400, map[string]string{"error": "lane_mismatch"})
			return
		}
		b.Lane = lane
	}
	n, err := h.inbox.store.ack(b.ID, b.Lane, b.All, h.now().UTC())
	if err != nil {
		writeHubJSON(w, 503, map[string]string{"error": "inbox_unavailable"})
		return
	}
	writeHubJSON(w, 200, map[string]int64{"acked": n})
}
