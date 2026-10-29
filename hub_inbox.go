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
	store  *laneInboxStore
	policy *deliveryPolicy
	// waiters bounds concurrent /v1/inbox/wait long-polls. Acquired before
	// the handler loop and released when it returns.
	waiters chan struct{}
	// flushMu serializes one flush pass and owns every field below it;
	// Close takes it to wait out an in-flight pass before closing the store.
	flushMu sync.Mutex
	// failures counts consecutive failed flush passes and nextAttempt holds
	// the backoff gate they set, so a broken sink costs one attempt per
	// growing window instead of one per tick. Only startup checks can latch
	// persistDisabled; an individual bad batch is quarantined.
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

	// hubInboxWaitMaxWait bounds one /v1/inbox/wait long-poll; the CLI
	// re-issues the request for longer --timeout values. hubInboxWaitMaxWaiters
	// bounds concurrent waiters so stalled readers cannot pin hub connections.
	hubInboxWaitMaxWait    = 60 * time.Second
	hubInboxWaitMaxWaiters = 64
)

func defaultHubInboxPath() string {
	root, err := os.UserConfigDir()
	if err != nil {
		root = "."
	}
	return filepath.Join(root, "panewire", "hub-inbox.sqlite3")
}

func newHubInbox(policyPath, storePath string) (*hubInbox, error) {
	if policyPath == "off" || storePath == "" {
		return nil, nil
	} // The kill switch never opens or modifies the inbox database.
	if policyPath == "" {
		policyPath = "default"
	}
	policy, err := loadDeliveryPolicy(policyPath)
	if err != nil {
		return nil, err
	}
	store, err := openLaneInboxStore(storePath)
	if err != nil {
		return nil, err
	}
	return &hubInbox{store: store, policy: policy, waiters: make(chan struct{}, hubInboxWaitMaxWaiters)}, nil
}

// prepareInbox runs once before the hub starts accepting events. Maintenance
// failures disable only digests; ordinary relay delivery, especially must-act
// interrupts, must remain available while the store needs repair.
func (h *HubServer) prepareInbox() {
	if h.inbox == nil {
		return
	}
	err := h.inbox.store.quarantineInvalidBatches()
	if err == nil {
		err = h.inbox.store.recoverMustAct(h.inbox.policy)
	}
	if err != nil {
		h.inbox.persistDisabled = true
		h.logger.Error("inbox digest delivery disabled: startup recovery failed; fix the inbox store and restart the hub", "error", err)
	}
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
// storage error for an inbox-class event. Interrupts fail open to the existing
// handoffkeep and node receipt machinery.
func (h *HubServer) receiveInbox(kind string, event *hubJobEventPayload, sender *hubAgent, replay bool) (result relayLaneEventResult, handled bool) {
	if h.inbox == nil || event.sinkOnly {
		return result, false
	}
	lane := h.receivingLane(kind, *event)
	class := h.inbox.policy.classify(lane, kind, *event, replay)
	if class == "inbox" && h.inboxImmediate(lane, kind) {
		class = "interrupt"
	}
	deadline, window := h.inbox.policy.timing(lane, class)
	if event.noDigest || h.inbox.policy.mustAct(lane, *event) || chatRelayEventPattern.MatchString(event.EventID) {
		window = 0
	}
	item, err := h.inbox.store.put(relayEventDedupeKey(kind, *event), lane, kind, class, *event, h.now().UTC(), deadline, window)
	if err != nil {
		if class == "interrupt" {
			event.DeliveryClass = class
			h.logger.Error("interrupt inbox persistence failed; continuing relay delivery", "kind", kind, "lane", lane)
			return result, false
		}
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

// receiverInboxState is the hub's view of the receiving lane's pane, taken
// from the lanes file and the node's latest heartbeat session snapshot.
// supported is true only when a fresh, complete snapshot reports the pane's
// CLI and the agents: policy marks that CLI supported; idle is true only for
// a positively idle/done status. Anything less — no route, no connected node,
// stale or truncated state, or a pane the node does not report — leaves both
// false: unknown is never treated as idle and never earns deferral.
type receiverInboxState struct {
	supported bool
	idle      bool
}

func (h *HubServer) receiverInboxState(lane string) receiverInboxState {
	route, ok := loadReportRelayRoutes(h.reportRelayPath)[lane]
	if !ok || route.Pane == "" {
		return receiverInboxState{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	node := h.nodes[route.Machine]
	if node == nil || node.state != "connected" {
		return receiverInboxState{}
	}
	snap := node.sessionSnapshot
	if snap == nil || snap.snapshotStatus != hubSnapshotStatusOK || snap.truncated {
		return receiverInboxState{}
	}
	now := h.now()
	if snap.receivedAt.After(now) || now.Sub(snap.receivedAt) > sessionsFindSnapshotMaxAge {
		return receiverInboxState{}
	}
	for _, session := range snap.sessions {
		if session.PaneID == route.Pane {
			return receiverInboxState{supported: h.inbox.policy.agentSupported(session.Harness), idle: eligibleIdleWakeStatus(session.Status)}
		}
	}
	return receiverInboxState{}
}

// inboxImmediate promotes an inbox-class event to immediate delivery when the
// receiving lane owns the completed job, when its pane cannot serve the inbox
// itself — an unsupported, unknown, or unreported agent CLI — or when a
// supported receiver is known idle. Everything else waits for the deadline
// digest, the safety net for supported receivers that are busy or whose
// reported status is unknown.
func (h *HubServer) inboxImmediate(lane, kind string) bool {
	if kind == "job.completed" || kind == "job.joined" {
		// receivingLane guarantees the recipient is the job's owner lane or
		// its parent — a completion the receiver owns never defers. Lanes
		// that merely observe a completion learn about it through events
		// that keep their own class.
		return true
	}
	state := h.receiverInboxState(lane)
	return !state.supported || state.idle
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
	if h.handoffkeep == nil || h.inboxDigestDisabled() {
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
		if reason := b.invalidReason(); reason != "" {
			if err := h.inbox.store.quarantineBatch(b.ID, reason); err != nil {
				h.logger.Error("inbox batch quarantine failed")
				failed = true
				return
			}
			continue
		}
		event := hubJobEventPayload{JobID: laneEventTransportID(b.Lane, b.ID), Epoch: 1, OwnerLane: b.Lane, EventID: b.ID, Text: b.Text, DeliveryClass: "interrupt", DeliverPolicy: "now", noDigest: true}
		result := h.relayLaneEventContext(h.closeCtx, event, nil)
		// Handoffkeep now owns the stable batch identity, including an unrouted
		// batch. Its usual replay owns retries from here, without re-escalating.
		if result.ID > 0 && !result.PersistFailed {
			if _, err := h.inbox.store.db.Exec(`UPDATE lane_inbox_batches SET sent=1 WHERE id=?`, b.ID); err != nil {
				h.logger.Error("inbox batch handoff recording failed")
				failed = true
				return
			}
			continue
		}
		if !result.PersistFailed {
			continue
		}
		if inboxBatchPermanentlyRejected(result.PersistStatus) {
			reason := fmt.Sprintf("handoffkeep permanently rejected batch: HTTP %d", result.PersistStatus)
			if err := h.inbox.store.quarantineBatch(b.ID, reason); err != nil {
				h.logger.Error("inbox batch quarantine failed")
				failed = true
				return
			}
			continue
		}
		failed = true
		// One failed persistence attempt ends the pass: whatever made this
		// batch fail applies to the rest, so the pass — and its one error —
		// happens at most once per backoff window.
		return
	}
}

func inboxBatchPermanentlyRejected(status int) bool {
	// Auth/configuration failures and retryable request failures apply to the
	// sink, not the batch's contents. Preserve those batches and back off.
	return status >= 400 && status < 500 && status != http.StatusUnauthorized &&
		status != http.StatusForbidden && status != http.StatusRequestTimeout &&
		status != http.StatusTooEarly && status != http.StatusTooManyRequests
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
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "inbox_disabled"})
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
		writeJSON(w, 503, map[string]string{"error": "inbox_unavailable"})
		return
	}
	writeJSON(w, 200, items)
}
func (h *HubServer) handleInboxCount(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeInbox(w, r) {
		return
	}
	counts, err := h.inbox.store.counts(r.URL.Query().Get("lane"))
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": "inbox_unavailable"})
		return
	}
	writeJSON(w, 200, counts)
}
func (h *HubServer) handleInboxRead(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeInbox(w, r) {
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeJSON(w, 400, map[string]string{"error": "invalid_id"})
		return
	}
	item, err := h.inbox.store.read(id, r.URL.Query().Get("lane"), h.now().UTC())
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, 404, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": "inbox_unavailable"})
		return
	}
	writeJSON(w, 200, item)
}

// handleInboxWait is the bounded long-poll counterpart of handleInboxList: it
// answers with the lane's oldest unread inbox item as soon as one exists, or a
// null item once the client-supplied wait runs out. It holds the same
// authentication as the other /v1 inbox routes — node credentials must name an
// owned lane, an operator token may wait on any lane.
func (h *HubServer) handleInboxWait(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeInbox(w, r) {
		return
	}
	lane := r.URL.Query().Get("lane")
	if !validReportRelayLaneName(lane) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "lane_required"})
		return
	}
	waitFor := hubInboxWaitMaxWait
	if raw := r.URL.Query().Get("timeout"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_timeout"})
			return
		}
		waitFor = parsed
	}
	if waitFor > hubInboxWaitMaxWait {
		waitFor = hubInboxWaitMaxWait
	}
	// A lane that already has an unread item answers without taking a waiter
	// slot — the cap bounds only requests that actually block.
	item, err := h.inbox.store.firstUnread(r.Context(), lane)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "inbox_unavailable"})
		return
	}
	if item != nil {
		writeJSON(w, http.StatusOK, map[string]any{"item": item})
		return
	}
	select {
	case h.inbox.waiters <- struct{}{}:
		defer func() { <-h.inbox.waiters }()
	default:
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "waiters_full"})
		return
	}
	deadline := time.Now().Add(waitFor)
	for {
		// Register for the next insert before checking, so a put that lands
		// between the check and the select still wakes this waiter.
		wake := h.inbox.store.waitChan()
		item, err := h.inbox.store.firstUnread(r.Context(), lane)
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "inbox_unavailable"})
			return
		}
		if item != nil {
			writeJSON(w, http.StatusOK, map[string]any{"item": item})
			return
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			writeJSON(w, http.StatusOK, map[string]any{"item": nil})
			return
		}
		timer := time.NewTimer(remaining)
		select {
		case <-wake:
		case <-r.Context().Done():
		case <-h.closeCtx.Done():
		case <-timer.C:
		}
		timer.Stop()
		if r.Context().Err() != nil || h.closeCtx.Err() != nil {
			return
		}
	}
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
		writeJSON(w, 400, map[string]string{"error": "invalid_request"})
		return
	}
	if lane := r.URL.Query().Get("lane"); lane != "" {
		if b.Lane != "" && b.Lane != lane {
			writeJSON(w, 400, map[string]string{"error": "lane_mismatch"})
			return
		}
		b.Lane = lane
	}
	n, err := h.inbox.store.ack(b.ID, b.Lane, b.All, h.now().UTC())
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": "inbox_unavailable"})
		return
	}
	writeJSON(w, 200, map[string]int64{"acked": n})
}
