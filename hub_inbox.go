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
	flushMu sync.Mutex
}

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
		h.queueRelayPersisted(kind, *event, sender, item.ID)
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

func (h *HubServer) flushInbox() {
	if h.inbox == nil || !h.inbox.flushMu.TryLock() {
		return
	}
	defer h.inbox.flushMu.Unlock()
	if h.closeCtx.Err() != nil {
		return
	}
	if err := h.inbox.store.makeBatches(h.now().UTC()); err != nil {
		h.logger.Error("inbox batching failed")
		return
	}
	batches, err := h.inbox.store.pendingBatches()
	if err != nil {
		h.logger.Error("inbox batch listing failed")
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
		}
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
