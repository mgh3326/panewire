package panewire

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
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
	nextPrune       time.Time
	// flushFailures counts failed flush passes for monitoring and tests.
	flushFailures uint64
	// wakes holds each inbox-only lane's coalescing window (inbox_wake.go).
	wakes inboxWakes
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

// newHubInbox opens the Postgres store when databaseURL is set and the SQLite
// file at storePath otherwise; an empty storePath disables only the SQLite
// fallback.
func newHubInbox(policyPath, storePath, databaseURL string) (*hubInbox, error) {
	if policyPath == "off" || (storePath == "" && databaseURL == "") {
		return nil, nil
	} // The kill switch never opens or modifies the inbox database.
	if policyPath == "" {
		policyPath = "default"
	}
	policy, err := loadDeliveryPolicy(policyPath)
	if err != nil {
		return nil, err
	}
	var store *laneInboxStore
	if databaseURL != "" {
		store, err = openLaneInboxStorePostgres(context.Background(), databaseURL)
	} else {
		store, err = openLaneInboxStore(storePath)
	}
	if err != nil {
		return nil, err
	}
	if err = store.ensureSpawnTasks(); err != nil {
		store.close()
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
	var err error
	quiet := func(lanes []string, inboxOnly bool) {
		for _, lane := range lanes {
			if err != nil {
				return
			}
			query := `UPDATE lane_inbox SET due_at=0,digest_at=0 WHERE lane=?`
			if inboxOnly {
				query += ` AND class='inbox'`
			}
			ctx, cancel := h.inbox.store.op(h.closeCtx)
			_, err = h.inbox.store.db.ExecContext(ctx, h.inbox.store.q(query), lane)
			cancel()
		}
	}
	summaryLanes := make([]string, 0, len(h.inbox.policy.Lanes))
	for lane, p := range h.inbox.policy.Lanes {
		if p.InboxSummary != nil {
			summaryLanes = append(summaryLanes, lane)
		}
	}
	quiet(summaryLanes, true)
	if err == nil {
		// A design lane may be absent from lanes: (its class is a glob), so sweep
		// every lane the store knows. Its pre-existing unread items must not
		// escalate after the class is enabled (hk 3410), nor those of any other
		// inbox-only lane (hk 3602).
		var lanes []string
		if lanes, err = h.inbox.store.laneNames(); err == nil {
			design := lanes[:0]
			for _, lane := range lanes {
				if h.inbox.policy.inboxOnlyClass(lane, h.now()) != "" {
					design = append(design, lane)
				}
			}
			quiet(design, false)
		}
	}
	if err == nil {
		err = h.inbox.store.quarantineInvalidBatches()
	}
	if err == nil {
		err = h.inbox.store.recoverMustAct(h.inbox.policy, h.now())
	}
	if err != nil {
		h.inbox.persistDisabled = true
		h.logger.Error("inbox digest delivery disabled: startup recovery failed; fix the inbox store and restart the hub", "error", h.inbox.store.loggable("startup recovery", err))
	}
	h.seedStartupInboxWakes()
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
//
// senderID is the node that relayed the event, or "" when the hub itself did
// (replay, HTTP ingress, its own digests); the row records it for the ack-state
// lookup.
func (h *HubServer) receiveInbox(kind string, event *hubJobEventPayload, sender *hubAgent, senderID string, replay bool, contexts ...context.Context) (result relayLaneEventResult, handled bool) {
	jobLookupContext := h.closeCtx
	if len(contexts) > 0 && contexts[0] != nil {
		jobLookupContext = contexts[0]
	}
	if h.inbox == nil && kind == "job.completed" && event.Late {
		return relayLaneEventResult{PersistFailed: true}, true
	}
	if h.inbox == nil || event.sinkOnly {
		return result, false
	}
	lane := h.receivingLane(kind, *event)
	if reason := h.designFilterReason(kind, *event, lane); reason != "" {
		if result, handled := h.receiveFiltered(kind, event, sender, senderID, reason); handled {
			return result, true
		}
		// hk's row belongs to another event: this one takes the inbox path.
	}
	if event.NotifyType == "" && h.inbox.policy.Lanes[lane].InboxSummary != nil {
		switch kind {
		case "job.completed", "job.joined":
			event.NotifyType = "report"
		case "job.escalate", "job.lost", "job.revoked":
			event.NotifyType = "decision"
		}
	}
	class := h.inbox.policy.classify(lane, kind, *event, replay)
	// hk 3426: a design lane keeps a worker's routine result in its inbox; no
	// owner-completion or idle promotion below puts it in the pane.
	quiet := h.inbox.policy.designQuiet(lane, kind, *event, func() bool { return h.jobHasTaskContext(jobLookupContext, event.JobID) }, func() bool { return h.laneEventFromWorker(senderID, event.PaneID, lane) })
	if quiet || (kind == "job.completed" && event.Late) {
		class = "inbox"
	}
	// hk 3276: a worker directive reaches the worker's pane at once. A lane
	// policy that files its findings or orders as inbox (kept for a digest or
	// an idle promotion) must not retain them. designQuiet decides alone for
	// a design lane: what it quiets is never a directive (hk 3434).
	directive := !quiet && isWorkerDirective(kind, *event, loadReportRelayRoutes(h.reportRelayPath)[lane])
	if directive && class == "inbox" {
		class = "interrupt"
	}
	holdClass := class == "interrupt"
	if class == "inbox" && !event.Late && !quiet && h.inboxImmediate(lane, kind) {
		class = "interrupt"
	}
	deadline, window := h.inbox.policy.timing(lane, class)
	if kind == "job.completed" && event.Late {
		deadline, window = 0, 0
	}
	if class == "inbox" && h.inbox.policy.Lanes[lane].InboxSummary != nil {
		// The external digest owns scheduling; no deadline may inject a second digest.
		deadline, window = 0, 0
	}
	if h.inbox.policy.designLane(lane) {
		// A design lane is never handed the "N unread inbox items" digest; its
		// unread items stay readable through `panewire inbox list` (hk 3410).
		deadline, window = 0, 0
	}
	// hk 3602: a design or desk lane, or a worker lane in the inbox trial, is
	// inbox-only. Whatever the class above, the event is filed unread and never
	// typed into the pane, digested or held; what would have been typed asks
	// for the lane's coalesced one-line wake instead. Only an operator chat
	// turn and an operator HTTP ingress event are exempt (inboxOnlyExempt);
	// refuseInboxOnlyBody holds the same rule where a body would be typed.
	inboxOnly := h.inbox.policy.inboxOnlyClass(lane, h.now())
	wake := false
	if inboxOnly != "" && !inboxOnlyExempt(*event) {
		wake = inboxWakeWorthy(class, quiet, kind, *event)
		class, directive, holdClass = "inbox", false, false
		deadline, window = 0, 0
	} else {
		inboxOnly = ""
	}
	// Ordinary deadline digests still exclude must-ack types. Working holds
	// retain each original ID so the receiver can acknowledge its items.
	if event.Late || event.noDigest || h.inbox.policy.mustAct(lane, *event) || mustAckNotifyTypes[event.NotifyType] {
		window = 0
	}
	if directive {
		window = 0
	}
	hold := time.Duration(0)
	if h.inbox.policy.holdPolicy(lane) != nil && !holdableEvent(kind, *event) {
		window = 0
	}
	if settings := h.inbox.policy.holdPolicy(lane); settings != nil && holdClass && holdableEvent(kind, *event) && !directive && h.receiverInboxState(lane).working {
		hold, _ = time.ParseDuration(settings.MaxHold)
		window = 0
	}
	item, err := h.inbox.store.putFrom(senderID, relayEventDedupeKey(kind, *event), lane, kind, class, *event, h.now().UTC(), deadline, window, hold)
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
	if wake && item.Fresh && item.Class == "inbox" {
		h.noteInboxWakeArrival(lane, inboxOnly)
	}
	if item.SupersededAt > 0 {
		if sender != nil {
			if kind == "lane.event" {
				h.queueLaneRelayPersisted(*event, sender, item.ID)
			} else {
				h.queueRelayPersisted(kind, *event, sender, item.ID)
			}
		}
		return relayLaneEventResult{ID: item.ID, Routed: true, Machine: "inbox", Duplicate: true}, true
	}
	if item.Class != "inbox" && item.DigestAt == 0 && item.HoldUntil == 0 {
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
	working   bool
}

func (h *HubServer) receiverInboxState(lane string) receiverInboxState {
	route, ok := loadReportRelayRoutes(h.reportRelayPath)[lane]
	if !ok || route.Pane == "" {
		return receiverInboxState{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	snap := h.freshSessionSnapshotLocked(route.Machine)
	if snap == nil {
		return receiverInboxState{}
	}
	for _, session := range snap.sessions {
		if session.PaneID == route.Pane {
			return receiverInboxState{supported: h.inbox.policy.agentSupported(session.Harness), idle: eligibleIdleWakeStatus(session.Status), working: strings.EqualFold(session.Status, "working")}
		}
	}
	return receiverInboxState{}
}

// jobHasTask reports whether the job's hk record names a task. The record is
// the one place a job's task is kept; the hub reads it through the client
// hub-placed spawns use. A missing or unreadable record counts as no task, so
// the completion is delivered as before (hk 3426).
func (h *HubServer) jobHasTask(jobID string) bool {
	return h.jobHasTaskContext(h.closeCtx, jobID)
}

func (h *HubServer) jobHasTaskContext(parent context.Context, jobID string) bool {
	ctx, cancel := context.WithTimeout(parent, nodeLoopHKTimeout)
	defer cancel()
	task, _, err := h.jobTaskID(ctx, jobID)
	if err != nil {
		h.logger.Warn("job record unreadable; design-lane completion delivered", "job", jobID, "error_class", hubHKReadErrorClass(err))
		return false
	}
	return task != ""
}

func hubHKReadErrorClass(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "unavailable"
	}
}

// jobTaskID returns the task id the job's hk record names ("" when it names
// none, or the record is missing) and the App it read the record with, so the
// caller reads the task through the same handoffkeep client.
func (h *HubServer) jobTaskID(ctx context.Context, jobID string) (string, *fleet.App, error) {
	app, err := h.newSpawnApp(io.Discard, io.Discard)
	if err == nil {
		err = app.CheckTrust()
	}
	var record map[string]any
	if err == nil {
		record, _, err = app.ExistingJob(ctx, jobID)
	}
	if err != nil {
		return "", nil, err
	}
	switch task := record["task_id"].(type) {
	case float64:
		if task > 0 {
			return strconv.FormatInt(int64(task), 10), app, nil
		}
	case string:
		return strings.TrimSpace(task), app, nil
	}
	return "", app, nil
}

// inboxImmediate promotes an inbox-class event to immediate delivery when the
// receiving lane owns the completed job, when its pane cannot serve the inbox
// itself — an unsupported, unknown, or unreported agent CLI — or when a
// supported receiver is known idle, unless the lane disables idle promotion.
// Everything else waits for the deadline digest.
func (h *HubServer) inboxImmediate(lane, kind string) bool {
	if h.inbox.policy.Lanes[lane].InboxSummary != nil {
		return false
	}
	if kind == "job.completed" || kind == "job.joined" {
		// receivingLane guarantees the recipient is the job's owner lane or
		// its parent — a completion the receiver owns never defers. Lanes
		// that merely observe a completion learn about it through events
		// that keep their own class.
		return true
	}
	if promote := h.inbox.policy.Lanes[lane].IdlePromote; promote != nil && !*promote {
		return false
	}
	state := h.receiverInboxState(lane)
	return !state.supported || state.idle
}

// replayInbox runs before either the ordinary replay or the once-only
// unconfirmed replay can claim an injection. Old handoffkeep rows are migrated
// to the inbox before being retired; a failed retire is harmless and retried.
func (h *HubServer) replayInbox(record handoffkeepRelayEvent) bool {
	return h.replayInboxAged(record, true)
}

// replayInboxAged is replayInbox with the receipt-age gate optional
// (relayReplayRetireReasonAged); the inbox settlement checks always apply.
func (h *HubServer) replayInboxAged(record handoffkeepRelayEvent, receiptAge bool) bool {
	if record.DeliveredAt != "" {
		return true
	}
	// A stale row is retired before it can be migrated: the inbox evidence
	// that would suppress it is pruned after the same replay window, so a row
	// whose handoffkeep mark once failed would otherwise come back as a new
	// item on every restart (hk 2976).
	if h.handoffkeep != nil && !strings.HasPrefix(record.DeliveredTo, relayReplayRetiredMarker) {
		if reason := h.relayReplayRetireReasonAged(record, receiptAge); reason != "" {
			h.retireRelayReplay(record, reason)
			return true
		}
	}
	if h.inbox == nil {
		return false
	}
	event := relayEventFromRecord(record)
	// settled is strictly broader than the old injected-only check: a receiver
	// read — including a must-ack item read before its ack — is evidence the
	// lane already has the event, so a restart must not inject it again (hk
	// 3410). An insert-time read of an interrupt never counts here.
	settled, err := h.inbox.store.settled(relayEventDedupeKey(record.Kind, event))
	if err != nil {
		h.logger.Warn("inbox replay evidence unavailable; replay deferred", "event_id", record.ID)
		return true
	}
	if settled {
		return true
	}
	if h.isHeldBatch(record.OwnerLane, record.EventID) || isUnconfirmedMarkRecord(record) {
		return false
	}
	result, handled := h.receiveInbox(record.Kind, &event, nil, "", true)
	if !handled {
		return false
	}
	if !result.PersistFailed && !result.AlreadyDelivered && h.handoffkeep != nil {
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
			h.pruneInbox()
			h.flushInbox()
			h.flushInboxWakes()
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
	// A held-batching failure must not starve ordinary digests or batches
	// that are already pending for other lanes.
	if err := h.makeHeldBatches(); err != nil {
		if h.closeCtx.Err() != nil {
			return
		}
		h.logger.Error("held inbox batching failed; continuing with ordinary batches")
		failed = true
	}
	if err := h.inbox.store.makeBatches(h.now().UTC()); err != nil {
		if h.closeCtx.Err() != nil {
			return
		}
		h.logger.Error("inbox batching failed")
		failed = true
		return
	}
	batches, err := h.inbox.store.pendingBatches()
	if err != nil {
		if h.closeCtx.Err() != nil {
			return
		}
		h.logger.Error("inbox batch listing failed")
		failed = true
		return
	}
	// A held batch that cannot be prepared (no document store, or the PUT keeps
	// failing) stays retained and blocks only its own lane; other lanes flush.
	blockedLanes := map[string]bool{}
	for _, b := range batches {
		if h.closeCtx.Err() != nil {
			return
		}
		if blockedLanes[b.Lane] {
			continue
		}
		if class := h.inbox.policy.inboxOnlyClass(b.Lane, h.now()); class != "" {
			// An inbox-only lane is never typed a batch (hk 3602): its items
			// are filed unread and the batch is retired untyped.
			if err := h.fileInboxOnlyBatch(b.Lane, class, b.ID); err != nil {
				if h.closeCtx.Err() != nil {
					return
				}
				h.logger.Error("inbox-only batch filing failed; batch retained", "lane", b.Lane)
				failed = true
			}
			continue
		}
		if h.inbox.policy.Lanes[b.Lane].InboxSummary != nil {
			if err := h.inbox.store.quarantineBatch(b.ID, "external scheduler owns lane digest"); err != nil {
				if h.closeCtx.Err() != nil {
					return
				}
				failed = true
			}
			continue
		}
		held := strings.HasPrefix(b.ID, "held-")
		if held {
			lane := b.Lane
			var err error
			b, err = h.prepareHeldBatch(b)
			if err != nil {
				if h.closeCtx.Err() != nil {
					return
				}
				h.logger.Error("held inbox preparation failed; batch retained", "lane", lane)
				failed = true
				blockedLanes[lane] = true
				continue
			}
		}
		if held && b.Text == "" {
			continue
		}
		if reason := b.invalidReason(); reason != "" {
			if err := h.inbox.store.quarantineBatch(b.ID, reason); err != nil {
				if h.closeCtx.Err() != nil {
					return
				}
				h.logger.Error("inbox batch quarantine failed")
				failed = true
				return
			}
			continue
		}
		relayText := b.Text
		if held {
			relayText = strings.Split(b.Text, " | ")[0] + " | panewire inbox list --lane " + b.Lane
		}
		event := hubJobEventPayload{JobID: laneEventTransportID(b.Lane, b.ID), Epoch: 1, OwnerLane: b.Lane, EventID: b.ID, Text: relayText, DeliveryClass: "interrupt", DeliverPolicy: "now", noDigest: true}
		result := h.relayLaneEventContext(h.closeCtx, event, nil)
		// Handoffkeep now owns the stable batch identity, including an unrouted
		// batch. Its usual replay owns retries from here, without re-escalating.
		if result.ID > 0 && !result.PersistFailed {
			if err := h.inbox.store.markBatchSent(b.ID); err != nil {
				if h.closeCtx.Err() != nil {
					return
				}
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
				if h.closeCtx.Err() != nil {
					return
				}
				h.logger.Error("inbox batch quarantine failed")
				failed = true
				return
			}
			continue
		}
		if h.closeCtx.Err() != nil {
			return
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

// authorizeInbox checks scope (inbox.read or inbox.ack) for the ?lane= the
// request names: any lane for the operator, a lane routed to the caller's own
// machine for a node, the lane its scope names for a service.
func (h *HubServer) authorizeInbox(w http.ResponseWriter, r *http.Request, scope string) bool {
	if access := h.authorize(r, scope, r.URL.Query().Get("lane")); access != hubAccessGranted {
		writeHubAccessDenied(w, access)
		return false
	}
	if h.inbox == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "inbox_disabled"})
		return false
	}
	return true
}

// authorizeInboxItem is authorizeInbox for read <id> and ack <id>. A node
// that names no lane is authorized by the item's own lane, when that lane is
// routed to the node: a worker pane need not know its lane to ack its item.
// It returns the lane the store must scope the item to ("" when the caller
// named none and needs no fallback). Any other refusal is the plain 401/403.
func (h *HubServer) authorizeInboxItem(w http.ResponseWriter, r *http.Request, scope string, id int64) (string, bool) {
	queryLane := r.URL.Query().Get("lane")
	access := h.authorize(r, scope, queryLane)
	if access != hubAccessGranted && queryLane == "" && h.inbox != nil && h.authenticate(r).kind == hubCallerNode {
		if lane, err := h.inbox.store.itemLane(id); err == nil && h.authorize(r, scope, lane) == hubAccessGranted {
			return lane, true
		}
	}
	if access != hubAccessGranted {
		writeHubAccessDenied(w, access)
		return "", false
	}
	if h.inbox == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "inbox_disabled"})
		return "", false
	}
	return queryLane, true
}

func (h *HubServer) handleInboxList(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeInbox(w, r, hubScopeInboxRead) {
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

// hubInboxAckStateMaxIDs bounds one ack-state request.
const hubInboxAckStateMaxIDs = 100

// handleInboxAckState answers, for each ?event_id=, the lane its row targets,
// the delivery stage that row reached, the stage's reason/machine/pane when it
// has one, and whether the row was acked and when — and nothing of the row's
// kind or body. The operator and a service holding inbox.ack_state see every
// row; a node sees only rows it relayed itself, so a sender can check its own
// events without reading any lane. A node with no visible row among the ids
// gets the 401 it gets everywhere else, whether the ids are another node's or
// unknown: the two must not be told apart. For anyone else an id with no row
// is reported in not_found, and the answer is 404 when no id has a row.
func (h *HubServer) handleInboxAckState(w http.ResponseWriter, r *http.Request) {
	caller := h.authenticate(r)
	sender := ""
	if caller.kind == hubCallerNode {
		sender = caller.id
	} else if access := h.authorize(r, hubScopeInboxAckState, ""); access != hubAccessGranted {
		writeHubAccessDenied(w, access)
		return
	}
	if h.inbox == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "inbox_disabled"})
		return
	}
	ids := r.URL.Query()["event_id"]
	if len(ids) == 0 || len(ids) > hubInboxAckStateMaxIDs {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	seen := make(map[string]bool, len(ids))
	unique := ids[:0:0]
	for _, id := range ids {
		if !validLaneEventID(id) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
			return
		}
		if !seen[id] {
			seen[id] = true
			unique = append(unique, id)
		}
	}
	states, err := h.inbox.store.ackStates(unique, sender)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "inbox_unavailable"})
		return
	}
	if len(states) == 0 && caller.kind == hubCallerNode {
		hubUnauthorized(w)
		return
	}
	response := struct {
		Events   []laneInboxAckState `json:"events"`
		NotFound []string            `json:"not_found"`
	}{Events: []laneInboxAckState{}, NotFound: []string{}}
	for _, id := range unique {
		if state, ok := states[id]; ok {
			response.Events = append(response.Events, state)
		} else {
			response.NotFound = append(response.NotFound, id)
		}
	}
	status := http.StatusOK
	if len(response.Events) == 0 {
		status = http.StatusNotFound
	}
	writeJSON(w, status, response)
}

func (h *HubServer) handleInboxCount(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeInbox(w, r, hubScopeInboxRead) {
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
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		if !h.authorizeInbox(w, r, hubScopeInboxRead) {
			return
		}
		writeJSON(w, 400, map[string]string{"error": "invalid_id"})
		return
	}
	lane, ok := h.authorizeInboxItem(w, r, hubScopeInboxRead, id)
	if !ok {
		return
	}
	item, err := h.inbox.store.read(id, lane, h.now().UTC())
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
	if !h.authorizeInbox(w, r, hubScopeInboxRead) {
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
		case <-h.drain.Started():
			// SIGTERM: end the poll with the retryable answer rather than
			// holding the caller until this process exits (hub_drain.go).
			timer.Stop()
			writeHubDraining(w)
			return
		case <-timer.C:
		}
		timer.Stop()
		if r.Context().Err() != nil || h.closeCtx.Err() != nil {
			return
		}
	}
}

func (h *HubServer) handleInboxAck(w http.ResponseWriter, r *http.Request) {
	// ack <id> names its item in the body, so a node that sent no lane is
	// authorized by the item's lane after the body is read. Every other
	// caller is decided before the body is parsed, as ever.
	nodeItem := r.URL.Query().Get("lane") == "" && h.authenticate(r).kind == hubCallerNode
	if !nodeItem && !h.authorizeInbox(w, r, hubScopeInboxAck) {
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
	if nodeItem {
		if b.All || b.Lane != "" {
			hubUnauthorized(w)
			return
		}
		lane, ok := h.authorizeInboxItem(w, r, hubScopeInboxAck, b.ID)
		if !ok {
			return
		}
		b.Lane = lane
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
