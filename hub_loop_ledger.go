package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Loop ledger (hk 5116, track A1). One in-memory ledger per hub process
// records every hub mechanism's liveness: last_tick, ticks, made, done,
// errors and the oldest pending age. A daily hk document carries one row per
// mechanism, and a lane.event notice names a mechanism that stopped ticking,
// stopped finishing work, or hit a previously silent error path.
//
// Made and done per mechanism (the design's table, §Track A):
//   - spawn_coordinator: pending spawn_operations rows seen -> rows scheduled
//     onto workers (terminal outcomes arrive through the workers).
//   - consult_coordinator: active consult rows read -> consults answered,
//     timed out, failed or respawned (a state transition).
//   - lane_stall_drain_wake: backlog lanes observed in a sweep -> drain lines
//     queued to a design lane's pane.
//   - inbox: flush passes -> inbox batches/digests sent.
//   - quota_ladder: quota successor rows plus expired ladder stages seen ->
//     hk records written plus ladder stages expired.
//   - pool_triggers_exhausted: live pools checked -> exhausted-pool sweeps
//     started.
//   - pool_actions: active pool_actions rows read -> actions reconciled to
//     final.
//   - placement_records: placement flush passes -> hk placement records
//     delivered.
//   - fleet_status: snapshot ticks -> snapshots written to hk.
//   - pane_sweep: pane-sweep ticks -> panes a node closed.
//   - reviewer_end: succeeded reviewer operations seen -> reviewer jobs ended.
//
// Docs: docs/loops.md. The row format leaves room for per-node rows: every
// row carries its own machine ("hub" here; hk 5117 adds node rows).

type loopMechanism string

const (
	loopSpawnCoordinator   loopMechanism = "spawn_coordinator"
	loopConsultCoordinator loopMechanism = "consult_coordinator"
	loopLaneStallDrainWake loopMechanism = "lane_stall_drain_wake"
	loopInbox              loopMechanism = "inbox"
	loopQuotaLadder        loopMechanism = "quota_ladder"
	loopPoolTriggers       loopMechanism = "pool_triggers_exhausted"
	loopPoolActions        loopMechanism = "pool_actions"
	loopPlacementRecords   loopMechanism = "placement_records"
	loopFleetStatus        loopMechanism = "fleet_status"
	loopPaneSweep          loopMechanism = "pane_sweep"
	loopReviewerEnd        loopMechanism = "reviewer_end"
)

// loopMechanisms is the closed row list: the daily document carries exactly
// these rows in this order.
var loopMechanisms = []loopMechanism{
	loopSpawnCoordinator,
	loopConsultCoordinator,
	loopLaneStallDrainWake,
	loopInbox,
	loopQuotaLadder,
	loopPoolTriggers,
	loopPoolActions,
	loopPlacementRecords,
	loopFleetStatus,
	loopPaneSweep,
	loopReviewerEnd,
}

// Notice thresholds live here and nowhere else.
const (
	// loopStaleIntervals fires when last_tick is older than this many
	// intervals for the mechanism.
	loopStaleIntervals = 3
	// loopMadeFloor and loopDoneRatio fire when made >= floor and
	// (done == 0 or done/made < ratio).
	loopMadeFloor = 20
	loopDoneRatio = 0.1
	// defaultLoopsNoticeLane is the lane the silent-stop notice goes to
	// unless --loops-notice-lane overrides it.
	defaultLoopsNoticeLane = "panewire-design"
	// loopsRowDocPrefix is the hk document prefix for the daily row; the full
	// key is prefix + YYYY-MM-DD.
	loopsRowDocPrefix = "ops/panewire-loops/"
	loopsRowDocKind   = "note"
	loopsRowVersion   = 1
)

type loopStat struct {
	Ticks         int64
	Made          int64
	Done          int64
	Errors        int64
	SilentErrors  int64
	LastTick      time.Time
	OldestPending time.Duration
}

// loopLedgerState is the HubServer's loop bookkeeping. Guarded by loopsMu,
// never by HubServer.mu, so a ledger call inside a store callback cannot
// deadlock a Sweep that already holds mu.
type loopLedgerState struct {
	mu           sync.Mutex
	stats        map[loopMechanism]*loopStat
	silentStreak map[string]bool
	noticeSent   map[string]string
	rowDate      string
	// progressBase holds each mechanism's cumulative made/done at the
	// start of progressDate; the progress rule runs on the current
	// date's deltas so a long healthy history cannot mask a new stall.
	progressBase  map[loopMechanism][2]int64
	progressDate  string
	rowRetryAfter time.Time
}

func newLoopLedgerState() *loopLedgerState {
	return &loopLedgerState{
		stats:        make(map[loopMechanism]*loopStat),
		silentStreak: make(map[string]bool),
		noticeSent:   make(map[string]string),
		progressBase: make(map[loopMechanism][2]int64),
	}
}

func (h *HubServer) loopLedger() *loopLedgerState {
	h.loopsMu.Lock()
	defer h.loopsMu.Unlock()
	if h.loops == nil {
		h.loops = newLoopLedgerState()
	}
	return h.loops
}

func (s *loopLedgerState) statFor(m loopMechanism) *loopStat {
	st, ok := s.stats[m]
	if !ok {
		st = &loopStat{}
		s.stats[m] = st
	}
	return st
}

// loopTick records one pass of a mechanism at the hub's clock.
func (h *HubServer) loopTick(m loopMechanism) {
	if h == nil {
		return
	}
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	s := st.statFor(m)
	now := h.now().UTC()
	s.Ticks++
	s.LastTick = now
}

// loopAddMade records items a pass picked up.
func (h *HubServer) loopAddMade(m loopMechanism, n int) {
	if h == nil || n <= 0 {
		return
	}
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	st.statFor(m).Made += int64(n)
}

// loopAddDone records items a pass finished.
func (h *HubServer) loopAddDone(m loopMechanism, n int) {
	if h == nil || n <= 0 {
		return
	}
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	st.statFor(m).Done += int64(n)
}

// loopSetOldestPending records the oldest pending age a pass observed.
func (h *HubServer) loopSetOldestPending(m loopMechanism, age time.Duration) {
	if h == nil || age < 0 {
		return
	}
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	st.statFor(m).OldestPending = age
}

// loopNoteError counts a visible error for a mechanism.
func (h *HubServer) loopNoteError(m loopMechanism) {
	if h == nil {
		return
	}
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	st.statFor(m).Errors++
}

// loopNoteSilentError counts a previously silent error path when its failure
// streak starts, and logs one line per streak: every later failure with the
// same key counts and logs nothing until loopClearSilent retires the streak
// with a success, so a latched outage cannot flood the ledger's counters.
func (h *HubServer) loopNoteSilentError(m loopMechanism, key string, err error) {
	if h == nil {
		return
	}
	st := h.loopLedger()
	st.mu.Lock()
	streak := st.silentStreak[key]
	if !streak {
		st.silentStreak[key] = true
		st.statFor(m).Errors++
		st.statFor(m).SilentErrors++
	}
	st.mu.Unlock()
	if streak {
		return
	}
	if err != nil {
		h.logger.Warn("hub loop silent error; retrying", "mechanism", string(m), "path", key, "error", err)
	} else {
		h.logger.Warn("hub loop skipped; retrying", "mechanism", string(m), "path", key)
	}
}

// loopClearSilent retires one silent streak after a success, so the next
// failure logs again.
func (h *HubServer) loopClearSilent(key string) {
	if h == nil {
		return
	}
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.silentStreak, key)
}

// loopSnapshot copies the ledger for the row writer and the notice check.
func (h *HubServer) loopSnapshot() map[loopMechanism]loopStat {
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	out := make(map[loopMechanism]loopStat, len(loopMechanisms))
	for _, m := range loopMechanisms {
		if s, ok := st.stats[m]; ok {
			out[m] = *s
		} else {
			out[m] = loopStat{}
		}
	}
	return out
}

// loopInterval is the mechanism's expected tick interval. Stale detection
// compares now-last_tick against three intervals.
func (h *HubServer) loopInterval(m loopMechanism) time.Duration {
	switch m {
	case loopSpawnCoordinator:
		return 2 * time.Second
	case loopConsultCoordinator:
		return consultPollInterval
	case loopLaneStallDrainWake:
		if h != nil && h.keepaliveInterval > 0 {
			return h.keepaliveInterval
		}
		return 10 * time.Second
	case loopInbox:
		return 100 * time.Millisecond
	case loopQuotaLadder:
		return quotaReconcileEvery
	case loopPoolTriggers:
		if h == nil {
			return defaultPoolTriggerInterval
		}
		if h.poolTriggerInterval <= 0 {
			return 0
		}
		return h.poolTriggerInterval
	case loopPoolActions:
		return poolActionInterval
	case loopPlacementRecords:
		return 2 * time.Second
	case loopFleetStatus:
		if h == nil {
			return defaultFleetStatusInterval
		}
		if h.fleetStatusInterval <= 0 {
			return 0
		}
		return h.fleetStatusInterval
	case loopPaneSweep:
		if h == nil {
			return defaultHubPaneSweepInterval
		}
		if h.paneSweepInterval <= 0 {
			return 0
		}
		return h.paneSweepInterval
	case loopReviewerEnd:
		return reviewerEndInterval
	}
	return time.Minute
}

type loopsRowEntry struct {
	Machine        string `json:"machine"`
	Mechanism      string `json:"mechanism"`
	IntervalS      int64  `json:"interval_s"`
	Ticks          int64  `json:"ticks"`
	Made           int64  `json:"made"`
	Done           int64  `json:"done"`
	Errors         int64  `json:"errors"`
	SilentErrors   int64  `json:"silent_errors"`
	LastTick       string `json:"last_tick"`
	OldestPendingS int64  `json:"oldest_pending_s"`
}

type loopsRowDoc struct {
	V       int             `json:"v"`
	At      time.Time       `json:"at"`
	Machine string          `json:"machine"`
	Rows    []loopsRowEntry `json:"rows"`
}

// loopsRowDocument builds the daily row at now. Every row carries its own
// machine so hk 5117 can append per-node rows beside these.
func (h *HubServer) loopsRowDocument(now time.Time) loopsRowDoc {
	snap := h.loopSnapshot()
	rows := make([]loopsRowEntry, 0, len(loopMechanisms))
	for _, m := range loopMechanisms {
		s := snap[m]
		last := ""
		if !s.LastTick.IsZero() {
			last = s.LastTick.UTC().Format(time.RFC3339)
		}
		rows = append(rows, loopsRowEntry{
			Machine:        "hub",
			Mechanism:      string(m),
			IntervalS:      int64(h.loopInterval(m) / time.Second),
			Ticks:          s.Ticks,
			Made:           s.Made,
			Done:           s.Done,
			Errors:         s.Errors,
			SilentErrors:   s.SilentErrors,
			LastTick:       last,
			OldestPendingS: int64(s.OldestPending / time.Second),
		})
	}
	return loopsRowDoc{V: loopsRowVersion, At: now.UTC(), Machine: "hub", Rows: rows}
}

// loopsRowKey names the hk document for a UTC date.
func loopsRowKey(date string) string { return loopsRowDocPrefix + date }

// loopsPublish writes the row body under key with the hub's hk credential,
// unless a test injected a writer.
func (h *HubServer) loopsPublish(ctx context.Context, key, body string) error {
	if h.loopsDocPublish != nil {
		return h.loopsDocPublish(ctx, key, body)
	}
	return fleet.PutHKDocument(ctx, key, loopsRowDocKind, "", body)
}

// loopsRowState reports whether the row document already exists. Unknown is
// true when the read itself failed for any reason other than absence: the
// caller then skips the write and retries the read on the next sweep, so a
// transient hk failure can never overwrite the day's counts with a
// restart-fresh ledger.
func (h *HubServer) loopsRowState(ctx context.Context, key string) (exists, unknown bool) {
	if h.loopsDocExists != nil {
		exists, err := h.loopsDocExists(ctx, key)
		return exists, err != nil
	}
	_, err := fleet.HKDocumentAnswer(ctx, key)
	if err == nil {
		return true, false
	}
	if fleet.HKDocumentMissing(err) {
		return false, false
	}
	return false, true
}

// loopsRowRetryAfter bounds how often an unreadable row is re-read: the
// synchronous hk GET must not hold the maintenance tick on every pass while
// hk is unreachable.
const loopsRowRetryAfter = time.Minute

// maybeWriteLoopsRow writes exactly one daily row per UTC date. It is called
// from the hub's slow sweeps; a failed write or an unreadable row counts in
// the ledger (against fleet_status, the hub's other hk writer) and is
// retried on the next sweep (an unreadable row at most once a minute).
// Rolling into a new date also re-baselines the progress rule to the
// cumulative counters, so the rule always runs on the current date's deltas.
func (h *HubServer) maybeWriteLoopsRow(ctx context.Context) {
	if h == nil {
		return
	}
	now := h.now().UTC()
	date := now.Format("2006-01-02")
	st := h.loopLedger()
	st.mu.Lock()
	if st.rowDate == date {
		st.mu.Unlock()
		return
	}
	if st.progressDate != date {
		st.progressDate = date
		for m, s := range st.stats {
			st.progressBase[m] = [2]int64{s.Made, s.Done}
		}
	}
	if now.Before(st.rowRetryAfter) {
		st.mu.Unlock()
		return
	}
	st.mu.Unlock()
	exists, unknown := h.loopsRowState(ctx, loopsRowKey(date))
	if unknown {
		h.loopNoteError(loopFleetStatus)
		h.logger.Warn("hub loops row unreadable; retrying", "key", loopsRowKey(date))
		st.mu.Lock()
		st.rowRetryAfter = now.Add(loopsRowRetryAfter)
		st.mu.Unlock()
		return
	}
	if exists {
		st.mu.Lock()
		st.rowDate = date
		st.mu.Unlock()
		return
	}
	body, err := json.Marshal(h.loopsRowDocument(now))
	if err != nil {
		h.loopNoteError(loopFleetStatus)
		return
	}
	if err := h.loopsPublish(ctx, loopsRowKey(date), string(body)); err != nil {
		h.loopNoteError(loopFleetStatus)
		h.logger.Warn("hub loops row not written; retrying", "key", loopsRowKey(date), "error", err)
		return
	}
	st.mu.Lock()
	st.rowDate = date
	st.mu.Unlock()
}

// loopsStaleHits returns the mechanisms whose last_tick is older than three
// intervals at now. A mechanism that never ticked is stale once the hub has
// been up longer than three intervals.
func (h *HubServer) loopsStaleHits(now time.Time) []loopMechanism {
	snap := h.loopSnapshot()
	var hits []loopMechanism
	for _, m := range loopMechanisms {
		s := snap[m]
		interval := h.loopInterval(m)
		if interval <= 0 {
			continue
		}
		if s.LastTick.IsZero() {
			if !h.startedAt.IsZero() && now.Sub(h.startedAt) > loopStaleIntervals*interval {
				hits = append(hits, m)
			}
			continue
		}
		if now.Sub(s.LastTick) > loopStaleIntervals*interval {
			hits = append(hits, m)
		}
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i] < hits[j] })
	return hits
}

// loopsProgressHits returns mechanisms with made >= floor that finished
// nothing or nearly nothing since the current UTC date began: done == 0, or
// done/made < ratio. The window is the date's deltas off the cumulative
// counters, so a long healthy history cannot mask a stall that started today.
func (h *HubServer) loopsProgressHits() []loopMechanism {
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	var hits []loopMechanism
	for _, m := range loopMechanisms {
		s, ok := st.stats[m]
		var made, done int64
		if ok {
			base := st.progressBase[m]
			made, done = s.Made-base[0], s.Done-base[1]
		}
		if made < loopMadeFloor {
			continue
		}
		if done == 0 || float64(done)/float64(made) < loopDoneRatio {
			hits = append(hits, m)
		}
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i] < hits[j] })
	return hits
}

// loopsSilentHits returns mechanisms with at least one previously silent
// error counted.
func (h *HubServer) loopsSilentHits() []loopMechanism {
	snap := h.loopSnapshot()
	var hits []loopMechanism
	for _, m := range loopMechanisms {
		if snap[m].SilentErrors >= 1 {
			hits = append(hits, m)
		}
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i] < hits[j] })
	return hits
}

type loopNoticeHit struct {
	Mechanism loopMechanism
	Reason    string
}

// loopNoticeHits merges the three notice conditions into one hit list.
func (h *HubServer) loopNoticeHits(now time.Time) []loopNoticeHit {
	var hits []loopNoticeHit
	for _, m := range h.loopsStaleHits(now) {
		hits = append(hits, loopNoticeHit{Mechanism: m, Reason: fmt.Sprintf("last_tick older than %d intervals", loopStaleIntervals)})
	}
	for _, m := range h.loopsProgressHits() {
		hits = append(hits, loopNoticeHit{Mechanism: m, Reason: fmt.Sprintf("made >= %d with done/made < %.0f%%", loopMadeFloor, loopDoneRatio*100)})
	}
	for _, m := range h.loopsSilentHits() {
		hits = append(hits, loopNoticeHit{Mechanism: m, Reason: "previously silent error path counted >= 1"})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Mechanism != hits[j].Mechanism {
			return hits[i].Mechanism < hits[j].Mechanism
		}
		return hits[i].Reason < hits[j].Reason
	})
	return hits
}

// loopsNoticeLane returns the configured notice lane.
func (h *HubServer) loopsNoticeLane() string {
	if h != nil && h.loopsLane != "" {
		return h.loopsLane
	}
	return defaultLoopsNoticeLane
}

// maybeSendLoopsNotice sends one lane.event line per hit mechanism, at most
// one notice per mechanism per day; several reasons for one mechanism share
// its one line.
func (h *HubServer) maybeSendLoopsNotice(ctx context.Context) {
	if h == nil {
		return
	}
	now := h.now().UTC()
	date := now.Format("2006-01-02")
	byMechanism := map[loopMechanism][]string{}
	for _, hit := range h.loopNoticeHits(now) {
		byMechanism[hit.Mechanism] = append(byMechanism[hit.Mechanism], hit.Reason)
	}
	for _, m := range loopMechanisms {
		reasons := byMechanism[m]
		if len(reasons) == 0 {
			continue
		}
		key := string(m) + "|" + date
		st := h.loopLedger()
		st.mu.Lock()
		sent := st.noticeSent[key] == date
		st.mu.Unlock()
		if sent {
			continue
		}
		text := fmt.Sprintf("loop %s: %s (machine hub, day %s)", m, strings.Join(reasons, "; "), date)
		event := hubJobEventPayload{
			EventID: "loops:" + string(m) + ":" + date,
			JobID:   string(m), OwnerLane: h.loopsNoticeLane(),
			NotifyType: "info", Text: utf8PrefixWithin(text, laneEventTextLimit), Host: "hub", Epoch: 1,
		}
		result := h.relayLaneEventContext(ctx, event, nil)
		if result.RejectedTooLong || result.PersistFailed {
			h.logger.Warn("loops notice not persisted; retrying on the next sweep", "mechanism", string(m), "lane", h.loopsNoticeLane())
			continue
		}
		st.mu.Lock()
		st.noticeSent[key] = date
		st.mu.Unlock()
	}
}
