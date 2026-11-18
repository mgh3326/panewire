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
// One counting discipline (per panewire-design decision on PR #675): each
// mechanism is flow or scan.
//   - flow: items with an identity enter and leave. made = an item seen for
//     the first time that day (its entry edge, deduped by id within the
//     day); done = the item's exit transition that day (terminal, sent or
//     cleared). The progress rule applies only to flow mechanisms.
//   - scan: a periodic pass with no item lifecycle. Ticks and errors only;
//     no progress rule.
// Every rule reads the current UTC date's bucket. Lifetime counters are not
// kept: at each date change the new day starts empty.
//
// Made and done per mechanism:
//   - spawn_coordinator (flow): pending spawn_operations rows (by request
//     id, first sighting per day) -> rows handed to a worker or a node
//     dispatch, or a start-condition release, give-up, refusal or
//     cancellation.
//   - consult_coordinator (flow): active consults (by consult id) ->
//     answered, timed out, failed or respawned.
//   - lane_stall_drain_wake (flow): owed drain lines (by lane, on the edge
//     into owed) -> lines sent or cleared.
//   - inbox (flow): batches (by batch id) and wake lanes (by lane) ->
//     batches marked sent, wake lines sent.
//   - quota_ladder (flow): quota successors (by job) and expired ladder
//     stages (by request id) -> hk records written, stages activated,
//     discarded or cancelled.
//   - pool_triggers (scan): ticks and errors only.
//   - pool_exhausted_sweep (flow): jobs on an exhausted pool (by job) ->
//     jobs notified or queued.
//   - pool_actions (flow): pool_actions rows (by action id) -> actions
//     reconciled to final.
//   - placement_records (flow): placement snapshots (by request and
//     attempt) -> hk placement records delivered, sealed or quarantined.
//   - fleet_status (scan): ticks and errors only.
//   - pane_sweep (scan): ticks and errors only.
//   - reviewer_end (scan): ticks and errors only.
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
	loopPoolTriggers       loopMechanism = "pool_triggers"
	loopPoolExhaustedSweep loopMechanism = "pool_exhausted_sweep"
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
	loopPoolExhaustedSweep,
	loopPoolActions,
	loopPlacementRecords,
	loopFleetStatus,
	loopPaneSweep,
	loopReviewerEnd,
}

type loopKind int

const (
	loopKindFlow loopKind = iota
	loopKindScan
)

// loopKindOf reports whether the mechanism counts items (flow) or only
// passes (scan). The progress rule applies to flow mechanisms only.
func loopKindOf(m loopMechanism) loopKind {
	switch m {
	case loopPoolTriggers, loopFleetStatus, loopPaneSweep, loopReviewerEnd:
		return loopKindScan
	default:
		return loopKindFlow
	}
}

// Notice thresholds live here and nowhere else.
const (
	// loopStaleIntervals fires when last_tick is older than this many
	// intervals for the mechanism.
	loopStaleIntervals = 3
	// loopMadeFloor and loopDoneRatio fire when the day's made >= floor
	// and (done == 0 or done/made < ratio).
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

// loopDayBucket is one UTC date's counts for one mechanism. seen dedupes
// flow items within the day, so a steady population counts once no matter
// how many passes observe it.
type loopDayBucket struct {
	loopStat
	seen map[string]bool
}

// loopLedgerState is the HubServer's loop bookkeeping. Guarded by loopsMu,
// never by HubServer.mu, so a ledger call inside a store callback cannot
// deadlock a Sweep that already holds mu.
type loopLedgerState struct {
	mu   sync.Mutex
	days map[string]map[loopMechanism]*loopDayBucket
	// silentStreak records the date each silent path last counted on.
	// Streaks are per date: an outage that is still failing after UTC
	// midnight counts and logs again on the new date, so no day goes
	// dark while its failures continue.
	silentStreak  map[string]string
	noticeSent    map[string]string
	rowRetryAfter time.Time
	// lastTick is each mechanism's most recent tick across process
	// lifetime. Liveness is not a daily counter: a date roll must not
	// reset it, or the first sweep of the day would report every slow
	// loop as stalled and burn its daily notice.
	lastTick map[loopMechanism]time.Time
}

func newLoopLedgerState() *loopLedgerState {
	return &loopLedgerState{
		days:         make(map[string]map[loopMechanism]*loopDayBucket),
		silentStreak: make(map[string]string),
		noticeSent:   make(map[string]string),
		lastTick:     make(map[loopMechanism]time.Time),
	}
}

// loopDateOf names the UTC date a timestamp belongs to.
func loopDateOf(t time.Time) string { return t.UTC().Format("2006-01-02") }

func (h *HubServer) loopLedger() *loopLedgerState {
	h.loopsMu.Lock()
	defer h.loopsMu.Unlock()
	if h.loops == nil {
		h.loops = newLoopLedgerState()
	}
	return h.loops
}

// loopNow reports the hub clock. A ledger call on a hub without one (a
// lightweight test fixture with no time source) reports false, and the
// caller skips the ledger rather than panicking.
func (h *HubServer) loopNow() (time.Time, bool) {
	if h == nil || h.now == nil {
		return time.Time{}, false
	}
	return h.now().UTC(), true
}

func (s *loopLedgerState) bucketFor(date string, m loopMechanism) *loopDayBucket {
	byDate, ok := s.days[date]
	if !ok {
		byDate = make(map[loopMechanism]*loopDayBucket)
		s.days[date] = byDate
	}
	b, ok := byDate[m]
	if !ok {
		b = &loopDayBucket{seen: make(map[string]bool)}
		byDate[m] = b
	}
	return b
}

func (s *loopLedgerState) bucketAt(date string, m loopMechanism) loopStat {
	if byDate, ok := s.days[date]; ok {
		if b, ok := byDate[m]; ok {
			return b.loopStat
		}
	}
	return loopStat{}
}

// loopTick records one pass of a mechanism at the hub's clock. Every pass
// ticks at its top, including passes that find nothing to do: the tick is
// the liveness signal.
func (h *HubServer) loopTick(m loopMechanism) {
	now, ok := h.loopNow()
	if !ok {
		return
	}
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	b := st.bucketFor(loopDateOf(now), m)
	b.Ticks++
	b.LastTick = now
	st.lastTick[m] = now
}

// loopNoteMade records a flow item's entry edge once per UTC date, deduped
// by id within the day. A steady population observed on every pass counts
// once no matter how many passes observe it.
func (h *HubServer) loopNoteMade(m loopMechanism, id string) {
	if id == "" {
		return
	}
	now, ok := h.loopNow()
	if !ok {
		return
	}
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	b := st.bucketFor(loopDateOf(now), m)
	key := "m:" + id
	if b.seen[key] {
		return
	}
	b.seen[key] = true
	b.Made++
}

// loopNoteDone records a flow item's exit transition once per UTC date,
// deduped by id within the day.
func (h *HubServer) loopNoteDone(m loopMechanism, id string) {
	if id == "" {
		return
	}
	now, ok := h.loopNow()
	if !ok {
		return
	}
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	b := st.bucketFor(loopDateOf(now), m)
	key := "d:" + id
	if b.seen[key] {
		return
	}
	b.seen[key] = true
	b.Done++
}

// loopSetOldestPending records the oldest pending age a pass observed.
func (h *HubServer) loopSetOldestPending(m loopMechanism, age time.Duration) {
	if age < 0 {
		return
	}
	now, ok := h.loopNow()
	if !ok {
		return
	}
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	st.bucketFor(loopDateOf(now), m).OldestPending = age
}

// loopNoteError counts a visible error for a mechanism in today's bucket.
func (h *HubServer) loopNoteError(m loopMechanism) {
	now, ok := h.loopNow()
	if !ok {
		return
	}
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	st.bucketFor(loopDateOf(now), m).Errors++
}

// loopNoteSilentError counts a previously silent error path once per UTC
// date per failure streak, and logs one line per date per streak: every
// later failure with the same key counts and logs nothing until the date
// changes or loopClearSilent retires the streak with a success, so a
// latched outage cannot flood one day's counters while no later day goes
// dark while its failures continue.
func (h *HubServer) loopNoteSilentError(m loopMechanism, key string, err error) {
	now, ok := h.loopNow()
	if !ok {
		return
	}
	today := loopDateOf(now)
	st := h.loopLedger()
	st.mu.Lock()
	counted := st.silentStreak[key] == today
	if !counted {
		st.silentStreak[key] = today
		b := st.bucketFor(today, m)
		b.Errors++
		b.SilentErrors++
	}
	st.mu.Unlock()
	if counted {
		return
	}
	if err != nil {
		h.logger.Warn("hub loop silent error; retrying", "mechanism", string(m), "path", key, "error", err)
	} else {
		h.logger.Warn("hub loop skipped; retrying", "mechanism", string(m), "path", key)
	}
}

// loopClearSilent retires one silent streak after a success, so the next
// failure counts and logs again.
func (h *HubServer) loopClearSilent(key string) {
	if h == nil {
		return
	}
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.silentStreak, key)
}

// loopSnapshot copies today's bucket for the row writer and the notice
// check. Tests assert on it.
func (h *HubServer) loopSnapshot() map[loopMechanism]loopStat {
	now, ok := h.loopNow()
	date := ""
	if ok {
		date = loopDateOf(now)
	}
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	out := make(map[loopMechanism]loopStat, len(loopMechanisms))
	for _, m := range loopMechanisms {
		out[m] = st.bucketAt(date, m)
	}
	return out
}

// loopInterval is the mechanism's expected tick interval. A non-positive
// interval means the loop is configured off; loopsStaleHits skips it.
// Stale detection compares now-last_tick against three intervals.
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
	case loopPoolExhaustedSweep:
		return 0
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

// loopsRowDocument builds the row for date from that date's bucket. Every
// row carries its own machine so hk 5117 can append per-node rows beside
// these.
func (h *HubServer) loopsRowDocument(date string, now time.Time) loopsRowDoc {
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	rows := make([]loopsRowEntry, 0, len(loopMechanisms))
	for _, m := range loopMechanisms {
		s := st.bucketAt(date, m)
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

// loopsRowRetryAfter bounds how often an unwritten row is retried: neither
// an unreadable row nor a failed write may hold the maintenance tick on
// every pass while hk is unreachable.
const loopsRowRetryAfter = time.Minute

// maybeWriteLoopsRow writes one row per UTC date, each from that date's
// bucket, at the first tick of the next date. It is called from the hub's
// slow sweeps. A failed write or an unreadable row counts in the ledger
// (against fleet_status, the hub's other hk writer) and backs off for a
// minute before the next attempt.
func (h *HubServer) maybeWriteLoopsRow(ctx context.Context) {
	now, ok := h.loopNow()
	if !ok {
		return
	}
	today := loopDateOf(now)
	st := h.loopLedger()
	st.mu.Lock()
	var pending []string
	for date := range st.days {
		if date >= today {
			continue
		}
		pending = append(pending, date)
	}
	sort.Strings(pending)
	if len(pending) == 0 || now.Before(st.rowRetryAfter) {
		st.mu.Unlock()
		return
	}
	st.mu.Unlock()
	for _, date := range pending {
		exists, unknown := h.loopsRowState(ctx, loopsRowKey(date))
		if unknown {
			h.loopNoteError(loopFleetStatus)
			h.logger.Warn("hub loops row unreadable; retrying", "key", loopsRowKey(date))
			h.loopBackoffRowRetry(now)
			return
		}
		if !exists {
			body, err := json.Marshal(h.loopsRowDocument(date, now))
			if err != nil {
				h.loopNoteError(loopFleetStatus)
				return
			}
			if err := h.loopsPublish(ctx, loopsRowKey(date), string(body)); err != nil {
				h.loopNoteError(loopFleetStatus)
				h.logger.Warn("hub loops row not written; retrying", "key", loopsRowKey(date), "error", err)
				h.loopBackoffRowRetry(now)
				return
			}
		}
		st.mu.Lock()
		delete(st.days, date)
		st.mu.Unlock()
	}
}

// loopBackoffRowRetry holds the next row read and write until now plus one
// minute, so an hk outage cannot hold the maintenance tick on every pass.
func (h *HubServer) loopBackoffRowRetry(now time.Time) {
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	st.rowRetryAfter = now.Add(loopsRowRetryAfter)
}

// loopsStaleHits returns the mechanisms whose last tick is older than three
// intervals at now. Liveness is process-lifetime, not bucketed: a mechanism
// that ticked yesterday is alive today until three intervals pass without a
// tick. A mechanism that never ticked is stale once the hub has been up
// longer than three intervals. A disabled interval (<= 0) is never stale.
func (h *HubServer) loopsStaleHits(now time.Time) []loopMechanism {
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	var hits []loopMechanism
	for _, m := range loopMechanisms {
		interval := h.loopInterval(m)
		if interval <= 0 {
			continue
		}
		lastTick := st.lastTick[m]
		if lastTick.IsZero() {
			if !h.startedAt.IsZero() && now.Sub(h.startedAt) > loopStaleIntervals*interval {
				hits = append(hits, m)
			}
			continue
		}
		if now.Sub(lastTick) > loopStaleIntervals*interval {
			hits = append(hits, m)
		}
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i] < hits[j] })
	return hits
}

// loopsProgressHits returns the flow mechanisms whose current date counts
// show made >= floor with nothing or nearly nothing finished: done == 0,
// or done/made < ratio. Scan mechanisms have no item lifecycle and are
// never judged by this rule.
func (h *HubServer) loopsProgressHits() []loopMechanism {
	snap := h.loopSnapshot()
	var hits []loopMechanism
	for _, m := range loopMechanisms {
		if loopKindOf(m) != loopKindFlow {
			continue
		}
		s := snap[m]
		if s.Made < loopMadeFloor {
			continue
		}
		if s.Done == 0 || float64(s.Done)/float64(s.Made) < loopDoneRatio {
			hits = append(hits, m)
		}
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i] < hits[j] })
	return hits
}

// loopsSilentHits returns the mechanisms with at least one previously
// silent error counted today.
func (h *HubServer) loopsSilentHits() []loopMechanism {
	now, ok := h.loopNow()
	if !ok {
		return nil
	}
	date := loopDateOf(now)
	st := h.loopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	var hits []loopMechanism
	for _, m := range loopMechanisms {
		if st.bucketAt(date, m).SilentErrors >= 1 {
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
// its one line. A failed persist is not marked sent, so the next sweep
// retries it.
func (h *HubServer) maybeSendLoopsNotice(ctx context.Context) {
	now, ok := h.loopNow()
	if !ok {
		return
	}
	date := loopDateOf(now)
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
