// Node loop ledger (hk 5117, track A2). Each node keeps the hub's discipline
// (hub_loop_ledger.go, docs/loops.md): flow or scan, day buckets, ticks at
// the top of every pass, and silent errors counted. The node sends its
// current-day bucket on the heartbeat (loops field, hubFeatureNodeLoops); the
// hub merges the rows into the same daily doc, one row set per machine.
//
// Counting discipline, shared with the hub:
//   - flow: items with an identity enter and leave. made = an item seen for
//     the first time that day (its entry edge, deduped by id within the day);
//     done = the item's exit transition that day. The progress rule applies to
//     flow mechanisms only.
//   - scan: a periodic pass with no item lifecycle. Ticks and errors only;
//     made/done calls on a scan mechanism are ignored, so a miscounted edge
//     cannot age into a progress notice.
// Every rule the hub applies reads the report's date bucket. Code that needs
// the current time uses client.nowUTC(); elapsed waits stay on the wall clock.

package panewire

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"sync"
	"time"
)

// hubFeatureNodeLoops is listed in hub.features by a hub that reads the
// heartbeat's loops ledger. The heartbeat is a closed payload, so an older
// hub rejects a field it does not know; the node sends the ledger only to a
// hub that advertised it (hk 5117). The hub half of the contract lives in
// hub_loop_ledger.go; the row format is docs/loops.md.
const hubFeatureNodeLoops = "node_loops"

type nodeLoopMechanism string

const (
	nodeTaskPaneSweep      nodeLoopMechanism = "task_pane_sweep"
	nodeWorktreeFlow       nodeLoopMechanism = "worktree_flow"
	nodeQuotaMeasure       nodeLoopMechanism = "quota_measure"
	nodeJobDirTTL          nodeLoopMechanism = "job_dir_ttl"
	nodeQuotaPoolReconcile nodeLoopMechanism = "quota_pool_reconcile"
	nodeLaneReconcile      nodeLoopMechanism = "lane_reconcile"
	nodeSpawnSettle        nodeLoopMechanism = "spawn_settle"
	nodeCreditRearm        nodeLoopMechanism = "credit_rearm"
	nodeHubCloseSettle     nodeLoopMechanism = "hub_close_settle"
)

// nodeLoopMechanisms is the closed row list: the heartbeat carries exactly
// these rows in this order.
var nodeLoopMechanisms = []nodeLoopMechanism{
	nodeTaskPaneSweep,
	nodeWorktreeFlow,
	nodeQuotaMeasure,
	nodeJobDirTTL,
	nodeQuotaPoolReconcile,
	nodeLaneReconcile,
	nodeSpawnSettle,
	nodeCreditRearm,
	nodeHubCloseSettle,
}

type nodeLoopKind int

const (
	nodeLoopKindFlow nodeLoopKind = iota
	nodeLoopKindScan
)

// nodeLoopKindOf reports whether the mechanism counts items (flow) or only
// passes (scan). The progress rule applies to flow mechanisms only.
func nodeLoopKindOf(m nodeLoopMechanism) nodeLoopKind {
	switch m {
	case nodeTaskPaneSweep, nodeQuotaMeasure, nodeJobDirTTL, nodeLaneReconcile, nodeCreditRearm:
		return nodeLoopKindScan
	default:
		return nodeLoopKindFlow
	}
}

// nodeLoopInterval is the mechanism's expected tick interval. Zero means the
// loop has no periodic cadence or is configured off, and the hub's stale rule
// skips it: spawn settle runs at node start and while rows remain, and quota
// measurement runs only while --state-publish sets pools.
func (client *HubClient) nodeLoopInterval(m nodeLoopMechanism) time.Duration {
	switch m {
	case nodeTaskPaneSweep, nodeWorktreeFlow:
		return client.taskPaneSweepInterval
	case nodeQuotaMeasure:
		if !client.statePublish {
			return 0
		}
		if client.stateQuotaInterval > 0 {
			return client.stateQuotaInterval
		}
		return defaultFleetQuotaInterval
	case nodeJobDirTTL:
		return jobDirTTLEvery
	case nodeQuotaPoolReconcile:
		return quotaPoolReconcileInterval
	case nodeLaneReconcile:
		return laneReconcileInterval
	case nodeSpawnSettle:
		return 0
	case nodeCreditRearm:
		return creditRearmInterval
	case nodeHubCloseSettle:
		return hubCloseSettleInterval
	default:
		return 0
	}
}

type nodeLoopStat struct {
	Ticks         int64
	Made          int64
	Done          int64
	Errors        int64
	SilentErrors  int64
	LastTick      time.Time
	OldestPending time.Duration
}

// nodeLoopDayBucket is one UTC date's counts for one mechanism. seen dedupes
// flow items within the day, so a steady population counts once no matter how
// many passes observe it.
type nodeLoopDayBucket struct {
	nodeLoopStat
	seen map[string]bool
}

// nodeLoopLedgerState is the HubClient's loop bookkeeping. Guarded by
// nodeLoopsMu, never by nodeCommandMu, so a ledger call inside a sweep that
// holds the command lock cannot deadlock it.
type nodeLoopLedgerState struct {
	mu    sync.Mutex
	epoch string
	days  map[string]map[nodeLoopMechanism]*nodeLoopDayBucket
	// silentStreak records the date each silent path last counted on.
	silentStreak map[string]string
	lastTick     map[nodeLoopMechanism]time.Time
}

func newNodeLoopLedgerState() *nodeLoopLedgerState {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return &nodeLoopLedgerState{
		epoch:        hex.EncodeToString(b),
		days:         make(map[string]map[nodeLoopMechanism]*nodeLoopDayBucket),
		silentStreak: make(map[string]string),
		lastTick:     make(map[nodeLoopMechanism]time.Time),
	}
}

func (client *HubClient) nodeLoopLedger() *nodeLoopLedgerState {
	client.nodeLoopsMu.Lock()
	defer client.nodeLoopsMu.Unlock()
	if client.nodeLoops == nil {
		client.nodeLoops = newNodeLoopLedgerState()
	}
	return client.nodeLoops
}

func (s *nodeLoopLedgerState) bucketFor(date string, m nodeLoopMechanism) *nodeLoopDayBucket {
	for d := range s.days {
		if d < date {
			delete(s.days, d)
		}
	}
	byDate, ok := s.days[date]
	if !ok {
		byDate = make(map[nodeLoopMechanism]*nodeLoopDayBucket)
		s.days[date] = byDate
	}
	b, ok := byDate[m]
	if !ok {
		b = &nodeLoopDayBucket{seen: make(map[string]bool)}
		byDate[m] = b
	}
	return b
}

// nodeLoopTick records one pass of a mechanism at the node's clock. Every
// pass ticks at its top, including passes that find nothing to do: the tick is
// the liveness signal.
func (client *HubClient) nodeLoopTick(m nodeLoopMechanism) {
	now := client.nowUTC()
	st := client.nodeLoopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	date := loopDateOf(now)
	b := st.bucketFor(date, m)
	b.Ticks++
	b.LastTick = now
	st.lastTick[m] = now
}

// nodeLoopNoteMade records a flow item's entry edge once per UTC date, deduped
// by id within the day. Calls on a scan mechanism are ignored: a scan pass
// has no item lifecycle, so there is no made edge to count.
func (client *HubClient) nodeLoopNoteMade(m nodeLoopMechanism, id string) {
	if nodeLoopKindOf(m) != nodeLoopKindFlow {
		return
	}
	now := client.nowUTC()
	st := client.nodeLoopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	b := st.bucketFor(loopDateOf(now), m)
	if b.seen[id] {
		return
	}
	b.seen[id] = true
	b.Made++
}

// nodeLoopNoteDone records a flow item's exit transition once per UTC date,
// deduped by id within the day. Calls on a scan mechanism are ignored.
func (client *HubClient) nodeLoopNoteDone(m nodeLoopMechanism, id string) {
	if nodeLoopKindOf(m) != nodeLoopKindFlow {
		return
	}
	now := client.nowUTC()
	st := client.nodeLoopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	b := st.bucketFor(loopDateOf(now), m)
	if b.seen["done\x00"+id] {
		return
	}
	b.seen["done\x00"+id] = true
	b.Done++
}

// nodeLoopSetOldestPending records the oldest pending age a pass observed.
func (client *HubClient) nodeLoopSetOldestPending(m nodeLoopMechanism, age time.Duration) {
	now := client.nowUTC()
	st := client.nodeLoopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	b := st.bucketFor(loopDateOf(now), m)
	if age > b.OldestPending {
		b.OldestPending = age
	}
}

// nodeLoopNoteError counts a visible error for a mechanism in today's bucket.
func (client *HubClient) nodeLoopNoteError(m nodeLoopMechanism) {
	now := client.nowUTC()
	st := client.nodeLoopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	st.bucketFor(loopDateOf(now), m).Errors++
}

// nodeLoopNoteSilentError counts a previously silent error path once per UTC
// date per failure streak, and logs one line per date per streak.
func (client *HubClient) nodeLoopNoteSilentError(m nodeLoopMechanism, key string, err error) {
	now := client.nowUTC()
	st := client.nodeLoopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	date := loopDateOf(now)
	if st.silentStreak[key] == date {
		return
	}
	st.silentStreak[key] = date
	b := st.bucketFor(date, m)
	b.Errors++
	b.SilentErrors++
	slog.Warn("node loop silent error", "mechanism", string(m), "key", key, "error", err)
}

// nodeLoopClearSilent retires one silent streak after a success, so the next
// failure counts and logs again.
func (client *HubClient) nodeLoopClearSilent(key string) {
	st := client.nodeLoopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.silentStreak, key)
}

// nodeLoopSnapshot copies today's bucket for tests.
func (client *HubClient) nodeLoopSnapshot() map[nodeLoopMechanism]nodeLoopStat {
	now := client.nowUTC()
	date := loopDateOf(now)
	st := client.nodeLoopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	out := make(map[nodeLoopMechanism]nodeLoopStat, len(nodeLoopMechanisms))
	for _, m := range nodeLoopMechanisms {
		if byDate, ok := st.days[date]; ok {
			if b, ok := byDate[m]; ok {
				out[m] = b.nodeLoopStat
			}
		}
	}
	return out
}

// hubNodeLoopsPayload is the heartbeat's optional loops field: the node's
// current-day bucket. Absent means the node predates the ledger; the hub
// shows such a machine as unknown and raises no notice for it.
type hubNodeLoopsPayload struct {
	Date  string           `json:"date"`
	Epoch string           `json:"epoch,omitempty"`
	Rows  []hubNodeLoopRow `json:"rows"`
}

// hubNodeLoopRow is one mechanism's current-day counts on the heartbeat. It
// mirrors loopsRowEntry without the machine: the hub fills that in from the
// authenticated sender.
type hubNodeLoopRow struct {
	Mechanism      string `json:"mechanism"`
	IntervalS      int64  `json:"interval_s"`
	Ticks          int64  `json:"ticks"`
	Made           int64  `json:"made"`
	Done           int64  `json:"done"`
	Errors         int64  `json:"errors"`
	SilentErrors   int64  `json:"silent_errors"`
	LastTick       string `json:"last_tick"`
	AgeS           int64  `json:"age_s"`
	OldestPendingS int64  `json:"oldest_pending_s"`
}

// nodeLoopsForHeartbeat copies the current UTC date's bucket in mechanism
// order for the heartbeat. The tick column is the liveness signal, so a row
// whose bucket is still empty after midnight carries the mechanism's most
// recent tick across process lifetime: without the fallback every interval
// mechanism would read stale from midnight until its first pass.
func (client *HubClient) nodeLoopsForHeartbeat() *hubNodeLoopsPayload {
	now := client.nowUTC()
	date := loopDateOf(now)
	st := client.nodeLoopLedger()
	st.mu.Lock()
	defer st.mu.Unlock()
	for d := range st.days {
		if d < date {
			delete(st.days, d)
		}
	}
	rows := make([]hubNodeLoopRow, 0, len(nodeLoopMechanisms))
	for _, m := range nodeLoopMechanisms {
		var s nodeLoopStat
		if byDate, ok := st.days[date]; ok {
			if b, ok := byDate[m]; ok {
				s = b.nodeLoopStat
			}
		}
		lastTick := s.LastTick
		if lastTick.IsZero() {
			lastTick = st.lastTick[m]
		}
		last := ""
		ageS := int64(-1)
		if !lastTick.IsZero() {
			last = lastTick.UTC().Format(time.RFC3339)
			diff := now.Sub(lastTick)
			if diff >= 0 {
				ageS = int64(diff / time.Second)
			} else {
				ageS = 0
			}
		}
		rows = append(rows, hubNodeLoopRow{
			Mechanism:      string(m),
			IntervalS:      int64(client.nodeLoopInterval(m) / time.Second),
			Ticks:          s.Ticks,
			Made:           s.Made,
			Done:           s.Done,
			Errors:         s.Errors,
			SilentErrors:   s.SilentErrors,
			LastTick:       last,
			AgeS:           ageS,
			OldestPendingS: int64(s.OldestPending / time.Second),
		})
	}
	return &hubNodeLoopsPayload{Date: date, Epoch: st.epoch, Rows: rows}
}

// nodeLoopMechanismKnown reports whether name is one of the closed node
// mechanism list. The hub rejects any other row: a closed payload keeps a
// broken node from writing a row no notice reader can use.
func nodeLoopMechanismKnown(name string) bool {
	for _, m := range nodeLoopMechanisms {
		if string(m) == name {
			return true
		}
	}
	return false
}

// decodeHubNodeLoops validates the heartbeat's loops field. Null reads as
// absent only by omission: an explicit null, like ended_panes, rejects the
// heartbeat, so null must never read as zero activity.
func decodeHubNodeLoops(raw json.RawMessage) (*hubNodeLoopsPayload, bool) {
	if isJSONNull(raw) {
		return nil, false
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || (len(fields) != 2 && len(fields) != 3) {
		return nil, false
	}
	for name := range fields {
		if name != "date" && name != "rows" && name != "epoch" {
			return nil, false
		}
	}
	rawDate, ok := fields["date"]
	if !ok {
		return nil, false
	}
	rawRows, ok := fields["rows"]
	if !ok {
		return nil, false
	}
	var date string
	if json.Unmarshal(rawDate, &date) != nil {
		return nil, false
	}
	if parsed, err := time.Parse("2006-01-02", date); err != nil || parsed.Format("2006-01-02") != date {
		return nil, false
	}
	var epoch string
	if rawEpoch, ok := fields["epoch"]; ok {
		if json.Unmarshal(rawEpoch, &epoch) != nil {
			return nil, false
		}
	}
	var rows []map[string]json.RawMessage
	if json.Unmarshal(rawRows, &rows) != nil || len(rows) == 0 || len(rows) > len(nodeLoopMechanisms) {
		return nil, false
	}
	seen := make(map[string]bool, len(rows))
	out := make([]hubNodeLoopRow, 0, len(rows))
	for _, item := range rows {
		if len(item) != 9 && len(item) != 10 {
			return nil, false
		}
		var row hubNodeLoopRow
		row.AgeS = -1
		encoded, _ := json.Marshal(item)
		if json.Unmarshal(encoded, &row) != nil {
			return nil, false
		}
		for name := range item {
			switch name {
			case "mechanism", "interval_s", "ticks", "made", "done", "errors", "silent_errors", "last_tick", "oldest_pending_s", "age_s":
			default:
				return nil, false
			}
		}
		if !nodeLoopMechanismKnown(row.Mechanism) || seen[row.Mechanism] {
			return nil, false
		}
		seen[row.Mechanism] = true
		if row.IntervalS < 0 || row.Ticks < 0 || row.Made < 0 || row.Done < 0 || row.Errors < 0 || row.SilentErrors < 0 || row.OldestPendingS < 0 || row.AgeS < -1 {
			return nil, false
		}
		if row.Ticks > 1<<40 || row.Made > 1<<40 || row.Done > 1<<40 || row.Errors > 1<<40 || row.SilentErrors > 1<<40 {
			return nil, false
		}
		if row.LastTick != "" {
			if _, err := time.Parse(time.RFC3339, row.LastTick); err != nil {
				return nil, false
			}
		}
		out = append(out, row)
	}
	return &hubNodeLoopsPayload{Date: date, Epoch: epoch, Rows: out}, true
}

// nodeLoopsForPeer returns the heartbeat's loops field, or nil while the hub
// has not advertised hubFeatureNodeLoops: an older hub rejects a field it
// does not know, so the ledger is never sent to one.
func (client *HubClient) nodeLoopsForPeer() *hubNodeLoopsPayload {
	peer := client.signalsPeer.Load()
	if peer == nil || !peer.acceptsNodeLoops.Load() {
		return nil
	}
	return client.nodeLoopsForHeartbeat()
}
