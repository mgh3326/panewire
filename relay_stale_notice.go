package panewire

// hk 6924: a real message the replay gate retires as stale (relayReplayMaxAge
// passed before a restart or node hello could type it) may be one its lane
// never saw. Most are superseded: the lane's task merged soon after the
// message arrived. The hub keeps a daily account of the retired rows and, once
// a day closes, files ONE inbox item per owning design lane. The item lists the
// rows the task did not overtake; the rows it did overtake are counted in its
// footer. The contract is in docs/relay-harness-delivery.md ("Stale replay
// notice").

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// staleReplayOvertakeWindow is the cut-off set by panewire-design: a row is
	// overtaken when its lane's task ended within this long after the row's
	// receipt.
	staleReplayOvertakeWindow = 6 * time.Hour
	// staleReplayListLimit bounds the rows one notice names; the rest are
	// counted.
	staleReplayListLimit = 10
	// staleReplayMoreReserve is the room kept for the "+N more not listed." count
	// at the end of a notice, so the count and the overtaken footer always fit.
	staleReplayMoreReserve = 32
	// staleReplayNoticeInterval is how often the filing pass looks for a closed
	// day.
	staleReplayNoticeInterval = 10 * time.Minute
	// staleReplayFiledKeep is how long a filed day's rows are kept for audit.
	staleReplayFiledKeep = 14 * 24 * time.Hour
	// staleReplayTaskDeadline bounds one hk task read.
	staleReplayTaskDeadline = 5 * time.Second
)

// staleReplaySpawn is what the notice reads of a worker job's spawn record: the
// design lane that owns the job, the hk task it was spawned for, and when the
// spawn was made.
type staleReplaySpawn struct {
	owner, task string
	created     time.Time
}

// staleReplayTaskRead is the hk task fields the overtake check reads. hk sets
// completed_at when a task enters the completed category (merged, deployed,
// completed) and canceled_at when it enters the canceled category (dropped,
// duplicate, icebox); a later merged -> deployed edit keeps completed_at, while
// updated_at moves with every edit and is not read here.
type staleReplayTaskRead struct {
	ID          int64      `json:"id"`
	State       string     `json:"state"`
	CompletedAt *time.Time `json:"completed_at"`
	CanceledAt  *time.Time `json:"canceled_at"`
}

// staleReplayDay is a notice day's UTC date.
func staleReplayDay(t time.Time) string { return t.UTC().Format(time.DateOnly) }

// noteStaleReplayRetire keeps a real message the replay gate retired as stale for
// the day's notice. It reports false when the row could not be kept (the store or
// the job lookup failed), so the caller leaves the handoffkeep row undelivered for
// a later replay; true otherwise, including for a row that needs no notice.
// Idle-wake reports are not messages a lane waits for, so they are not kept.
func (h *HubServer) noteStaleReplayRetire(record handoffkeepRelayEvent, reason string) bool {
	if reason != "stale" || h.inbox == nil || h.inbox.store == nil {
		return true
	}
	if record.Kind == "lane.event" {
		if _, idle := decodeIdleWakeRelayText(record.Text); idle {
			return true
		}
	}
	// The gate retires a stale row only when handoffkeep stamped its receipt, so
	// the parse cannot fail on this path.
	received, err := time.Parse(time.RFC3339Nano, record.ReceivedAt)
	if err != nil {
		return true
	}
	ctx := context.Background()
	now := h.now().UTC()
	job, err := h.staleReplayJobOf(ctx, record, received)
	if err != nil {
		h.logger.Warn("stale replay row job could not be read; it stays undelivered for a later replay", "event_id", record.ID, "lane", record.OwnerLane)
		return false
	}
	spawn, found, err := h.staleReplaySpawnOf(ctx, job)
	if err != nil {
		h.logger.Warn("stale replay row owner could not be read; it stays undelivered for a later replay", "event_id", record.ID, "lane", record.OwnerLane, "job", job)
		return false
	}
	row := staleReplayRow{
		RelayID: record.ID, NoticeDay: staleReplayDay(now), WorkerLane: record.OwnerLane, Job: job,
		Kind: record.Kind, Sender: staleReplaySender(record.Text), HKEventID: record.EventID,
		ReceivedMS: received.UnixMilli(), RetiredMS: now.UnixMilli(),
	}
	switch {
	case found && !h.inbox.policy.designLane(spawn.owner):
		// The job belongs to a lane no design lane reads; there is no design
		// notice for it.
		h.logger.Info("stale replay row belongs to a job with no design owner; left out of the daily notice", "event_id", record.ID, "lane", record.OwnerLane, "job", job)
		return true
	case found:
		row.DesignLane, row.Task = spawn.owner, spawn.task
	default:
		// No design owner can be named. The row is listed on the hub's notice
		// lane rather than dropped.
		row.DesignLane = h.loopsNoticeLane()
		h.logger.Warn("stale replay row could not be resolved to a design owner; listed on the hub notice lane", "event_id", record.ID, "lane", record.OwnerLane, "job", job)
	}
	if err := h.inbox.store.insertStaleReplayRow(ctx, row); err != nil {
		h.logger.Warn("stale replay row was not kept for the daily notice; it stays undelivered for a later replay", "event_id", record.ID, "lane", record.OwnerLane)
		return false
	}
	return true
}

// staleReplayJobOf names the worker job a retired row belongs to, from durable
// state first. A job.* row carries its job. A lane.event row takes the job its
// lane was bound to when the row arrived (noteStaleReplayLane). A row received
// before any binding was kept falls back to the lanes.json route, but only as a
// hint: the route's job must have been spawned before the row arrived, so a lane
// taken over later does not pass its job to an older row.
func (h *HubServer) staleReplayJobOf(ctx context.Context, record handoffkeepRelayEvent, received time.Time) (string, error) {
	if strings.HasPrefix(record.Kind, "job.") {
		return record.JobID, nil
	}
	job, bound, err := h.inbox.store.staleReplayJobAt(ctx, record.OwnerLane, received.UnixMilli())
	if err != nil || bound {
		return job, err
	}
	hint := loadReportRelayRoutes(h.reportRelayPath)[record.OwnerLane].Job
	if hint == "" {
		return "", nil
	}
	spawn, found, err := h.staleReplaySpawnOf(ctx, hint)
	if err != nil || !found || spawn.created.After(received) {
		return "", err
	}
	return hint, nil
}

// noteStaleReplayLane binds a worker lane to the job its lanes.json route names
// at a receipt, so a row retired after the route is gone (releaseDeadLanes, reap
// or a closed pane removes it) still names the job it was received for. The
// route is read from the file, not from the routing decision: a lane whose node
// is down gets no routed target, and its rows are the ones that wait longest. It
// is best effort: a failed write leaves that row to the lanes.json hint.
func (h *HubServer) noteStaleReplayLane(lane string) {
	if h.inbox == nil || h.inbox.store == nil {
		return
	}
	job := loadReportRelayRoutes(h.reportRelayPath)[lane].Job
	if job == "" {
		return
	}
	if err := h.inbox.store.insertStaleReplayLane(context.Background(), lane, job, h.now().UnixMilli()); err != nil {
		h.logger.Warn("stale replay lane binding was not kept", "lane", lane, "job", job)
	}
}

// staleReplaySpawnOf reads a worker job's spawn record from the hub's durable
// spawn store: the design owner and the task the job was spawned for. An empty
// job, or a job with no spawn record or no owner, is not found.
func (h *HubServer) staleReplaySpawnOf(ctx context.Context, job string) (staleReplaySpawn, bool, error) {
	if job == "" {
		return staleReplaySpawn{}, false, nil
	}
	if h.staleReplaySpawn != nil {
		spawn, found := h.staleReplaySpawn(ctx, job)
		return spawn, found, nil
	}
	task, err := h.inbox.store.latestSpawnForJob(ctx, job)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && task.Job != job) {
		return staleReplaySpawn{}, false, nil
	}
	if err != nil {
		return staleReplaySpawn{}, false, err
	}
	owner := task.Owner
	if owner == "" {
		owner = hubSpawnFlagArg(task.Request.Args, "--owner")
	}
	spawn := staleReplaySpawn{owner: owner, task: hubSpawnFlagArg(task.Request.Args, "--task"), created: task.Created}
	return spawn, owner != "", nil
}

// staleReplaySender is the sender prefix of a relay text: the "[label]" a
// lane.event starts with. Empty when the text has none.
func staleReplaySender(text string) string {
	rest, ok := strings.CutPrefix(text, "[")
	if !ok {
		return ""
	}
	label, _, found := strings.Cut(rest, "]")
	if !found || label == "" || len(label) > staleReplayWidthSend {
		return ""
	}
	return label
}

// runStaleReplayNotice files the notices of closed days until the hub stops.
func (h *HubServer) runStaleReplayNotice(ctx context.Context) {
	if h.inbox == nil || h.inbox.store == nil {
		return
	}
	ticker := time.NewTicker(staleReplayNoticeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.closeCtx.Done():
			return
		case <-ticker.C:
			h.fileStaleReplayNotices(ctx)
		}
	}
}

// fileStaleReplayNotices files, for every closed day with unfiled rows, one
// notice per design lane, then prunes filed rows past their keep window. Only
// days before today (UTC) are filed, so a day's rows are final when its notice
// goes out.
func (h *HubServer) fileStaleReplayNotices(ctx context.Context) {
	if h.inbox == nil || h.inbox.store == nil {
		return
	}
	today := staleReplayDay(h.now())
	days, err := h.inbox.store.staleReplayDaysBefore(ctx, today)
	if err != nil {
		h.logger.Warn("stale replay notice days could not be read")
		return
	}
	for _, day := range days {
		h.fileStaleReplayDay(ctx, day)
	}
	cutoff := staleReplayDay(h.now().Add(-staleReplayFiledKeep))
	if err := h.inbox.store.pruneStaleReplayRows(ctx, cutoff); err != nil {
		h.logger.Warn("stale replay notice rows were not pruned")
	}
}

// fileStaleReplayDay files one day's notices, one per design lane. A lane whose
// notice is not filed keeps its rows for the next pass; a lane with nothing to
// list (every row overtaken) is closed without an item. Only the rows read here
// are closed: a row retired while the pass runs is left owed for the next one.
func (h *HubServer) fileStaleReplayDay(ctx context.Context, day string) {
	lanes, err := h.inbox.store.staleReplayLanes(ctx, day)
	if err != nil {
		h.logger.Warn("stale replay notice lanes could not be read", "day", day)
		return
	}
	for _, lane := range lanes {
		rows, err := h.inbox.store.staleReplayRowsOf(ctx, day, lane)
		if err != nil {
			h.logger.Warn("stale replay notice rows could not be read", "day", day, "lane", lane)
			continue
		}
		listed, overtaken := h.staleReplayPartition(ctx, rows)
		if len(listed) > 0 && !h.fileStaleReplayItem(ctx, day, lane, listed, overtaken) {
			continue
		}
		ids := make([]int64, 0, len(rows))
		for _, row := range rows {
			ids = append(ids, row.RelayID)
		}
		if err := h.inbox.store.markStaleReplayFiled(ctx, ids); err != nil {
			h.logger.Warn("stale replay notice was filed but its rows were not closed; it is filed again", "day", day, "lane", lane)
		}
	}
}

// fileStaleReplayItem hands the notice to the design lane's inbox. Its event id
// names the day, so a retry after a lost answer is a duplicate, not a second
// item. It reports whether the item is held.
func (h *HubServer) fileStaleReplayItem(ctx context.Context, day, lane string, listed []staleReplayRow, overtaken int) bool {
	eventID := "stale-replay-" + day
	event := hubJobEventPayload{
		JobID: laneEventTransportID(lane, eventID), Epoch: 1, OwnerLane: lane, EventID: eventID,
		Text: staleReplayNoticeText(day, lane, listed, overtaken), DeliveryClass: "inbox", NotifyType: "info",
	}
	result := h.relayLaneEventContext(ctx, event, nil)
	if result.PersistFailed || result.RejectedTooLong {
		h.logger.Warn("stale replay notice was not filed; it is retried", "day", day, "lane", lane, "rows", len(listed))
		return false
	}
	h.logger.Info("stale replay notice filed", "day", day, "lane", lane, "listed", len(listed), "overtaken", overtaken)
	return true
}

// staleReplayPartition splits the rows of one lane into those the task did not
// overtake (listed) and those it did (counted). A row whose task cannot be read
// is listed.
func (h *HubServer) staleReplayPartition(ctx context.Context, rows []staleReplayRow) (listed []staleReplayRow, overtaken int) {
	tasks := map[string]staleReplayTaskRead{}
	for _, row := range rows {
		if row.Task != "" {
			task, seen := tasks[row.Task]
			if !seen {
				task = h.staleReplayTaskRead(ctx, row.Task)
				tasks[row.Task] = task
			}
			if staleReplayOvertaken(task, time.UnixMilli(row.ReceivedMS)) {
				overtaken++
				continue
			}
		}
		listed = append(listed, row)
	}
	return listed, overtaken
}

// staleReplayTaskRead reads the task from hk. An unreadable task comes back
// with no ID, which staleReplayOvertaken treats as not overtaking anything.
func (h *HubServer) staleReplayTaskRead(parent context.Context, task string) staleReplayTaskRead {
	id, err := strconv.ParseInt(task, 10, 64)
	if err != nil || id < 1 || h.handoffkeep == nil {
		return staleReplayTaskRead{}
	}
	ctx, cancel := context.WithTimeout(parent, staleReplayTaskDeadline)
	defer cancel()
	status, payload, err := h.handoffkeep.do(ctx, http.MethodGet, h.handoffkeep.endpoint("/v1/tasks/"+strconv.FormatInt(id, 10)), nil)
	if err != nil || status != http.StatusOK {
		return staleReplayTaskRead{}
	}
	var read staleReplayTaskRead
	if json.Unmarshal(payload, &read) != nil || read.ID != id || read.State == "" {
		return staleReplayTaskRead{}
	}
	return read
}

// staleReplayOvertaken reports whether the task ended within the overtake window
// after the row's receipt. The end is hk's completed_at or canceled_at, the
// stamp of the category the task entered, and only an end state counts: a task
// parked in icebox has not ended.
func staleReplayOvertaken(task staleReplayTaskRead, received time.Time) bool {
	if task.ID == 0 || !staleReplayTaskEnded(task.State) {
		return false
	}
	until := received.Add(staleReplayOvertakeWindow)
	return staleReplayWithin(task.CompletedAt, received, until) || staleReplayWithin(task.CanceledAt, received, until)
}

// staleReplayWithin reports whether an hk end stamp is set and after the receipt,
// but no later than the window's end.
func staleReplayWithin(at *time.Time, received, until time.Time) bool {
	return at != nil && at.After(received) && !at.After(until)
}

// staleReplayTaskEnded reports whether an hk task state is an end state: merged,
// deployed, completed, dropped or duplicate. icebox is parked, not finished.
func staleReplayTaskEnded(state string) bool {
	switch state {
	case "merged", "deployed", "completed", "dropped", "duplicate":
		return true
	}
	return false
}

// staleReplayNoticeText is the inbox item's text: the listed rows first, then
// the count of overtaken rows. Each entry names the handoffkeep row id, the key
// a reader looks the row up by, and is bounded, so the item stays inside one
// lane.event. Room for the footer is reserved before each entry is added, so the
// count of rows not listed and the overtaken count are never cut.
func staleReplayNoticeText(day, lane string, listed []staleReplayRow, overtaken int) string {
	footer := ""
	if overtaken > 0 {
		footer = fmt.Sprintf(" Overtaken by their task within 6 h of receipt, not listed: %d.", overtaken)
	}
	reserve := len(footer) + staleReplayMoreReserve
	var b strings.Builder
	fmt.Fprintf(&b, "[panewire] stale replay %s for %s: %d real message(s) retired stale (received more than 24 h before a replay) and not overtaken by their task:", day, lane, len(listed))
	for i, row := range listed {
		received := time.UnixMilli(row.ReceivedMS).UTC()
		age := time.Duration(row.RetiredMS-row.ReceivedMS) * time.Millisecond
		entry := fmt.Sprintf(" | %s %s received %s age %s row %d hk %s", orDash(utf8PrefixWithin(row.Sender, staleReplayWidthSend)),
			orDash(utf8PrefixWithin(row.Kind, staleReplayWidthKind)), received.Format(time.RFC3339), age.Round(time.Minute),
			row.RelayID, orDash(utf8PrefixWithin(row.HKEventID, 48)))
		if i == staleReplayListLimit || b.Len()+len(entry)+reserve > laneEventTextLimit {
			fmt.Fprintf(&b, " +%d more not listed.", len(listed)-i)
			break
		}
		b.WriteString(entry)
	}
	b.WriteString(footer)
	return utf8PrefixWithin(b.String(), laneEventTextLimit)
}
