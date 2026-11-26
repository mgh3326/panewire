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
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// staleReplayOvertakeWindow is the cut-off set by panewire-design: a row is
	// overtaken when its lane's task was merged, closed or completed within this
	// long after the row's receipt.
	staleReplayOvertakeWindow = 6 * time.Hour
	// staleReplayListLimit bounds the rows one notice names; the rest are
	// counted.
	staleReplayListLimit = 10
	// staleReplayNoticeInterval is how often the filing pass looks for a closed
	// day.
	staleReplayNoticeInterval = 10 * time.Minute
	// staleReplayFiledKeep is how long a filed day's rows are kept for audit.
	staleReplayFiledKeep = 14 * 24 * time.Hour
	// staleReplayTaskDeadline bounds one hk task read.
	staleReplayTaskDeadline = 5 * time.Second
)

// staleReplaySpawn is what the notice reads of a worker job's spawn record: the
// design lane that owns the job and the hk task it was spawned for.
type staleReplaySpawn struct{ owner, task string }

// staleReplayTaskRead is the hk task fields the overtake check reads.
type staleReplayTaskRead struct {
	ID        int64     `json:"id"`
	State     string    `json:"state"`
	Category  string    `json:"category"`
	UpdatedAt time.Time `json:"updated_at"`
}

// staleReplayDay is a notice day's UTC date.
func staleReplayDay(t time.Time) string { return t.UTC().Format(time.DateOnly) }

// noteStaleReplayRetire records a real message the replay gate retired as stale,
// for the day's notice. Idle-wake reports are not messages a lane waits for,
// and a row received after its worker job recorded job.completed is the hk 6900
// case, which that fix already stops re-delivering; neither is recorded. A row
// whose worker job has no design owner cannot be filed, so it is logged and
// left out.
func (h *HubServer) noteStaleReplayRetire(record handoffkeepRelayEvent, reason string) {
	if reason != "stale" || h.inbox == nil || h.inbox.store == nil {
		return
	}
	if record.Kind == "lane.event" {
		if _, idle := decodeIdleWakeRelayText(record.Text); idle {
			return
		}
	}
	received, err := time.Parse(time.RFC3339Nano, record.ReceivedAt)
	if err != nil {
		return
	}
	now := h.now().UTC()
	route := loadReportRelayRoutes(h.reportRelayPath)[record.OwnerLane]
	if route.Job == "" {
		h.logger.Warn("stale replay row has no worker job; left out of the daily notice", "event_id", record.ID, "lane", record.OwnerLane)
		return
	}
	if h.staleReplayAfterJobCompletion(route.Job, received) {
		return
	}
	ctx := context.Background()
	spawn, ok := h.staleReplaySpawnOf(ctx, route.Job)
	if !ok || !h.inbox.policy.designLane(spawn.owner) {
		h.logger.Warn("stale replay row has no design owner; left out of the daily notice", "event_id", record.ID, "lane", record.OwnerLane, "job", route.Job)
		return
	}
	row := staleReplayRow{
		RelayID: record.ID, NoticeDay: staleReplayDay(now), DesignLane: spawn.owner, WorkerLane: record.OwnerLane,
		Job: route.Job, Task: spawn.task, Kind: record.Kind, Sender: staleReplaySender(record.Text), HKEventID: record.EventID,
		ReceivedMS: received.UnixMilli(), RetiredMS: now.UnixMilli(),
	}
	if err := h.inbox.store.insertStaleReplayRow(ctx, row); err != nil {
		h.logger.Warn("stale replay row was not kept for the daily notice", "event_id", record.ID, "lane", record.OwnerLane)
	}
}

// staleReplayAfterJobCompletion is the hk 6900 exclusion, kept behind this one
// predicate so it can be dropped once 6900 is live: the row was received after
// its worker job's completion was recorded. The completion receipt is the
// record's LastSeen; a hub restart forgets it, so after one the rows of a
// completed job are not excluded here.
func (h *HubServer) staleReplayAfterJobCompletion(job string, received time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	rec := h.jobs[job]
	return rec != nil && rec.Completed && !rec.LastSeen.IsZero() && received.After(rec.LastSeen)
}

// staleReplaySpawnOf reads the worker job's spawn record from the hub's durable
// spawn store: the design owner and the task the job was spawned for.
func (h *HubServer) staleReplaySpawnOf(ctx context.Context, job string) (staleReplaySpawn, bool) {
	if h.staleReplaySpawn != nil {
		return h.staleReplaySpawn(ctx, job)
	}
	task, err := h.inbox.store.latestSpawnForJob(ctx, job)
	if err != nil || task.Job != job {
		return staleReplaySpawn{}, false
	}
	owner := task.Owner
	if owner == "" {
		owner = hubSpawnFlagArg(task.Request.Args, "--owner")
	}
	return staleReplaySpawn{owner: owner, task: hubSpawnFlagArg(task.Request.Args, "--task")}, owner != ""
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
// list (every row overtaken) is closed without an item.
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
		if err := h.inbox.store.markStaleReplayFiled(ctx, day, lane); err != nil {
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
// after the row's receipt: its end is taken as hk's updated_at, the last time
// the task changed. A later edit only moves the end later, so an edit can list a
// row that was overtaken, never hide one that was not.
func staleReplayOvertaken(task staleReplayTaskRead, received time.Time) bool {
	if task.ID == 0 || !staleReplayTaskEnded(task.State, task.Category) {
		return false
	}
	return task.UpdatedAt.After(received) && !task.UpdatedAt.After(received.Add(staleReplayOvertakeWindow))
}

// staleReplayTaskEnded reports whether hk's task is merged, closed or completed:
// its completed or canceled category, or one of the end states.
func staleReplayTaskEnded(state, category string) bool {
	if category == "completed" || category == "canceled" {
		return true
	}
	switch state {
	case "merged", "deployed", "done", "dropped", "canceled", "duplicate":
		return true
	}
	return false
}

// staleReplayNoticeText is the inbox item's text: the listed rows first, then
// the count of overtaken rows. Each entry is bounded, so the item stays inside
// one lane.event.
func staleReplayNoticeText(day, lane string, listed []staleReplayRow, overtaken int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[panewire] stale replay %s for %s: %d real message(s) retired stale (received more than 24 h before a replay) and not overtaken by their task:", day, lane, len(listed))
	for i, row := range listed {
		if i == staleReplayListLimit {
			fmt.Fprintf(&b, " +%d more not listed.", len(listed)-i)
			break
		}
		received := time.UnixMilli(row.ReceivedMS).UTC()
		age := time.Duration(row.RetiredMS-row.ReceivedMS) * time.Millisecond
		fmt.Fprintf(&b, " | %s %s received %s age %s hk %s", orDash(utf8PrefixWithin(row.Sender, staleReplayWidthSend)),
			orDash(utf8PrefixWithin(row.Kind, staleReplayWidthKind)), received.Format(time.RFC3339), age.Round(time.Minute),
			orDash(utf8PrefixWithin(row.HKEventID, 48)))
	}
	if overtaken > 0 {
		fmt.Fprintf(&b, " Overtaken by their task within 6 h of receipt, not listed: %d.", overtaken)
	}
	return utf8PrefixWithin(b.String(), laneEventTextLimit)
}
