package panewire

// Result identity (hk 6822, hk 4289 T5; docs/quiet-delivery.md, "A READY and
// its completion"). A worker's READY and the job.completed that concludes it
// share one identifier, the READY's producer event id. The completion carries
// it as result_id (`panewire done --ready-event`, `panewire emit --result-id`),
// and the hub folds the completion into the READY's unread inbox item in place
// of a second item. The item is the merge's receipt: its merged_completions
// set names every completion folded into it, so a repeat changes nothing.
// Whatever cannot be merged is delivered as before.

import (
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"time"

	"github.com/mgh3326/panewire/internal/laneevent"
)

// hubFeatureResultID is listed in hub.features by a hub that accepts result_id
// on a job.completed. An older hub refuses a payload with an unknown key, and
// with it the completion, so a node sends the field only to a hub that listed
// it (relayWire).
const hubFeatureResultID = "result-id-v1"

const (
	// filteredReasonReadyRepeat follows filteredDeliveryPrefix on the hk row of
	// a merged completion that would otherwise have been typed into the pane.
	filteredReasonReadyRepeat = "ready-repeat"
	// inboxWakeReadyRepeat is the wake line's class when a merge announces a
	// READY again on a lane that is not inbox-only.
	inboxWakeReadyRepeat = "ready-repeat"
)

type completionMerge int

const (
	// completionNotMerged: the completion is delivered as before.
	completionNotMerged completionMerge = iota
	// completionMerged: this call folded it into the READY item.
	completionMerged
	// completionRepeated: the READY item already lists it.
	completionRepeated
)

// mergeCompletion folds completion into lane's unread READY item named by its
// result_id, in one transaction. It changes nothing and answers
// completionNotMerged when the completion already has an item of its own (it
// was delivered before the READY arrived), when no `--type ready` lane.event
// of that id is filed for the lane, or when that READY was acked or
// superseded: the receiver is done with it, so the completion is news. A
// completion the item already lists is completionRepeated in any state. A
// delivered READY that is not acked still takes it: reaching the pane does not
// show that the receiver read it.
//
// The merge supersedes what the completion's own item would have (cause).
func (s *laneInboxStore) mergeCompletion(lane string, completion hubJobEventPayload, now time.Time) (laneInboxItem, completionMerge, error) {
	key := relayEventDedupeKey("job.completed", completion)
	entry := laneevent.MergedCompletionKey(key)
	ctx, done := s.op(s.life)
	defer done()
	s.mu.Lock()
	defer s.mu.Unlock()
	none := func(err error) (laneInboxItem, completionMerge, error) {
		return laneInboxItem{}, completionNotMerged, err
	}
	tx, err := s.beginTx(ctx)
	if err != nil {
		return none(err)
	}
	defer tx.Rollback()
	if completion.Cause != "" {
		if err := s.dialect.lockLaneCause(ctx, tx, lane, completion.Cause); err != nil {
			return none(err)
		}
	}
	own, ownArgs := s.eventKeyPredicate(key)
	var delivered int
	if err := tx.QueryRowContext(ctx, s.q(`SELECT count(*) FROM lane_inbox WHERE `+own), ownArgs...).Scan(&delivered); err != nil || delivered > 0 {
		return none(err)
	}
	where, args := s.eventKeyPredicate(relayLaneEventOutboxKey(lane, completion.ResultID))
	item, err := scanLaneInbox(tx.QueryRowContext(ctx, s.q(`SELECT `+laneInboxColumns+` FROM lane_inbox WHERE `+where), args...))
	if errors.Is(err, sql.ErrNoRows) {
		return none(nil)
	}
	if err != nil {
		return none(err)
	}
	// The key names a lane.event of this lane; only a READY takes a completion,
	// not another must-ack type such as a decision.
	if item.Event.NotifyType != "ready" {
		return none(nil)
	}
	if slices.Contains(item.Event.MergedCompletions, entry) {
		return item, completionRepeated, nil
	}
	foldCompletion(&item.Event, completion, entry)
	raw, err := json.Marshal(item.Event)
	if err != nil {
		return none(err)
	}
	// The write is the one gate: only a READY still owed to the lane (must-ack,
	// not acked, not superseded) takes the completion. Testing it here, not on
	// the row read above, also holds against an ack that commits in between.
	result, err := tx.ExecContext(ctx, s.q(`UPDATE lane_inbox SET payload=? WHERE id=? AND must_ack=1 AND acked_at=0 AND superseded_at=0`), string(raw), item.ID)
	if err != nil {
		return none(err)
	}
	if written, err := s.dialect.rowsMatched(result); err != nil || written != 1 {
		return none(err)
	}
	superseded := 0
	if completion.Cause != "" {
		successor := laneInboxItem{ID: item.ID, Lane: lane, Kind: "job.completed", Event: completion}
		if superseded, err = s.supersede(ctx, tx, successor, math.MaxInt64, now); err != nil {
			return none(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return none(err)
	}
	if superseded > 0 {
		s.noteBatchesChanged()
		s.noteWorkChanged()
	}
	s.noteInboxChanged(lane)
	return item, completionMerged, nil
}

// foldCompletion copies what a completion adds to its READY and records its
// merged_completions entry. An empty field never erases the READY's own.
func foldCompletion(ready *hubJobEventPayload, completion hubJobEventPayload, entry string) {
	for _, field := range []struct {
		into *string
		from string
	}{
		{&ready.ReportPath, completion.ReportPath},
		{&ready.ReportLastLine, completion.ReportLastLine},
		{&ready.PR, completion.PR},
		{&ready.Head, completion.Head},
	} {
		if field.from != "" {
			*field.into = field.from
		}
	}
	ready.MergedCompletions = append(ready.MergedCompletions, entry)
}

// receiveReadyRepeat answers a completion that names its READY. handled is
// false when it does not merge, and receiveInbox then delivers it as before; a
// store failure is such a case too, so it can cost a second item and never the
// completion. typed says the completion would have taken the relay path into
// the pane: it is then recorded in hk as delivered to hub/filtered:ready-repeat
// and acknowledged only once hk holds that, so the producer sends it again
// after a failed write. A completion bound for the inbox, or a hub with no hk
// client, has the READY item as its receipt and writes no hk row.
func (h *HubServer) receiveReadyRepeat(kind string, event hubJobEventPayload, sender *hubAgent, senderID, lane string, typed bool) (relayLaneEventResult, bool) {
	item, merge, err := h.inbox.store.mergeCompletion(lane, event, h.now().UTC())
	if err != nil {
		h.logger.Warn("completion not merged into its READY; delivered on its own", "lane", lane, "job", event.JobID, "error", h.inbox.store.loggable("ready merge", err))
		return relayLaneEventResult{}, false
	}
	if merge == completionNotMerged {
		return relayLaneEventResult{}, false
	}
	// Only the first merge announces, and before the hk write: a resend after a
	// failed write is a repeat and announces nothing.
	if merge == completionMerged && item.DeliveredAt > 0 {
		h.resurfaceReady(lane, item)
	}
	result := relayLaneEventResult{ID: item.ID, Routed: true, Machine: "inbox", Duplicate: merge == completionRepeated}
	if typed && h.handoffkeep != nil {
		stored, filed, ok := h.fileFilteredInHK(h.closeCtx, kind, event, senderID, filteredReasonReadyRepeat)
		if !ok {
			return relayLaneEventResult{PersistFailed: true}, true
		}
		if filed {
			result.ID, result.Machine, result.AlreadyDelivered = stored.ID, filteredDeliveryPrefix+filteredReasonReadyRepeat, true
		}
	}
	if sender != nil {
		h.queueRelayPersisted(kind, event, sender, result.ID)
	}
	return result, true
}

// resurfaceReady announces an unacked READY the pane already got, now that a
// completion joined it, through the inbox wake line.
func (h *HubServer) resurfaceReady(lane string, item laneInboxItem) {
	if !inboxWakeFiledWorthy([]laneInboxItem{item}) {
		return
	}
	class := h.inbox.policy.inboxOnlyClass(lane, h.now())
	if class == "" {
		class = inboxWakeReadyRepeat
	}
	h.noteInboxWakeArrival(lane, class)
}
