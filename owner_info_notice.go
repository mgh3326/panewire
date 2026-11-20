package panewire

// Owner info notices (hk 5454). An info lane.event that names the job it is
// about carries a notice. receiveInbox decides it from that job's saved
// --owner-notify mode, the same way it withholds a job.completed (hk 5360):
// the withheld notice is filed in hk under its own reason and counted in
// owner_notice_decisions, and the decision is applied only from that record on
// a retry. Contract: docs/hub-spawn-operations.md, "Owner info notices".
//
//	never : the owner lane gets none of the job's info notices; each is counted.
//	final : only a terminal notice is sent; any other is counted.
//	absent: the notice is sent as before.

import (
	"context"

	"github.com/mgh3326/panewire/internal/fleet"
)

// ownerInfoNotice names the job an owner info lane.event is about, the cause
// its count is grouped under in the loops row, and whether it is terminal (the
// one kind --owner-notify final still sends).
type ownerInfoNotice struct {
	Job      string `json:"job"`
	Cause    string `json:"cause"`
	Terminal bool   `json:"terminal,omitempty"`
}

// ownerNoticeFor is the notice a producer attaches to an info event about job.
func ownerNoticeFor(job, cause string, terminal bool) *ownerInfoNotice {
	return &ownerInfoNotice{Job: job, Cause: cause, Terminal: terminal}
}

// validOwnerInfoNotice reports whether a notice names a job and a cause the
// loops row can group. An invalid notice is dropped, so its event takes the
// ordinary path rather than vanishing.
func validOwnerInfoNotice(n *ownerInfoNotice) bool {
	return n != nil && hubJobIDPattern.MatchString(n.Job) && validSpawnNoticeCause(n.Cause)
}

// ownerInfoDecision is the one rule for an info notice about a job whose spawn
// names mode. An absent mode sends it.
func ownerInfoDecision(mode string, terminal bool) string {
	switch {
	case mode == fleet.OwnerNotifyNever:
		return ownerDecisionCount
	case mode == fleet.OwnerNotifyFinal && !terminal:
		return ownerDecisionCount
	}
	return ownerDecisionDeliver
}

// ownerInfoHeld reports whether an info lane.event for lane is withheld: it is
// recorded and counted under its notice's cause instead of being sent. The
// notice is returned whether or not the event is held, for the caller's record.
func (h *HubServer) ownerInfoHeld(ctx context.Context, event hubJobEventPayload, lane string) (*ownerInfoNotice, bool) {
	if event.NotifyType != "info" || !validOwnerInfoNotice(event.Notice) {
		return nil, false
	}
	mode := h.ownerNotifyModeFor(ctx, event.Notice.Job, lane)
	return event.Notice, ownerInfoDecision(mode, event.Notice.Terminal) == ownerDecisionCount
}

// ownerInfoRecordKey is the owner_notice_decisions key of an info notice. The
// "notice:" prefix keeps it apart from a job.completed key ("job:").
func ownerInfoRecordKey(eventID string) string {
	return "notice:" + eventID
}
