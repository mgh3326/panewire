package panewire

// Contract: docs/relay-harness-delivery.md, "Escalation".
//
// What the incident pass knows of a worker lane (hk 6821). A row skipped there
// is skipped for good, so the pass skips one only on an answer that stays: a
// job on a node that is away comes back running or completed, and holds the
// notice until then. A hub without an inbox store asks unconfirmedNoticeMoot
// at the drop and keeps nothing (hub_relay_escalation_legacy.go).

import (
	"cmp"
	"context"
)

// noticeAnswer is whether somebody can act on a notice about a worker lane.
type noticeAnswer string

const (
	noticeTell      noticeAnswer = ""             // post it
	noticeHold      noticeAnswer = "job_orphaned" // only a job on a node that is away could act: the row stays due
	noticeJobEnded  noticeAnswer = "job_ended"    // final: every job completed
	noticeTaskEnded noticeAnswer = "task_ended"   // final: every job completed or its task ended
)

// final reports whether the answer may end a row's notice for good.
func (a noticeAnswer) final() bool { return a == noticeJobEnded || a == noticeTaskEnded }

// passLane is one reading of a worker lane's jobs, taken under one lock.
type passLane struct {
	design    string // the lane told: the parent, else a live job's owner, else an orphaned job's
	ownerAway bool   // an orphaned job names an owner for the lane, registered or not
	jobs      noticeJobSet
	answer    *noticeAnswer // jobs and their tasks, read at the first use that needs it
}

// passLanes is what one pass knows of each lane, read once, so that no two of
// its steps see the lane differently. A job that changes meanwhile is read by
// the next pass: every row the pass applies a reading to was due before it.
type passLanes map[string]*passLane

func (h *HubServer) passLane(lanes passLanes, lane string) *passLane {
	if read, ok := lanes[lane]; ok {
		return read
	}
	routes := loadReportRelayRoutes(h.reportRelayPath)
	h.mu.Lock()
	defer h.mu.Unlock()
	live, away, ownerAway := h.jobDesignLanesLocked(routes, lane)
	read := &passLane{
		design:    cmp.Or(parentDesignLane(routes, lane), live, away),
		ownerAway: ownerAway,
		jobs:      h.noticeLaneJobsLocked(routes, lane),
	}
	lanes[lane] = read
	return read
}

// passAnswer is the lane's answer for a row that is joining or is to be named.
func (h *HubServer) passAnswer(lanes passLanes, lane string) noticeAnswer {
	read := h.passLane(lanes, lane)
	if read.answer == nil {
		answer := h.laneJobsAnswer(lane, read.jobs)
		read.answer = &answer
	}
	return *read.answer
}

// closingAnswer is the lane's answer for a close, which reads no task between
// notices: the pass's answer if it has one, else what the same reading of the
// job records says.
func (h *HubServer) closingAnswer(lanes passLanes, lane string) noticeAnswer {
	read := h.passLane(lanes, lane)
	if read.answer != nil {
		return *read.answer
	}
	return read.jobs.recordsAnswer()
}

// recordsAnswer is what the job records say without their tasks. A lane the hub
// knows no job in is told: a restart forgets jobs, and a lane may run work the
// hub never placed.
func (j noticeJobSet) recordsAnswer() noticeAnswer {
	switch {
	case !j.known || len(j.live) > 0:
		return noticeTell
	case len(j.orphaned) > 0:
		return noticeHold
	}
	return noticeJobEnded
}

// laneJobsAnswer is the answer with the tasks read. A live job whose task has
// not ended is told, and so is any job whose record or task cannot be read.
func (h *HubServer) laneJobsAnswer(lane string, jobs noticeJobSet) noticeAnswer {
	if records := jobs.recordsAnswer(); !jobs.known || records == noticeJobEnded {
		return records
	}
	ctx, cancel := context.WithTimeout(h.closeCtx, handoffkeepTimeout)
	defer cancel()
	for _, id := range jobs.live {
		if ended, _ := h.noticeTaskEnded(ctx, lane, id); !ended {
			return noticeTell
		}
	}
	answer := noticeTaskEnded
	for _, id := range jobs.orphaned {
		ended, readable := h.noticeTaskEnded(ctx, lane, id)
		if !readable {
			return noticeTell
		}
		if !ended {
			answer = noticeHold
		}
	}
	return answer
}
