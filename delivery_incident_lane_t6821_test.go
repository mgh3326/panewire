package panewire

// hk 6821: the incident pass's reading of a worker lane -- who is told, and
// whether the answer is tell, hold or final.

import (
	"os"
	"testing"
)

func TestIncidentLaneAnswer(t *testing.T) {
	// Job ids, by the task their record names: 1 is open, 2 has ended, 9 cannot
	// be read; "bare" has no task, and "lost" has no record, which names none.
	jobs := t3431Jobs{records: map[string]map[string]any{
		"open":   {"state": "running", "task_id": float64(1), "label": t3431Worker},
		"ended":  {"state": "running", "task_id": float64(2), "label": t3431Worker},
		"ended2": {"state": "running", "task_id": float64(2), "label": t3431Worker},
		"dark":   {"state": "running", "task_id": float64(9), "label": t3431Worker},
		"bare":   {"state": "running", "label": t3431Worker},
	}}
	const live, away, done = "live", "away", "done"
	for _, tc := range []struct {
		name  string
		jobs  map[string]string // id to liveness
		want  noticeAnswer
		close noticeAnswer // what a close reads from the job records alone
	}{
		{"no job", nil, noticeTell, noticeTell},
		{"completed", map[string]string{"open": done}, noticeJobEnded, noticeJobEnded},
		{"live, open task", map[string]string{"open": live}, noticeTell, noticeTell},
		{"live, ended task", map[string]string{"ended": live}, noticeTaskEnded, noticeTell},
		{"live, no task", map[string]string{"bare": live}, noticeTell, noticeTell},
		{"live, no record", map[string]string{"lost": live}, noticeTell, noticeTell},
		{"orphaned, open task", map[string]string{"open": away}, noticeHold, noticeHold},
		{"orphaned, no task", map[string]string{"bare": away}, noticeHold, noticeHold},
		{"orphaned, ended task", map[string]string{"ended": away}, noticeTaskEnded, noticeHold},
		{"orphaned, unreadable task", map[string]string{"dark": away}, noticeTell, noticeHold},
		{"orphaned, no record", map[string]string{"lost": away}, noticeHold, noticeHold},
		{"orphaned open beside orphaned ended", map[string]string{"open": away, "ended": away}, noticeHold, noticeHold},
		{"orphaned open beside orphaned unreadable", map[string]string{"open": away, "dark": away}, noticeTell, noticeHold},
		{"live ended beside orphaned open", map[string]string{"ended": live, "open": away}, noticeHold, noticeTell},
		{"live ended beside orphaned ended", map[string]string{"ended": live, "ended2": away}, noticeTaskEnded, noticeTell},
		{"live open beside orphaned open", map[string]string{"open": live, "bare": away}, noticeTell, noticeTell},
		{"completed beside orphaned", map[string]string{"ended": done, "open": away}, noticeHold, noticeHold},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := t3431Start(t, jobs)
			rig.task("1", "in_progress", "started")
			rig.task("2", "done", "completed")
			for id, liveness := range tc.jobs {
				rig.job(id, liveness == done, liveness == away)
			}
			if got := rig.hub.closingAnswer(passLanes{}, t3431Worker); got != tc.close {
				t.Errorf("records answer=%q, want %q", got, tc.close)
			}
			lanes := passLanes{}
			if got := rig.hub.passAnswer(lanes, t3431Worker); got != tc.want {
				t.Errorf("answer=%q, want %q", got, tc.want)
			}
			// The pass keeps its reading: jobs that change meanwhile wait for the next.
			for id := range tc.jobs {
				rig.job(id, true, false)
			}
			if got := rig.hub.passAnswer(lanes, t3431Worker); got != tc.want {
				t.Errorf("answer after the jobs changed=%q, want %q", got, tc.want)
			}
			if got := rig.hub.closingAnswer(lanes, t3431Worker); got != tc.want {
				t.Errorf("close's answer after the jobs changed=%q, want the pass's %q", got, tc.want)
			}
		})
	}
}

// A takeover binds the lane to its successor's job (hk 6816).
func TestIncidentLaneAnswerFollowsTheTakeoverJob(t *testing.T) {
	rig := t3431Start(t, t3431LiveJobWithTask())
	rig.task(t3431Task, "in_progress", "started")
	routes := `{"lanes":{"` + t3431Worker + `":{"machine":"host-a","pane":"w1:pW","parent":"` + t3431Design + `","job":"successor","taken_over_from":"` + t3431Job + `"},"` + t3431Design + `":{"machine":"host-a","pane":"w1:pD"}}}`
	if err := os.WriteFile(rig.hub.reportRelayPath, []byte(routes), 0o600); err != nil {
		t.Fatal(err)
	}
	rig.job(t3431Job, false, false)
	rig.job("successor", true, false)
	if got := rig.hub.passAnswer(passLanes{}, t3431Worker); got != noticeJobEnded {
		t.Fatalf("answer=%q, want the completed successor's job_ended", got)
	}
}

// A lane with no parent is named by its job's owner: a live job's before an
// orphaned one's.
func TestIncidentLaneDesign(t *testing.T) {
	const awayOwner, unrouted = "design-away", "design-unrouted"
	routes := map[string]reportRelayRoute{
		t3431Worker: {Machine: "host-a", Pane: "w1:pW"},
		t3431Design: {Machine: "host-a", Pane: "w1:pD"},
		awayOwner:   {Machine: "host-a", Pane: "w1:pA"},
	}
	for _, tc := range []struct {
		name      string
		jobs      []hubJobRecord
		design    string
		ownerAway bool
	}{
		{name: "a live owner first", jobs: []hubJobRecord{{HubActiveJob: HubActiveJob{OwnerLane: awayOwner}, Orphaned: true}, {HubActiveJob: HubActiveJob{OwnerLane: t3431Design}}}, design: t3431Design, ownerAway: true},
		{name: "an orphaned owner", jobs: []hubJobRecord{{HubActiveJob: HubActiveJob{OwnerLane: awayOwner}, Orphaned: true}}, design: awayOwner, ownerAway: true},
		{name: "an orphaned owner with no route", jobs: []hubJobRecord{{HubActiveJob: HubActiveJob{OwnerLane: unrouted}, Orphaned: true}}, ownerAway: true},
		{name: "a completed job", jobs: []hubJobRecord{{HubActiveJob: HubActiveJob{OwnerLane: awayOwner}, Completed: true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := t3431StartWith(t, t3431LiveJobWithTask(), routes, "")
			rig.hub.mu.Lock()
			for i, job := range tc.jobs {
				job.AgentLabel = t3431Worker
				rig.hub.jobs[t6821Num(int64(i))] = &job
			}
			rig.hub.mu.Unlock()
			if got := rig.hub.passLane(passLanes{}, t3431Worker); got.design != tc.design || got.ownerAway != tc.ownerAway {
				t.Fatalf("design=%q ownerAway=%v, want %q %v", got.design, got.ownerAway, tc.design, tc.ownerAway)
			}
		})
	}
}
