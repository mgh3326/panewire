package fleet

import (
	"fmt"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleetevent"
)

// Fleet events (fleet-event v1, see internal/fleetevent) make refusals and
// placement decisions visible after the fact. They are written to
// <state dir>/events/fleet-events.jsonl and never change control flow.

// emitFleetEvent appends one event under the App's state dir.
func (a *App) emitFleetEvent(kind, job, outcome, reason, detail string) {
	a.fleetEventWriter().Emit(kind, job, outcome, reason, detail)
}

func (a *App) fleetEventWriter() *fleetevent.Writer {
	w := fleetevent.New(a.stateDir(), "panewire", a.Now)
	w.Sink = a.EventSink
	return w
}

// jobResultEvent records whether a done carried a result the job now holds.
// The #311 duplicate attach, its conflict and a reviewer's resultless done are
// otherwise indistinguishable in the durable record.
func (a *App) jobResultEvent(id, outcome, reason, detail string) {
	a.fleetEventWriter().EmitEvent(fleetevent.Event{Kind: "job_result", Job: id, Outcome: outcome, Reason: reason, Detail: detail})
}

// spawnEvents records how a spawn ended: its refusal, or, for --host local
// (which never goes through place), the forced placement it completed.
func (a *App) spawnEvents(s Spawn, err error) {
	if err != nil {
		a.refusalEvent("spawn", s.Job, err)
		if !s.DryRun && s.Sel != nil && str(s.Sel, "outcome_code") != outcomeAdmitted {
			a.fleetEventWriter().EmitEvent(fleetevent.Event{Kind: "placement", Job: s.Job, RequestID: a.RequestID, Outcome: "refused", Reason: str(s.Sel, "outcome_code"), Selection: selectionTerms(s.Sel)})
		}
		return
	}
	if s.Host == "local" {
		a.placementEvent(s, map[string]any{"source": "forced"}, nil)
	}
}

// refusalEvent records a spawn or plan that ended in an error. The reason is
// the exit status class (docs/config.md); the detail carries the status and
// the message, redacted and bounded by the writer.
func (a *App) refusalEvent(kind, job string, err error) {
	if err == nil {
		return
	}
	code := exitCode(err)
	if code == 0 {
		// An untyped error exits 1 from Main; record what the caller saw.
		code = exitFailure
	}
	outcome := "refused"
	switch code {
	case exitWaitEscalated, exitWaitLost, exitWaitTimeout, exitSubmitUnconfirmed, exitLaneUnregistered:
		// The job got past admission; these are failures after the fact.
		outcome = "failed"
	}
	if kind == "spawn" && a.RequestID != "" {
		a.fleetEventWriter().EmitEvent(fleetevent.Event{
			Kind: kind, Job: job, RequestID: a.RequestID, Outcome: outcome,
			Reason: exitReason(code), Detail: fmt.Sprintf("exit=%d %s", code, err.Error()),
		})
		return
	}
	a.emitFleetEvent(kind, job, outcome, exitReason(code), fmt.Sprintf("exit=%d %s", code, err.Error()))
}

// exitReason names an exit status for the event's reason field.
func exitReason(code int) string {
	switch code {
	case 0:
		return "error"
	case exitFailure:
		return "failure"
	case exitUsage:
		return "usage"
	case exitUploadTimeout:
		return "upload_timeout"
	case exitEvictionUnconfirmed:
		return "quota_unconfirmed"
	case exitConfig:
		return "config"
	case exitDuplicate:
		return "duplicate"
	case exitStateConflict:
		return "state_conflict"
	case exitSubmitUnconfirmed:
		return "submit_unconfirmed"
	case exitSeatRefused:
		return "seat_refused"
	case exitPreflight:
		return "preflight"
	case exitMockRefused:
		return "mock_refused"
	case exitLocalDisabled:
		return "local_disabled"
	case exitPlacement:
		return "placement"
	case exitLaneUnregistered:
		return "lane_unregistered"
	case exitWaitEscalated:
		return "wait_escalated"
	case exitWaitLost:
		return "wait_lost"
	case exitWaitTimeout:
		return "wait_timeout"
	case exitRoleWait:
		return "role_wait"
	case exitRoleEscalate:
		return "role_escalate"
	case exitTimeout:
		return "timeout"
	case exitUnreachable:
		return "unreachable"
	}
	return fmt.Sprintf("exit_%d", code)
}

// ranking names the metric that ordered the awake candidates: the hub score,
// the raw load fallback (some candidate had no score), or no ranking at all.
func (p *placePlan) ranking() string {
	if p.wake != nil {
		if ok, _ := p.wake["ok"].(bool); ok {
			return "wake"
		}
	}
	if len(p.ordered) < 2 {
		return "single_candidate"
	}
	for _, c := range p.ordered {
		if c.score == nil {
			return "raw_load"
		}
	}
	return "score"
}

// placementEvent records a placement decision: the chosen host, the ranking
// metric and the hosts that refused before it. A failed placement is recorded
// by the spawn refusal instead; a dry run records nothing.
func (a *App) placementEvent(s Spawn, rec map[string]any, err error) {
	if err != nil || rec == nil || s.DryRun {
		return
	}
	chosen, _ := rec["chose"].(string)
	ranking, _ := rec["ranking"].(string)
	if src, _ := rec["source"].(string); src == "forced" {
		chosen, ranking = s.Host, "forced"
		if chosen == "local" {
			chosen = a.machine()
		}
	}
	if ranking == "" {
		ranking = "unknown"
	}
	var refused []string
	candidates, _ := rec["candidates"].([]any)
	for _, raw := range candidates {
		row, _ := raw.(map[string]any)
		host, _ := row["host"].(string)
		outcome, _ := row["outcome"].(string)
		if host != "" && outcome != "" && host != chosen {
			refused = append(refused, host+":"+fleetevent.Code(outcome))
		}
	}
	reason := ranking
	var terms []fleetevent.Candidate
	if s.placement != nil && s.placement.plan != nil {
		terms, reason = placementTerms(s.placement.plan, chosen, ranking)
	}
	detail := fmt.Sprintf("chosen=%s ranking=%s candidates=%d", chosen, ranking, len(candidates))
	if pool, _ := rec["pool"].(string); pool != "" {
		detail += " pool=" + pool
	}
	if len(refused) > 0 {
		detail += " refused=" + strings.Join(refused, ",")
	}
	a.fleetEventWriter().EmitEvent(fleetevent.Event{Kind: "placement", Job: s.Job, RequestID: a.RequestID, Outcome: "placed", Reason: reason, Detail: detail, Chosen: chosen, Candidates: terms, Selection: selectionTerms(s.Sel)})
}

// selectionTerms projects the ordered host/rung trace without account keys,
// free-text gate reasons or other selection metadata. A host appears once: the
// credit band's pass 2 row replaces the pass 1 row in its place when it tried a rung, so the event
// does not spend MaxCandidates on a host twice.
func selectionTerms(sel map[string]any) []fleetevent.HostSelection {
	var out []fleetevent.HostSelection
	at := map[string]int{}
	hosts, _ := sel["hosts"].([]any)
	for _, raw := range hosts {
		h, _ := raw.(map[string]any)
		rank, _ := h["rank"].(int)
		if rank == 0 {
			continue
		}
		x := fleetevent.HostSelection{Host: str(h, "host"), Rank: rank, Outcome: str(h, "admission_outcome")}
		tries, _ := h["tried"].([]any)
		for _, rawTry := range tries {
			r, _ := rawTry.(map[string]any)
			x.Tried = append(x.Tried, fleetevent.RungAttempt{Profile: str(r, "profile"), Effort: str(r, "effort"), Pool: str(r, "pool"), Outcome: str(r, "outcome")})
		}
		if i, ok := at[x.Host]; ok {
			// A pass 2 row that tried nothing (not_tried after an earlier
			// host admitted) must not erase what pass 1 recorded.
			if len(x.Tried) > 0 {
				out[i] = x
			}
			continue
		}
		at[x.Host] = len(out)
		out = append(out, x)
	}
	return out
}

// placementTerms projects the plan's candidates for the placement event: each
// host's score and the hub's terms behind it (cpu, mem, io, load, jobs, power,
// bridge), and the reason the chosen host won. The reason is the hub's own
// when the chosen host is the one the hub named (policy_order, challenger,
// below_free_floor, all_loaded), else the ranking metric. It carries numbers and host ids only;
// no screen text, brief or argv.
func placementTerms(p *placePlan, chosen, ranking string) ([]fleetevent.Candidate, string) {
	reason := ranking
	out := make([]fleetevent.Candidate, 0, len(p.candidates))
	for _, c := range p.candidates {
		row := fleetevent.Candidate{Host: c.name, Source: c.source, Score: c.score, Outcome: c.outcome}
		if c.score != nil && c.signals.Terms == nil && row.Source != "hub-only" {
			row.Source = "signals-unavailable"
		}
		if t := c.signals.Terms; t != nil {
			row.Jobs = &t.Jobs
			free := t.Free()
			row.Free = &free
			if c.source != "hub-only" || c.signals.CPUBusyShort != nil || c.signals.CPUBusy5m != nil {
				row.CPU = &t.CPU
			}
			if c.source != "hub-only" || c.signals.MemAvailable != nil {
				row.Mem = &t.Mem
			}
			if c.source != "hub-only" || c.signals.SwapInMBPS != nil {
				row.IO = &t.IO
			}
			if c.source != "hub-only" || c.signals.Load1PerCore != nil {
				row.Load = &t.Load
			}
		}
		row.Power, row.Bridge = c.signals.Power, c.signals.BridgedJobs
		if row.Outcome == "" {
			row.Outcome = "not_chosen"
		}
		if c.name == chosen {
			row.Outcome = "chosen"
			if c.decided {
				reason = c.decision
				if reason == "" {
					reason = "policy_order"
				}
			}
		}
		out = append(out, row)
	}
	return out, reason
}

// quotaObservationEvent records a target account whose shared quota
// observation made the host unusable (stale, unobserved, degraded, or a
// failed re-measure). age_min is -1 when the account was never measured.
func (a *App) quotaObservationEvent(host, pool, reason string, acc SharedAccount) {
	if reason == "" {
		return
	}
	outcome := "unusable"
	if strings.Contains(reason, "stale") || strings.Contains(reason, "not_observed") || reason == "measure_failed" {
		outcome = "stale"
	}
	age := int64(-1)
	if !acc.MeasuredAt.IsZero() {
		age = int64(a.Now().Sub(acc.MeasuredAt) / time.Minute)
	}
	a.emitFleetEvent("quota_observation", "", outcome, reason, fmt.Sprintf("target=%s pool=%s age_min=%d", host, pool, age))
}
