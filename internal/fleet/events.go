package fleet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type Event struct {
	Kind      string         `json:"kind"`
	CreatedAt string         `json:"created_at"`
	TS        string         `json:"ts"`
	Payload   map[string]any `json:"payload"`
	// Outcome is the flat outcome a `panewire job close` record carries
	// (completed, abandoned or superseded); other events leave it empty.
	Outcome string `json:"outcome"`
	when    time.Time
	mtime   time.Time
	name    string
}
type Job struct {
	ID                                                                                                string
	Owner, Parent, Role, Label, Pane, Tab, Task, Family, Profile, Effort, Report, HubLane, Machine, T string
	Keep, Terminal, Reaped                                                                            bool
	// SpawnRole is the role the job.spawned receipt names (reviewer, consult, ...).
	// The claim records only implementer or builder, so the role a quota stop is
	// handled by comes from here (QuotaRole).
	SpawnRole string
	// Ended is the kind of the newest terminal event since the last claim,
	// spawn or reprompt; empty while the job is live.
	Ended string
	// Outcome is the outcome the newest terminal event declared, if it named
	// one (job close). Inherited is the job whose task the claim inherited
	// (--parent-job); both are empty when the events do not say.
	Outcome, Inherited string
	At                 time.Time
	Receipt            map[string]any
	Events             []Event
}

func (a *App) events(id string) ([]Event, error) {
	if !validID(id) {
		return nil, fail(exitUsage, "invalid job id")
	}
	dir := filepath.Join(a.Jobs, id, "events")
	if e := noSymlinks(dir); e != nil {
		return nil, e
	}
	entries, e := os.ReadDir(dir)
	if e != nil {
		return nil, e
	}
	var out []Event
	for _, ent := range entries {
		if ent.Type()&os.ModeSymlink != 0 {
			return nil, fail(exitStateConflict, "symlink event refused")
		}
		if ent.IsDir() || filepath.Ext(ent.Name()) != ".json" {
			continue
		}
		info, e := ent.Info()
		if e != nil {
			return nil, e
		}
		b, e := os.ReadFile(filepath.Join(dir, ent.Name()))
		if e != nil {
			return nil, e
		}
		var ev Event
		if e = json.Unmarshal(b, &ev); e != nil {
			return nil, fail(exitStateConflict, "unreadable job event %s", ent.Name())
		}
		if ev.Kind == "" {
			return nil, fail(exitStateConflict, "event kind missing")
		}
		ev.name = ent.Name()
		ev.when = info.ModTime()
		ev.mtime = info.ModTime()
		for _, s := range []string{ev.CreatedAt, ev.TS} {
			if t, e := time.Parse(time.RFC3339Nano, s); e == nil {
				ev.when = t
				break
			}
		}
		out = append(out, ev)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].when.Equal(out[j].when) {
			if !out[i].mtime.Equal(out[j].mtime) {
				return out[i].mtime.Before(out[j].mtime)
			}
			return out[i].name < out[j].name
		}
		return out[i].when.Before(out[j].when)
	})
	return out, nil
}
func (a *App) job(id string) (Job, error) {
	evs, e := a.events(id)
	j := Job{ID: id, Events: evs}
	if e != nil {
		return j, e
	}
	for _, ev := range evs {
		p := ev.Payload
		switch ev.Kind {
		case "job.claim", "job.claimed", "job.reclaim":
			j.Owner = str(p, "owner_lane")
			j.Parent = str(p, "parent_lane")
			if r := str(p, "role"); r != "" {
				if r == "builder" || j.Role == "" {
					j.Role = r
				}
			}
			j.Task = scalar(p["task_id"])
			j.Inherited = str(p, "task_inherited_from")
			j.T = str(p, "t_level")
			j.Terminal, j.Ended, j.Outcome = false, "", ""
			j.Reaped = false
		case "job.spawned":
			j.Receipt = p
			j.SpawnRole = str(p, "role")
			j.Pane = str(p, "pane_id")
			j.Tab = str(p, "tab_id")
			j.Label = str(p, "label")
			if j.Label == "" {
				j.Label = str(p, "agent_label")
			}
			j.Family = str(p, "family")
			j.Profile = str(p, "profile")
			j.Effort = str(p, "effort")
			j.Report = str(p, "report")
			j.HubLane = str(p, "hub_lane")
			j.Machine = str(p, "hub_lane_machine")
			j.Terminal, j.Ended, j.Outcome = false, "", ""
			j.Reaped = false
			if j.Task == "" {
				j.Task = scalar(p["task_id"])
			}
		case "job.lane_registered":
			// A takeover registers its lane after job.spawned, and the hub may
			// have moved a predecessor's row, so the recorded name is the
			// pane's route.
			if lane := str(p, "hub_lane"); lane != "" {
				j.HubLane = lane
			}
			if machine := str(p, "hub_lane_machine"); machine != "" {
				j.Machine = machine
			}
		case "job.reprompted":
			j.Terminal, j.Ended, j.Outcome = false, "", ""
		case "job.completed", "job.joined", "job.revoked", "job.handed_off", "job.lost":
			j.Terminal, j.Ended = true, ev.Kind
			j.Outcome = firstNonempty(ev.Outcome, str(p, "outcome"))
			j.At = ev.when
		case "job.reaped":
			j.Reaped = true
		}
		if boolean(p, "keep", false) {
			j.Keep = true
		}
	}
	if j.Receipt == nil && j.Task == "" {
		return j, fail(exitStateConflict, "no claim or receipt for job %s", id)
	}
	return j, nil
}
func scalar(v any) string {
	switch n := v.(type) {
	case string:
		return n
	case float64:
		b, _ := json.Marshal(n)
		return string(b)
	}
	return ""
}
func (a *App) event(ctx context.Context, id, kind string, p map[string]any) error {
	// No local claim is needed: a job claimed on another host (a remote
	// placement) still has its events recorded here.
	_, e := a.ledger().AppendEvent(ctx, id, kind, p, nil)
	return ledgerFail(e, "event")
}

type DurableJob struct {
	State   string         `json:"state"`
	Owner   string         `json:"owner_lane"`
	Label   string         `json:"agent_label"`
	Receipt DurableReceipt `json:"receipt"`
}

// DurableReceipt is the newest job.spawned receipt of a durable job: nil when
// the job has none (a released job stays reclaimable that way).
type DurableReceipt map[string]any

func (a *App) durable(ctx context.Context, id string) (DurableJob, error) {
	var j DurableJob
	v, e := a.ledger().JobGet(ctx, id)
	if e != nil {
		return j, ledgerFail(e, "job-get")
	}
	if v.State == "" {
		return j, fail(exitStateConflict, "unreadable durable job %s", id)
	}
	j = DurableJob{State: v.State, Owner: v.OwnerLane, Label: v.AgentLabel}
	if v.Receipt != nil {
		j.Receipt = DurableReceipt{}
		for k, val := range v.Receipt {
			j.Receipt[k] = val
		}
	}
	return j, nil
}
