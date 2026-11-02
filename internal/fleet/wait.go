package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// waitOutcome is one terminal signal for spawn --wait: a durable job event on
// this host (local spawns) or a lane message relayed through the hub
// (delegated spawns and READY/ESC signals).
type waitOutcome struct {
	name   string
	report string
	pane   string
	detail string
}

// afterSpawn runs the --wait half of a spawn once the pane exists — locally
// or, when placement delegated the spawn, back on the initiator. A delegated
// job additionally watches its hk record (the W4 rule).
func (a *App) afterSpawn(ctx context.Context, s Spawn, delegated bool) error {
	if s.Wait {
		return a.waitJob(ctx, s, delegated)
	}
	return nil
}

// waitDeadline resolves --wait-timeout, then the PANEWIRE_COMPLETION_TIMEOUT_S
// convention used by the completion watcher, then a one-hour default.
func (a *App) waitDeadline(s Spawn) (time.Duration, error) {
	raw := s.WaitTimeout
	if raw == "" {
		raw = a.envOr("PANEWIRE_COMPLETION_TIMEOUT_S", "WRK_COMPLETION_TIMEOUT_S")
	}
	if raw == "" {
		return time.Hour, nil
	}
	if d, ok := timeoutValue(raw); ok {
		return d, nil
	}
	return 0, fail(exitUsage, "invalid --wait-timeout %q", raw)
}

// timeoutValue parses a positive duration or bare seconds, shared by
// --wait-timeout, PANEWIRE_COMPLETION_TIMEOUT_S and the completion watcher so a
// duration form cannot be accepted by one and rejected by the other.
func timeoutValue(raw string) (time.Duration, bool) {
	if d, e := duration(raw); e == nil && d > 0 {
		return d, true
	}
	if n, e := strconv.Atoi(raw); e == nil && n > 0 && int64(n) <= math.MaxInt64/int64(time.Second) {
		return time.Duration(n) * time.Second, true
	}
	return 0, false
}

// waitJob blocks until the job's outcome reaches this host: a durable
// job.completed / escalate / lost / revoked / joined / handed_off event, or
// the READY/ESC convention on the parent or owner lane. The loop reads
// event artifacts and the lane inbox with bounded backoff — no resident
// process — so it survives the caller being backgrounded. Distinct exit
// codes carry the outcome: 0 done/ready/joined/handed_off, 83 escalate,
// 84 lost/revoked, 85 timeout.
func (a *App) waitJob(ctx context.Context, s Spawn, delegated ...bool) error {
	timeout, e := a.waitDeadline(s)
	if e != nil {
		return e
	}
	lane := s.Owner
	if s.Role == "builder" && s.Parent != "" {
		lane = s.Parent
	}
	if lane == "" {
		lane = "default"
	}
	deadline := a.Now().Add(timeout)
	delay := 2 * time.Second
	warned := false
	hkWarned := false
	hkWatch := len(delegated) > 0 && delegated[0]
	for {
		out := a.waitOutcome(ctx, s, lane, &warned, &hkWarned, hkWatch)
		if out.name != "" {
			fmt.Fprintf(a.Out, "wait job=%s outcome=%s report=%s pane=%s\n", s.Job, out.name, out.report, out.pane)
			switch out.name {
			case "escalate":
				return fail(exitWaitEscalated, "job %s escalated: %s", s.Job, firstNonempty(out.detail, "escalation"))
			case "lost", "revoked":
				return fail(exitWaitLost, "job %s %s", s.Job, out.name)
			}
			return nil
		}
		if !a.Now().Before(deadline) {
			fmt.Fprintf(a.Out, "wait job=%s outcome=timeout waited_s=%d\n", s.Job, int64(timeout.Seconds()))
			return fail(exitWaitTimeout, "wait timeout for job %s", s.Job)
		}
		if e := a.Sleep(ctx, delay); e != nil {
			return e
		}
		if delay *= 2; delay > 30*time.Second {
			delay = 30 * time.Second
		}
	}
}

// waitOutcome scans the job's durable event artifacts first, then the lane
// inbox when the hub is configured. Event reads tolerate a missing dir (a
// delegated job only has the initiator-side placement record here).
func (a *App) waitOutcome(ctx context.Context, s Spawn, lane string, warned, hkWarned *bool, hkWatch bool) waitOutcome {
	evs, e := a.events(s.Job)
	if e != nil && !errors.Is(e, os.ErrNotExist) && !*warned {
		*warned = true
		a.warn("job events unreadable: %v", e)
	}
	if e == nil {
		for _, ev := range evs {
			p := ev.Payload
			switch ev.Kind {
			case "job.completed":
				return waitOutcome{name: "completed", report: str(p, "report"), pane: str(p, "pane_id")}
			case "job.escalate":
				return waitOutcome{name: "escalate", report: str(p, "report"), pane: str(p, "pane_id"), detail: str(p, "question")}
			case "job.lost":
				return waitOutcome{name: "lost", pane: str(p, "pane_id")}
			case "job.revoked":
				return waitOutcome{name: "revoked", pane: str(p, "pane_id")}
			case "job.joined":
				return waitOutcome{name: "joined", pane: str(p, "pane_id")}
			case "job.handed_off":
				return waitOutcome{name: "handed_off", pane: str(p, "pane_id")}
			}
		}
	}
	if hkWatch {
		if out := a.jobRecordOutcome(ctx, s.Job, hkWarned); out.name != "" {
			return out
		}
	}
	if lane == "" || str(a.Config.Hub, "hub_url") == "" {
		return waitOutcome{}
	}
	args := append([]string{"inbox", "list", "--lane", lane, "--json"}, a.controlHubArgs(true)...)
	r := a.call(ctx, "panewire", args...)
	if r.Code != 0 {
		if !*warned {
			*warned = true
			a.warn("lane inbox read failed; watching local events only")
		}
		return waitOutcome{}
	}
	var msgs []struct {
		Kind  string `json:"kind"`
		Event struct {
			Text     string `json:"text"`
			JobID    string `json:"job_id"`
			Job      string `json:"job"`
			Report   string `json:"report"`
			Question string `json:"question"`
		} `json:"event"`
	}
	if e := json.Unmarshal([]byte(r.Out), &msgs); e != nil {
		if !*warned {
			*warned = true
			a.warn("lane inbox unreadable; watching local events only")
		}
		return waitOutcome{}
	}
	marker := "[" + s.Job + "]"
	for _, m := range msgs {
		ev := m.Event
		if ev.JobID != s.Job && ev.Job != s.Job && !strings.Contains(ev.Text, marker) {
			continue
		}
		switch m.Kind {
		case "job.completed":
			return waitOutcome{name: "completed", report: ev.Report}
		case "job.escalate":
			return waitOutcome{name: "escalate", report: ev.Report, detail: firstNonempty(ev.Question, ev.Text)}
		case "job.lost":
			return waitOutcome{name: "lost"}
		case "job.revoked":
			return waitOutcome{name: "revoked"}
		case "job.joined":
			return waitOutcome{name: "joined"}
		case "job.handed_off":
			return waitOutcome{name: "handed_off"}
		case "lane.event":
			if !strings.Contains(ev.Text, marker) {
				continue
			}
			if strings.Contains(ev.Text, "READY") {
				return waitOutcome{name: "ready", report: lastPipeField(ev.Text)}
			}
			if strings.Contains(ev.Text, "ESC") {
				return waitOutcome{name: "escalate", detail: ev.Text}
			}
		}
	}
	return waitOutcome{}
}

// lastPipeField returns the text after the final "|" in a lane signal like
// "[job] READY | <pr> | <report>", or "" when there is none.
func lastPipeField(text string) string {
	if i := strings.LastIndex(text, "|"); i >= 0 {
		return strings.TrimSpace(text[i+1:])
	}
	return ""
}
