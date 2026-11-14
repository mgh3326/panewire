package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// laneEventMaxBytes bounds one events-lane record, as the relay scanner does.
const laneEventMaxBytes = 16 << 10

// laneTerminalMarker is the head of a lane event that ends a job. The optional
// [label] prefix is the form most workers use; a marker later in the text, such
// as "READY withheld" inside a progress note, does not count.
var laneTerminalMarker = regexp.MustCompile(`^(\[[^\]]*\]\s*)?(READY|DONE)\b`)

// watchExecutable names the binary a detached watcher runs; tests point it at
// a stand-in.
var watchExecutable = os.Executable

func (a *App) startWatch(job string) error {
	return a.startWatchMode(job, false)
}

func (a *App) startWatchMode(job string, rearmed bool) error {
	// Unit fakes never spawn real processes.
	if _, ok := a.Runner.(ExecRunner); !ok {
		return nil
	}
	exe, e := watchExecutable()
	if e != nil {
		return e
	}
	dir := filepath.Join(a.Jobs, job)
	if e = os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	log, e := os.OpenFile(filepath.Join(dir, "panewire-watch.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer log.Close()
	cmd := watchCommand(exe, job, rearmed)
	cmd.Env = append(os.Environ(), "HERDR_SESSION="+a.herdrSession(), "ARBITER_INBOX_ROOT="+a.Jobs)
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.Stdin = nil
	if e = cmd.Start(); e != nil {
		return e
	}
	return cmd.Process.Release()
}

// watchCommand builds the watcher's command. The watcher must outlive the
// daemon that starts it, because a rollout restarts the daemon while a job
// runs and the watcher is what records that job's result (hk 4345):
//
//   - Setsid gives it its own session and process group, so a signal to the
//     daemon's group, and launchd's group kill, miss it.
//   - The service manager must also leave it alone: systemd KillMode=process
//     (KillMode=mixed still SIGKILLs the cgroup's remaining processes once the
//     main process exits) and AbandonProcessGroup in the launchd plist. Both
//     are in deploy/; TestDeployUnitsLetWatchersOutliveDaemon pins them.
func watchCommand(exe, job string, rearmed bool) *exec.Cmd {
	args := []string{"_watch", job}
	if rearmed {
		args = append(args, "--rearmed")
	}
	cmd := exec.Command(exe, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	return cmd
}

// RearmWatchers starts missing completion watchers for live jobs at node startup.
func (a *App) RearmWatchers(start func(string) error, log func(string)) error {
	return a.rearmWatchers(start, log, false)
}

func (a *App) rearmWatchers(start func(string) error, log func(string), pendingOnly bool) error {
	entries, err := os.ReadDir(a.Jobs)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var failures []error
	for _, entry := range entries {
		id := entry.Name()
		if !entry.IsDir() || !validID(id) {
			continue
		}
		j, err := a.job(id)
		if err != nil {
			continue
		}
		credit, pendingCredit := pendingCreditRejection(j)
		if pendingOnly && !pendingCredit {
			continue
		}
		if pendingOnly && creditPublicationPaused(j, credit) && !creditPublicationAccountReady(a, j, credit) {
			continue
		}
		if (j.Reaped || j.Pane == "") && !pendingCredit || a.watchActive(id) {
			continue
		}
		// A job ended by an announced quota stop is not finished until its
		// hand-off grace has run: a watcher that died during it is re-armed.
		if _, announced := announcedQuotaStop(j); j.Terminal && !announced && !pendingCredit {
			continue
		}
		if err := start(id); err != nil {
			failures = append(failures, fmt.Errorf("re-arm watcher %s: %w", id, err))
			continue
		}
		log(id)
	}
	return errors.Join(failures...)
}

func (a *App) pauseCreditPublication(ctx context.Context, j Job, stop quotastop.Stop, cause error) error {
	latest, err := a.job(j.ID)
	if err != nil {
		return err
	}
	if creditPublicationPaused(latest, stop) {
		return nil
	}
	reason := "publication_unavailable"
	if errors.Is(cause, errCreditAccountMismatch) {
		reason = "receipt_account_mismatch"
	} else if errors.Is(cause, errCreditAccountUnidentifiable) {
		reason = "account_not_identifiable"
	}
	if latest.Owner != "" {
		at := stop.CreditExhaustedAt.UTC().Format(time.RFC3339Nano)
		text := fmt.Sprintf("[%s] CREDIT_CLOSE_PAUSED | pool=%s at=%s | 공유 exhausted-credit 게시가 일시 중단됐습니다(reason=%s). 계정 fingerprint를 확인하면 다음 안전 재시도에서 복구합니다.", j.ID, stop.Pool, at, reason)
		eventID := fmt.Sprintf("credit-close-paused-%s-%d", j.ID, stop.CreditExhaustedAt.Unix())
		r := a.call(ctx, "panewire", "emit", "--kind", "lane.event", "--lane", latest.Owner, "--type", "info", "--event-id", eventID, "--inbox-root", filepath.Dir(a.Jobs), "--text", text)
		if err := check(r, "credit publication notice"); err != nil {
			return err
		}
	} else {
		a.warn("credit rejection publication paused without an owner lane (job %s, pool %s)", j.ID, stop.Pool)
	}
	return a.event(ctx, j.ID, "job.credit_publication_paused", map[string]any{
		"pool":   stop.Pool,
		"at":     stop.CreditExhaustedAt.UTC().Format(time.RFC3339Nano),
		"reason": reason,
	})
}

// RearmPendingCreditWatchers retries durable credit publications without
// restarting other completion watchers.
func RearmPendingCreditWatchers(jobs, session string, start func(string) error, log func(string)) error {
	a, err := New(os.Stdout, os.Stderr)
	if err != nil {
		return err
	}
	a.Jobs, a.HerdrSession = jobs, session
	if start == nil {
		start = func(id string) error { return a.startWatchMode(id, true) }
	}
	return a.rearmWatchers(start, log, true)
}

// RearmNodeWatchers uses the node's jobs root and session for detached watchers.
func RearmNodeWatchers(jobs, session string, start func(string) error, log func(string)) error {
	a, err := New(os.Stdout, os.Stderr)
	if err != nil {
		return err
	}
	a.Jobs, a.HerdrSession = jobs, session
	if start == nil {
		start = func(id string) error { return a.startWatchMode(id, true) }
	}
	return a.RearmWatchers(start, log)
}
func (a *App) watch(ctx context.Context, args []string) error {
	if (len(args) != 1 && (len(args) != 2 || args[1] != "--rearmed")) || !validID(args[0]) {
		return fail(exitUsage, "invalid watch job")
	}
	id := args[0]
	lock, e := os.OpenFile(filepath.Join(a.Jobs, id, ".panewire-watch.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if !tryLock(lock) {
		return nil
	}
	timeout := time.Hour
	if v := a.envOr("PANEWIRE_COMPLETION_TIMEOUT_S", "WRK_COMPLETION_TIMEOUT_S"); v != "" {
		d, ok := timeoutValue(v)
		if !ok {
			return fail(exitUsage, "invalid completion timeout")
		}
		timeout = d
	}
	decisionTimeout := 6 * time.Hour
	if v := a.envOr("PANEWIRE_DECISION_TIMEOUT_S", ""); v != "" {
		var ok bool
		decisionTimeout, ok = timeoutValue(v)
		if !ok {
			return fail(exitUsage, "invalid decision timeout")
		}
	}
	deadline := a.Now().Add(timeout)
	var size int64 = -1
	var modified time.Time
	pending := false
	wasWaiting := false
	missing := 0
	seen := len(args) == 1
	latePoll := false
	var screenRead quotaStopWatch
	var handOff quotaHandOff
	var creditAttempts int
	quotaRetry := false
	var lastCredit time.Time
	for {
		j, e := a.job(id)
		if e != nil {
			return e
		}
		// An open hub close owns this attempt's end (watch_hub_close.go):
		// completion, the decision timeout, the deadline exit and the
		// missing-pane fallback all wait, and the watcher writes only the
		// intended record once the node's close operation is final.
		if in, ok := pendingHubCloseIntent(j); ok && !j.Reaped {
			out, err := a.reconcileHubCloseIntent(ctx, j, in)
			if err != nil {
				return err
			}
			switch out {
			case hubCloseEnded:
				return nil
			case hubCloseWait:
				if err := a.Sleep(ctx, 5*time.Second); err != nil {
					return err
				}
				continue
			}
			if j, e = a.job(id); e != nil {
				return e
			}
		}
		// The job.lost announced for a quota stop (by this watcher for a
		// reviewer, by the manager for a handed-over implementer) is not
		// an end until the grace for the hub's replacement runs out.
		if !handOff.active {
			if stop, ok := announcedQuotaStop(j); ok && !j.Reaped {
				handOff = quotaHandOff{stop: stop, until: a.Now().Add(a.QuotaHandOffGrace), active: true}
			}
		}
		credit, creditPending := pendingCreditRejection(j)
		intent, hasIntent := pendingQuotaIntent(j)
		quotaRetry = hasIntent
		agents, e := a.agents(ctx)
		if e != nil && hasIntent && !creditPending && !j.Reaped {
			// An open close intent outlives a herdr outage: hk alone can
			// finish it, and anything else waits for herdr.
			out, err := a.reconcileQuotaIntent(ctx, j, intent, false, false, false, Agent{})
			if err != nil {
				return err
			}
			if out == quotaIntentEnded {
				return nil
			}
			a.warn("quota-stop close intent waits for herdr: %v", e)
			if err := a.Sleep(ctx, 5*time.Second); err != nil {
				return err
			}
			continue
		}
		if e != nil && !creditPending {
			if workerEscalated(j) {
				return a.releaseQuota(ctx, j)
			}
			if j.Reaped || j.Terminal && !handOff.active {
				return nil
			}
			return e
		}
		present := false
		var paneAgent Agent
		for _, ag := range agents {
			if ag.Pane == j.Pane {
				present, paneAgent = true, ag
			}
		}
		if !creditPending && present {
			if stop, ok := a.creditStopped(j, paneAgent); ok && !stop.CreditExhaustedAt.Equal(lastCredit) && !creditRejectionPublished(j, stop) {
				credit = stop
				payload := map[string]any{"pool": stop.Pool, "at": stop.CreditExhaustedAt.UTC().Format(time.RFC3339Nano)}
				if paneAgent.StateChangeSeq != nil {
					payload["state_change_seq"] = *paneAgent.StateChangeSeq
				}
				if err := a.event(ctx, id, "job.credit_rejection", payload); err != nil {
					return err
				}
				j, e = a.job(id)
				if e != nil {
					return e
				}
				credit, creditPending = pendingCreditRejection(j)
			}
		}
		if creditPending {
			if err := a.publishCreditRejection(ctx, j, credit); err != nil {
				creditAttempts++
				a.warn("credit rejection publication failed (attempt %d): %v", creditAttempts, err)
				if errors.Is(err, errCreditAccountMismatch) || errors.Is(err, errCreditAccountUnidentifiable) || creditAttempts >= 3 {
					return a.pauseCreditPublication(ctx, j, credit, err)
				}
				if err := a.Sleep(ctx, 5*time.Second); err != nil {
					return err
				}
				continue
			}
			creditAttempts = 0
			lastCredit = credit.CreditExhaustedAt
			published := map[string]any{"pool": credit.Pool, "at": credit.CreditExhaustedAt.UTC().Format(time.RFC3339Nano)}
			if !j.Terminal && !j.Reaped && !workerEscalated(j) && j.QuotaRole() != "consult" && !handOff.active {
				// The pane may have resumed while the rejection was being
				// written, so the hand-off is decided on a fresh row, not on
				// the snapshot read before the write (hk 4505).
				current, currentPresent, currentReachable := a.paneAfterPublication(ctx, j.Pane)
				for !currentReachable {
					// The rejection stays pending until the pane is read: writing
					// job.credit_published now would end the retry for good (hk 4505).
					a.warn("credit hand-off waits for herdr: agent list unavailable")
					if err := a.Sleep(ctx, 5*time.Second); err != nil {
						return err
					}
					current, currentPresent, currentReachable = a.paneAfterPublication(ctx, j.Pane)
				}
				if safeCreditStopHandoff(j, credit, current, currentPresent, currentReachable) {
					if a.QuotaHandOffGrace <= 0 {
						if err := a.quotaStopLost(ctx, j, credit, true); err != nil {
							return err
						}
						return a.event(ctx, id, "job.credit_published", published)
					}
					if err := a.quotaStopAnnounce(ctx, j, credit); err != nil {
						return err
					}
					// A fresh row without a seq keeps the seq the rejection was read
					// in, so a later changed seq still shows the pane moved on (hk 4505).
					seq := current.StateChangeSeq
					if seq == nil {
						seq, _ = creditRejectionStateChangeSeq(j, credit)
					}
					handOff = quotaHandOff{stop: credit, until: a.Now().Add(a.QuotaHandOffGrace), seq: seq, active: true}
				}
			}
			if err := a.event(ctx, id, "job.credit_published", published); err != nil {
				return err
			}
		}
		if j.Reaped || j.Terminal && !handOff.active {
			return nil
		}
		if workerEscalated(j) {
			return a.releaseQuota(ctx, j)
		}
		waiting, err := a.updateDecision(ctx, j)
		if err != nil && waiting == nil {
			return err
		}
		if err != nil {
			a.warn("decision reply query unavailable: %v", err)
		}
		if wasWaiting && waiting == nil {
			deadline = a.Now().Add(timeout)
			latePoll = false
		}
		wasWaiting = waiting != nil

		if j.Report != "" {
			if info, e := os.Stat(j.Report); e == nil && info.Mode().IsRegular() {
				// An open close intent is settled against hk first (below): completing
				// the report here could close it locally while hk holds, or may hold,
				// the lost record.
				if !handOff.active && !hasIntent && pending && size == info.Size() && modified.Equal(info.ModTime()) && a.workerSignalledEnd(j) {
					return a.lifecycleWithOptions(ctx, "done", []string{id, "--report", j.Report}, true, len(args) == 2)
				}
				pending = true
				size = info.Size()
				modified = info.ModTime()
			} else {
				pending = false
			}
		}
		if waiting != nil && !hasIntent && !(pending && a.workerSignalledEnd(j)) && !a.Now().Before(waiting.at.Add(decisionTimeout)) {
			if e := a.putJobRecord(ctx, id, map[string]any{"state": "lost", "at": a.Now().UTC()}); e != nil {
				a.jobRecordFailed(ctx, id, "lost", e)
			}
			if e := a.event(ctx, id, "job.lost", map[string]any{"pane_id": j.Pane, "reason": "decision_unanswered"}); e != nil {
				return e
			}
			a.recordLostFailure(ctx, j)
			return a.releaseQuota(ctx, j)
		}
		// An announced quota stop is watched through its hand-off grace, past
		// the ordinary completion deadline: its fallback ends the job and
		// releases the lease, which nothing else would.
		if waiting == nil && !handOff.active && !quotaRetry && a.Now().After(deadline) && (!pending || latePoll) {
			fmt.Fprintln(a.Err, "watch-expired: job stays live; quota retained")
			return nil
		}
		if waiting == nil && a.Now().After(deadline) {
			latePoll = true
		}
		if present {
			seen = true
			missing = 0
		} else if seen {
			missing++
		}
		// An idle reviewer pane stopped by a quota screen with no report will
		// not finish: end the job at once with the reason, so mergewake
		// respawns the review on another model instead of waiting out a
		// grace (hk 4507). The hub's replacement needs a live hk record, so a
		// reviewer stopped on its screen is not replaced there; only an implementer
		// the manager announced, and a credit stop, get the grace.
		// The lost record is what mergewake acts on, so job.lost and the lease
		// release wait for it. The close is a durable intent (watch_quota_stop.go
		// quotaIntent): recorded before the first hk write with the exact
		// end_reason, reconciled on every poll past the completion deadline,
		// across read errors, herdr outages and restarts. Only a successful
		// contradictory look at the pane cancels it.
		// Not while a hand-off is active: its stop is already announced.
		if !handOff.active && j.QuotaRole() != "consult" {
			if hasIntent {
				out, err := a.reconcileQuotaIntent(ctx, j, intent, true, present, pending, paneAgent)
				if err != nil {
					return err
				}
				switch out {
				case quotaIntentEnded:
					return nil
				case quotaIntentCancelled:
					quotaRetry = false
				}
			} else if present && !pending {
				if stop, seen := a.quotaStopScreen(ctx, j, paneAgent, &screenRead); seen == quotaStopFound && stop.CreditExhaustedAt.IsZero() {
					if err := a.recordQuotaIntent(ctx, j, stop); err != nil {
						return err
					}
					// The next poll reconciles it with a fresh look.
					quotaRetry = true
				}
			}
		}

		if handOff.active {
			if handOff.expired(a.Now(), paneAgent, present) {
				return a.quotaStopLost(ctx, j, handOff.stop, false)
			}
			if present && !handOff.unchanged(paneAgent) {
				// The pane moved on during the grace (someone resumed it): the
				// stop is stale and the job lives. job.reprompted makes the
				// job live again, so the announced job.lost is not adopted a
				// second time and the grace does not restart.
				if e := a.event(ctx, id, "job.reprompted", map[string]any{"reason": "quota_stop_resumed", "pane_id": j.Pane}); e != nil {
					return e
				}
				handOff = quotaHandOff{}
			}
		}
		// A pane that vanished during a quota hand-off is the hub closing it
		// after placing the successor: the hk record is the hub's to write
		// (with the successor pointer), and the watcher ends the job after the
		// grace instead.
		// An unresolved close intent is settled against hk first: the bare lost
		// write below would end the job without the quota_stop record and close the
		// intent with its event.
		if missing >= 2 && !handOff.active && !quotaRetry {
			if e := a.putJobRecord(ctx, id, map[string]any{"state": "lost", "at": a.Now().UTC()}); e != nil {
				a.jobRecordFailed(ctx, id, "lost", e)
			}
			if e = a.event(ctx, id, "job.lost", map[string]any{"pane_id": j.Pane}); e != nil {
				return e
			}
			a.recordLostFailure(ctx, j)
			return a.releaseQuota(ctx, j)
		}
		if e = a.Sleep(ctx, 5*time.Second); e != nil {
			return e
		}
	}
}

func workerEscalated(j Job) bool {
	escalated := false
	for _, ev := range j.Events {
		switch ev.Kind {
		case "job.spawned", "job.reclaim", "job.reprompted":
			escalated = false
		case "job.escalate":
			escalated = true
		}
	}
	return escalated
}

// workerSignalledEnd requires a READY or DONE event from this attempt.
func (a *App) workerSignalledEnd(j Job) bool {
	var since time.Time
	for _, ev := range j.Events {
		switch ev.Kind {
		case "job.spawned", "job.reclaim", "job.reprompted":
			since = ev.when
		}
	}
	return a.paneEmittedMarker(j.Pane, since)
}

// paneEmittedMarker scans events-lane for a terminal marker the pane emitted at
// or after since. Lane events carry no job id, so the pane that `panewire emit`
// stamps is what ties an event to a job. An unreadable record is not a signal.
func (a *App) paneEmittedMarker(pane string, since time.Time) bool {
	if pane == "" {
		return false
	}
	dir := filepath.Join(filepath.Dir(a.Jobs), "events-lane")
	entries, e := os.ReadDir(dir)
	if e != nil {
		return false
	}
	// Records are stamped in whole seconds, so compare at that resolution.
	since = since.Truncate(time.Second)
	for _, entry := range entries {
		info, e := entry.Info()
		if e != nil || !info.Mode().IsRegular() || filepath.Ext(entry.Name()) != ".json" || info.Size() > laneEventMaxBytes || info.ModTime().Before(since) {
			continue
		}
		body, e := os.ReadFile(filepath.Join(dir, entry.Name()))
		if e != nil {
			continue
		}
		var record struct {
			Kind string `json:"kind"`
			Pane string `json:"pane_id"`
			Text string `json:"text"`
		}
		if json.Unmarshal(body, &record) == nil && record.Kind == "lane.event" && record.Pane == pane && laneTerminalMarker.MatchString(record.Text) {
			return true
		}
	}
	return false
}

var laneDecisionMarker = regexp.MustCompile(`^(\[[^\]]*\]\s*)?ESC\b`)

type decisionWait struct {
	id string
	at time.Time
}

// updateDecision reconstructs the pending question from durable job and lane
// records. Only a subsequent pane event or a recorded delivery answers it.
func (a *App) updateDecision(ctx context.Context, j Job) (*decisionWait, error) {
	var since time.Time
	var waiting *decisionWait
	seen := map[string]bool{}
	for _, ev := range j.Events {
		switch ev.Kind {
		case "job.spawned", "job.reclaim", "job.reprompted":
			since = ev.when
			waiting = nil
			seen = map[string]bool{}
		case "job.waiting_decision":
			id := str(ev.Payload, "event_id")
			at, _ := time.Parse(time.RFC3339Nano, str(ev.Payload, "time"))
			waiting = &decisionWait{id: id, at: at}
			seen[id] = true
		case "job.decision_resumed":
			seen[str(ev.Payload, "event_id")] = true
			waiting = nil
		}
	}
	dir := filepath.Join(filepath.Dir(a.Jobs), "events-lane")
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || filepath.Ext(entry.Name()) != ".json" || info.Size() > laneEventMaxBytes || info.ModTime().Before(since.Truncate(time.Second)) {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			continue
		}
		var r struct {
			Kind string `json:"kind"`
			Pane string `json:"pane_id"`
			Text string `json:"text"`
			ID   string `json:"event_id"`
		}
		if json.Unmarshal(body, &r) != nil || r.Kind != "lane.event" || r.Pane != j.Pane || j.Pane == "" || r.ID == "" {
			continue
		}
		at := info.ModTime()
		if waiting != nil && at.After(waiting.at) {
			if err := a.event(ctx, j.ID, "job.decision_resumed", map[string]any{"event_id": waiting.id}); err != nil {
				return nil, err
			}
			waiting = nil
		}
		if !laneDecisionMarker.MatchString(r.Text) || seen[r.ID] {
			continue
		}
		payload := map[string]any{"event_id": r.ID, "time": at.UTC().Format(time.RFC3339Nano)}
		if err := a.event(ctx, j.ID, "job.waiting_decision", payload); err != nil {
			return nil, err
		}
		waiting = &decisionWait{id: r.ID, at: at}
		seen[r.ID] = true
	}
	if waiting != nil {
		reply := a.DecisionReply
		if reply == nil {
			reply = decisionReply
		}
		answered, err := reply(ctx, j.Pane, j.HubLane, waiting.at)
		if err != nil {
			return waiting, err
		}
		if answered {
			if err := a.event(ctx, j.ID, "job.decision_resumed", map[string]any{"event_id": waiting.id}); err != nil {
				return nil, err
			}
			waiting = nil
		}
	}
	return waiting, nil
}
