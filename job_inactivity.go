package panewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/quotastop"
)

type stallSafetyDetails struct {
	Job     HubActiveJob    `json:"job"`
	Agent   HerdrAgentState `json:"agent"`
	Stop    quotastop.Stop  `json:"stop"`
	Quota   bool            `json:"quota"`
	Attempt string          `json:"attempt"`
}

type stallSafetyExamination struct {
	job, pane, attempt, screen, explanation, details, actionResult string
	since                                                          int64
	hkDone, actionDone                                             int
}

// stallSafetyConfirmPerPass bounds the hk job reads one scan pass makes, so a
// node with many journals older than the active window cannot flood hk (hk
// 4836).
const stallSafetyConfirmPerPass = 4

// errStallSafetyHKUnset is a manager without an hk job reader: its past-window
// path can confirm nothing, so it examines nothing.
var errStallSafetyHKUnset = errors.New("hk job read not configured")

// stallSafetyConfirmation is one job's cached hk verdict: whether hk and the
// pane still vouch for the job, when the verdict was made, the pane it was
// made on, and whether its refusal has been logged.
type stallSafetyConfirmation struct {
	ok     bool
	at     time.Time
	pane   string
	logged bool
}

type stallSafetyManager struct {
	state     *stallSafetyState
	root      string
	limit     time.Duration
	now       func() time.Time
	logger    *slog.Logger
	lookup    func(context.Context, string) (HubActiveJob, bool)
	oldJobs   func(context.Context, time.Time) map[string]HubActiveJob
	hkJob     func(context.Context, string) (map[string]any, error)
	hkTask    func(context.Context, string) ([]byte, error)
	session   func(context.Context, string, string) fleet.HarnessSession
	signalled func(context.Context, HubActiveJob) (string, bool, error)
	busy      func(context.Context, HubActiveJob, HerdrAgentState) (bool, error)
	sequence  func(context.Context, string) (int64, string, error)
	read      func(context.Context, string) (string, error)
	explain   func(context.Context, string) (string, error)
	detect    func(context.Context, string, HerdrAgentState, string, string, time.Time) (quotastop.Stop, bool)
	record    func(context.Context, string, string, string) error
	act       func(context.Context, stallSafetyExamination, stallSafetyDetails, string, time.Time) (string, error)
	// confirmMu guards the per-job hk verdicts and the pass's hk read budget.
	confirmMu   sync.Mutex
	confirmed   map[string]stallSafetyConfirmation
	confirmPass int
}

func (d *Daemon) initStallSafety() error {
	manager, err := newStallSafetyManager(d.idleWake.quota, d.idleWake, d.cfg.StallSafetyLimit)
	if err == nil {
		d.stallSafety = manager
	}
	return err
}

func (d *Daemon) scanStallSafety(ctx context.Context, agents []HerdrAgentState, at time.Time) error {
	return d.stallSafety.scan(ctx, agents, at)
}

func (d *Daemon) runStallSafety(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		client, err := NewHerdrClient(d.cfg.HerdrSocket)
		if err == nil {
			readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			agents, readErr := client.AgentStates(readCtx)
			cancel()
			_ = client.Close()
			if readErr == nil {
				if err := d.scanStallSafety(ctx, agents, d.stallSafety.now().UTC()); err != nil {
					d.cfg.Logger.Warn("stall safety scan failed", "error", err)
				}
			} else if err := d.stallSafety.drain(ctx, d.stallSafety.now().UTC()); err != nil {
				d.cfg.Logger.Warn("stall safety delivery failed", "error", err)
			}
		} else if err := d.stallSafety.drain(ctx, d.stallSafety.now().UTC()); err != nil {
			d.cfg.Logger.Warn("stall safety delivery failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func newStallSafetyManager(q *quotaStopManager, idle *idleWakeManager, limit time.Duration) (*stallSafetyManager, error) {
	state, err := newStallSafetyState(q.store)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 30 * time.Minute
	}
	m := &stallSafetyManager{state: state, root: q.root, limit: limit, now: time.Now, logger: q.logger, confirmed: map[string]stallSafetyConfirmation{}}
	m.lookup = q.jobForPane
	m.oldJobs = q.oldJobsByPane
	m.hkJob = q.hk.job
	m.hkTask = q.hk.task
	if m.hkJob == nil {
		m.hkJob = func(context.Context, string) (map[string]any, error) { return nil, errStallSafetyHKUnset }
	}
	if m.hkTask == nil {
		m.hkTask = func(context.Context, string) ([]byte, error) { return nil, errStallSafetyHKUnset }
	}
	m.session = func(ctx context.Context, pane, kind string) fleet.HarnessSession {
		a := &fleet.App{Runner: fleet.ExecRunner{Timeout: 5 * time.Second}, HerdrSession: q.session("", pane)}
		return a.LiveHarnessSession(ctx, pane, kind)
	}
	m.sequence = func(ctx context.Context, pane string) (int64, string, error) {
		q.store.mu.Lock()
		defer q.store.mu.Unlock()
		var seq int64
		var status string
		err := q.store.db.QueryRowContext(ctx, `SELECT state_change_seq,agent_status FROM idle_wake_panes WHERE pane_id=?`, pane).Scan(&seq, &status)
		return seq, status, err
	}
	m.signalled = func(ctx context.Context, job HubActiveJob) (string, bool, error) {
		a := &fleet.App{Jobs: filepath.Join(q.root, "jobs")}
		return a.StallAttemptState(job.JobID)
	}
	m.busy = func(ctx context.Context, job HubActiveJob, agent HerdrAgentState) (bool, error) {
		a := &fleet.App{Runner: fleet.ExecRunner{Timeout: 5 * time.Second}, HerdrSession: q.session(job.JobID, agent.PaneID)}
		return a.StallForegroundBusy(ctx, agent.PaneID, agent.Harness)
	}
	m.read = q.read
	m.explain = func(ctx context.Context, pane string) (string, error) {
		out, err := herdrCommandOutput(withRelaySession(ctx, q.session("", pane)), pane, "agent", "explain", pane, "--json")
		return string(out), err
	}
	m.detect = func(ctx context.Context, job string, agent HerdrAgentState, screen, explanation string, now time.Time) (quotastop.Stop, bool) {
		rule, kind := parseHerdrExplain([]byte(explanation))
		if kind == "" {
			kind = agent.Harness
		}
		evidence := devinPaneEvidence(withRelaySession(ctx, q.session(job, agent.PaneID)), agent.PaneID, kind)
		stop, _, ok := quotastop.Detect(screen, kind, rule, evidence, now, time.Local)
		_, _, _, profile := q.jobFacts(job, "")
		return fleet.QuotaStopPool(stop, profile), ok
	}
	m.record = q.hk.note
	m.act = func(ctx context.Context, r stallSafetyExamination, detail stallSafetyDetails, key string, now time.Time) (string, error) {
		idle.mu.Lock()
		defer idle.mu.Unlock()
		current, ok := m.lookup(ctx, r.pane)
		if !ok {
			// The examination may belong to a job older than the active
			// window. hk must still vouch for it before the net acts on it.
			if old, found := m.oldJobs(ctx, now)[r.pane]; found && old.JobID == r.job && m.vouched(ctx, old, detail.Agent, now, false) {
				current, ok = old, true
			}
		}
		if !ok || current.JobID != r.job {
			return "job no longer active", nil
		}
		attempt, signalled, err := m.signalled(ctx, current)
		if err != nil {
			return "", err
		}
		if signalled {
			return "job signalled READY or ESC", nil
		}
		if attempt != detail.Attempt {
			return "job attempt changed", nil
		}
		seq, status, err := m.sequence(ctx, r.pane)
		if err != nil {
			return "", err
		}
		if status == "working" || status == "unknown" {
			return "pane state changed", nil
		}
		episodeSeq, active, err := m.state.episodeSequence(ctx, r.job, r.pane, r.attempt, r.since)
		if err != nil {
			return "", err
		}
		if !active {
			return "inactivity episode ended", nil
		}
		if seq != episodeSeq {
			return "", fmt.Errorf("waiting for current pane observation")
		}
		if detail.Quota {
			job := current
			role, inherited, taskID, _ := q.jobFacts(job.JobID, job.Role)
			job.Role = role
			candidate := idleWakeCandidate{PaneID: r.pane, WorkspaceID: detail.Agent.WorkspaceID, Label: detail.Agent.Label, Status: status, EventID: "stall:" + key, JobID: job.JobID, OwnerLane: job.OwnerLane}
			q.store.mu.Lock()
			_, err := q.store.db.ExecContext(ctx, `INSERT OR IGNORE INTO quota_stall_wakes(event_id,pane_id,state_change_seq) VALUES(?,?,?)`, candidate.EventID, r.pane, seq)
			q.store.mu.Unlock()
			if err != nil {
				return "", err
			}
			return "quota stop submitted to policy", q.actOnQuotaStop(ctx, candidate, job, inherited, taskID, detail.Stop, "stall-examination", now)
		}
		text := stallNotificationText(r.job, r.pane, r.screen)
		id := "stall:" + key
		record := emitRecord{Type: "lane.event", Epoch: 1, CreatedAt: time.UnixMilli(r.since).UTC().Format(time.RFC3339Nano), OwnerLane: detail.Job.OwnerLane, EventID: id, Text: text, PaneID: r.pane, NotifyType: "blocked"}
		if _, err := ensureLaneEmitRecord(q.root, record); err != nil {
			return "", err
		}
		if q.enqueue != nil {
			q.enqueue(hubScannedRelayEvent{Kind: "lane.event", NotifyType: "blocked", HubActiveJob: HubActiveJob{JobID: laneEventTransportID(record.OwnerLane, id), Epoch: 1, OwnerLane: record.OwnerLane}, PaneID: r.pane, EventID: id, Text: text})
		}
		return "blocked event recorded for owner", nil
	}
	return m, nil
}

func (m *stallSafetyManager) scan(ctx context.Context, agents []HerdrAgentState, now time.Time) error {
	if err := m.state.retainPanes(ctx, agents); err != nil {
		return err
	}
	m.confirmMu.Lock()
	m.confirmPass = 0
	m.confirmMu.Unlock()
	var old map[string]HubActiveJob
	oldRead := false
	for _, agent := range agents {
		job, ok := m.lookup(ctx, agent.PaneID)
		if !ok {
			// The pane may still host a job that started before the active
			// window. Only hk can say it is still running; until it does, the
			// old local journal stays unexamined (hk 4836).
			if agent.Status == "working" || agent.Status == "unknown" {
				continue
			}
			if !oldRead {
				old, oldRead = m.oldJobs(ctx, now), true
			}
			candidate, found := old[agent.PaneID]
			if !found || !m.vouched(ctx, candidate, agent, now, true) {
				continue
			}
			job, ok = candidate, true
		}
		if !ok || job.OwnerLane == "" {
			continue
		}
		if agent.Status == "working" || agent.Status == "unknown" {
			if _, err := m.state.observe(ctx, job.JobID, agent.PaneID, "", true, false, 0, now, m.limit); err != nil {
				return err
			}
			continue
		}
		attempt, signalled, err := m.signalled(ctx, job)
		if err != nil {
			continue
		}
		if signalled {
			if _, err := m.state.observe(ctx, job.JobID, agent.PaneID, attempt, true, false, 0, now, m.limit); err != nil {
				return err
			}
			continue
		}
		seq, status, err := m.sequence(ctx, agent.PaneID)
		if err != nil || seq <= 0 || status != agent.Status {
			continue
		}
		busy, err := m.busy(ctx, job, agent)
		if err != nil {
			continue
		}
		due, err := m.state.observe(ctx, job.JobID, agent.PaneID, attempt, false, busy, seq, now, m.limit)
		if err != nil {
			return err
		}
		if !due {
			continue
		}
		since, active, err := m.state.episodeStart(ctx, job.JobID, agent.PaneID, attempt)
		if err != nil {
			return err
		}
		if !active {
			continue
		}
		readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		screen, err := m.read(readCtx, agent.PaneID)
		if err != nil {
			screen = "screen unavailable"
		}
		explanation, err := m.explain(readCtx, agent.PaneID)
		if err != nil {
			explanation = "herdr explanation unavailable"
		}
		stop, quota := m.detect(readCtx, job.JobID, agent, screen, explanation, now)
		cancel()
		details, err := json.Marshal(stallSafetyDetails{Job: job, Agent: agent, Stop: stop, Quota: quota, Attempt: attempt})
		if err != nil {
			return err
		}
		if _, err = m.state.recordExamination(ctx, job.JobID, agent.PaneID, attempt, since, screen, explanation, string(details)); err != nil {
			return err
		}
	}
	return m.drain(ctx, now)
}

func (m *stallSafetyManager) drain(ctx context.Context, now time.Time) error {
	m.state.store.mu.Lock()
	rows, err := m.state.store.db.QueryContext(ctx, `SELECT job,pane,attempt,since_ms,screen,explanation,details,hk_done,action_done,action_result FROM stall_safety_examinations WHERE details<>'' AND (hk_done=0 OR action_done=0)`)
	if err != nil {
		m.state.store.mu.Unlock()
		return err
	}
	var pending []stallSafetyExamination
	for rows.Next() {
		var r stallSafetyExamination
		if err = rows.Scan(&r.job, &r.pane, &r.attempt, &r.since, &r.screen, &r.explanation, &r.details, &r.hkDone, &r.actionDone, &r.actionResult); err != nil {
			break
		}
		pending = append(pending, r)
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	m.state.store.mu.Unlock()
	if err != nil {
		return err
	}
	for _, r := range pending {
		var details stallSafetyDetails
		if err := json.Unmarshal([]byte(r.details), &details); err != nil {
			return err
		}
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%d", r.job, r.pane, r.attempt, r.since)))
		key := hex.EncodeToString(sum[:])
		if r.actionDone == 0 {
			if result, err := m.act(ctx, r, details, key, now); err == nil {
				r.actionResult = result
				if err := m.mark(ctx, r, "action_done"); err != nil {
					return err
				}
				r.actionDone = 1
				r.hkDone = 0
			}
		}
		if r.hkDone == 0 {
			action := r.actionResult
			if action == "" {
				action = "pending; delivery will be retried"
			}
			body := fmt.Sprintf("job: %s\npane: %s\naction: %s\nherdr explanation:\n%s\nscreen:\n%s\n", r.job, r.pane, action, r.explanation, r.screen)
			if err := m.record(ctx, "stall-examination/"+key, r.job, body); err == nil {
				if err := m.mark(ctx, r, "hk_done"); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (m *stallSafetyManager) mark(ctx context.Context, r stallSafetyExamination, column string) error {
	if column != "hk_done" && column != "action_done" {
		return fmt.Errorf("invalid stall outbox field")
	}
	m.state.store.mu.Lock()
	defer m.state.store.mu.Unlock()
	query := `UPDATE stall_safety_examinations SET hk_done=1 WHERE job=? AND pane=? AND attempt=? AND since_ms=?`
	if column == "action_done" {
		_, err := m.state.store.db.ExecContext(ctx, `UPDATE stall_safety_examinations SET action_done=1,action_result=?,hk_done=0 WHERE job=? AND pane=? AND attempt=? AND since_ms=?`, r.actionResult, r.job, r.pane, r.attempt, r.since)
		return err
	}
	_, err := m.state.store.db.ExecContext(ctx, query, r.job, r.pane, r.attempt, r.since)
	return err
}

// vouched reports whether hk and the live pane still vouch for a job older
// than the active window, caching the verdict per job for the examination
// interval. capped charges the read to one scan pass's budget and answers no,
// like a refusal, once that budget is spent; a verdict already cached costs
// nothing. A refusal or an unreadable hk is logged once per job, so a node
// with many stale journals cannot flood the log either.
func (m *stallSafetyManager) vouched(ctx context.Context, job HubActiveJob, agent HerdrAgentState, now time.Time, capped bool) bool {
	m.confirmMu.Lock()
	entry, cached := m.confirmed[job.JobID]
	if cached && entry.pane == agent.PaneID && now.Sub(entry.at) < m.limit {
		m.confirmMu.Unlock()
		return entry.ok
	}
	if capped && m.confirmPass >= stallSafetyConfirmPerPass {
		m.confirmMu.Unlock()
		return false
	}
	if capped {
		m.confirmPass++
	}
	m.confirmMu.Unlock()

	ok, err := m.hkVouches(ctx, job, agent)
	reason := ""
	switch {
	case err != nil:
		reason = err.Error()
	case !ok:
		reason = "hk does not vouch for the job"
	}
	m.confirmMu.Lock()
	logged := reason != "" && !entry.logged
	m.confirmed[job.JobID] = stallSafetyConfirmation{ok: ok, at: now, pane: agent.PaneID, logged: entry.logged || logged}
	m.confirmMu.Unlock()
	if logged {
		m.logger.Warn("stall safety left the job unexamined", "job", job.JobID, "pane", agent.PaneID, "reason", reason)
	}
	return ok
}

// hkVouches proves that a job older than the active window is still the job
// running on agent's pane: hk's job record is not terminal, the job's task (if
// any) is developing or reviewing, and hk's record and the pane's live harness
// session match the job's own job.spawned receipt. Any mismatch is a no; an
// unreadable hk is an error.
func (m *stallSafetyManager) hkVouches(ctx context.Context, job HubActiveJob, agent HerdrAgentState) (bool, error) {
	record, err := m.hkJob(ctx, job.JobID)
	if err != nil {
		return false, err
	}
	switch state, _ := record["state"].(string); state {
	case "claimed", "spawned":
	case "completed", "escalated", "lost":
		return false, nil
	default:
		return false, nil
	}
	task, taskOK := hkRecordTask(record)
	if !taskOK {
		return false, nil
	}
	if task != "" {
		body, err := m.hkTask(ctx, task)
		if err != nil {
			return false, err
		}
		var bound struct {
			State string `json:"state"`
			Refs  struct {
				Job string `json:"job_id"`
			} `json:"refs"`
		}
		if json.Unmarshal(body, &bound) != nil {
			return false, nil
		}
		if bound.Refs.Job != job.JobID || (bound.State != "developing" && bound.State != "reviewing") {
			return false, nil
		}
	}
	spawn, ok := stallSpawnIdentity(filepath.Join(m.root, "jobs", job.JobID, "events"))
	if !ok || spawn.pane != agent.PaneID || spawn.pane != job.Pane {
		return false, nil
	}
	if pane, _ := record["pane_id"].(string); pane != spawn.pane {
		return false, nil
	}
	if harness, _ := record["harness"].(string); spawn.harness.Kind == "" || harness != spawn.harness.Kind {
		return false, nil
	}
	if !usableHarnessSession(spawn.harness) {
		return false, nil
	}
	live := m.session(ctx, agent.PaneID, spawn.harness.Kind)
	if live.Kind != spawn.harness.Kind || live.ID != spawn.harness.ID || !usableHarnessSession(live) {
		return false, nil
	}
	return true, nil
}

// usableHarnessSession reports whether a harness session reference names a
// conversation: an unknown or unavailable reference cannot prove that the pane
// still hosts the session the job spawned.
func usableHarnessSession(session fleet.HarnessSession) bool {
	return session.Kind != "" && session.ID != "" && session.ID != "unknown" &&
		session.Source != "" && session.Source != "unavailable"
}

// hkRecordTask is the task id an hk job record names; ok is false when the
// record carries a value that cannot name a task, which reads as unreadable
// rather than as a job without a task.
func hkRecordTask(record map[string]any) (string, bool) {
	switch id := record["task_id"].(type) {
	case nil:
		return "", true
	case float64:
		if id == float64(int64(id)) && id > 0 {
			return strconv.FormatInt(int64(id), 10), true
		}
	case string:
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			return "", true
		}
		if n, err := strconv.ParseInt(trimmed, 10, 64); err == nil && n > 0 {
			return trimmed, true
		}
	}
	return "", false
}

// stallSpawn is what a job's newest job.spawned receipt says about where it
// landed: the pane and the harness session it started.
type stallSpawn struct {
	pane    string
	harness fleet.HarnessSession
}

// stallSpawnIdentity reads the newest job.spawned receipt under eventsDir, as
// both the flat local form and the payload form write it. The newest receipt
// wins, because a reclaimed job can have been spawned on another pane.
func stallSpawnIdentity(eventsDir string) (stallSpawn, bool) {
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		return stallSpawn{}, false
	}
	var found stallSpawn
	ok := false
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "-job.spawned.json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(eventsDir, entry.Name()))
		if err != nil || len(data) > 16<<10 {
			continue
		}
		var event struct {
			PaneID         string               `json:"pane_id"`
			HarnessSession fleet.HarnessSession `json:"harness_session"`
			Payload        struct {
				PaneID         string               `json:"pane_id"`
				HarnessSession fleet.HarnessSession `json:"harness_session"`
			} `json:"payload"`
		}
		if json.Unmarshal(data, &event) != nil {
			continue
		}
		harness := event.Payload.HarnessSession
		if harness.Kind == "" && harness.ID == "" && harness.Source == "" {
			harness = event.HarnessSession
		}
		found = stallSpawn{
			pane:    firstNonempty(event.Payload.PaneID, event.PaneID),
			harness: harness,
		}
		ok = true
	}
	return found, ok
}
