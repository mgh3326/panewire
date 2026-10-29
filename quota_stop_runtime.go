package panewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

type quotaStopManager struct {
	store      *Store
	root       string
	act        bool
	logger     *slog.Logger
	read       func(context.Context, string) (string, error)
	command    func(context.Context, ...string) ([]byte, error)
	send       func(context.Context, string, string, string) relayInjectResult
	enqueue    func(hubScannedRelayEvent) bool
	lastPolicy time.Time
	policy     quotaPolicy
	policyOK   bool
}

func newQuotaStopManager(s *Store, root string, act bool, logger *slog.Logger) (*quotaStopManager, error) {
	_, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS quota_stop_decisions (event_id TEXT PRIMARY KEY, job TEXT NOT NULL, pane TEXT NOT NULL, label TEXT NOT NULL, pool TEXT NOT NULL DEFAULT '', reset TEXT NOT NULL DEFAULT '', decision TEXT NOT NULL DEFAULT '', reason TEXT NOT NULL DEFAULT '', resume_text TEXT NOT NULL DEFAULT '', due INTEGER NOT NULL DEFAULT 0, attempted INTEGER NOT NULL DEFAULT 0, hk_recorded INTEGER NOT NULL DEFAULT 0, action_recorded INTEGER NOT NULL DEFAULT 0, hk_retry INTEGER NOT NULL DEFAULT 0, created INTEGER NOT NULL DEFAULT 0, result TEXT NOT NULL DEFAULT '')`)
	if err != nil {
		return nil, err
	}
	q := &quotaStopManager{store: s, root: root, act: act, logger: logger}
	q.read = func(ctx context.Context, pane string) (string, error) {
		b, e := exec.CommandContext(ctx, "herdr", "agent", "read", pane, "--source", "visible", "--lines", "60").Output()
		return string(b), e
	}
	q.command = func(ctx context.Context, args ...string) ([]byte, error) {
		bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return exec.CommandContext(bounded, "handoffkeep", args...).Output()
	}
	q.send = func(ctx context.Context, pane, pool, text string) relayInjectResult {
		harness := strings.ToLower(relayInjectHarness(ctx, pane))
		if (pool == "devin-free" && harness != "devin") || (pool == "claude" && harness != "claude") || (pool == "codex" && harness != "codex") {
			return relayInjectResult{Outcome: relayInjectMaybeInPane, Evidence: "harness_changed"}
		}
		return relayInjectForHarness(ctx, pane, harness, text, nil)
	}
	return q, nil
}
func quotaExcluded(label string) bool {
	s := strings.ToLower(strings.TrimSpace(label))
	return s == "" || s == "operator-desk" || strings.HasSuffix(s, "-integrator") || strings.HasPrefix(s, "director-") || strings.HasPrefix(s, "fleet-tooling-") || strings.HasPrefix(s, "consult-")
}
func (q *quotaStopManager) jobForPane(pane string) (HubActiveJob, bool) {
	var found HubActiveJob
	count := 0
	entries, err := os.ReadDir(filepath.Join(q.root, "jobs"))
	if err != nil {
		return found, false
	}
	for _, entry := range entries {
		if !entry.IsDir() || !hubJobIDPattern.MatchString(entry.Name()) {
			continue
		}
		scanned, ok := scanHubJobEventDetails(filepath.Join(q.root, "jobs", entry.Name(), "events"), entry.Name())
		if ok && scanned.job.Pane == pane {
			found = scanned.job
			count++
		}
	}
	return found, count == 1 && found.JobID != "" && !quotaExcluded(found.AgentLabel)
}
func (q *quotaStopManager) loadPolicy(ctx context.Context, now time.Time) (quotaPolicy, bool) {
	if !q.lastPolicy.IsZero() && now.Sub(q.lastPolicy) < 5*time.Minute {
		return q.policy, q.policyOK
	}
	q.lastPolicy = now
	cache := filepath.Join(q.root, "quota-stop-policy-cache.json")
	b, e := q.command(ctx, "doc", "get", "policy/quota-stop")
	if e == nil {
		var doc struct {
			Body string `json:"body"`
		}
		if json.Unmarshal(b, &doc) == nil {
			if p, ok := parseQuotaPolicy(doc.Body); ok {
				q.policy = p
				q.policyOK = true
				_ = os.MkdirAll(q.root, 0700)
				_ = os.WriteFile(cache, []byte(doc.Body), 0600)
				return p, true
			}
		}
	}
	if b, e = os.ReadFile(cache); e == nil {
		if p, ok := parseQuotaPolicy(string(b)); ok {
			q.policy = p
			q.policyOK = true
			return p, true
		}
	}
	q.policy = quotaPolicy{}
	q.policyOK = false
	return q.policy, false
}
func (q *quotaStopManager) task(ctx context.Context, job HubActiveJob) (int, time.Duration, bool) {
	b, err := q.command(ctx, "tasks", "list", "--limit", "1000")
	if err != nil {
		return 0, 0, false
	}
	var v struct {
		Tasks []struct {
			Priority int     `json:"priority"`
			Expected *string `json:"expected"`
			Refs     struct {
				Job string `json:"job_id"`
			} `json:"refs"`
		} `json:"tasks"`
	}
	if json.Unmarshal(b, &v) != nil || len(v.Tasks) >= 1000 {
		return 0, 0, false
	}
	n := 0
	priority := 0
	var duration time.Duration
	for _, t := range v.Tasks {
		if t.Refs.Job == job.JobID {
			n++
			priority = t.Priority
			if t.Expected != nil {
				duration, _ = quotaDuration(*t.Expected)
			}
		}
	}
	return priority, duration, n == 1
}
func (q *quotaStopManager) observe(ctx context.Context, c idleWakeCandidate, now time.Time) {
	job, ok := q.jobForPane(c.PaneID)
	if !ok || quotaExcluded(c.Label) {
		return
	}
	// Reserve the wake before touching the screen. A restart can never reread it.
	q.store.mu.Lock()
	result, err := q.store.db.ExecContext(ctx, `INSERT OR IGNORE INTO quota_stop_decisions(event_id,job,pane,label,created) VALUES(?,?,?,?,?)`, c.EventID, job.JobID, c.PaneID, job.AgentLabel, now.UnixMilli())
	q.store.mu.Unlock()
	if err != nil {
		return
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return
	}
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	screen, err := q.read(readCtx, c.PaneID)
	cancel()
	if err != nil {
		return
	}
	stop, ok := classifyQuotaStop(screen, now, time.Local)
	if !ok {
		return
	}
	policy, policyOK := q.loadPolicy(ctx, now)
	priority, expected, taskOK := q.task(ctx, job)
	q.store.mu.Lock()
	var waits int
	_ = q.store.db.QueryRowContext(ctx, `SELECT count(*) FROM quota_stop_decisions WHERE job=? AND attempted=1`, job.JobID).Scan(&waits)
	q.store.mu.Unlock()
	decision, reason := quotaDecision(policy, policyOK, priority, expected, waits, stop, now)
	if !taskOK {
		decision, reason = "handover", "task_unavailable"
	}
	reset := "unknown"
	due := int64(0)
	if !stop.Reset.IsZero() {
		reset = stop.Reset.UTC().Format(time.RFC3339)
		sum := sha256.Sum256([]byte(c.EventID))
		due = stop.Reset.Add(time.Duration(1+int(sum[0])%5) * time.Second).UnixMilli()
	}
	q.store.mu.Lock()
	_, err = q.store.db.ExecContext(ctx, `UPDATE quota_stop_decisions SET pool=?,reset=?,decision=?,reason=?,resume_text=?,due=?,action_recorded=? WHERE event_id=?`, stop.Pool, reset, decision, reason, policy.ResumeText, due, boolInt(!q.act), c.EventID)
	q.store.mu.Unlock()
	if err == nil {
		q.logger.Info("quota-stop decision", "job", job.JobID, "pane", c.PaneID, "pool", stop.Pool, "reset", reset, "decision", decision, "reason", reason, "shadow", !q.act)
	}
}
func (q *quotaStopManager) tick(ctx context.Context, now time.Time) {
	// The outbox is drained separately from the one-per-wake screen read.
	q.store.mu.Lock()
	rows, err := q.store.db.QueryContext(ctx, `SELECT event_id,job,pane,label,pool,reset,decision,reason,resume_text,due,attempted,hk_recorded,action_recorded,created FROM quota_stop_decisions WHERE decision<>'' AND ((hk_recorded=0 AND hk_retry<=?) OR (action_recorded=0 AND (decision='handover' OR due<=?))) ORDER BY action_recorded, event_id LIMIT 1`, now.UnixMilli(), now.UnixMilli())
	if err != nil {
		q.store.mu.Unlock()
		return
	}
	type row struct {
		id, job, pane, label, pool, reset, decision, reason, text string
		due, created                                              int64
		attempted, hk, action                                     int
	}
	var pending []row
	for rows.Next() {
		var r row
		if rows.Scan(&r.id, &r.job, &r.pane, &r.label, &r.pool, &r.reset, &r.decision, &r.reason, &r.text, &r.due, &r.attempted, &r.hk, &r.action, &r.created) == nil {
			pending = append(pending, r)
		}
	}
	_ = rows.Close()
	q.store.mu.Unlock()
	for _, r := range pending {
		sum := sha256.Sum256([]byte(r.id))
		key := hex.EncodeToString(sum[:])
		msg := fmt.Sprintf("[quota-stop] %s %s %s reset=%s decision=%s reason=%s", r.job, r.pane, r.pool, r.reset, r.decision, r.reason)
		if r.hk == 0 {
			f, e := os.CreateTemp("", "quota-stop-*.md")
			if e == nil {
				_, e = f.WriteString(msg + "\n")
				_ = f.Close()
				if e == nil {
					_, e = q.command(ctx, "doc", "put", "--key", "quota-stop/"+key, "--kind", "note", "--job", r.job, "--file", f.Name())
				}
				_ = os.Remove(f.Name())
				if e == nil {
					q.update(ctx, r.id, "hk_recorded", 1)
				} else {
					q.store.mu.Lock()
					_, _ = q.store.db.ExecContext(ctx, `UPDATE quota_stop_decisions SET hk_retry=? WHERE event_id=?`, now.Add(time.Minute).UnixMilli(), r.id)
					q.store.mu.Unlock()
				}
			}
		}
		if !q.act {
			q.update(ctx, r.id, "action_recorded", 1)
			continue
		}
		if r.decision == "handover" {
			event := emitRecord{Type: "lane.event", Epoch: 1, CreatedAt: time.UnixMilli(r.created).UTC().Format(time.RFC3339Nano), OwnerLane: "operator-desk", EventID: "quota-stop:" + key, Text: msg}
			if _, e := ensureLaneEmitRecord(q.root, event); e == nil {
				if q.enqueue != nil {
					q.enqueue(hubScannedRelayEvent{Kind: "lane.event", HubActiveJob: HubActiveJob{JobID: laneEventTransportID("operator-desk", event.EventID), Epoch: 1, OwnerLane: "operator-desk"}, EventID: event.EventID, Text: msg})
				}
				q.update(ctx, r.id, "action_recorded", 1)
			}
			continue
		}
		if r.due > now.UnixMilli() {
			continue
		}
		// Resume support requires a fresh pane identity and screen check at execution.
		// An unproven target is left untouched and handed over instead.
		job, ok := q.jobForPane(r.pane)
		if !ok || job.JobID != r.job || job.AgentLabel != r.label {
			q.failResume(ctx, r.id, "target_changed")
			continue
		}
		if r.attempted != 0 {
			q.failResume(ctx, r.id, "submission_unproven")
			continue
		}
		readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		screen, e := q.read(readCtx, r.pane)
		cancel()
		if e != nil {
			q.failResume(ctx, r.id, "screen_unavailable")
			continue
		}
		stop, classified := classifyQuotaStop(screen, now, time.Local)
		if !classified || stop.Pool != r.pool || stop.Choice {
			q.failResume(ctx, r.id, "screen_changed_or_dialog")
			continue
		}
		q.store.mu.Lock()
		result, e := q.store.db.ExecContext(ctx, `UPDATE quota_stop_decisions SET attempted=1 WHERE event_id=? AND attempted=0 AND NOT EXISTS (SELECT 1 FROM quota_stop_decisions d WHERE d.job=? AND d.attempted=1) AND EXISTS (SELECT 1 FROM idle_wake_candidates c JOIN idle_wake_panes p ON p.pane_id=c.pane_id AND p.state_change_seq=c.state_change_seq WHERE c.event_id=? AND p.agent_status IN ('idle','done','blocked'))`, r.id, r.job, r.id)
		q.store.mu.Unlock()
		if e != nil {
			continue
		}
		n, _ := result.RowsAffected()
		if n != 1 {
			q.failResume(ctx, r.id, "resume_limit_or_state_changed")
			continue
		}
		marker := relayNonce(relayHeld{Lane: "operator-desk", JobID: r.id, Text: r.text})
		sendCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		sent := q.send(sendCtx, r.pane, r.pool, r.text+" "+marker)
		cancel()
		if sent.Outcome == relayInjectDelivered && sent.Evidence != "" && slices.Contains(sent.Proven, marker) {
			q.update(ctx, r.id, "action_recorded", 1)
		} else {
			q.failResume(ctx, r.id, "submission_unproven")
		}
	}
}
func (q *quotaStopManager) update(ctx context.Context, id, col string, value int) {
	if col != "hk_recorded" && col != "action_recorded" {
		return
	}
	q.store.mu.Lock()
	defer q.store.mu.Unlock()
	_, _ = q.store.db.ExecContext(ctx, `UPDATE quota_stop_decisions SET `+col+`=? WHERE event_id=?`, value, id)
}
func (q *quotaStopManager) failResume(ctx context.Context, id, reason string) {
	q.store.mu.Lock()
	defer q.store.mu.Unlock()
	_, _ = q.store.db.ExecContext(ctx, `UPDATE quota_stop_decisions SET decision='handover',reason=?,hk_recorded=0 WHERE event_id=?`, reason, id)
}
