package panewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/quota/devin"
	"github.com/mgh3326/panewire/internal/quotastop"
)

type quotaStopManager struct {
	store   *Store
	root    string
	act     bool
	logger  *slog.Logger
	read    func(context.Context, string) (string, error)
	session func(job, pane string) string
	// hk is the manager's hk access, over HTTP with the svc-panewire
	// credential (hk 3607); tests substitute it.
	hk      quotaStopHK
	send    func(context.Context, string, string, string) relayInjectResult
	enqueue func(hubScannedRelayEvent) bool
	// announce records job.lost reason quota_stop for a handed-over job, which
	// the hub then replaces (hub_quota_replace.go).
	announce   func(ctx context.Context, job, pane string, stop quotastop.Stop) error
	mu         sync.Mutex
	lastPolicy time.Time
	policy     quotaPolicy
	policyOK   bool
	// clock times the observation phases; nil is time.Now.
	clock func() time.Time
	// show reports a pane's quota wait token, or clears it with an empty
	// value and zero ttl; nil leaves panes unmarked (herdr without
	// pane.report_metadata, or a test).
	show func(ctx context.Context, pane, value string, ttl time.Duration) error
	// showMu guards shown: the panes this manager reported and when each
	// needs its next report. tick and observe both publish.
	showMu sync.Mutex
	shown  map[string]time.Time
}

func newQuotaStopManager(s *Store, root string, act bool, logger *slog.Logger) (*quotaStopManager, error) {
	_, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS quota_stop_decisions (event_id TEXT PRIMARY KEY, job TEXT NOT NULL, pane TEXT NOT NULL, label TEXT NOT NULL, pool TEXT NOT NULL DEFAULT '', reset TEXT NOT NULL DEFAULT '', decision TEXT NOT NULL DEFAULT '', reason TEXT NOT NULL DEFAULT '', resume_text TEXT NOT NULL DEFAULT '', due INTEGER NOT NULL DEFAULT 0, attempted INTEGER NOT NULL DEFAULT 0, hk_recorded INTEGER NOT NULL DEFAULT 0, action_recorded INTEGER NOT NULL DEFAULT 0, hk_retry INTEGER NOT NULL DEFAULT 0, created INTEGER NOT NULL DEFAULT 0, result TEXT NOT NULL DEFAULT '')`)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS quota_blocked_wakes (event_id TEXT PRIMARY KEY, pane_id TEXT NOT NULL, workspace_id TEXT NOT NULL, label TEXT NOT NULL, state_change_seq INTEGER NOT NULL, changed_at INTEGER NOT NULL, due_at INTEGER NOT NULL, processed INTEGER NOT NULL DEFAULT 0)`); err != nil {
		return nil, err
	}
	if !storeHasColumn(s.db, "quota_stop_decisions", "action_retry") {
		if _, err := s.db.Exec(`ALTER TABLE quota_stop_decisions ADD COLUMN action_retry INTEGER NOT NULL DEFAULT 0`); err != nil {
			return nil, err
		}
	}
	// role and owner are the job's role and owner lane when the wake was
	// reserved: the role decides whether the manager acts in shadow mode (hk
	// 3334) and the owner receives the one info lane event.
	for _, column := range []string{"role", "owner"} {
		if !storeHasColumn(s.db, "quota_stop_decisions", column) {
			if _, err := s.db.Exec(`ALTER TABLE quota_stop_decisions ADD COLUMN ` + column + ` TEXT NOT NULL DEFAULT ''`); err != nil {
				return nil, err
			}
		}
	}
	q := &quotaStopManager{store: s, root: root, act: act, logger: logger, shown: map[string]time.Time{}}
	q.session = func(job, pane string) string {
		return nodeJobHerdrSessionByPaneFromRoot(root, pane)
	}
	q.announce = func(ctx context.Context, job, pane string, stop quotastop.Stop) error {
		return fleet.AnnounceQuotaStop(ctx, filepath.Join(root, "jobs"), job, pane, stop)
	}
	q.read = func(ctx context.Context, pane string) (string, error) {
		session := ""
		if q.session != nil {
			session = q.session("", pane)
		}
		readCtx := withRelaySession(ctx, session)
		b, e := herdrCommandOutput(readCtx, pane, "agent", "read", pane, "--source", "visible", "--lines", "60")
		return string(b), e
	}
	q.hk = quotaStopHK{
		policy: func(ctx context.Context) ([]byte, error) { return fleet.HKDocumentAnswer(ctx, quotaStopPolicyKey) },
		task:   func(ctx context.Context, id string) ([]byte, error) { return fleet.HKTaskAnswer(ctx, id) },
		tasks: func(ctx context.Context, afterID int64) ([]byte, error) {
			return fleet.HKTaskListPageAnswer(ctx, afterID, quotaStopTaskLimit)
		},
		note: func(ctx context.Context, key, job, body string) error {
			return fleet.PutHKDocument(ctx, key, "note", job, body)
		},
	}
	q.send = func(ctx context.Context, pane, pool, text string) relayInjectResult {
		session := ""
		if q.session != nil {
			session = q.session("", pane)
		}
		sendCtx := withRelaySession(ctx, session)
		harness := strings.ToLower(relayInjectHarness(sendCtx, pane))
		if (pool == "devin-free" && harness != "devin") || (pool == "claude" && harness != "claude") || (pool == "codex" && harness != "codex") {
			return relayInjectResult{Outcome: relayInjectMaybeInPane, Evidence: "harness_changed"}
		}
		return relayInjectForHarnessGuarded(sendCtx, pane, harness, text, nil, func(screen string) bool {
			stop, ok := classifyQuotaStop(screen, time.Now(), time.Local)
			_, composerOK := relayComposer(harness, screen)
			return ok && stop.Pool == pool && !stop.Choice && composerOK
		})
	}
	return q, nil
}

// quotaStopHK is the quota stop manager's hk access (hk 3607), which ran
// `handoffkeep doc get policy/quota-stop`, `tasks list --limit 1000` and `doc
// put --kind note --job` until then. policy and tasks give hk's 2xx answer,
// which the manager reads as it read that CLI output; any other outcome is
// an error. note writes one note document for the job.
type quotaStopHK struct {
	policy func(context.Context) ([]byte, error)
	task   func(context.Context, string) ([]byte, error)
	tasks  func(context.Context, int64) ([]byte, error)
	note   func(ctx context.Context, key, job, body string) error
}

const (
	quotaStopPolicyKey = "policy/quota-stop"
	// quotaStopTaskLimit is the task lookup page size. Full pages are followed
	// by stable ID cursors until hk returns a short page.
	quotaStopTaskLimit = 1000
)

func quotaExcluded(label string) bool {
	s := strings.ToLower(strings.TrimSpace(label))
	return s == "" || s == "operator-desk" || s == "director" || s == "consult" || strings.HasSuffix(s, "-integrator") || strings.HasPrefix(s, "director-") || strings.HasPrefix(s, "fleet-tooling-") || strings.HasPrefix(s, "consult-")
}

func quotaTaskID(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func quotaTaskPriority(value any) (int, bool) {
	switch n := value.(type) {
	case nil:
		return 0, true
	case float64:
		if n == float64(int(n)) {
			return int(n), true
		}
	case string:
		priority, err := strconv.Atoi(strings.TrimSpace(n))
		return priority, err == nil
	}
	return 0, false
}

func (q *quotaStopManager) jobForPane(ctx context.Context, pane string) (HubActiveJob, bool) {
	var found HubActiveJob
	count := 0
	entries, err := os.ReadDir(filepath.Join(q.root, "jobs"))
	if err != nil {
		return found, false
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return HubActiveJob{}, false
		}
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

// jobFacts reads what the job's own events say that the scanned active job
// does not keep: the role and the task it inherited. The role is the spawn
// receipt's (reviewer, tester, worker): the claim always says worker, and the
// scanned job keeps only worker and captain. inherited is the builder job a
// tester's task is still bound to (the claim's task_inherited_from).
func (q *quotaStopManager) jobFacts(jobID, scanned string) (role, inherited, taskID string) {
	dir := filepath.Join(q.root, "jobs", jobID, "events")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return scanned, "", ""
	}
	role = scanned
	claimRole, spawnRole := "", ""
	slices.SortFunc(entries, func(a, b os.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil || len(contents) > 16<<10 {
			continue
		}
		var event hubInboxEvent
		if json.Unmarshal(contents, &event) != nil {
			continue
		}
		var extra struct {
			Inherited string `json:"task_inherited_from"`
			TaskID    any    `json:"task_id"`
			Payload   struct {
				Inherited string `json:"task_inherited_from"`
				TaskID    any    `json:"task_id"`
			} `json:"payload"`
		}
		_ = json.Unmarshal(contents, &extra)
		switch kind := event.eventKind(); kind {
		case "job.claimed", "job.claim":
			if id := firstHubValue(quotaTaskID(extra.TaskID), quotaTaskID(extra.Payload.TaskID)); id != "" {
				taskID = id
			}
			if event.role() != "" {
				claimRole = event.role()
			}
			if v := firstHubValue(extra.Inherited, extra.Payload.Inherited); v != "" {
				inherited = v
			}
		case "job.spawned":
			if taskID == "" {
				taskID = firstHubValue(quotaTaskID(extra.TaskID), quotaTaskID(extra.Payload.TaskID))
			}
			if event.role() != "" {
				spawnRole = event.role()
			}
		}
	}
	switch {
	case spawnRole != "":
		role = spawnRole
	case claimRole != "":
		role = claimRole
	}
	return role, inherited, taskID
}

func (q *quotaStopManager) loadPolicy(ctx context.Context, now time.Time) (quotaPolicy, bool) {
	if !q.lastPolicy.IsZero() && now.Sub(q.lastPolicy) < 5*time.Minute {
		return q.policy, q.policyOK
	}
	q.lastPolicy = now
	cache := filepath.Join(q.root, "quota-stop-policy-cache.json")
	b, e := q.hk.policy(ctx)
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

// task finds the hk task bound to the job, or to the builder job a tester
// inherited its task from.
func (q *quotaStopManager) task(ctx context.Context, job HubActiveJob, inherited, taskID string) (int, time.Duration, bool) {
	if taskID != "" {
		b, err := q.hk.task(ctx, taskID)
		if err != nil {
			q.logger.Warn("quota-stop task lookup failed", "job", job.JobID, "task", taskID, "error", err)
			return 0, 0, false
		}
		var t struct {
			Priority any     `json:"priority"`
			Expected *string `json:"expected"`
			Refs     struct {
				Job string `json:"job_id"`
			} `json:"refs"`
		}
		if json.Unmarshal(b, &t) != nil || t.Refs.Job != job.JobID && (inherited == "" || t.Refs.Job != inherited) {
			q.logger.Warn("quota-stop task lookup failed", "job", job.JobID, "task", taskID, "error", "unreadable_or_unbound_task")
			return 0, 0, false
		}
		priority, valid := quotaTaskPriority(t.Priority)
		if !valid {
			q.logger.Warn("quota-stop task lookup failed", "job", job.JobID, "task", taskID, "error", "invalid_priority")
			return 0, 0, false
		}
		var duration time.Duration
		if t.Expected != nil {
			duration, _ = quotaDuration(*t.Expected)
		}
		return priority, duration, true
	}
	n := 0
	priority := 0
	var duration time.Duration
	var afterID int64
	for {
		var v struct {
			Tasks []struct {
				ID       json.Number `json:"id"`
				Priority any         `json:"priority"`
				Expected *string     `json:"expected"`
				Refs     struct {
					Job string `json:"job_id"`
				} `json:"refs"`
			} `json:"tasks"`
		}
		b, err := q.hk.tasks(ctx, afterID)
		if err != nil {
			q.logger.Warn("quota-stop task lookup failed", "job", job.JobID, "after_id", afterID, "error", err)
			return 0, 0, false
		}
		if json.Unmarshal(b, &v) != nil {
			q.logger.Warn("quota-stop task lookup failed", "job", job.JobID, "after_id", afterID, "error", "unreadable_task_page")
			return 0, 0, false
		}
		for _, t := range v.Tasks {
			if t.Refs.Job == job.JobID || inherited != "" && t.Refs.Job == inherited {
				n++
				parsedPriority, valid := quotaTaskPriority(t.Priority)
				if !valid {
					q.logger.Warn("quota-stop task lookup failed", "job", job.JobID, "task", quotaTaskID(t.ID), "error", "invalid_priority")
					return 0, 0, false
				}
				priority = parsedPriority
				if t.Expected != nil {
					duration, _ = quotaDuration(*t.Expected)
				}
			}
		}
		if len(v.Tasks) < quotaStopTaskLimit {
			break
		}
		lastID, err := strconv.ParseInt(string(v.Tasks[len(v.Tasks)-1].ID), 10, 64)
		if err != nil || lastID <= afterID {
			q.logger.Warn("quota-stop task lookup failed", "job", job.JobID, "after_id", afterID, "error", "invalid_task_page_cursor")
			return 0, 0, false
		}
		afterID = lastID
	}
	if n != 1 {
		reason := "no_matching_task"
		if n > 1 {
			reason = "ambiguous_matching_tasks"
		}
		q.logger.Warn("quota-stop task lookup failed", "job", job.JobID, "error", reason)
		return priority, duration, false
	}
	return priority, duration, true
}

// Observation budgets. Each phase has its own bound, so a slow job scan or a
// slow reservation can no longer leave the screen read no time at all (hk
// 2846: every shadow decision on one host was screen_read_timeout while a
// read takes ~10 ms).
const (
	quotaStopScanBudget = 2 * time.Second
	quotaStopReadBudget = 5 * time.Second
	// quotaStopLookupBudget bounds the policy and task lookups a quota screen
	// needs; the decision is recorded even when they run out.
	quotaStopLookupBudget = 5 * time.Second
)

func (q *quotaStopManager) observeBounded(ctx context.Context, c idleWakeCandidate, now time.Time) {
	q.observe(ctx, c, now)
}

func (q *quotaStopManager) observe(ctx context.Context, c idleWakeCandidate, now time.Time) {
	clock := q.clock
	if clock == nil {
		clock = time.Now
	}
	started := clock()
	scanCtx, scanCancel := context.WithTimeout(ctx, quotaStopScanBudget)
	defer scanCancel()
	job, ok := q.jobForPane(scanCtx, c.PaneID)
	if !ok || quotaExcluded(c.Label) {
		return
	}
	var inherited string
	var taskID string
	job.Role, inherited, taskID = q.jobFacts(job.JobID, job.Role)
	// Reserve the wake before touching the screen. A restart can never reread it.
	fresh, _, err := q.reserveDecision(scanCtx, c, job, now)
	if err != nil || !fresh {
		return
	}
	scanned := clock()
	// The screen read and herdr's agent explain share one budget, so the herdr
	// lookup cannot add a second timeout to each serial scan.
	readCtx, cancel := context.WithTimeout(ctx, quotaStopReadBudget)
	defer cancel()
	screen, err := q.read(readCtx, c.PaneID)
	readErr := readCtx.Err()
	read := clock()
	timing := []any{"scan_ms", scanned.Sub(started).Milliseconds(), "read_ms", read.Sub(scanned).Milliseconds()}
	if err != nil {
		// A wake is already reserved and cannot be replayed safely. Record a
		// conservative handover using a fresh DB context. The reason says
		// which bound ended the read: its own deadline, the caller's
		// cancellation, or the read command itself.
		reason := "screen_read_failed"
		switch {
		case ctx.Err() != nil:
			reason = "observation_canceled"
		case readErr == context.DeadlineExceeded:
			reason = "screen_read_timeout"
		}
		q.logger.Info("quota-stop observation", append([]any{"pane", c.PaneID, "outcome", reason}, timing...)...)
		dbCtx, done := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer done()
		q.recordDecisionFor(dbCtx, c, job.Role, now, "unknown", "unknown", "handover", reason, "", 0)
		return
	}
	rule, agent := herdrQuotaSignal(readCtx, c.PaneID)
	stop, basis, ok := quotastop.Detect(screen, agent, rule, now, time.Local)
	outcome := "not_quota"
	if ok {
		outcome = "quota"
	}
	// Every read screen is counted, quota or not, so shadow mode shows that it
	// classified real screens. Screen text is never logged.
	q.logger.Info("quota-stop observation", append([]any{"pane", c.PaneID, "outcome", outcome, "basis", basis}, timing...)...)
	if !ok {
		return
	}
	// A devin free-tier limit is a fact about the host, not the pane: record
	// it even when this stop is only observed, so the next devin measurement
	// publishes it and later selections skip the free rung until its reset.
	if stop.Pool == "devin-free" {
		if err := fleet.RecordFreeLimit(devin.Config{Now: func() time.Time { return now }}, job.JobID, stop); err != nil {
			q.logger.Warn("free-limit observation failed", "error", err)
		}
	}
	// recordDecisionFor logs a failed write itself; there is nothing more to do here.
	_ = q.actOnQuotaStop(ctx, c, job, inherited, taskID, stop, basis, now)
}

// reserveDecision creates the decision row for candidate c if it does not
// exist yet. fresh reports that this call created it; decided reports that the
// row already carries a decision, so the stop was acted on before.
func (q *quotaStopManager) reserveDecision(ctx context.Context, c idleWakeCandidate, job HubActiveJob, now time.Time) (fresh, decided bool, err error) {
	q.store.mu.Lock()
	defer q.store.mu.Unlock()
	result, err := q.store.db.ExecContext(ctx, `INSERT OR IGNORE INTO quota_stop_decisions(event_id,job,pane,label,created,role,owner) VALUES(?,?,?,?,?,?,?)`, c.EventID, job.JobID, c.PaneID, job.AgentLabel, now.UnixMilli(), job.Role, job.OwnerLane)
	if err != nil {
		return false, false, err
	}
	if n, _ := result.RowsAffected(); n == 1 {
		return true, false, nil
	}
	var decision string
	if err := q.store.db.QueryRowContext(ctx, `SELECT decision FROM quota_stop_decisions WHERE event_id=?`, c.EventID).Scan(&decision); err != nil {
		return false, false, err
	}
	return false, decision != "", nil
}

// herdrQuotaSignal reads herdr's agent detection for pane: the rule that
// matched and the agent kind. Any failure reads as no signal, so the
// classifier decides alone. herdr 0.9.5 returns agent and matched_rule at the
// top level; the result-wrapped shape is accepted too.
func herdrQuotaSignal(ctx context.Context, pane string) (rule, agent string) {
	out, err := herdrCommandOutput(ctx, pane, "agent", "explain", pane, "--json")
	if err != nil {
		return "", ""
	}
	return parseHerdrExplain(out)
}

// parseHerdrExplain reads the matched rule and agent kind from herdr agent
// explain JSON. The top-level keys win; a result wrapper fills what they lack.
func parseHerdrExplain(out []byte) (rule, agent string) {
	var reply struct {
		Agent       string          `json:"agent"`
		MatchedRule json.RawMessage `json:"matched_rule"`
		Result      *struct {
			Agent       string          `json:"agent"`
			MatchedRule json.RawMessage `json:"matched_rule"`
		} `json:"result"`
	}
	if json.Unmarshal(out, &reply) != nil {
		return "", ""
	}
	agent, matched := reply.Agent, reply.MatchedRule
	if reply.Result != nil {
		if agent == "" {
			agent = reply.Result.Agent
		}
		if len(matched) == 0 {
			matched = reply.Result.MatchedRule
		}
	}
	return ruleName(matched), agent
}

// ruleName accepts a matched rule as a bare id or as an object with an id.
func ruleName(raw json.RawMessage) string {
	var name string
	if json.Unmarshal(raw, &name) == nil {
		return name
	}
	var obj struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(raw, &obj) == nil {
		return obj.ID
	}
	return ""
}

// actOnQuotaStop decides the policy action for a stop recognised on job and
// records it in the outbox. tick then writes the hk quota-stop note and runs
// the hand-over or resume. basis names the signal that recognised the stop.
func (q *quotaStopManager) actOnQuotaStop(ctx context.Context, c idleWakeCandidate, job HubActiveJob, inherited, taskID string, stop quotaStop, basis string, now time.Time) error {
	// The action owns the reservation so direct callers (the stall net) work
	// without observe. A row already decided means this stop was acted on.
	resCtx, resCancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	_, decided, err := q.reserveDecision(resCtx, c, job, now)
	resCancel()
	if err != nil {
		q.logger.Warn("quota-stop decision was not reserved", "error", err)
		return err
	}
	if decided {
		return nil
	}
	lookupCtx, lookupCancel := context.WithTimeout(ctx, quotaStopLookupBudget)
	defer lookupCancel()
	policy, policyOK := q.loadPolicy(lookupCtx, now)
	priority, expected, taskOK := q.task(lookupCtx, job, inherited, taskID)
	// Preserve the decision even if the bounded external lookup timed out.
	dbCtx, dbCancel := context.WithTimeout(context.WithoutCancel(lookupCtx), time.Second)
	defer dbCancel()
	q.store.mu.Lock()
	var waits int
	waitErr := q.store.db.QueryRowContext(dbCtx, `SELECT count(*) FROM quota_stop_decisions WHERE job=? AND attempted=1`, job.JobID).Scan(&waits)
	q.store.mu.Unlock()
	decision, reason := quotaDecision(policy, policyOK, priority, expected, waits, stop, now)
	if quotaActsFor(job.Role) {
		started, _ := time.Parse(time.RFC3339, job.StartedAt)
		decision, reason = quotaDecisionJob(policy, policyOK, priority, expected, started, waits, stop, now)
	}
	if !taskOK || waitErr != nil {
		decision, reason = "handover", "task_unavailable"
	}
	// herdr refuses agent.prompt on a blocked pane (agent_blocked) and sends
	// nothing, so a blocked wake can only be observed and handed over; a
	// resume would always fail and spend the pane's one wait (hk 2850).
	if c.Status == "blocked" && decision == "wait" {
		decision, reason = "handover", "blocked_dialog"
	}
	reset := "unknown"
	due := int64(0)
	if !stop.Reset.IsZero() {
		reset = stop.Reset.UTC().Format(time.RFC3339)
		sum := sha256.Sum256([]byte(c.EventID))
		due = stop.Reset.Add(time.Duration(1+int(sum[0])%5) * time.Second).UnixMilli()
	}
	q.logger.Info("quota-stop action", "pane", c.PaneID, "basis", basis, "pool", stop.Pool, "decision", decision, "reason", reason)
	recorded, err := q.recordDecisionFor(dbCtx, c, job.Role, now, stop.Pool, reset, decision, reason, policy.ResumeText, due)
	if err != nil || !recorded || decision != "wait" {
		return err
	}
	// The wait is visible on the pane from the moment it is decided; tick
	// refreshes a token that outlives one report and the resume or hand-over
	// clears it.
	if q.quotaWaits(job.Role, stop.Pool) {
		q.showQuotaWait(ctx, c.PaneID, stop.Pool, reset, now)
	}
	return nil
}

// recordDecision deduplicates known resets exactly; unknown resets are scoped
// to an hour so a later independent stop on a reused pane is still reported.
func (q *quotaStopManager) recordDecision(ctx context.Context, c idleWakeCandidate, now time.Time, pool, reset, decision, reason, resume string, due int64) error {
	_, err := q.recordDecisionFor(ctx, c, "", now, pool, reset, decision, reason, resume, due)
	return err
}

// recordDecisionFor is recordDecision for a job of role: a worker or tester's
// decision is left for the manager to act on even in shadow mode. recorded is
// false when a duplicate decision or an existing pane+reset left the row as it
// was, so a caller can tell that this call decided nothing.
func (q *quotaStopManager) recordDecisionFor(ctx context.Context, c idleWakeCandidate, role string, now time.Time, pool, reset, decision, reason, resume string, due int64) (recorded bool, err error) {
	shadow := !q.act && !quotaActsOn(role, pool)
	q.store.mu.Lock()
	result, err := q.store.db.ExecContext(ctx, `UPDATE quota_stop_decisions SET pool=?,reset=?,decision=?,reason=?,resume_text=?,due=?,action_recorded=? WHERE event_id=? AND decision='' AND NOT EXISTS (SELECT 1 FROM quota_stop_decisions d WHERE d.pane=? AND d.reset=? AND d.decision<>'' AND d.event_id<>? AND (? <> 'unknown' OR d.created / 3600000 = ?))`, pool, reset, decision, reason, resume, due, boolInt(shadow), c.EventID, c.PaneID, reset, c.EventID, reset, now.UnixMilli()/3600000)
	q.store.mu.Unlock()
	if err != nil {
		q.logger.Warn("quota-stop decision was not recorded", "error", err)
		return false, err
	}
	n, _ := result.RowsAffected()
	if n != 0 {
		q.logger.Info("quota-stop decision", "pane", c.PaneID, "pool", pool, "reset", reset, "decision", decision, "reason", reason, "shadow", shadow)
	}
	return n != 0, nil
}
func (q *quotaStopManager) tick(ctx context.Context, now time.Time, advanceSettle bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if advanceSettle {
		q.drainBlocked(ctx, now)
	}
	// A pending wait keeps its pane token before the outbox can clear it.
	q.refreshQuotaWaits(ctx, now)
	// The outbox is drained separately from the one-per-wake screen read.
	q.store.mu.Lock()
	rows, err := q.store.db.QueryContext(ctx, `SELECT event_id,job,pane,label,pool,reset,decision,reason,resume_text,due,attempted,hk_recorded,action_recorded,created,action_retry,role,owner FROM quota_stop_decisions WHERE decision<>'' AND ((hk_recorded=0 AND hk_retry<=?) OR (action_recorded=0 AND action_retry<=? AND (decision='handover' OR due<=?))) ORDER BY action_recorded, event_id LIMIT 1`, now.UnixMilli(), now.UnixMilli(), now.UnixMilli())
	if err != nil {
		q.store.mu.Unlock()
		return
	}
	type row struct {
		id, job, pane, label, pool, reset, decision, reason, text, role, owner string
		due, created, actionRetry                                              int64
		attempted, hk, action                                                  int
	}
	var pending []row
	for rows.Next() {
		var r row
		if rows.Scan(&r.id, &r.job, &r.pane, &r.label, &r.pool, &r.reset, &r.decision, &r.reason, &r.text, &r.due, &r.attempted, &r.hk, &r.action, &r.created, &r.actionRetry, &r.role, &r.owner) == nil {
			pending = append(pending, r)
		}
	}
	_ = rows.Close()
	q.store.mu.Unlock()
	for _, r := range pending {
		sum := sha256.Sum256([]byte(r.id))
		key := hex.EncodeToString(sum[:])
		msg := fmt.Sprintf("[quota-stop] %s %s %s reset=%s decision=%s reason=%s", r.job, r.pane, r.pool, r.reset, r.decision, r.reason)
		// An action may be due while hk is still in backoff; do not retry hk early.
		var hkDue int64
		q.store.mu.Lock()
		_ = q.store.db.QueryRowContext(ctx, `SELECT hk_retry FROM quota_stop_decisions WHERE event_id=?`, r.id).Scan(&hkDue)
		q.store.mu.Unlock()
		if r.hk == 0 && hkDue <= now.UnixMilli() {
			if e := q.hk.note(ctx, "quota-stop/"+key, r.job, msg+"\n"); e == nil {
				q.update(ctx, r.id, "hk_recorded", 1)
			} else {
				q.store.mu.Lock()
				_, _ = q.store.db.ExecContext(ctx, `UPDATE quota_stop_decisions SET hk_retry=? WHERE event_id=?`, now.Add(time.Minute).UnixMilli(), r.id)
				q.store.mu.Unlock()
			}
		}
		if r.action != 0 || r.actionRetry > now.UnixMilli() {
			continue
		}
		// A worker or tester is acted on whatever --quota-stop-act says (hk
		// 3334); every other role is only recorded unless the flag is set.
		if !q.act && !quotaActsOn(r.role, r.pool) {
			q.update(ctx, r.id, "action_recorded", 1)
			continue
		}
		if r.decision == "handover" && quotaActsOn(r.role, r.pool) {
			q.handOver(ctx, r.id, r.job, r.pane, r.pool, r.reset, now)
			continue
		}
		if r.decision == "handover" {
			event := emitRecord{Type: "lane.event", Epoch: 1, CreatedAt: time.UnixMilli(r.created).UTC().Format(time.RFC3339Nano), OwnerLane: "operator-desk", EventID: "quota-stop:" + key, Text: msg}
			if _, e := ensureLaneEmitRecord(q.root, event); e == nil {
				if q.enqueue != nil {
					q.enqueue(hubScannedRelayEvent{Kind: "lane.event", HubActiveJob: HubActiveJob{JobID: laneEventTransportID("operator-desk", event.EventID), Epoch: 1, OwnerLane: "operator-desk"}, EventID: event.EventID, Text: msg})
				}
				q.update(ctx, r.id, "action_recorded", 1)
				q.clearQuotaWait(ctx, r.pane)
			} else {
				q.backoffAction(ctx, r.id, now)
			}
			continue
		}
		if r.due > now.UnixMilli() {
			continue
		}
		// Resume support requires a fresh pane identity and screen check at execution.
		// An unproven target is left untouched and handed over instead.
		job, ok := q.jobForPane(ctx, r.pane)
		if !ok || job.JobID != r.job || job.AgentLabel != r.label {
			q.failResume(ctx, r.id, r.pane, "target_changed")
			continue
		}
		if r.attempted != 0 {
			q.failResume(ctx, r.id, r.pane, "submission_unproven")
			continue
		}
		// A pane that is blocked now is never resumed, and its screen is not read: herdr would refuse the
		// prompt, and the refusal would spend the one wait. Hand it over
		// without marking an attempt.
		var status string
		q.store.mu.Lock()
		_ = q.store.db.QueryRowContext(ctx, `SELECT agent_status FROM idle_wake_panes WHERE pane_id=?`, r.pane).Scan(&status)
		q.store.mu.Unlock()
		if status == "blocked" {
			q.failResume(ctx, r.id, r.pane, "blocked_dialog")
			continue
		}
		readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		screen, e := q.read(readCtx, r.pane)
		cancel()
		if e != nil {
			q.failResume(ctx, r.id, r.pane, "screen_unavailable")
			continue
		}
		stop, classified := classifyQuotaStop(screen, now, time.Local)
		if !classified || stop.Pool != r.pool || stop.Choice {
			q.failResume(ctx, r.id, r.pane, "screen_changed_or_dialog")
			continue
		}
		q.store.mu.Lock()
		result, e := q.store.db.ExecContext(ctx, `UPDATE quota_stop_decisions SET attempted=1 WHERE event_id=? AND attempted=0 AND NOT EXISTS (SELECT 1 FROM quota_stop_decisions d WHERE d.job=? AND d.attempted=1) AND EXISTS (SELECT 1 FROM idle_wake_candidates c JOIN idle_wake_panes p ON p.pane_id=c.pane_id AND p.state_change_seq=c.state_change_seq WHERE c.event_id=? AND p.agent_status IN ('idle','done'))`, r.id, r.job, r.id)
		q.store.mu.Unlock()
		if e != nil {
			q.backoffAction(ctx, r.id, now)
			continue
		}
		n, _ := result.RowsAffected()
		if n != 1 {
			q.failResume(ctx, r.id, r.pane, "resume_limit_or_state_changed")
			continue
		}
		marker := relayNonce(relayHeld{Lane: "operator-desk", JobID: r.id, Text: r.text})
		sendCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		sent := q.send(sendCtx, r.pane, r.pool, r.text+" "+marker)
		cancel()
		if sent.Outcome == relayInjectDelivered && sent.Evidence != "" && slices.Contains(sent.Proven, marker) {
			q.update(ctx, r.id, "action_recorded", 1)
			q.clearQuotaWait(ctx, r.pane)
			if quotaActsOn(r.role, r.pool) {
				q.infoToOwner(r.owner, "quota-stop-resumed:"+r.id, fmt.Sprintf("resumed %s (%s quota reset) in pane %s", r.job, r.pool, r.pane), now)
			}
		} else {
			q.failResume(ctx, r.id, r.pane, "submission_unproven")
		}
	}
}

// handOver announces job.lost reason quota_stop for a worker or tester the
// policy hands over. The hub places the replacement on another pool, closes the
// pane with a screen capture and writes the hk record lost with
// handed_over_to=<new job>; it sends the owner the info lane event. When it
// cannot, the job's watcher ends the job after its grace, without a
// replacement. A write that fails is retried after a backoff.
func (q *quotaStopManager) handOver(ctx context.Context, id, job, pane, pool, reset string, now time.Time) {
	stop := quotastop.Stop{Pool: pool}
	if at, err := time.Parse(time.RFC3339, reset); err == nil {
		stop.Reset = at
	}
	if q.announce == nil || q.announce(ctx, job, pane, stop) != nil {
		q.backoffAction(ctx, id, now)
		return
	}
	q.update(ctx, id, "action_recorded", 1)
	q.clearQuotaWait(ctx, pane)
}

// infoToOwner sends the owner one info lane event; a lane event that cannot be
// written is dropped, since the action it reports is already done.
func (q *quotaStopManager) infoToOwner(owner, eventID, text string, now time.Time) {
	if owner == "" || q.enqueue == nil {
		return
	}
	record := emitRecord{Type: "lane.event", Epoch: 1, CreatedAt: now.UTC().Format(time.RFC3339Nano), OwnerLane: owner, EventID: eventID, Text: text, NotifyType: "info"}
	if _, err := ensureLaneEmitRecord(q.root, record); err != nil {
		q.logger.Warn("quota-stop owner notice was not written", "error", err)
		return
	}
	q.enqueue(hubScannedRelayEvent{Kind: "lane.event", NotifyType: "info", HubActiveJob: HubActiveJob{JobID: laneEventTransportID(owner, eventID), Epoch: 1, OwnerLane: owner}, EventID: eventID, Text: text})
}

func (q *quotaStopManager) update(ctx context.Context, id, col string, value int) {
	if col != "hk_recorded" && col != "action_recorded" {
		return
	}
	q.store.mu.Lock()
	defer q.store.mu.Unlock()
	_, _ = q.store.db.ExecContext(ctx, `UPDATE quota_stop_decisions SET `+col+`=? WHERE event_id=?`, value, id)
}
func (q *quotaStopManager) failResume(ctx context.Context, id, pane, reason string) {
	q.store.mu.Lock()
	_, _ = q.store.db.ExecContext(ctx, `UPDATE quota_stop_decisions SET decision='handover',reason=?,hk_recorded=0 WHERE event_id=?`, reason, id)
	q.store.mu.Unlock()
	// The wait ended in a hand-over, so the pane stops advertising it.
	q.clearQuotaWait(ctx, pane)
}

func (q *quotaStopManager) backoffAction(ctx context.Context, id string, now time.Time) {
	q.store.mu.Lock()
	defer q.store.mu.Unlock()
	_, _ = q.store.db.ExecContext(ctx, `UPDATE quota_stop_decisions SET action_retry=? WHERE event_id=?`, now.Add(time.Minute).UnixMilli(), id)
}

func (q *quotaStopManager) drainBlocked(ctx context.Context, now time.Time) {
	q.store.mu.Lock()
	rows, err := q.store.db.QueryContext(ctx, `SELECT b.event_id,b.pane_id,b.workspace_id,b.label,b.state_change_seq,b.changed_at FROM quota_blocked_wakes b JOIN idle_wake_panes p ON p.pane_id=b.pane_id AND p.state_change_seq=b.state_change_seq AND p.agent_status='blocked' WHERE b.processed=0 AND b.due_at<=? ORDER BY b.due_at LIMIT 1`, now.UnixMilli())
	var c idleWakeCandidate
	var seq, changed int64
	found := false
	if err == nil {
		if rows.Next() && rows.Scan(&c.EventID, &c.PaneID, &c.WorkspaceID, &c.Label, &seq, &changed) == nil {
			found = true
		}
		_ = rows.Close()
	}
	if found {
		_, err = q.store.db.ExecContext(ctx, `UPDATE quota_blocked_wakes SET processed=1 WHERE event_id=? AND processed=0`, c.EventID)
	}
	q.store.mu.Unlock()
	if !found || err != nil {
		return
	}
	c.Status = "blocked"
	c.StateChangeSeq = uint64(seq)
	c.ChangedAt = time.UnixMilli(changed)
	q.observeBounded(ctx, c, now)
}
