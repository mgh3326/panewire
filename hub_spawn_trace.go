package panewire

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/mgh3326/panewire/internal/fleet"
)

const (
	spawnSentryServerName   = "panewire-hub"
	spawnSentryFlushTimeout = 2 * time.Second
)

// This private client never attaches HTTP middleware or changes the crash
// reporter's sampling/budget. Only accepted spawn operations enter it.
func (h *HubServer) recordSpawnTrace(task hubSpawnTask, result hubSpawnOutcome) {
	h.logSpawnOutcome(task, result)
	// A reconciling operation is not an outcome. Its error event and trace claim
	// belong to the terminal outcome that follows, so it must not take them here.
	if spawnOutcomeStatus(result) == "reconciling" {
		return
	}
	if h.spawnSentryClient() == nil || h.inbox == nil {
		return
	}
	ctx, done := h.inbox.store.op(h.closeCtx)
	defer done()
	hub := sentry.NewHub(h.spawnSentry, sentry.NewScope())
	host, profile := spawnOutcomeTarget(task, result)
	role := result.Role
	if role == "" {
		role = hubSpawnArg(task.Request.Args, "--role")
	}
	if role == "" {
		role = "implementer"
	}
	tags := map[string]string{"job": task.Job, "host": host, "profile": profile, "role": role, "rc": strconv.Itoa(result.RC)}

	for key, value := range tags {
		if value != "" {
			hub.Scope().SetTag(key, value)
		}
	}
	if result.RC != 0 {
		eventID := fmt.Sprintf("%s:%d:%s", task.Request.RequestID, task.Attempt, spawnOutcomeStatus(result))
		recorded, err := h.inbox.store.db.ExecContext(ctx, h.inbox.store.q(`UPDATE spawn_notifications SET telemetry_recorded=1 WHERE event_id=? AND telemetry_recorded=0`), eventID)
		if err == nil {
			if n, _ := h.inbox.store.dialect.rowsMatched(recorded); n == 1 {
				if level, capture := spawnSentryFailurePolicy(result.Reason); capture {
					reason := spawnTraceReason(result.Reason)
					hub.CaptureEvent(&sentry.Event{Level: level, Message: "SPAWN_FAILED " + result.Stage + "/" + reason, Fingerprint: []string{"spawn", result.Stage, reason}, Tags: map[string]string{"stage": result.Stage, "reason": reason, "request": task.Request.RequestID, "attempt": strconv.Itoa(task.Attempt)}})
				}
			}
		}
	}
	claimed, err := h.inbox.store.db.ExecContext(ctx, h.inbox.store.q(`UPDATE spawn_operations SET trace_recorded=1 WHERE request_id=? AND attempt=? AND trace_recorded=0`), task.Request.RequestID, task.Attempt)
	if err != nil {
		return
	}
	n, _ := h.inbox.store.dialect.rowsMatched(claimed)
	if n != 1 {
		return
	}
	// The established transaction name remains stable across the CLI rename.
	trace := sentry.StartTransaction(sentry.SetHubOnContext(context.Background(), hub), "runtug.spawn", sentry.WithSpanSampled(sentry.SampledTrue))
	trace.Op = "spawn"
	trace.EndTime = h.now().UTC()
	// The trace starts at acceptance (task.Created), so the time an earlier
	// attempt or a queue wait took is inside it, not an unattributed gap. A row
	// from before the column has no acceptance time: it keeps the attempt window.
	start := task.Created
	if start.IsZero() {
		start = task.Deadline.Add(-time.Duration(task.Request.DeadlineSeconds) * time.Second)
	}
	trace.StartTime = start
	for key, value := range tags {
		if value != "" {
			trace.SetTag(key, value)
			hub.Scope().SetTag(key, value)
		}
	}
	// The node's queue as this attempt's first scheduling pass counted it (hk
	// 7112, hk 7125). Only the landed trace carries it: a refused, deadline or
	// orphan outcome does not. A read failure leaves the tags off this trace.
	if result.RC == 0 {
		open, queued, recorded, err := h.inbox.store.spawnDispatchQueue(ctx, task.Request.RequestID, task.Attempt)
		if err != nil {
			h.logger.Warn("hub spawn queue unreadable; the landed trace has no queue tags", "request_id", task.Request.RequestID, "error", err)
		} else if recorded {
			trace.SetTag("node_open_dispatches", strconv.Itoa(open))
			trace.SetTag("node_queued_spawns", strconv.Itoa(queued))
		}
	}
	// Node replies provide elapsed durations, not synchronized clocks. Stage spans
	// run back to back and end at the window's end instead of inventing a
	// cross-host causal timeline (hk 7112). The steps
	// after placement are node claim, pane start (launch), brief submit (submit)
	// and landing proof (landing); see docs/hub-spawn-operations.md (hk 5585).
	// Each attempt renewed before this one is a window of its own, and the
	// landed attempt's window opens where the last of them ended (hk 6074; see
	// docs/hub-spawn-operations.md#trace-attempts).
	history, err := h.inbox.store.spawnTraceHistory(ctx, task.Request.RequestID)
	if err != nil {
		h.logger.Warn("hub spawn trace history unreadable; trace starts at the landed attempt", "request_id", task.Request.RequestID, "error", err)
		history = nil
	}
	open, earliest := layoutRenewedAttempts(trace, start, history)
	if reach := layoutSpawnWindow(trace, open, trace.EndTime, result.StageMS); reach.Before(earliest) {
		earliest = reach
	}
	if earliest.Before(trace.StartTime) {
		trace.StartTime = earliest
	}
	trace.Finish()
}

// spawnSentryRefusalPolicies are the closed set of requester-side refusal
// codes with special Sentry handling. Admission refusals remain visible as
// warnings because quota and policy gates can be fleet-level issues; caller
// input errors and withdrawals do not consume the shared error-event quota.
var spawnSentryRefusalPolicies = [...]struct {
	code    string
	level   sentry.Level
	capture bool
}{
	{code: "admission_refused", level: sentry.LevelWarning, capture: true},
	{code: "brief_invalid"},
	{code: "requester_withdrawn"},
	{code: "usage"},
}

func spawnSentryFailurePolicy(reason string) (sentry.Level, bool) {
	code, _, _ := strings.Cut(reason, ":")
	if code == "spawn_failed" {
		code, _, _ = strings.Cut(strings.TrimPrefix(reason, "spawn_failed:"), ":")
	}
	for _, policy := range spawnSentryRefusalPolicies {
		if code == policy.code {
			return policy.level, policy.capture
		}
	}
	return sentry.LevelError, true
}

// spawnSentryClient is the hub's private Sentry client, built once from the
// configured DSN; nil when reporting is not configured.
func (h *HubServer) spawnSentryClient() *sentry.Client {
	h.spawnTraceOnce.Do(func() {
		if h.spawnSentry == nil {
			client, err := sentry.NewClient(spawnSentryOptions(strings.TrimSpace(os.Getenv(crashReportDSNEnv))))
			if err == nil {
				h.spawnSentry = client
			}
		}
	})
	return h.spawnSentry
}

// spawnOutcomeTarget is the host and profile an outcome is about, falling back
// to the bound node request for an outcome that does not name them.
func spawnOutcomeTarget(task hubSpawnTask, result hubSpawnOutcome) (host, profile string) {
	host, profile = result.Host, result.Profile
	if task.Node != nil {
		if host == "" {
			host = task.Node.Machine
		}
		if profile == "" {
			profile = hubSpawnArg(task.Node.Args, "-m")
		}
	}
	if host == "" {
		host = "unassigned"
	}
	if profile == "" {
		profile = "auto"
	}
	return host, profile
}

// logSpawnOutcome is the hub journal's line for a finished operation. The reason
// is redacted refusal text; the request's brief and argv never reach the log.
func (h *HubServer) logSpawnOutcome(task hubSpawnTask, result hubSpawnOutcome) {
	host, profile := spawnOutcomeTarget(task, result)
	attrs := []any{"request_id", task.Request.RequestID, "job", task.Job, "host", host, "profile", profile, "rc", result.RC, "stage", result.Stage, "reason", result.Reason}
	if spawnOutcomeStatus(result) == "reconciling" {
		// Info, not Warn: nothing was refused, and ownership is retained while the
		// request may still land.
		h.logger.Info("hub spawn reconciling", attrs...)
		return
	}
	if result.RC != 0 {
		h.logger.Warn("hub spawn refused", attrs...)
		return
	}
	h.logger.Info("hub spawn landed", attrs...)
}

// spawnSentryOptions configures the spawn client. ServerName is fixed so the OS
// hostname never reaches Sentry; the host tag names the fleet machine instead.
func spawnSentryOptions(dsn string) sentry.ClientOptions {
	return sentry.ClientOptions{
		Dsn:                   dsn,
		Release:               "panewire@" + fleet.BuildVersion,
		Environment:           "fleet",
		EnableTracing:         true,
		TracesSampleRate:      1,
		SendDefaultPII:        false,
		ServerName:            spawnSentryServerName,
		BeforeSend:            scrubSentryEvent,
		BeforeSendTransaction: scrubSentryEvent,
	}
}

// flushSpawnTrace delivers what the stopped spawn workers queued. Consuming the
// once keeps a late trace from building a client that nothing would flush.
func (h *HubServer) flushSpawnTrace() {
	h.spawnTraceOnce.Do(func() {})
	if h.spawnSentry != nil {
		h.spawnSentry.Flush(spawnSentryFlushTimeout)
	}
}

func spawnTraceReason(reason string) string {
	// Refusal text follows the code in a reason. It never reaches a fingerprint
	// except through spawnAdmissionRefusalClass, whose result is a bounded code.
	code, text, _ := strings.Cut(reason, ": ")
	if code == "admission_refused" {
		if class := spawnAdmissionRefusalClass(text); class != "" {
			return code + ":" + class
		}
		return code
	}
	reason = code
	switch reason {
	case "timeout", "disconnected", "unknown", "spawn_failed", "configuration", "parent_job_not_found", "quota_unconfirmed", "spawn_disabled", "cwd_unmapped", "brief_doc_unavailable", "node_upgrade_required", spawnOrphanedReason, spawnAbandonedReason, spawnWithdrawnReason, spawnHandoverWithdrawnReason, spawnUnresolvedReason:
		return reason
	}
	if code, ok := strings.CutPrefix(reason, "spawn_failed:"); ok && fleet.IsSpawnRefusalCode(code) {
		return reason
	}
	for _, family := range []string{"checkout_refused", "quota_unconfirmed"} {
		if strings.HasPrefix(reason, family+":") {
			return family
		}
	}
	return "spawn_failed"
}

// spawnAdmissionRefusalClass names the cause behind an admission_refused
// reason's free text so one Sentry issue groups one cause. Only the refusal
// table's closed codes become a class; anything it cannot classify is "other",
// so free text (a path, a host, a job name) never reaches a fingerprint or a
// tag and one cause cannot split across issues.
func spawnAdmissionRefusalClass(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	if code := fleet.SpawnRefusalCode(text, 0); code != "" {
		return code
	}
	return "other"
}
