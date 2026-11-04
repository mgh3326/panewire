package panewire

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
)

// This private client never attaches HTTP middleware or changes the crash
// reporter's sampling/budget. Only accepted spawn operations enter it.
func (h *HubServer) recordSpawnTrace(task hubSpawnTask, result hubSpawnOutcome) {
	h.logSpawnOutcome(task, result)
	h.spawnTraceOnce.Do(func() {
		if h.spawnSentry == nil {
			client, err := sentry.NewClient(sentry.ClientOptions{Dsn: strings.TrimSpace(os.Getenv(crashReportDSNEnv)), EnableTracing: true, TracesSampleRate: 1, SendDefaultPII: false})
			if err == nil {
				h.spawnSentry = client
			}
		}
	})
	if h.spawnSentry == nil || h.inbox == nil {
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
		role = "worker"
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
			if n, _ := recorded.RowsAffected(); n == 1 {
				reason := spawnTraceReason(result.Reason)
				hub.CaptureEvent(&sentry.Event{Level: sentry.LevelError, Message: "SPAWN_FAILED", Fingerprint: []string{"spawn", result.Stage, reason}, Tags: map[string]string{"stage": result.Stage, "reason": reason}})
			}
		}
	}
	claimed, err := h.inbox.store.db.ExecContext(ctx, h.inbox.store.q(`UPDATE spawn_operations SET trace_recorded=1 WHERE request_id=? AND attempt=? AND trace_recorded=0`), task.Request.RequestID, task.Attempt)
	if err != nil {
		return
	}
	n, _ := claimed.RowsAffected()
	if n != 1 {
		return
	}
	// The established transaction name remains stable across the CLI rename.
	trace := sentry.StartTransaction(sentry.SetHubOnContext(context.Background(), hub), "runtug.spawn", sentry.WithSpanSampled(sentry.SampledTrue))
	trace.Op = "spawn"
	trace.StartTime = task.Deadline.Add(-time.Duration(task.Request.DeadlineSeconds) * time.Second)
	trace.EndTime = h.now().UTC()
	for key, value := range tags {
		if value != "" {
			trace.SetTag(key, value)
			hub.Scope().SetTag(key, value)
		}
	}
	// Node replies provide elapsed durations, not synchronized clocks. End-align
	// stage spans instead of inventing a cross-host causal timeline.
	for _, stage := range []string{"claim", "placement", "checkout", "launch", "landing"} {
		if ms, ok := result.StageMS[stage]; ok && ms >= 0 && ms <= 3600000 {
			child := trace.StartChild(stage)
			child.EndTime = trace.EndTime
			child.StartTime = child.EndTime.Add(-time.Duration(ms) * time.Millisecond)
			if child.StartTime.Before(trace.StartTime) {
				trace.StartTime = child.StartTime
			}
			child.Finish()
		}
	}
	trace.Finish()
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
	if result.RC != 0 {
		h.logger.Warn("hub spawn refused", attrs...)
		return
	}
	h.logger.Info("hub spawn landed", attrs...)
}

func spawnTraceReason(reason string) string {
	// Refusal text follows the code in a reason. It never reaches a fingerprint.
	reason, _, _ = strings.Cut(reason, ": ")
	switch reason {
	case "timeout", "disconnected", "unknown", "spawn_failed", "configuration", "admission_refused", "parent_job_not_found", "quota_unconfirmed", "spawn_disabled", "cwd_unmapped", "brief_doc_unavailable", "node_upgrade_required":
		return reason
	}
	for _, family := range []string{"checkout_refused", "quota_unconfirmed"} {
		if strings.HasPrefix(reason, family+":") {
			return family
		}
	}
	return "spawn_failed"
}
