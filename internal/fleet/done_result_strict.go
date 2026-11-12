package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"
)

var (
	doneResultBudget  = 30 * time.Second
	doneResultBackoff = []time.Duration{time.Second, 2 * time.Second}
	doneResultCodeRE  = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)
)

type doneResultFailure struct {
	code   int
	reason string
}

func (e *doneResultFailure) Error() string { return e.reason }

func (a *App) strictDoneFailure(ctx context.Context, id string, e error) error {
	var failure *doneResultFailure
	if !errors.As(e, &failure) {
		failure = &doneResultFailure{code: exitFailure, reason: doneResultReason(e)}
	}
	_ = a.event(ctx, id, "job.record_failed", map[string]any{"op": "completed", "reason": failure.reason})
	a.warn("job %s completed record failed: reason=%s", id, failure.reason)
	return fail(failure.code, "job %s reason=%s", id, failure.reason)
}

func doneResultReason(e error) string {
	var backend *BackendError
	if errors.As(e, &backend) {
		if backend.Code == "secret_like_content" {
			if backend.Pattern != "" && safeHKPattern.MatchString(backend.Pattern) {
				return "secret_like_content:" + backend.Pattern
			}
			return "secret_like_content"
		}
		if doneResultCodeRE.MatchString(backend.Code) {
			return backend.Code
		}
		if backend.Status > 0 {
			return fmt.Sprintf("http_%d", backend.Status)
		}
	}
	if errors.Is(e, errHKTransport) || errors.Is(e, context.DeadlineExceeded) || errors.Is(e, context.Canceled) {
		return "hk_unreachable"
	}
	if e == nil {
		return "hk_unconfigured"
	}
	return "hk_unconfigured"
}

func (a *App) strictCompletedPut(ctx context.Context, id string, fields map[string]any, result any, reportDoc string, receipt map[string]any) error {
	store, e := a.jobStore()
	if e != nil {
		return &doneResultFailure{code: exitFailure, reason: "hk_unconfigured"}
	}
	started := time.Now()
	deadline := started.Add(doneResultBudget)
	attempts := 0
	var last error
	repaired := false
	for attempts < 3 {
		left := time.Until(deadline)
		if left <= 0 {
			reason := "hk_unreachable"
			if last != nil {
				reason = doneResultReason(last)
			}
			return &doneResultFailure{code: exitFailure, reason: reason}
		}
		if ctx.Err() != nil {
			return &doneResultFailure{code: exitFailure, reason: doneResultReason(ctx.Err())}
		}
		callCtx, cancel := context.WithTimeout(ctx, left)
		attempts++
		last = store.Put(callCtx, id, fields)
		cancel()
		if last == nil {
			return nil
		}
		if ctx.Err() != nil {
			return &doneResultFailure{code: exitFailure, reason: doneResultReason(ctx.Err())}
		}
		if !ambiguousDoneResultFailure(last) && !isJobStateConflict(last) {
			return &doneResultFailure{code: exitFailure, reason: doneResultReason(last)}
		}
		callCtx, cancel = context.WithTimeout(ctx, time.Until(deadline))
		if time.Until(deadline) <= 0 {
			cancel()
			return &doneResultFailure{code: exitFailure, reason: doneResultReason(last)}
		}
		record, getErr := store.Get(callCtx, id)
		cancel()
		if getErr != nil {
			return &doneResultFailure{code: exitFailure, reason: doneResultReason(last)}
		}
		switch state := str(record, "state"); state {
		case "completed":
			stored, hasResult := record["result"]
			if !hasResult || stored == nil {
				return &doneResultFailure{code: exitStateConflict, reason: "terminal_without_result"}
			}
			if !sameJSONValue(stored, result) {
				return &doneResultFailure{code: exitStateConflict, reason: "result_conflict"}
			}
			if reportDoc != "" && str(record, "report_doc") != reportDoc {
				return &doneResultFailure{code: exitStateConflict, reason: "report_conflict"}
			}
			return nil
		case "escalated":
			return &doneResultFailure{code: exitStateConflict, reason: "job_escalated"}
		case "lost":
			return &doneResultFailure{code: exitStateConflict, reason: "job_lost"}
		case "claimed":
			if repaired {
				return &doneResultFailure{code: exitStateConflict, reason: "no_spawn_evidence"}
			}
			spawned, ok := localSpawnRepair(receipt)
			if !ok {
				return &doneResultFailure{code: exitStateConflict, reason: "no_spawn_evidence"}
			}
			repaired = true
			left = time.Until(deadline)
			if left <= 0 {
				return &doneResultFailure{code: exitFailure, reason: doneResultReason(last)}
			}
			callCtx, cancel = context.WithTimeout(ctx, left)
			repairErr := store.Put(callCtx, id, spawned)
			cancel()
			if repairErr != nil {
				return &doneResultFailure{code: exitFailure, reason: doneResultReason(repairErr)}
			}
			attempts--
			continue
		case "spawned":
			if attempts >= 3 {
				return &doneResultFailure{code: exitFailure, reason: doneResultReason(last)}
			}
		default:
			return &doneResultFailure{code: exitStateConflict, reason: "no_spawn_evidence"}
		}
		if attempts > 0 && attempts <= len(doneResultBackoff) {
			wait := doneResultBackoff[attempts-1]
			left = time.Until(deadline)
			if wait > left {
				wait = left
			}
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return &doneResultFailure{code: exitFailure, reason: doneResultReason(ctx.Err())}
			case <-timer.C:
			}
		}
	}
	return &doneResultFailure{code: exitFailure, reason: doneResultReason(last)}
}

func ambiguousDoneResultFailure(e error) bool {
	if e == nil || errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(e, errHKTransport) {
		return true
	}
	var backend *BackendError
	if errors.As(e, &backend) {
		return backend.Status == http.StatusTooManyRequests || backend.Status >= 500
	}
	return false
}

func isJobStateConflict(e error) bool {
	var backend *BackendError
	return errors.As(e, &backend) && backend.Status == http.StatusConflict && backend.Code == "job_state_conflict"
}

func localSpawnRepair(receipt map[string]any) (map[string]any, bool) {
	if receipt == nil {
		return nil, false
	}
	fields := map[string]any{"state": "spawned"}
	for source, target := range map[string]string{
		"role": "role", "label": "label", "family": "family", "profile": "profile",
		"model_id": "model", "effort": "effort", "pool": "pool", "owner_lane": "owner_lane", "parent_lane": "parent_lane",
	} {
		if value := str(receipt, source); value != "" {
			fields[target] = value
		}
	}
	if pane := str(receipt, "pane_id"); pane != "" {
		fields["pane_id"] = pane
	}
	return fields, true
}

func spawnReceiptForRepair(events []Event) map[string]any {
	var receipt map[string]any
	for _, event := range events {
		switch event.Kind {
		case "job.spawned":
			receipt = event.Payload
		case "job.lost", "job.revoked", "job.reaped":
			receipt = nil
		}
	}
	return receipt
}

func (a *App) attachDuplicateResultStrict(ctx context.Context, j Job, id, report, digest string, result json.RawMessage, reportDoc, reportSHA string) (string, error) {
	if result == nil {
		return "", nil
	}
	store, e := a.jobStore()
	if e != nil {
		return "", &doneResultFailure{code: exitFailure, reason: "hk_unconfigured"}
	}
	record, e := store.Get(ctx, id)
	if e != nil {
		return "", &doneResultFailure{code: exitFailure, reason: doneResultReason(e)}
	}
	if stored, ok := record["result"]; ok && stored != nil {
		if sameJSONValue(stored, result) {
			if reportDoc != "" && str(record, "report_doc") != reportDoc {
				return "", &doneResultFailure{code: exitStateConflict, reason: "report_conflict"}
			}
			return "result=equal", nil
		}
		a.warn("result conflict: job %s already has a different result; keeping it", id)
		a.jobResultEvent(id, "conflict", "different_result", "stored result kept")
		a.captureSentryWarning([]string{"job-result-conflict"}, "JOB_RESULT_CONFLICT "+id, map[string]string{"job": id})
		return "", &doneResultFailure{code: exitStateConflict, reason: "result_conflict"}
	}
	fields := map[string]any{"state": "completed", "result": result}
	if reportDoc != "" {
		fields["report_doc"] = reportDoc
		if reportSHA != "" {
			fields["report_sha256"] = reportSHA
		}
	}
	if e = a.strictCompletedPut(ctx, id, fields, result, reportDoc, spawnReceiptForRepair(j.Events)); e != nil {
		return "", e
	}
	if e = a.event(ctx, id, "job.result_attached", map[string]any{"report": report, "report_sha256": digest}); e != nil {
		return "", e
	}
	a.jobResultEvent(id, "attached", "duplicate_done", "duplicate completion")
	return "result=attached", nil
}
