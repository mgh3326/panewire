package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/mgh3326/panewire/internal/arbiter"
)

func (a *App) lifecycle(ctx context.Context, kind string, args []string) error {
	return a.lifecycleWithOptions(ctx, kind, args, false, false)
}

// lifecycleWithOptions is lifecycle for the completion watcher: watchResult
// attaches the job-dir findings.json (hk 2927), late marks a completion a
// re-armed watcher found (hk 2986).
func (a *App) lifecycleWithOptions(ctx context.Context, kind string, args []string, watchResult, late bool) error {
	if len(args) == 0 || !validID(args[0]) {
		return fail(exitUsage, "%s requires a job id", kind)
	}
	id := args[0]
	f := flags(kind)
	report := f.String("report", "", "")
	question := f.String("question", "", "")
	resultFile := f.String("result", "", "")
	completionIDFlag := f.String("completion-id", "", "")
	if e := f.Parse(args[1:]); e != nil {
		return fail(exitUsage, "%v", e)
	}
	if f.NArg() > 0 {
		return fail(exitUsage, "unexpected arguments")
	}
	if kind != "done" && *completionIDFlag != "" {
		return fail(exitUsage, "%s accepts no --completion-id", kind)
	}
	j, e := a.job(id)
	if e != nil {
		return e
	}
	if kind == "restore" {
		if *report != "" || *question != "" {
			return fail(exitUsage, "restore accepts only JOB")
		}
		return a.restore(ctx, j)
	}
	if kind == "escalate" && (*question == "" || j.Parent == "") {
		return fail(exitUsage, "escalate requires --question and a recorded parent lane")
	}
	var result json.RawMessage
	var rejected string
	if !watchResult && *resultFile != "" {
		if kind != "done" {
			return fail(exitUsage, "%s accepts no --result", kind)
		}
		result, e = readResult(*resultFile)
		if e != nil {
			return e
		}
	}
	if *report == "" && kind == "done" {
		*report = newestReport(filepath.Join(a.Jobs, id))
	}
	if kind == "done" && *report == "" {
		return fail(exitUsage, "done requires an existing report")
	}
	if watchResult {
		*resultFile, rejected = watcherResultFile(a.Jobs, id, *report)
		if *resultFile != "" {
			result, e = readResult(*resultFile)
			if e != nil {
				result, rejected = nil, "result failed validation"
			}
		}
	}
	strictResult := kind == "done" && !watchResult && result != nil && !boolean(j.Receipt, "headless", false)
	body := []byte{}
	if *report != "" {
		if e = noSymlinks(*report); e != nil {
			return e
		}
		body, e = os.ReadFile(*report)
		if e != nil {
			return e
		}
	}
	lock, e := os.OpenFile(filepath.Join(a.Jobs, id, ".panewire-operation.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); e != nil {
		return e
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	j, e = a.job(id)
	if e != nil {
		return e
	}
	if watchResult && (j.Terminal || j.Reaped) {
		return nil
	}
	if rejected != "" {
		if e = a.event(ctx, id, "job.result_rejected", map[string]any{"reason": rejected}); e != nil {
			return e
		}
		a.warn("watcher result rejected: %s", rejected)
	}
	hash := sha256.Sum256(body)
	digest := hex.EncodeToString(hash[:])
	completionID := ""
	doc, docSHA := "", ""
	if kind == "done" && *report != "" {
		completionID, e = completionIdentity(id, *completionIDFlag, body)
		if e != nil {
			return e
		}
	}
	eventKind := "job.completed"
	if kind == "escalate" {
		eventKind = "job.escalate"
	}
	rounds := 0
	usedPath := false
	priorCompletion := false
	completionIDConflict := false
	for _, ev := range j.Events {
		if ev.Kind == eventKind {
			rounds++
			if kind == "done" {
				priorCompletion = true
			}
			matches := str(ev.Payload, "report_sha256") == digest && str(ev.Payload, "question") == *question
			if kind == "done" && str(ev.Payload, "completion_id") != "" {
				matches = str(ev.Payload, "completion_id") == completionID && str(ev.Payload, "report_sha256") == digest
				if str(ev.Payload, "completion_id") == completionID && str(ev.Payload, "report_sha256") != digest {
					completionIDConflict = true
				}
			}
			if matches {
				if kind == "done" && str(ev.Payload, "document") == "" {
					// Completion notification can precede report publication for
					// watcher/headless uploads. Retry the publication before taking
					// the idempotent duplicate return path.
					continue
				}
				suffix := ""
				if kind == "done" {
					var status string
					if strictResult {
						status, e = a.attachDuplicateResultStrict(ctx, j, id, *report, digest, result, str(ev.Payload, "document"), str(ev.Payload, "report_sha256"))
						if e != nil {
							return a.strictDoneFailure(ctx, id, e)
						}
					} else {
						if e = a.releaseQuota(ctx, j); e != nil {
							return e
						}
						status, e = a.attachDuplicateResult(ctx, j, id, *report, digest, result, str(ev.Payload, "document"), str(ev.Payload, "report_sha256"))
						if e != nil {
							return e
						}
					}
					if strictResult {
						if e = a.releaseQuota(ctx, j); e != nil {
							return e
						}
					}
					if status != "" {
						suffix = " " + status
					}
				}
				fmt.Fprintf(a.Out, "OK job=%s report=%s duplicate=yes%s\n", id, *report, suffix)
				return nil
			}
			if str(ev.Payload, "report") == *report {
				usedPath = true
			}
		}
	}
	if completionIDConflict {
		_ = a.event(ctx, id, "job.record_failed", map[string]any{"op": "report_upload", "reason": "report_conflict"})
		return fail(exitStateConflict, "job %s report upload failed reason=report_conflict", id)
	}
	if kind != "done" && usedPath && *report != "" {
		ext := filepath.Ext(*report)
		stem := strings.TrimSuffix(*report, ext)
		for n := rounds + 1; ; n++ {
			path := fmt.Sprintf("%s-r%d%s", stem, n, ext)
			file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if os.IsExist(err) {
				continue
			}
			if err != nil {
				return err
			}
			_, err = file.Write(body)
			closeErr := file.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
			*report = path
			break
		}
	}
	if kind == "done" && *report != "" {
		doc, docSHA, e = a.publishCompletionReport(ctx, id, j.Task, completionID, body)
		if e != nil {
			reason := completionUploadReason(e)
			if strictResult {
				code := exitFailure
				if reason == "report_conflict" || strings.HasPrefix(reason, "secret_like_content") {
					code = exitStateConflict
				}
				return a.strictDoneFailure(ctx, id, &doneResultFailure{code: code, reason: reason})
			}
			if watchResult || boolean(j.Receipt, "headless", false) {
				a.warn("report upload failed; local report retained reason=%s", reason)
			} else {
				_ = a.event(ctx, id, "job.record_failed", map[string]any{"op": "report_upload", "reason": reason})
				if completionUploadCanRetry(e) {
					a.warn("report upload failed; local report retained; retry with the same --completion-id")
					return fail(exitFailure, "job %s report upload failed reason=%s; retry with --completion-id %s", id, reason, completionID)
				}
				if reason == "report_conflict" || strings.HasPrefix(reason, "secret_like_content") {
					a.warn("report publication refused; local report retained reason=%s", reason)
					return fail(exitStateConflict, "job %s report publication refused reason=%s", id, reason)
				}
				a.warn("report upload failed; local report retained reason=%s", reason)
				return fail(exitFailure, "job %s report upload failed reason=%s", id, reason)
			}
		}
	} else if kind == "escalate" && *report != "" {
		doc, docSHA, e = a.publishEscalationReport(ctx, id, j.Task, *report, body)
		if e != nil {
			a.warn("report upload failed; local report retained")
			doc, docSHA = "", ""
		}
	}
	if priorCompletion && result != nil && kind == "done" {
		if strictResult {
			_, e = a.attachDuplicateResultStrict(ctx, j, id, *report, digest, result, doc, docSHA)
			if e != nil {
				return a.strictDoneFailure(ctx, id, e)
			}
		} else {
			if _, e = a.attachDuplicateResult(ctx, j, id, *report, digest, result, doc, docSHA); e != nil {
				return e
			}
		}
	}
	recordWrite := !priorCompletion
	pinOnlyRecordWrite := false
	if priorCompletion && kind == "done" && doc != "" {
		store, storeErr := a.jobStore()
		if storeErr == nil {
			current, getErr := store.Get(ctx, id)
			if getErr == nil && str(current, "report_doc") == "" && str(current, "state") == "spawned" {
				recordWrite = true
				pinOnlyRecordWrite = true
			} else if getErr != nil {
				a.warn("completed report pin check failed: reason=%s", doneResultReason(getErr))
			}
		} else {
			a.warn("completed report pin check failed: reason=hk_unconfigured")
		}
	}
	a.recordJobUsage(ctx, j, false)
	// The hk record state is written before the relay: one PUT carries the
	// terminal state, the report doc with its sha256 pin and — for done
	// --result — the findings.
	state := "completed"
	if kind == "escalate" {
		state = "escalated"
	}
	rec := map[string]any{"state": state}
	if doc != "" {
		rec["report_doc"] = doc
		rec["report_sha256"] = docSHA
	}
	if result != nil {
		rec["result"] = result
	}
	recordFields := rec
	putResult := result
	if pinOnlyRecordWrite {
		recordFields = map[string]any{"state": state, "report_doc": doc, "report_sha256": docSHA}
	}
	if strictResult && recordWrite {
		if e = a.strictCompletedPut(ctx, id, recordFields, putResult, doc, docSHA, spawnReceiptForRepair(j.Events)); e != nil {
			if isDoneResultFailure(e, "result_conflict") {
				a.recordDoneResultConflict(id)
			}
			return a.strictDoneFailure(ctx, id, e)
		}
	} else if recordWrite {
		if e := a.putJobRecord(ctx, id, recordFields); e != nil {
			if kind == "done" && result != nil && boolean(j.Receipt, "headless", false) {
				a.warn("job %s completed record failed: reason=%s", id, doneResultReason(e))
				_ = a.event(ctx, id, "job.record_failed", map[string]any{"op": state})
			} else {
				a.jobRecordFailed(ctx, id, state, e)
			}
		}
	}
	p := map[string]any{"report": *report, "report_sha256": digest, "question": *question, "owner_lane": j.Owner, "parent_lane": j.Parent, "label": j.Label, "pane_id": j.Pane, "document": doc}
	if completionID != "" {
		p["completion_id"] = completionID
	}
	if late {
		p["late"] = true
	}
	if e = a.event(ctx, id, eventKind, p); e != nil {
		return e
	}
	if kind == "escalate" {
		if _, rerr := a.recordFailedOutcome(ctx, j, j.Task, "escalated", a.PRs); rerr != nil {
			a.warn("failure rep not recorded: %v", rerr)
		}
	}
	if kind == "done" && result == nil && j.SpawnRole == "reviewer" {
		a.jobResultEvent(id, "missing", "no_result", "")
		a.warn("reviewer done carried no result: %s", id)
	}
	owner := j.Owner
	if j.Parent != "" && (kind == "escalate" || j.Role == "builder" || j.Role == "captain") {
		owner = j.Parent
	}
	last := strings.TrimSpace(string(body))
	if lines := strings.Split(last, "\n"); len(lines) > 0 {
		last = lines[len(lines)-1]
	}
	if doc != "" {
		last += " doc:" + doc
	}
	if completionID != "" {
		last += " completion:" + completionID
	}
	argsEmit := []string{"emit", "--kind", eventKind, "--job", id, "--owner-lane", owner, "--epoch", "1", "--label", j.Label, "--host", a.machine(), "--pane", j.Pane, "--inbox-root", filepath.Dir(a.Jobs)}
	if late {
		argsEmit = append(argsEmit, "--late", "--deliver", "inbox")
	}
	if *report != "" {
		argsEmit = append(argsEmit, "--report", *report, "--report-last-line", last)
	}
	if *question != "" {
		argsEmit = append(argsEmit, "--question", *question)
	}
	r := a.call(ctx, "panewire", argsEmit...)
	relay := ""
	if r.Code != 0 {
		relay = " relay=file-only"
		a.warn("relay failed; durable arbiter event retained")
		_ = a.event(ctx, id, "job.relay_failed", map[string]any{"kind": eventKind, "exit": r.Code})
	}
	if j.Task != "" {
		comment := "panewire " + eventKind + " job=" + id + " report=" + *report
		if e = hkCheck(hkCommentTask(ctx, j.Task, comment), "handoffkeep"); e != nil {
			a.warn("task comment failed: %v", e)
		}
	}
	if kind == "done" {
		if e = a.releaseQuota(ctx, j); e != nil {
			return e
		}
	}
	fmt.Fprintf(a.Out, "OK job=%s report=%s kind=%s%s\n", id, *report, eventKind, relay)
	return nil
}
func readResult(path string) (json.RawMessage, error) {
	if e := noSymlinks(path); e != nil {
		return nil, e
	}
	// Read at most one byte past the limit: the watcher runs this on any
	// worker-written findings.json, which must not be loaded whole.
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	body, e := io.ReadAll(io.LimitReader(f, 64<<10+1))
	if e != nil {
		return nil, e
	}
	if len(body) > 64<<10 {
		return nil, fail(exitUsage, "--result exceeds 64 KiB")
	}
	var v any
	if e := json.Unmarshal(body, &v); e != nil {
		return nil, fail(exitUsage, "--result is not valid JSON: %v", e)
	}
	if _, ok := v.(map[string]any); !ok {
		return nil, fail(exitUsage, "--result must be a JSON object")
	}
	return json.RawMessage(body), nil
}

// watcherResultFile finds the result a finished pane filed in its job dir:
// findings.json (a reviewer's convention), then result.json (`panewire
// delegate`, delegatePaths.result). The report's directory is checked first,
// where a worker writes both files.
func watcherResultFile(jobs, id, report string) (string, string) {
	var candidates []string
	if report != "" {
		candidates = append(candidates, filepath.Join(filepath.Dir(report), "findings.json"))
	}
	candidates = append(candidates, filepath.Join(jobs, id, "findings.json"), filepath.Join(jobs, id, "result.json"))
	seen := map[string]bool{}
	for _, path := range candidates {
		if seen[path] {
			continue
		}
		seen[path] = true
		info, e := os.Lstat(path)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil || !info.Mode().IsRegular() {
			return "", "result is not a regular file"
		}
		return path, ""
	}
	return "", ""
}

// attachDuplicateResult stores the result a later `done --result` carries when
// attachDuplicateResult repairs the record for an already-published duplicate.
// It completes a spawned record from the event pin, preserves existing pins,
// and never overwrites a stored result.
func (a *App) attachDuplicateResult(ctx context.Context, j Job, id, report, digest string, result json.RawMessage, eventDoc, eventSHA string) (string, error) {
	store, e := a.jobStore()
	if e != nil {
		if result == nil {
			a.warn("duplicate job record repair unavailable: reason=%s", doneResultReason(e))
			return "", nil
		}
		return "", fail(exitFailure, "job %s completion record unavailable reason=%s", id, doneResultReason(e))
	}
	rec, e := store.Get(ctx, id)
	if e != nil {
		if errors.Is(e, errJobNotFound) {
			if result != nil {
				return "result=unattached", nil
			}
			return "", nil
		}
		if result == nil {
			a.warn("duplicate job record repair unreadable: reason=%s", doneResultReason(e))
			return "", nil
		}
		return "", fail(exitFailure, "job %s completion record unreadable reason=%s", id, doneResultReason(e))
	}
	state := str(rec, "state")
	if state == "claimed" {
		spawned, ok := localSpawnRepair(spawnReceiptForRepair(j.Events))
		if !ok {
			if result == nil {
				a.warn("duplicate job record repair skipped: state=claimed reason=no_spawn_evidence")
				return "", nil
			}
			return "", fail(exitStateConflict, "job %s cannot complete from state=claimed reason=no_spawn_evidence", id)
		}
		if putErr := store.Put(ctx, id, spawned); putErr != nil {
			readback, readErr := store.Get(ctx, id)
			readbackState := str(readback, "state")
			if readErr == nil && (readbackState == "spawned" || readbackState == "completed") {
				rec, state = readback, readbackState
			} else {
				if result == nil {
					a.warn("duplicate job record repair unconfirmed: reason=%s", doneResultReason(putErr))
					return "", nil
				}
				a.jobRecordFailed(ctx, id, "spawned", putErr)
				return "", fail(exitFailure, "job %s completion record failed reason=%s", id, doneResultReason(putErr))
			}
		} else {
			state = "spawned"
		}
	}
	storedResult := rec["result"]
	storedResultSame := false
	if result != nil && storedResult != nil {
		if !sameJSONValue(storedResult, result) {
			a.warn("result conflict: job %s already has a different result; keeping it", id)
			a.jobResultEvent(id, "conflict", "different_result", "stored result kept")
			a.captureSentryWarning([]string{"job-result-conflict"}, "JOB_RESULT_CONFLICT "+id, map[string]string{"job": id})
			return "result=conflict", nil
		}
		storedResultSame = true
	}
	if state == "completed" && storedResultSame {
		return "result=equal", nil
	}
	if state == "completed" && result == nil {
		return "", nil
	}
	if state != "spawned" && state != "completed" {
		if result == nil {
			a.warn("duplicate job record repair skipped: state=%s", state)
			return "", nil
		}
		return "", fail(exitStateConflict, "job %s cannot complete from state=%s", id, state)
	}
	fields := map[string]any{"state": "completed"}
	if result != nil && !storedResultSame {
		fields["result"] = result
	}
	wantDoc, wantSHA := str(rec, "report_doc"), str(rec, "report_sha256")
	if wantDoc == "" && eventDoc != "" {
		fields["report_doc"], fields["report_sha256"] = eventDoc, eventSHA
		wantDoc, wantSHA = eventDoc, eventSHA
	}
	if e := store.Put(ctx, id, fields); e != nil {
		readback, readErr := store.Get(ctx, id)
		confirmed := readErr == nil && str(readback, "state") == "completed"
		if confirmed && result != nil && !storedResultSame {
			confirmed = sameJSONValue(readback["result"], result)
		}
		if confirmed && wantDoc != "" {
			confirmed = str(readback, "report_doc") == wantDoc && str(readback, "report_sha256") == wantSHA
		}
		if !confirmed {
			if result == nil {
				a.warn("duplicate job record repair unconfirmed: reason=%s", doneResultReason(e))
				return "", nil
			}
			a.jobRecordFailed(ctx, id, "completed", e)
			return "", fail(exitFailure, "job %s completion record failed reason=%s", id, doneResultReason(e))
		}
	}
	if result == nil {
		return "", nil
	}
	if storedResultSame {
		return "result=equal", nil
	}
	if e := a.event(ctx, id, "job.result_attached", map[string]any{"report": report, "report_sha256": digest}); e != nil {
		return "", e
	}
	a.jobResultEvent(id, "attached", "duplicate_done", "duplicate completion")
	return "result=attached", nil
}

// sameJSONValue compares two decoded JSON values: key order, whitespace and
// number spelling do not matter.
func sameJSONValue(x, y any) bool {
	cx, ok := canonicalJSON(x)
	if !ok {
		return false
	}
	cy, ok := canonicalJSON(y)
	return ok && cx == cy
}

func canonicalJSON(v any) (string, bool) {
	b, e := json.Marshal(v)
	if e != nil {
		return "", false
	}
	var decoded any
	if e := json.Unmarshal(b, &decoded); e != nil {
		return "", false
	}
	b, e = json.Marshal(decoded)
	if e != nil {
		return "", false
	}
	return string(b), true
}

func (a *App) releaseQuota(ctx context.Context, j Job) error {
	pools, e := a.ledger().QuotaPools(ctx, j.ID)
	if e != nil {
		return ledgerFail(e, "status")
	}
	for _, pool := range pools {
		if pool == "" {
			return fail(exitStateConflict, "invalid quota record")
		}
		if e := a.ledger().ReleaseQuotaPool(ctx, j.ID, pool, false); e != nil {
			return ledgerFail(e, "release")
		}
	}
	return nil
}
func (a *App) restore(ctx context.Context, j Job) error {
	d, e := a.durable(ctx, j.ID)
	if e != nil {
		return e
	}
	if d.State != "released" {
		return fail(exitUsage, "restore requires released job")
	}
	agents, e := a.agents(ctx)
	if e != nil {
		return e
	}
	found := 0
	for _, ag := range agents {
		if ag.Pane == j.Pane {
			if ag.Tab != j.Tab || ag.Name != j.Label || !liveStatus(ag.Status) {
				return fail(exitUsage, "restore pane identity or liveness mismatch")
			}
			found++
		}
	}
	if found != 1 || j.Pane == "" || j.Tab == "" {
		return fail(exitUsage, "restore requires one verified existing pane")
	}
	role := "worker"
	if j.Role == "builder" || j.Role == "captain" {
		role = "builder"
	}
	claim := arbiter.Claim{ReclaimReleased: true, Job: j.ID, Lane: j.Owner, AgentLabel: j.Label, T: j.T, Role: role, TaskID: j.Task}
	if role == "builder" {
		claim.ParentLane = j.Parent
	}
	if _, e = a.ledger().Claim(ctx, claim); e != nil {
		return ledgerFail(e, "claim")
	}
	if e = a.event(ctx, j.ID, "job.spawned", j.Receipt); e != nil {
		return fail(exitFailure, "job reclaimed; restoring receipt failed: %v", e)
	}
	if e = a.startWatch(j.ID); e != nil {
		return e
	}
	fmt.Fprintf(a.Out, "OK job=%s restored pane=%s tab=%s label=%s\n", j.ID, j.Pane, j.Tab, j.Label)
	return nil
}
func liveStatus(s string) bool { return s == "working" || s == "idle" || s == "done" }

// newestReport picks the most recently modified report*.md in dir. Equal
// modification times fall back to lexical name order so the choice is stable;
// files that vanish between the glob and the stat are skipped.
func newestReport(dir string) string {
	type candidate struct {
		path string
		mod  time.Time
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "report*.md"))
	var found []candidate
	for _, m := range matches {
		if st, e := os.Stat(m); e == nil {
			found = append(found, candidate{m, st.ModTime()})
		}
	}
	if len(found) == 0 {
		return ""
	}
	slices.SortStableFunc(found, func(x, y candidate) int {
		if c := y.mod.Compare(x.mod); c != 0 {
			return c
		}
		return strings.Compare(x.path, y.path)
	})
	return found[0].path
}
