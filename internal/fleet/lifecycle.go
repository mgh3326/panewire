package fleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/mgh3326/handoffkeep/client"

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
	if e := f.Parse(args[1:]); e != nil {
		return fail(exitUsage, "%v", e)
	}
	if f.NArg() > 0 {
		return fail(exitUsage, "unexpected arguments")
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
	eventKind := "job.completed"
	if kind == "escalate" {
		eventKind = "job.escalate"
	}
	rounds := 0
	usedPath := false
	for _, ev := range j.Events {
		if ev.Kind == eventKind {
			rounds++
			if str(ev.Payload, "report_sha256") == digest && str(ev.Payload, "question") == *question {
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
						status, e = a.attachDuplicateResult(ctx, id, *report, digest, result)
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
	if usedPath && *report != "" {
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
	// The report goes to hk over HTTP with the svc-panewire credential (hk
	// 3607): kind report, the job and the report's bytes, under the upload
	// bound, linked to the job's hk task when it has a numeric one, with
	// origin class job (hk 3615). docSHA is the sha256 of the uploaded bytes,
	// which the record pins next to report_doc.
	//
	// done publishes to the content-addressed key and writes the
	// job.report_published receipt (lifecycle_publish.go); escalate keeps the
	// report file's name as its key.
	doc, docSHA := "", ""
	if kind == "done" {
		doc = publishedDocument(j.Events, digest)
		if doc == "" {
			key, e := a.publishReport(ctx, j, body, digest)
			if e != nil {
				a.warn("report upload failed; local report retained")
			} else if e = a.event(ctx, id, eventReportPublished, map[string]any{"report_sha256": digest, "document": key}); e != nil {
				return e
			} else {
				doc = key
			}
		}
		if doc != "" {
			docSHA = digest
		}
	} else if *report != "" {
		key := "reports/" + id + "/" + filepath.Base(*report)
		b, e := os.ReadFile(*report)
		if e == nil {
			upload := client.Document{Key: key, Kind: "report", Job: id, Body: string(b), OriginClass: "job"}
			if task, err := hkTaskID(j.Task); err == nil {
				upload.TaskID = &task
			}
			e = hkPutDocument(ctx, upload)
		}
		if e == nil {
			doc = key
			sum := sha256.Sum256(b)
			docSHA = hex.EncodeToString(sum[:])
		} else {
			a.warn("report upload failed; local report retained")
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
	if strictResult {
		if e = a.strictCompletedPut(ctx, id, rec, result, doc, spawnReceiptForRepair(j.Events)); e != nil {
			if isDoneResultFailure(e, "result_conflict") {
				a.recordDoneResultConflict(id)
			}
			return a.strictDoneFailure(ctx, id, e)
		}
	} else if e := a.putJobRecord(ctx, id, rec); e != nil {
		if kind == "done" && result != nil && boolean(j.Receipt, "headless", false) {
			a.warn("job %s completed record failed: reason=%s", id, doneResultReason(e))
			_ = a.event(ctx, id, "job.record_failed", map[string]any{"op": state})
		} else {
			a.jobRecordFailed(ctx, id, state, e)
		}
	}
	p := map[string]any{"report": *report, "report_sha256": digest, "question": *question, "owner_lane": j.Owner, "parent_lane": j.Parent, "label": j.Label, "pane_id": j.Pane, "document": doc}
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
// a completion for the same report already exists and this call returned
// duplicate. The record may still have no result (the watcher or an earlier
// done completed without one); a stored result is never overwritten.
func (a *App) attachDuplicateResult(ctx context.Context, id, report, digest string, result json.RawMessage) (string, error) {
	if result == nil {
		return "", nil
	}
	store, e := a.jobStore()
	if e != nil {
		a.warn("result attach failed: %v", e)
		return "result=unattached", nil
	}
	rec, e := store.Get(ctx, id)
	if e != nil {
		a.warn("result attach: job record unreadable: %v", e)
		return "result=unattached", nil
	}
	if stored := rec["result"]; stored != nil {
		if sameJSONValue(stored, result) {
			return "result=equal", nil
		}
		a.warn("result conflict: job %s already has a different result; keeping it", id)
		a.jobResultEvent(id, "conflict", "different_result", "stored result kept")
		a.captureSentryWarning([]string{"job-result-conflict"}, "JOB_RESULT_CONFLICT "+id, map[string]string{"job": id})
		return "result=conflict", nil
	}
	// hk accepts a result only in a write that also names completed; the job is
	// already completed locally, so this re-states the same state.
	if e := store.Put(ctx, id, map[string]any{"state": "completed", "result": result}); e != nil {
		a.warn("result attach failed: %v", e)
		return "result=unattached", nil
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
