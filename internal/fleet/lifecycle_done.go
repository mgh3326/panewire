package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/handoffkeep/client"
)

// doneInput is one `done` for the report whose bytes are body.
type doneInput struct {
	report   string
	body     []byte
	digest   string
	question string
	result   json.RawMessage
	// strict: --result on an interactive done, which needs the hk record
	// confirmed with that result (hk 3402, #426).
	strict bool
	// watcher is the caller with no one to read an exit status: it warns on a
	// step that failed and carries on.
	watcher bool
	late    bool
	// recovered: the report file is gone because `done` deleted it after the
	// pin (hk 3685), and digest is the one the completion that announced it
	// named. body is empty, so the step never publishes again; only the
	// receipts decide what remains.
	recovered bool
}

// settledReceipt is a job.record_settled receipt: the hk record is terminal
// and, when result_sha256 is set, holds that result. report_sha256 is the
// completing report, the one the record's pin belongs to.
type settledReceipt struct {
	sha, resultSHA string
	pinned         bool
}

// doneReceipts are the local receipts of the steps of `done`, read from the
// job's events. A fact is never inferred from another fact's event.
type doneReceipts struct {
	published map[string]string // report sha256 → document
	notified  map[string]bool   // report sha256 + "\x00" + question
	settled   []settledReceipt
	pins      map[string]string // report sha256 → outcome of the pin write
	// firstCompleted is the sha256 of the first job.completed event, the
	// completing report of a job that completed before settlements were
	// receipted.
	firstCompleted string
}

func readDoneReceipts(events []Event) doneReceipts {
	r := doneReceipts{published: map[string]string{}, notified: map[string]bool{}, pins: map[string]string{}}
	for _, ev := range events {
		sha := str(ev.Payload, "report_sha256")
		switch ev.Kind {
		case eventReportPublished:
			r.published[sha] = str(ev.Payload, "document")
		case "job.completed":
			r.notified[sha+"\x00"+str(ev.Payload, "question")] = true
			if r.firstCompleted == "" {
				r.firstCompleted = sha
			}
		case eventReportPinned:
			r.pins[sha] = str(ev.Payload, "outcome")
		case eventRecordSettled:
			r.settled = append(r.settled, settledReceipt{sha: sha, resultSHA: str(ev.Payload, "result_sha256"), pinned: boolean(ev.Payload, "pinned", false)})
		}
	}
	return r
}

// completing is the sha256 of the report the job's record belongs to: the one
// that settled it, else the first one announced, else digest, the report this
// run would settle it with.
func (r doneReceipts) completing(digest string) string {
	switch {
	case len(r.settled) > 0:
		return r.settled[0].sha
	case r.firstCompleted != "":
		return r.firstCompleted
	}
	return digest
}

// settledWith is the receipt that proves the record is terminal holding
// resultSHA: any receipt when the caller carries no result.
func (r doneReceipts) settledWith(resultSHA string) (settledReceipt, bool) {
	for _, s := range r.settled {
		if resultSHA == "" || s.resultSHA == resultSHA {
			return s, true
		}
	}
	return settledReceipt{}, false
}

// pinSettled reports whether the pin of the report with sha256 digest has its
// receipt: the settling write carried it, or a pin write was answered.
func (r doneReceipts) pinSettled(digest string) bool {
	if r.pins[digest] != "" {
		return true
	}
	for _, s := range r.settled {
		if s.sha == digest && s.pinned {
			return true
		}
	}
	return false
}

// holds reports whether the hk record names this report: the settling write
// carried the pin, or a pin write answered "pinned". A "held" pin names
// another report and is not this one's.
func (r doneReceipts) holds(digest string) bool {
	if digest == "" {
		return false
	}
	if r.pins[digest] == pinPinned {
		return true
	}
	for _, s := range r.settled {
		if s.sha == digest && s.pinned {
			return true
		}
	}
	return false
}

// held reports whether a pin write was answered "held": the record names
// another report, so this one can never take the pin.
func (r doneReceipts) held(digest string) bool { return digest != "" && r.pins[digest] == pinHeld }

// completedReportDigest is the sha256 of the bytes the newest job.completed
// announcing report carried. A report file the fleet deleted after the pin
// (hk 3685) is named by that receipt, so a rerun of `done` still knows the
// bytes hk holds.
func completedReportDigest(j Job, report string) (string, bool) {
	digest := ""
	for _, ev := range j.Events {
		if ev.Kind == "job.completed" && str(ev.Payload, "report") == report {
			if sha := str(ev.Payload, "report_sha256"); sha != "" {
				digest = sha
			}
		}
	}
	return digest, digest != ""
}

// resultDigest is the sha256 of the result's canonical JSON, "" for none.
func resultDigest(result json.RawMessage) string {
	if result == nil {
		return ""
	}
	var v any
	if json.Unmarshal(result, &v) != nil {
		return ""
	}
	canonical, ok := canonicalJSON(v)
	if !ok {
		return ""
	}
	return reportDigest([]byte(canonical))
}

// doneReport runs the steps of `done` for one report, each skipped when its
// receipt exists:
//
//  1. publish: hk holds the report's bytes (job.report_published)
//  2. settle: the hk record is terminal, written by strictCompletedPutUntil
//     (job.record_settled)
//  3. pin: the terminal record names the completing report's document, a
//     once-only late write (job.report_pinned). hk answers 200 for the first
//     pin and 409 when the record already holds another; the 409 is an
//     outcome, logged, not an error.
//  4. notify: job.completed and the relay, once per report, with the exact
//     document key when step 1 succeeded
//
// A report whose steps all have receipts is a duplicate: it makes no hk call.
// Publication never blocks completion. The one blocker is strict --result:
// hk takes a result only in the spawned→completed write, so the record must be
// confirmed holding it.
func (a *App) doneReport(ctx context.Context, j Job, in doneInput) error {
	id := j.ID
	receipts := readDoneReceipts(j.Events)
	var ran []string

	doc := receipts.published[in.digest]
	if doc == "" && !in.recovered {
		key, e := a.publishReport(ctx, j, in.report, in.body, in.digest)
		if e != nil {
			a.warn("report upload failed; local report retained")
		} else if e = a.event(ctx, id, eventReportPublished, map[string]any{"report_sha256": in.digest, "document": key}); e != nil {
			return e
		} else {
			doc = key
			ran = append(ran, "published")
		}
	}

	resultSHA := resultDigest(in.result)
	if _, ok := receipts.settledWith(resultSHA); !ok {
		// The report upload above and the session read below can take time: a
		// revocation that lands meanwhile (written without this job's locks)
		// stops the write, so ownership is read again after them, and
		// strictCompletedPutUntil reads it before every PUT it makes.
		a.recordJobUsage(ctx, j, false)
		if e := a.refuseReassigned(j); e != nil {
			return e
		}
		completing := receipts.completing(in.digest)
		fields := map[string]any{"state": "completed", "at": a.Now().UTC()}
		pin := doc != "" && completing == in.digest
		if pin {
			fields["report_doc"] = doc
			fields["report_sha256"] = in.digest
		}
		if in.result != nil {
			fields["result"] = in.result
		}
		var wanted any
		if in.result != nil {
			wanted = in.result
		}
		holds, e := a.strictCompletedPutUntil(ctx, id, fields, wanted, spawnReceiptForRepair(j.Events), time.Now().Add(doneResultBudget))
		switch {
		case e == nil:
			p := map[string]any{"report_sha256": completing, "pinned": pin && holds}
			if resultSHA != "" {
				p["result_sha256"] = resultSHA
			}
			if e = a.event(ctx, id, eventRecordSettled, p); e != nil {
				return e
			}
			receipts.settled = append(receipts.settled, settledReceipt{sha: completing, resultSHA: resultSHA, pinned: pin && holds})
			ran = append(ran, "settled")
		case isDoneResultFailure(e, "job_reassigned"):
			return e
		case in.strict:
			if isDoneResultFailure(e, "result_conflict") {
				a.recordDoneResultConflict(id)
			}
			return a.strictDoneFailure(ctx, id, e)
		case isDoneResultFailure(e, "result_conflict"):
			// An unattended caller still raises the fleet-level conflict
			// signal: nobody reads its stderr.
			a.recordDoneResultConflict(id)
			a.warn("job %s completed record failed: reason=result_conflict", id)
			_ = a.event(ctx, id, "job.record_failed", map[string]any{"op": "completed", "reason": "result_conflict"})
		default:
			a.jobRecordFailed(ctx, id, "completed", e)
		}
	}

	// Step 3: the terminal record's pin. The record, not a local settlement
	// receipt, is the authority for the step's outcome: a same-id retry whose
	// record ended before the upload landed reads it here and attaches the
	// pair (hk 4354), a record already naming these bytes is a no-op, and one
	// naming another report is a refusal. The settlement receipt only says
	// this run settled a record the pin can attach to; a job with no hk
	// record (or one the settlement left unwritten) keeps today's warning-only
	// path in step 2.
	pinPending, refused := false, doc != "" && receipts.held(in.digest)
	pinned := doc != "" && receipts.holds(in.digest)
	if doc != "" && len(receipts.settled) > 0 && receipts.completing(in.digest) == in.digest && !receipts.pinSettled(in.digest) {
		// A retry whose settlement is already receipted skips step 2, so the
		// upload above is rechecked here, just before the pin write.
		if e := a.refuseReassigned(j); e != nil {
			return e
		}
		res, e := a.pinReport(ctx, id, doc, in.digest)
		switch {
		case e != nil:
			pinPending = true
			a.warn("report pin failed: reason=%s", doneResultReason(e))
		case res.outcome == pinAbsent:
			// The record is not completed: its state is another step's fact.
		default:
			p := map[string]any{"report_sha256": in.digest, "document": res.document, "outcome": res.outcome}
			if e = a.event(ctx, id, eventReportPinned, p); e != nil {
				return e
			}
			pinned, refused = res.outcome == pinPinned, res.outcome == pinHeld
			if pinned {
				ran = append(ran, "pinned")
			}
		}
	}

	relay := ""
	if !receipts.notified[in.digest+"\x00"+in.question] {
		p := map[string]any{"report": in.report, "report_sha256": in.digest, "question": in.question, "owner_lane": j.Owner, "parent_lane": j.Parent, "label": j.Label, "pane_id": j.Pane, "document": doc}
		if in.late {
			p["late"] = true
		}
		if e := a.event(ctx, id, "job.completed", p); e != nil {
			return e
		}
		if in.result == nil && j.SpawnRole == "reviewer" {
			a.jobResultEvent(id, "missing", "no_result", "")
			a.warn("reviewer done carried no result: %s", id)
		}
		relay = a.relayReport(ctx, j, "job.completed", in.report, in.question, in.body, doc, in.late)
		a.commentOnTask(ctx, j, "job.completed", in.report)
		ran = append(ran, "notified")
	}
	if e := a.releaseQuota(ctx, j); e != nil {
		return e
	}
	line := fmt.Sprintf("OK job=%s report=%s duplicate=yes%s", id, in.report, repaired(ran))
	if slices.Contains(ran, "notified") {
		line = fmt.Sprintf("OK job=%s report=%s kind=job.completed%s", id, in.report, relay)
	}
	fmt.Fprintln(a.Out, line)
	// hk holds this report's bytes and the record names them: the local file
	// only duplicates them (hk 3685 item 2). The marker keeps job.done's
	// answer for the job whose report this deletes.
	if pinned && a.removePinnedReport(in) {
		a.markJobDone(id, in.digest, line)
	}
	if !in.watcher {
		switch {
		case doc == "":
			return fail(exitFailure, "job %s completed; report not in hk; rerun the same command", id)
		case pinPending:
			return fail(exitFailure, "job %s completed; report not pinned in hk; rerun the same command", id)
		case refused:
			return fail(exitStateConflict, "job %s completed; hk record names another report; local report kept", id)
		}
	}
	return nil
}

// removePinnedReport deletes the local report whose bytes hk holds under the
// record's pin, leaving only the pinned copy. A path outside the jobs tree
// stays: `done --report` accepts one, and deleting outside the fleet's own
// tree is not this step's to do. A file whose bytes changed after the
// completion stays too: they are no longer the ones the record names. Every
// refusal warns and none fails `done`; a report whose upload never landed keeps
// its file for the [reap] net (hk 3685 item 3).
func (a *App) removePinnedReport(in doneInput) bool {
	path := filepath.Clean(in.report)
	if !within(a.Jobs, path) {
		a.warn("report kept: %s is outside the jobs root", in.report)
		return false
	}
	body, e := os.ReadFile(path)
	switch {
	case errors.Is(e, os.ErrNotExist):
		return false
	case e != nil:
		a.warn("report delete skipped: %v", e)
		return false
	case reportDigest(body) != in.digest:
		a.warn("report kept: %s changed after the completion", in.report)
		return false
	}
	if e := os.Remove(path); e != nil {
		a.warn("report delete failed: %v", e)
		return false
	}
	return true
}

// The pin step's outcomes. "pinned" and "held" are the receipts' vocabulary;
// "absent" means the record is not a completed one, so there is nothing for
// the late write to attach, and no receipt is written.
const (
	pinPinned = "pinned"
	pinHeld   = "held"
	pinAbsent = "absent"
)

// pinResult is step 3's answer: the outcome, and S's document (the one the
// receipt names, written or not). wrote says the step issued hk's write rather
// than only its read, which is what the republish sweep charges its per-pass
// budget for (report_republish.go).
type pinResult struct {
	outcome  string
	document string
	wrote    bool
}

// pinReport is the late write of report_doc and report_sha256 on a terminal
// record (step 3). The record is read first, so the write is the one hk takes
// once (hk 4354) only when the record holds no report, while a record that
// already names these bytes is a no-op and one naming another report is the
// "held" outcome: the retry's report can never take that pin. A write hk
// refuses with 409 anyway (another writer pinned in between) is the same
// outcome. The error is a write hk did not answer, or a record it did not
// read; the next done repeats the step.
func (a *App) pinReport(ctx context.Context, id, doc, digest string) (pinResult, error) {
	store, e := a.jobStore()
	if e != nil {
		return pinResult{}, e
	}
	rec, e := store.Get(ctx, id)
	if e != nil {
		return pinResult{}, e
	}
	if str(rec, "state") != "completed" {
		return pinResult{outcome: pinAbsent}, nil
	}
	heldDoc, heldSHA := str(rec, "report_doc"), str(rec, "report_sha256")
	switch {
	case heldDoc == "" && heldSHA == "":
		// No report on the record: the once-only late pin.
	case heldSHA == digest:
		return pinResult{outcome: pinPinned, document: heldDoc}, nil
	default:
		a.warn("report pin kept: job %s record already names another report", id)
		return pinResult{outcome: pinHeld, document: doc}, nil
	}
	switch e = a.putJobRecord(ctx, id, map[string]any{"report_doc": doc, "report_sha256": digest}); {
	case e == nil:
		return pinResult{outcome: pinPinned, document: doc, wrote: true}, nil
	case isJobStateConflict(e):
		a.warn("report pin kept: job %s record already names another report", id)
		return pinResult{outcome: pinHeld, document: doc, wrote: true}, nil
	}
	return pinResult{wrote: true}, e
}

// repaired renders the steps a rerun finished, as " published=yes ...".
func repaired(ran []string) string {
	var b strings.Builder
	for _, step := range ran {
		fmt.Fprintf(&b, " %s=yes", step)
	}
	return b.String()
}

// relayReport emits the completion to the parent lane's inbox: the report
// path and its last line, with the document key when the report is in hk. A
// completion names the report by that hk key in place of the local path,
// which the fleet deletes once the record pins it (hk 3685). It returns
// " relay=file-only" when the emit failed and the durable arbiter event alone
// carries it.
func (a *App) relayReport(ctx context.Context, j Job, eventKind, report, question string, body []byte, doc string, late bool) string {
	owner := j.Owner
	if j.Parent != "" && (eventKind == "job.escalate" || j.Role == "builder") {
		owner = j.Parent
	}
	last := strings.TrimSpace(string(body))
	if lines := strings.Split(last, "\n"); len(lines) > 0 {
		last = lines[len(lines)-1]
	}
	if doc != "" {
		if eventKind == "job.completed" {
			// The hk key replaces the path the desk used to see, so the doc:
			// suffix would name it a second time.
			report = doc
		} else {
			last += " doc:" + doc
		}
	}
	argsEmit := []string{"emit", "--kind", eventKind, "--job", j.ID, "--owner-lane", owner, "--epoch", "1", "--label", j.Label, "--host", a.machine(), "--pane", j.Pane, "--inbox-root", filepath.Dir(a.Jobs)}
	if late {
		argsEmit = append(argsEmit, "--late", "--deliver", "inbox")
	}
	if report != "" {
		argsEmit = append(argsEmit, "--report", report, "--report-last-line", last)
	}
	if question != "" {
		argsEmit = append(argsEmit, "--question", question)
	}
	r := a.call(ctx, "panewire", argsEmit...)
	if r.Code == 0 {
		return ""
	}
	a.warn("relay failed; durable arbiter event retained")
	_ = a.event(ctx, j.ID, "job.relay_failed", map[string]any{"kind": eventKind, "exit": r.Code})
	return " relay=file-only"
}

// commentOnTask notes the event on the job's hk task, when it has one.
func (a *App) commentOnTask(ctx context.Context, j Job, eventKind, report string) {
	if j.Task == "" {
		return
	}
	comment := "panewire " + eventKind + " job=" + j.ID + " report=" + report
	if e := hkCheck(hkCommentTask(ctx, j.Task, comment), "handoffkeep"); e != nil {
		a.warn("task comment failed: %v", e)
	}
}

// escalateReport is `escalate`: it uploads the report under the report file's
// name, writes the escalated record with the report pin, announces the
// question and records the failure outcome. A repeat of the same report and
// question never reaches it (lifecycleWithOptions).
func (a *App) escalateReport(ctx context.Context, j Job, report, question string, body []byte, digest string, late bool) error {
	id := j.ID
	doc, docSHA := "", ""
	if report != "" {
		key := "reports/" + id + "/" + filepath.Base(report)
		b, e := os.ReadFile(report)
		if e == nil {
			upload := client.Document{Key: key, Kind: "report", Job: id, Body: string(b), OriginClass: "job"}
			if task, err := hkTaskID(j.Task); err == nil {
				upload.TaskID = &task
			}
			e = hkPutDocument(ctx, upload)
		}
		if e == nil {
			doc, docSHA = key, reportDigest(b)
		} else {
			a.warn("report upload failed; local report retained")
		}
	}
	a.recordJobUsage(ctx, j, false)
	if e := a.refuseReassigned(j); e != nil {
		return e
	}
	rec := map[string]any{"state": "escalated", "at": a.Now().UTC()}
	if doc != "" {
		rec["report_doc"] = doc
		rec["report_sha256"] = docSHA
	}
	if e := a.putJobRecord(ctx, id, rec); e != nil {
		a.jobRecordFailed(ctx, id, "escalated", e)
	}
	p := map[string]any{"report": report, "report_sha256": digest, "question": question, "owner_lane": j.Owner, "parent_lane": j.Parent, "label": j.Label, "pane_id": j.Pane, "document": doc}
	if late {
		p["late"] = true
	}
	if e := a.event(ctx, id, "job.escalate", p); e != nil {
		return e
	}
	if _, rerr := a.recordFailedOutcome(ctx, j, j.Task, "escalated", a.PRs); rerr != nil {
		a.warn("failure rep not recorded: %v", rerr)
	}
	relay := a.relayReport(ctx, j, "job.escalate", report, question, body, doc, late)
	a.commentOnTask(ctx, j, "job.escalate", report)
	fmt.Fprintf(a.Out, "OK job=%s report=%s kind=job.escalate%s\n", id, report, relay)
	return nil
}
