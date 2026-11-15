package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
)

// Report republish for the node's periodic sweep (hk 4372).
//
// A `done` whose hk upload failed still writes its completion:
// job.completed {report, report_sha256, document:""}. The report stays local
// and nothing retried the upload short of rerunning `done`. The node's sweep
// closes that gap for ended jobs: it republishes exactly the bytes the
// completion announced, through the done path's content-addressed PUT
// (publishReport), writes the job.report_published receipt, and pins the
// terminal record when the report is the completing one, as step 3 of `done`
// does. A pin write hk did not answer is repeated by a later pass.
//
// It publishes only the bytes the job announced. A file whose sha256 is no
// longer the announced one is still changing, or belongs to a later round that
// a new `done` alone announces: publishing it would pin bytes no completion
// named, so the sweep waits and logs the refusal once instead.
//
// A report hk already holds and the record already pins is nothing: `done`
// deletes that local file once the pin lands (hk 3685), so the receipts, not
// the file, decide whether a report has work left.
//
// The upload runs without the job's operation lock: it can wait on hk for the
// upload bound, and a worker's `done` (which takes that lock) must never wait
// behind it. The receipt and pin writes take the lock only briefly; neither
// waits on hk's upload bound, and both re-read the job under it so a `done`
// that pinned, deleted and receipted in between is not raced.
//
// Attempts are bounded per pass, so an hk outage costs at most
// reportRepublishPerPass uploads and reportRepublishPerPass pin attempts each
// pass, and a failed one is left for the next pass rather than retried at once.
// The two budgets are separate: a pin hk will not answer can never hold the
// upload another job needs. A record hk keeps answering "not completed", or
// stops answering at all, moves to the pin slow lane after a few passes, so a
// stuck record cannot hold a pin slot either. Each republished report and each
// refusal is logged once per reason: the sweep runs for the daemon's life and a
// report that cannot be published now would otherwise log every pass.

// reportRepublishPerPass bounds one sweep pass: this many report uploads and,
// separately, this many pin attempts that write or that hk did not answer. A
// node catching up on a backlog finishes it over the next passes, and an hk
// outage cannot hold a pass open past this many hk timeouts.
const reportRepublishPerPass = 5

// reportPinAttempts is how many consecutive passes a pin hk did not answer is
// retried on before the report moves to the slow lane. One unanswered pass is
// repeated by the next; a record that never answers cannot keep spending a pin
// slot ahead of jobs that can finish.
const reportPinAttempts = 3

// reportPinSlowEvery is how many passes the slow lane waits between pin probes:
// one an hour at the default five-minute sweep. The repair stays available; its
// cost does not.
const reportPinSlowEvery = 12

// announcedReport is one report a job's completion announced: the path the
// completion carries and the sha256 of the bytes it was completed with.
type announcedReport struct {
	path, sha string
}

// announcedReports lists, in event order, the distinct reports a job's
// job.completed events announced. A later round at the same path announces its
// own bytes, so each sha256 is one report with one publish receipt.
func announcedReports(events []Event) []announcedReport {
	var out []announcedReport
	seen := map[string]bool{}
	for _, ev := range events {
		if ev.Kind != "job.completed" {
			continue
		}
		path, sha := str(ev.Payload, "report"), str(ev.Payload, "report_sha256")
		if path == "" || sha == "" || seen[sha] {
			continue
		}
		seen[sha] = true
		out = append(out, announcedReport{path: path, sha: sha})
	}
	return out
}

// reportPathFor is the file the announcement's report path names, resolved
// against the job's recorded worktree when the caller gave a path relative to
// it (a worker's `done --report`). "" when neither says where the file is.
func reportPathFor(j Job, report string) string {
	if filepath.IsAbs(report) {
		return report
	}
	if cwd := str(j.Receipt, "cwd"); cwd != "" {
		return filepath.Join(cwd, report)
	}
	return ""
}

// reportPinState is the sweep's memory of one report's pin: how many
// consecutive attempts hk did not answer, and the pass before which the sweep
// stops trying.
type reportPinState struct {
	fails int
	after int
}

func reportPinKey(job, sha string) string { return job + "\x00" + sha }

// reportPinWaits reports whether the pin slow lane still holds this report.
func (s *TaskPaneSweeper) reportPinWaits(job, sha string) bool {
	return s.reportPass < s.reportPins[reportPinKey(job, sha)].after
}

// reportPinSlow sends the report's pin to the slow lane: hk answered with a
// record that is not a completed one, which this sweep cannot change.
func (s *TaskPaneSweeper) reportPinSlow(job, sha string) {
	if s.reportPins == nil {
		s.reportPins = map[string]reportPinState{}
	}
	k := reportPinKey(job, sha)
	st := s.reportPins[k]
	st.after = s.reportPass + reportPinSlowEvery
	s.reportPins[k] = st
}

// reportPinUnanswered counts a pin attempt hk did not answer, and after
// reportPinAttempts in a row sends the report to the slow lane.
func (s *TaskPaneSweeper) reportPinUnanswered(job, sha string) {
	if s.reportPins == nil {
		s.reportPins = map[string]reportPinState{}
	}
	k := reportPinKey(job, sha)
	st := s.reportPins[k]
	st.fails++
	if st.fails >= reportPinAttempts {
		st.after = s.reportPass + reportPinSlowEvery
	}
	s.reportPins[k] = st
}

// reportPinSettled forgets the report: its pin receipt is written.
func (s *TaskPaneSweeper) reportPinSettled(job, sha string) {
	delete(s.reportPins, reportPinKey(job, sha))
}

// pinDue reports that the completing report's pin is still owed: a settle
// receipt proves the record terminal, and no pin write was answered. An empty
// outcome is a pin hk did not answer; the next pass repeats it.
func pinDue(receipts doneReceipts, sha string) bool {
	return len(receipts.settled) > 0 && receipts.completing(sha) == sha && !receipts.pinSettled(sha)
}

// reportPinDue reports that this pass should try the report's pin: the
// completing report owes one, and the slow lane is not holding it.
func (s *TaskPaneSweeper) reportPinDue(receipts doneReceipts, job, sha string) bool {
	return pinDue(receipts, sha) && !s.reportPinWaits(job, sha)
}

// reportPinCost is what one pin attempt charged the pass: an hk write, or an
// attempt hk did not answer, which may have been a read that waited on hk's
// timeout. Only a read that answered without writing is free: it is a fact, not
// a wait, so it cannot hold the pass open the way an upload does.
func reportPinCost(res pinResult, err error) int {
	if err != nil || res.wrote {
		return 1
	}
	return 0
}

// reportRepublishPending reports whether a completion announced a report this
// pass has work for: one with no publish receipt and no pin naming its bytes, or
// the completing report whose pin is still owed and not on the slow lane. It
// reads receipts only, no file, so a job whose reports are all in hk costs a
// lock-free check.
func (s *TaskPaneSweeper) reportRepublishPending(j Job) bool {
	receipts := readDoneReceipts(j.Events)
	for _, rep := range announcedReports(j.Events) {
		if publishedDocument(j.Events, rep.sha) == "" {
			return true
		}
		if s.reportPinDue(receipts, j.ID, rep.sha) {
			return true
		}
	}
	return false
}

// RepublishReports runs one bounded republish pass over the jobs under a.Jobs.
// It logs what it did through the sweeper's Info. The error is only the jobs
// dir being unreadable: an hk failure is one report's, logged and left to the
// next pass. Only the node's own sweep loop calls it, once per pass, so the
// pass counter and log-once state it keeps on the sweeper need no lock; the
// shared sweep state it does not touch is the mutex's (pane_sweep.go).
func (s *TaskPaneSweeper) RepublishReports(ctx context.Context, a *App) error {
	entries, err := os.ReadDir(a.Jobs)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	s.reportPass++
	uploads, pins := reportRepublishPerPass, reportRepublishPerPass
	for _, entry := range entries {
		if uploads <= 0 && pins <= 0 {
			break
		}
		if !entry.IsDir() || !validID(entry.Name()) {
			continue
		}
		j, err := a.job(entry.Name())
		if err != nil || (!j.Terminal && !j.Reaped) || !s.reportRepublishPending(j) {
			continue
		}
		put, pinned := s.republishJobReports(ctx, a, j, uploads, pins)
		uploads -= put
		pins -= pinned
	}
	return nil
}

// republishJobReports republishes one ended job's announced reports and
// finishes their receipts and pins. uploads and pins are what the pass has left;
// it returns what it spent of each, so an hk outage can never cost the pass more
// than its budgets. The upload runs outside the job's operation lock; only
// finishReport takes it.
func (s *TaskPaneSweeper) republishJobReports(ctx context.Context, a *App, j Job, uploads, pins int) (int, int) {
	receipts := readDoneReceipts(j.Events)
	put, pinned := 0, 0
	for _, rep := range announcedReports(j.Events) {
		doc := publishedDocument(j.Events, rep.sha)
		if doc == "" && receipts.pinSettled(rep.sha) {
			// The record already names these bytes, so hk holds them, and
			// `done` may have deleted the file after the pin (hk 3685).
			continue
		}
		published := false
		if doc == "" {
			if put >= uploads {
				continue
			}
			path := reportPathFor(j, rep.path)
			if path == "" {
				s.reportKept(j.ID, rep.sha, rep.path, "report_path_unknown")
				continue
			}
			if err := noSymlinks(path); err != nil {
				s.reportKept(j.ID, rep.sha, rep.path, "report_path_refused")
				continue
			}
			body, err := os.ReadFile(path)
			switch {
			case errors.Is(err, os.ErrNotExist):
				s.reportKept(j.ID, rep.sha, rep.path, "report_missing")
				continue
			case err != nil:
				s.reportKept(j.ID, rep.sha, rep.path, "report_unreadable")
				continue
			case reportDigest(body) != rep.sha:
				// The file no longer holds the announced bytes: it is still
				// being written, or a later round owns it. Only a new `done`
				// announces those bytes, so this pass leaves the file alone.
				s.reportKept(j.ID, rep.sha, rep.path, "report_changed")
				continue
			}
			put++
			key, err := a.publishReport(ctx, j, path, body, rep.sha)
			if err != nil {
				// No receipt is written, so the next pass retries; this one
				// moves on to the next report instead of retrying here.
				s.reportKept(j.ID, rep.sha, rep.path, "publish_failed")
				continue
			}
			doc, published = key, true
		}
		// A receipt alone is nothing unless the completing report still owes
		// its pin; a report this pass just published owes its receipt either
		// way. A `done` may have written both while the upload ran, in which
		// case finishReport re-reads and does nothing.
		if !published && !s.reportPinDue(receipts, j.ID, rep.sha) {
			continue
		}
		pinned += s.finishReport(ctx, a, j.ID, rep, doc, published, pins-pinned)
	}
	return put, pinned
}

// finishReport writes the publish receipt a republished report is owed and the
// completing report's record pin, under the job's operation lock, so a
// concurrent `done` cannot race the read-then-write. Both writes are local or
// single-record hk calls; the upload that may wait on hk is never under the
// lock. It returns the pin write it charged the pass (0 or 1). A busy lock
// leaves both writes to the next pass, since the upload already landed in hk.
func (s *TaskPaneSweeper) finishReport(ctx context.Context, a *App, id string, rep announcedReport, doc string, published bool, budget int) int {
	unlock, err := TryLockJobOperation(filepath.Join(a.Jobs, id))
	if errors.Is(err, ErrJobOperationBusy) {
		s.reportKept(id, rep.sha, rep.path, "job_busy")
		return 0
	}
	if err != nil {
		s.reportKept(id, rep.sha, rep.path, "job_dir_unreadable")
		return 0
	}
	defer unlock()
	// Re-read under the lock: a done that finished meanwhile leaves receipts
	// this pass must see, including the pin, after which it deletes the file.
	current, err := a.job(id)
	if err != nil {
		return 0
	}
	receipts := readDoneReceipts(current.Events)
	if publishedDocument(current.Events, rep.sha) == "" {
		if err := a.event(ctx, id, eventReportPublished, map[string]any{"report_sha256": rep.sha, "document": doc}); err != nil {
			s.reportKept(id, rep.sha, rep.path, "receipt_failed")
			return 0
		}
	}
	// The receipt is in place, so the report is republished: the line is owed
	// only now, and names the same report field the refusals do. A receipt
	// write that fails says so instead, and the next pass's upload logs the
	// republish when the receipt lands.
	if published {
		s.reportRepublished(id, rep.sha, rep.path, doc)
	}
	if budget <= 0 || !s.reportPinDue(receipts, id, rep.sha) {
		return 0
	}
	res, err := a.pinReport(ctx, id, doc, rep.sha)
	switch {
	case err != nil:
		s.reportPinUnanswered(id, rep.sha)
		s.reportKept(id, rep.sha, rep.path, "pin_failed")
		return reportPinCost(res, err)
	case res.outcome == pinAbsent:
		// The record is not a completed one: no receipt, as step 3 of `done`
		// leaves it, and this sweep cannot make it completed. Every pass would
		// spend a pin slot on the same answer, so it goes to the slow lane.
		s.reportPinSlow(id, rep.sha)
		return 0
	}
	if err := a.event(ctx, id, eventReportPinned, map[string]any{"report_sha256": rep.sha, "document": res.document, "outcome": res.outcome}); err != nil {
		s.reportKept(id, rep.sha, rep.path, "pin_receipt_failed")
		return reportPinCost(res, nil)
	}
	s.reportPinSettled(id, rep.sha)
	return reportPinCost(res, nil)
}

// reportRepublished logs one republished report, once.
func (s *TaskPaneSweeper) reportRepublished(job, sha, report, document string) {
	if !s.noteReport(job, sha, "republished") {
		return
	}
	s.info("report republished", "job", job, "report_sha256", sha, "report", report, "document", document)
}

// reportKept logs one refusal to republish, once per report and reason.
func (s *TaskPaneSweeper) reportKept(job, sha, report, reason string) {
	if !s.noteReport(job, sha, reason) {
		return
	}
	s.info("report republish kept", "job", job, "report_sha256", sha, "report", report, "reason", reason)
}

// noteReport records that this sweeper logged key and reports whether it had
// not logged it before. The sweeper lives for the daemon's run, so a refusal
// that repeats every pass is one line.
func (s *TaskPaneSweeper) noteReport(job, sha, key string) bool {
	if s.reportNoted == nil {
		s.reportNoted = map[string]bool{}
	}
	k := job + "\x00" + sha + "\x00" + key
	if s.reportNoted[k] {
		return false
	}
	s.reportNoted[k] = true
	return true
}
