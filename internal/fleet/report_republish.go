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
// reportRepublishPerPass uploads or pin writes each pass, and a failed one is
// left for the next pass rather than retried at once. Each republished report
// and each refusal is logged once per reason: the sweep runs for the daemon's
// life and a report that cannot be published now would otherwise log every
// pass.

// reportRepublishPerPass bounds the hk writes one sweep pass makes: each
// report upload and each pin. A node catching up on a backlog finishes it over
// the next passes, and an hk outage cannot hold a pass open past this many
// upload timeouts.
const reportRepublishPerPass = 5

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

// pinDue reports that the completing report's pin is still owed: a settle
// receipt proves the record terminal, and no pin write was answered. An empty
// outcome is a pin hk did not answer; the next pass repeats it.
func pinDue(receipts doneReceipts, sha string) bool {
	return len(receipts.settled) > 0 && receipts.completing(sha) == sha && !receipts.pinSettled(sha)
}

// reportRepublishPending reports whether a completion announced a report this
// pass has work for: one with no publish receipt and no pin naming its bytes,
// or the completing report whose pin is still owed. It reads receipts only, no
// file, so a job whose reports are all in hk costs a lock-free check.
func reportRepublishPending(events []Event) bool {
	receipts := readDoneReceipts(events)
	for _, rep := range announcedReports(events) {
		if publishedDocument(events, rep.sha) == "" || pinDue(receipts, rep.sha) {
			return true
		}
	}
	return false
}

// RepublishReports runs one bounded republish pass over the jobs under a.Jobs.
// It logs what it did through the sweeper's Info. The error is only the jobs
// dir being unreadable: an hk failure is one report's, logged and left to the
// next pass. Only the node's own sweep loop calls it, once per pass, so the
// log-once state it keeps on the sweeper needs no lock; the shared sweep state
// it does not touch is the mutex's (pane_sweep.go).
func (s *TaskPaneSweeper) RepublishReports(ctx context.Context, a *App) error {
	entries, err := os.ReadDir(a.Jobs)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	budget := reportRepublishPerPass
	for _, entry := range entries {
		if budget <= 0 {
			break
		}
		if !entry.IsDir() || !validID(entry.Name()) {
			continue
		}
		j, err := a.job(entry.Name())
		if err != nil || (!j.Terminal && !j.Reaped) || !reportRepublishPending(j.Events) {
			continue
		}
		budget -= s.republishJobReports(ctx, a, j, budget)
	}
	return nil
}

// republishJobReports republishes one ended job's announced reports and
// finishes their receipts and pins, making at most budget hk writes. It
// returns the writes it attempted, failed ones included: an hk outage must not
// cost more than the pass's bound. The upload runs outside the job's operation
// lock; only finishReport takes it.
func (s *TaskPaneSweeper) republishJobReports(ctx context.Context, a *App, j Job, budget int) int {
	receipts := readDoneReceipts(j.Events)
	writes := 0
	for _, rep := range announcedReports(j.Events) {
		if writes >= budget {
			break
		}
		doc := publishedDocument(j.Events, rep.sha)
		if doc == "" && receipts.pinSettled(rep.sha) {
			// The record already names these bytes, so hk holds them, and
			// `done` may have deleted the file after the pin (hk 3685).
			continue
		}
		published := false
		if doc == "" {
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
			writes++
			key, err := a.publishReport(ctx, j, path, body, rep.sha)
			if err != nil {
				// No receipt is written, so the next pass retries; this one
				// moves on to the next report instead of retrying here.
				s.reportKept(j.ID, rep.sha, rep.path, "publish_failed")
				continue
			}
			doc, published = key, true
			s.reportRepublished(j.ID, rep.sha, path, key)
		}
		// A receipt alone is nothing unless the completing report still owes
		// its pin; a report this pass just published owes its receipt either
		// way. A `done` may have written both while the upload ran, in which
		// case finishReport re-reads and does nothing.
		if !published && !pinDue(receipts, rep.sha) {
			continue
		}
		writes += s.finishReport(ctx, a, j.ID, rep, doc, budget-writes)
	}
	return writes
}

// finishReport writes the publish receipt a republished report is owed and the
// completing report's record pin, under the job's operation lock, so a
// concurrent `done` cannot race the read-then-write. Both writes are local or
// single-record hk calls; the upload that may wait on hk is never under the
// lock. It returns the hk writes it made: one for a pin attempt, zero when the
// receipt alone was written today. A busy lock leaves both to the next pass,
// since the upload already landed in hk.
func (s *TaskPaneSweeper) finishReport(ctx context.Context, a *App, id string, rep announcedReport, doc string, budget int) int {
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
	if !pinDue(receipts, rep.sha) || budget <= 0 {
		return 0
	}
	outcome, err := a.pinReport(ctx, id, doc, rep.sha)
	switch {
	case err != nil:
		s.reportKept(id, rep.sha, rep.path, "pin_failed")
		return 1
	case outcome.outcome == pinAbsent:
		// The record is not completed: no receipt, as step 3 of `done` leaves
		// it; the next pass repeats the pin.
		return 1
	}
	if err := a.event(ctx, id, eventReportPinned, map[string]any{"report_sha256": rep.sha, "document": outcome.document, "outcome": outcome.outcome}); err != nil {
		s.reportKept(id, rep.sha, rep.path, "pin_receipt_failed")
		return 1
	}
	return 1
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
