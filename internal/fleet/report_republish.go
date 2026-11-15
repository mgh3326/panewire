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
// does.
//
// It publishes only the bytes the job announced. A file whose sha256 is no
// longer the announced one is still changing, or belongs to a later round that
// a new `done` alone announces: publishing it would pin bytes no completion
// named, so the sweep waits and logs the refusal once instead.
//
// Attempts are bounded per pass, so an hk outage costs at most
// reportRepublishPerPass uploads each pass, and a failed upload is left for the
// next pass rather than retried at once. Each republished report and each
// refusal is logged once per reason: the sweep runs for the daemon's life and a
// report that cannot be published now would otherwise log every pass.

// reportRepublishPerPass bounds the report uploads one sweep pass makes. A
// node catching up on a backlog publishes it over the next passes, and an hk
// outage cannot hold a pass open past this many upload timeouts.
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

// unpublishedAnnouncement reports whether a completion announced a report with
// no publish receipt and no pin naming its sha256: the only work a republish
// pass has for a job. It reads the receipts only, no file, so a job whose
// reports are all in hk costs a lock-free check.
func unpublishedAnnouncement(events []Event) bool {
	receipts := readDoneReceipts(events)
	for _, rep := range announcedReports(events) {
		if publishedDocument(events, rep.sha) == "" && !receipts.pinSettled(rep.sha) {
			return true
		}
	}
	return false
}

// RepublishReports runs one bounded republish pass over the jobs under a.Jobs.
// It logs what it did through the sweeper's Info. The error is only the jobs
// dir being unreadable: an hk failure is one report's, logged and left to the
// next pass.
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
		if err != nil || (!j.Terminal && !j.Reaped) || !unpublishedAnnouncement(j.Events) {
			continue
		}
		budget -= s.republishJobReports(ctx, a, j, budget)
	}
	return nil
}

// republishJobReports republishes one ended job's announced reports that have
// no publish receipt, making at most budget uploads. It returns the uploads it
// attempted, failed ones included: an hk outage must not cost more than the
// pass's bound.
func (s *TaskPaneSweeper) republishJobReports(ctx context.Context, a *App, j Job, budget int) int {
	// The sweep publishes under the lock a `done` holds (lifecycle.go), so the
	// report bytes and the receipts cannot change under it.
	unlock, err := TryLockJobOperation(filepath.Join(a.Jobs, j.ID))
	if errors.Is(err, ErrJobOperationBusy) {
		s.reportKept(j.ID, "", "", "job_busy")
		return 0
	}
	if err != nil {
		s.reportKept(j.ID, "", "", "job_dir_unreadable")
		return 0
	}
	defer unlock()
	// Re-read under the lock: a done that finished while the pass read the job
	// leaves receipts this pass must see.
	latest, err := a.job(j.ID)
	if err != nil || (!latest.Terminal && !latest.Reaped) {
		return 0
	}
	j = latest
	receipts := readDoneReceipts(j.Events)
	attempts := 0
	for _, rep := range announcedReports(j.Events) {
		if attempts >= budget {
			break
		}
		// A receipt means hk holds these bytes. A pin naming the same sha256
		// means the record already points at them, so there is nothing left to
		// publish even when the local receipt is absent.
		if publishedDocument(j.Events, rep.sha) != "" || receipts.pinSettled(rep.sha) {
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
			// The file no longer holds the announced bytes: it is still being
			// written, or a later round owns it. Only a new `done` announces
			// those bytes, so this pass leaves the file alone.
			s.reportKept(j.ID, rep.sha, rep.path, "report_changed")
			continue
		}
		attempts++
		doc, err := a.publishReport(ctx, j, path, body, rep.sha)
		if err != nil {
			// No receipt is written, so the next pass retries; this one moves on
			// to the next report instead of retrying here.
			s.reportKept(j.ID, rep.sha, rep.path, "publish_failed")
			continue
		}
		if err := a.event(ctx, j.ID, eventReportPublished, map[string]any{"report_sha256": rep.sha, "document": doc}); err != nil {
			s.reportKept(j.ID, rep.sha, rep.path, "receipt_failed")
			continue
		}
		s.reportRepublished(j.ID, rep.sha, rep.path, doc)
		// Step 3 of `done`: the completing report's document is pinned on the
		// terminal record, once. A record that never settled is left to a rerun
		// of `done`, which is the only writer of that step's precondition.
		if len(receipts.settled) == 0 || receipts.completing(rep.sha) != rep.sha || receipts.pinSettled(rep.sha) {
			continue
		}
		outcome, err := a.pinReport(ctx, j.ID, doc, rep.sha)
		if outcome == "" {
			s.reportKept(j.ID, rep.sha, rep.path, "pin_failed")
			continue
		}
		if err := a.event(ctx, j.ID, eventReportPinned, map[string]any{"report_sha256": rep.sha, "document": doc, "outcome": outcome}); err != nil {
			s.reportKept(j.ID, rep.sha, rep.path, "pin_receipt_failed")
			continue
		}
		receipts.pins[rep.sha] = outcome
	}
	return attempts
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
