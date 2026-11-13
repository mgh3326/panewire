package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleetevent"
)

// The job dir TTL (hk 3624, docs/job-dir-ttl.md) deletes a job directory on
// this host once nothing can still need it: hk holds the job's end and every
// artifact the directory recorded, no pane of the job is left, and nothing
// has written to the directory for the TTL. mergewake's reap and the node's
// pane.capture read a missing job directory as "not a fleet job" and then
// never close its pane, so a directory whose pane may still exist is never
// deleted. Any read error or doubt keeps the directory.
//
// A pass is a dry run unless Apply is set, which only `panewire reap --apply
// --expire-job-dirs` and the node sweep loop under [reap] job_dir_ttl do.

const (
	// DefaultJobDirTTL is the TTL `reap --expire-job-dirs` uses when
	// [reap] job_dir_ttl is unset.
	DefaultJobDirTTL = 72 * time.Hour
	// MinJobDirTTL is the shortest TTL a pass runs with. It stays above the
	// 24 h relay hold ceiling, so no held relay row outlives its job dir.
	MinJobDirTTL = 24 * time.Hour
	// jobDirLock is the job's operation lock, the one `panewire done` holds
	// (lifecycle.go); the gate is decided again under it before a delete.
	jobDirLock = ".panewire-operation.lock"
	// jobDirTombstone starts the name a dir is renamed to right before it is
	// deleted: <root>/.expiring-<job>-<unixnano>. A leading dot is never a
	// valid job id, so nothing reads a tombstone as a job.
	jobDirTombstone = ".expiring-"
)

// ConfiguredJobDirTTL is [reap] job_dir_ttl; ok is false when it is unset.
// LoadConfig refuses a value jobDirTTLValue cannot read.
func ConfiguredJobDirTTL(c Config) (time.Duration, bool) {
	text, _ := c.Reap["job_dir_ttl"].(string)
	ttl := jobDirTTLValue(text)
	return ttl, ttl > 0
}

// jobDirTTLValue reads a job_dir_ttl value ("72h", "3d"); 0 when it does not
// parse or is under MinJobDirTTL.
func jobDirTTLValue(text string) time.Duration {
	d, err := duration(strings.TrimSpace(text))
	if err != nil || d < MinJobDirTTL {
		return 0
	}
	return d
}

// JobDirTTL is the job dir expiry pass over a.Jobs. The zero value with a
// TTL is a dry run.
type JobDirTTL struct {
	// TTL is how long nothing may have written to a dir; at least
	// MinJobDirTTL.
	TTL time.Duration
	// Apply deletes the dirs the gate lets go; false only lists them.
	Apply bool
	// Only limits the pass to these job ids; empty means every dir.
	Only []string
	// Out receives one line per dir and the summary; nil discards them.
	Out io.Writer
	// Exclusive, when set, is held around each locked re-check and delete
	// (the node's one-acting-command lock) and returns its release.
	Exclusive func() func()
	// Every skips a pass that comes sooner than this after the last one;
	// zero runs every pass.
	Every time.Duration
	last  time.Time
	// beforeRecheck runs between the scan and the locked re-check of a dir
	// the scan would delete; tests only.
	beforeRecheck func(job string)
	// beforeRename and afterRename run right before and right after the dir
	// is renamed to its tombstone; tests only.
	beforeRename func(job string)
	afterRename  func(job, tombstone string)
}

// JobDirTTLSummary counts one pass: dirs deleted (or, in a dry run, that
// would be), dirs kept, the bytes freed (or that would be) and the dirs per
// reason.
type JobDirTTLSummary struct {
	Delete, Keep int
	Bytes        int64
	Reasons      map[string]int
}

// jobDirVerdict is the gate's answer for one dir: delete or keep, why, and
// the dir's shape when it was read.
type jobDirVerdict struct {
	remove bool
	reason string
	shape  jobDirShape
}

// jobDirShape is the inventory a re-check compares to see that a dir did not
// change between the scan and the delete: its file count, bytes, newest
// mtime and the relative path of every entry, the operation lock left out.
type jobDirShape struct {
	files  int
	bytes  int64
	newest time.Time
	paths  []string
}

func (s jobDirShape) same(o jobDirShape) bool {
	return s.files == o.files && s.bytes == o.bytes && s.newest.Equal(o.newest) && slices.Equal(s.paths, o.paths)
}

// Pass decides every dir under a.Jobs, deletes those the gate lets go when
// Apply is set, and writes one line per dir and a summary to Out. Only an
// unreadable jobs root or a TTL under the floor is an error; a dir the gate
// cannot decide is kept.
func (p *JobDirTTL) Pass(ctx context.Context, a *App) (JobDirTTLSummary, error) {
	sum := JobDirTTLSummary{Reasons: map[string]int{}}
	if p.TTL < MinJobDirTTL {
		return sum, fail(exitConfig, "job dir TTL %s is under the %s floor", p.TTL, MinJobDirTTL)
	}
	now := a.Now()
	if p.Every > 0 && !p.last.IsZero() && now.Sub(p.last) < p.Every {
		return sum, nil
	}
	p.last = now
	entries, err := os.ReadDir(a.Jobs)
	if errors.Is(err, fs.ErrNotExist) {
		return sum, nil
	}
	if err != nil {
		return sum, err
	}
	root, err := filepath.EvalSymlinks(a.Jobs)
	if err != nil {
		return sum, err
	}
	out := p.Out
	if out == nil {
		out = io.Discard
	}
	panes := jobDirPanes{}
	for _, entry := range entries {
		if !entry.IsDir() && entry.Type()&fs.ModeSymlink == 0 {
			continue
		}
		id := entry.Name()
		if len(p.Only) > 0 && !slices.Contains(p.Only, id) {
			continue
		}
		var v jobDirVerdict
		if strings.HasPrefix(id, jobDirTombstone) {
			// A tombstone an earlier pass could not delete or rename back:
			// listed for the operator, never deleted or restored here.
			v.reason = "tombstone"
		} else {
			v = p.decide(ctx, a, root, id, panes)
		}
		if v.remove && p.Apply {
			v = p.expire(ctx, a, root, id, v)
		}
		action := "keep"
		if v.remove {
			action = "delete"
			sum.Delete++
			sum.Bytes += v.shape.bytes
		} else {
			sum.Keep++
		}
		sum.Reasons[v.reason]++
		fmt.Fprintf(out, "job=%s action=%s reason=%s bytes=%d\n", id, action, v.reason, v.shape.bytes)
	}
	var reasons []string
	for _, reason := range slices.Sorted(maps.Keys(sum.Reasons)) {
		reasons = append(reasons, fmt.Sprintf("%s:%d", reason, sum.Reasons[reason]))
	}
	fmt.Fprintf(out, "job-dir-ttl apply=%t ttl=%s delete=%d keep=%d bytes=%d reasons=%s\n", p.Apply, p.TTL, sum.Delete, sum.Keep, sum.Bytes, strings.Join(reasons, ","))
	slog.Info("job dir ttl pass", "apply", p.Apply, "delete", sum.Delete, "keep", sum.Keep, "bytes", sum.Bytes)
	return sum, nil
}

// decide runs the delete gate for the dir id under root. The first condition
// that fails names the reason to keep it.
func (p *JobDirTTL) decide(ctx context.Context, a *App, root, id string, panes jobDirPanes) jobDirVerdict {
	path := filepath.Join(a.Jobs, id)
	if reason := confineJobDir(root, path, id); reason != "" {
		return jobDirVerdict{reason: reason}
	}
	if !validID(id) {
		return jobDirVerdict{reason: "invalid-id"}
	}
	shape, err := jobDirShapeOf(path)
	v := jobDirVerdict{shape: shape}
	keep := func(reason string) jobDirVerdict {
		v.reason = reason
		return v
	}
	if err != nil {
		return keep("unreadable")
	}
	newest := shape.newest
	var j Job
	submitter, request := false, ""
	// leftover is a dir with neither events/ nor a submission whose job
	// ledger is gone: it is kept unless hk holds a terminal record (hk 3685).
	leftover := false
	// recorded holds every report path the receipt, a completion or the
	// submission named, as recorded; jobDirReports confirms those in the dir.
	var recorded []any
	if _, err = os.Lstat(filepath.Join(path, "events")); err == nil {
		if j, err = a.job(id); err != nil {
			return keep("events-unreadable")
		}
		if _, pending := pendingCreditRejection(j); pending {
			return keep("credit-publication-pending")
		}
		for _, ev := range j.Events {
			if ev.when.After(newest) {
				newest = ev.when
			}
			if report, ok := ev.Payload["report"]; ok {
				recorded = append(recorded, report)
			}
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return keep("unreadable")
	} else {
		// A submitter-side dir: the job ran on another node, so its events
		// and pane live there (finding 3 of hk 3526 unit 6).
		sub, s, err := readSubmissionSpawn(path)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			// Neither events/ nor a submission: the job's ledger is gone, not
			// the job. hk decides at the record check below.
			leftover = true
			j = Job{ID: id}
		case err != nil:
			return keep("submission-unreadable")
		case s.Job != id:
			return keep("submission-mismatch")
		default:
			submitter, request = true, sub.RequestID
			j = Job{ID: id, Task: s.Task, Keep: s.Keep}
			recorded = append(recorded, s.Report)
			if s.ReportRelative != "" {
				// Relative to the jobs root of the node that ran it; the same
				// layout here names this dir. Joined as written, never through
				// filepath.Join: cleaning would turn link/../summary.md into
				// summary.md before jobDirReportFiles sees it. A value that is
				// absolute or not clean (a . or .. segment, a doubled or
				// trailing separator) is not clean once joined either, so it
				// keeps the dir there as report-path-unreadable, at its place
				// in the gate order.
				jobs := strings.TrimSuffix(filepath.Clean(a.Jobs), "/")
				recorded = append(recorded, jobs+"/"+s.ReportRelative)
			}
		}
	}
	if j.Keep {
		return keep("protected")
	}
	if a.watchActive(id) {
		return keep("watcher-active")
	}
	if a.Now().Sub(newest) < p.TTL {
		return keep("too-young")
	}
	// A leftover has no receipt, so its pane is named only by the hk job
	// record (pane_id, label), when the record holds them (hk 3700). The record
	// is read first. A leftover whose record names no pane keeps the dir as
	// pane-unknown, because absence cannot be shown.
	var rec map[string]any
	var reason string
	if leftover {
		rec, reason = a.jobDirRecord(ctx, id, false, "")
		if reason == "hk-missing" {
			// hk holds no record for this dir: it was never a job.
			reason = "not-a-job"
		}
		if reason != "" {
			return keep(reason)
		}
		j.Pane, j.Label = str(rec, "pane_id"), str(rec, "label")
		// The job ran on this node, so its pane is in this node's herdr session.
		j.Receipt = map[string]any{"herdr_session": a.herdrSession()}
	}
	if !submitter {
		if pane := panes.check(ctx, a, j); pane != "" {
			return keep(pane)
		}
	}
	if !leftover {
		rec, reason = a.jobDirRecord(ctx, id, submitter, request)
		if reason != "" {
			return keep(reason)
		}
	}
	if reason = a.jobDirArtifacts(ctx, root, path, j, rec, recorded); reason != "" {
		return keep(reason)
	}
	v.remove, v.reason = true, "confirmed"
	if submitter {
		v.reason = "submission-terminal"
		if rec == nil {
			v.reason = "submission-spawn-failed"
		}
	}
	return v
}

// expire takes the job's operation lock, decides the gate again and deletes
// the dir only when it still passes and did not change since the scan: it
// renames the dir to a tombstone, checks the tombstone holds what the dir
// held, and only then removes it; a changed tombstone is renamed back.
func (p *JobDirTTL) expire(ctx context.Context, a *App, root, id string, scan jobDirVerdict) jobDirVerdict {
	if p.beforeRecheck != nil {
		p.beforeRecheck(id)
	}
	if p.Exclusive != nil {
		defer p.Exclusive()()
	}
	kept := func(reason string) jobDirVerdict {
		scan.remove, scan.reason = false, reason
		return scan
	}
	path := filepath.Join(a.Jobs, id)
	// Never open the lock through a dir swapped for a symlink.
	if reason := confineJobDir(root, path, id); reason != "" {
		return kept(reason)
	}
	dir := filepath.Join(root, id)
	lock, err := secureOpen(filepath.Join(dir, jobDirLock), true, a.warn)
	if err != nil {
		return kept("lock-unavailable")
	}
	defer lock.Close()
	if !tryLock(lock) {
		return kept("locked")
	}
	again := p.decide(ctx, a, root, id, jobDirPanes{})
	if !again.remove {
		return again
	}
	// The gate's herdr and hk reads are all done. An inventory taken now
	// sees a write that landed while they ran; the one decide took came
	// before them.
	before, err := jobDirShapeOf(dir)
	if err != nil {
		return kept("unreadable")
	}
	if !before.same(scan.shape) {
		return kept("changed-during-pass")
	}
	if reason := confineJobDir(root, path, id); reason != "" {
		return kept(reason)
	}
	// The dir leaves the job namespace in one atomic rename to a tombstone
	// in the same root. A writer that wrote before it shows in the
	// tombstone's inventory. One that runs MkdirAll under the job's name
	// after it makes a fresh dir there, which this pass does not delete: a
	// later pass judges it like any other dir. If the tombstone changed, any
	// dir under the job's name, empty or not, makes the rename back fail
	// (os.Rename refuses an existing directory target) and the tombstone
	// stays. A writer holding an fd opened before the rename can still
	// write unseen (docs/job-dir-ttl.md).
	tomb := filepath.Join(root, fmt.Sprintf("%s%s-%d", jobDirTombstone, id, a.Now().UnixNano()))
	if _, err = os.Lstat(tomb); !errors.Is(err, fs.ErrNotExist) {
		return kept("tombstone-busy")
	}
	if p.beforeRename != nil {
		p.beforeRename(id)
	}
	if err = os.Rename(dir, tomb); err != nil {
		return kept("delete-failed")
	}
	if p.afterRename != nil {
		p.afterRename(id, tomb)
	}
	if after, err := jobDirShapeOf(tomb); err != nil || !after.same(before) {
		if err = os.Rename(tomb, dir); err != nil {
			// Never deleted, never retried: the next pass lists it as a
			// tombstone for the operator.
			slog.Error("job dir tombstone left", "job", id, "tombstone", filepath.Base(tomb))
			a.fleetEventWriter().EmitEvent(fleetevent.Event{Kind: "job_dir_expired", Job: id, Outcome: "tombstone_left", Reason: "rename-back-failed", Detail: "tombstone=" + filepath.Base(tomb)})
			return kept("tombstone-left")
		}
		return kept("changed-during-pass")
	}
	// RemoveAll unlinks a symlink found inside the dir; it never follows one.
	if err = os.RemoveAll(tomb); err != nil {
		slog.Error("job dir expiry failed", "job", id, "reason", again.reason, "tombstone", filepath.Base(tomb))
		a.fleetEventWriter().EmitEvent(fleetevent.Event{Kind: "job_dir_expired", Job: id, Outcome: "tombstone_left", Reason: "delete-failed", Detail: "tombstone=" + filepath.Base(tomb)})
		return kept("delete-failed")
	}
	slog.Info("job dir expired", "job", id, "reason", again.reason, "bytes", again.shape.bytes)
	// The record goes to the state dir, never into the dir just deleted.
	a.fleetEventWriter().EmitEvent(fleetevent.Event{Kind: "job_dir_expired", Job: id, Outcome: "deleted", Reason: again.reason, Detail: fmt.Sprintf("bytes=%d", again.shape.bytes)})
	return again
}

// confineJobDir names why the jobs-root entry path (named id) must not be
// touched: its real path leaves the root (outside-root), or it is a symlink
// or not the directory root/id itself (symlink). "" for a plain directory
// directly under the root.
func confineJobDir(root, path, id string) string {
	info, err := os.Lstat(path)
	if err != nil {
		return "unreadable"
	}
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		if info.Mode()&fs.ModeSymlink != 0 {
			return "symlink"
		}
		return "unreadable"
	}
	if !within(root, real) {
		return "outside-root"
	}
	if info.Mode()&fs.ModeSymlink != 0 || !info.IsDir() || real != filepath.Join(root, id) {
		return "symlink"
	}
	return ""
}

// jobDirShapeOf walks dir without following a symlink.
func jobDirShapeOf(dir string) (jobDirShape, error) {
	var s jobDirShape
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == dir || filepath.Dir(path) == dir && entry.Name() == jobDirLock {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.ModTime().After(s.newest) {
			s.newest = info.ModTime()
		}
		if !entry.IsDir() {
			s.files++
			s.bytes += info.Size()
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		s.paths = append(s.paths, filepath.ToSlash(rel))
		return nil
	})
	return s, err
}

// readSubmissionSpawn reads a submitter-side dir's submission.json and the
// spawn its arguments name. A missing receipt is fs.ErrNotExist.
func readSubmissionSpawn(dir string) (Submission, Spawn, error) {
	path := filepath.Join(dir, "submission.json")
	info, err := os.Lstat(path)
	if err != nil {
		return Submission{}, Spawn{}, err
	}
	if !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return Submission{}, Spawn{}, errors.New("submission receipt is not a regular file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Submission{}, Spawn{}, err
	}
	var sub Submission
	if err = json.Unmarshal(raw, &sub); err != nil {
		return Submission{}, Spawn{}, err
	}
	s, err := parseSpawn(sub.Args)
	return sub, s, err
}

// jobDirPanes caches herdr's pane inventory per session for one scan.
type jobDirPanes map[string]jobDirInventory

// jobDirInventory is one session's `herdr pane list`, which names plain
// shells as well as agents, and its `herdr agent list`.
type jobDirInventory struct {
	panes  []Pane
	agents []Agent
	err    error
}

// check names why j's pane may still exist: the job recorded neither a pane
// nor a label (pane-unknown), its receipt names no herdr session
// (pane-session-unknown, a receipt from before the session was recorded),
// herdr cannot be read (pane-unreadable), or the pane inventory or agent
// list of the recorded session names the recorded pane id or label
// (pane-live). The agent list alone never proves a pane gone: a shell pane
// whose agent exited is only in the pane list.
func (panes jobDirPanes) check(ctx context.Context, a *App, j Job) string {
	if j.Pane == "" && j.Label == "" {
		return "pane-unknown"
	}
	session := str(j.Receipt, "herdr_session")
	if session == "" {
		return "pane-session-unknown"
	}
	list, ok := panes[session]
	if !ok {
		route := a.laneRoute(j)
		if list.panes, list.err = route.panes(ctx); list.err == nil {
			list.agents, list.err = route.agents(ctx)
		}
		panes[session] = list
	}
	if list.err != nil {
		return "pane-unreadable"
	}
	for _, pane := range list.panes {
		if j.Pane != "" && pane.Pane == j.Pane || j.Label != "" && (pane.Label == j.Label || pane.Name == j.Label) {
			return "pane-live"
		}
	}
	for _, ag := range list.agents {
		if j.Pane != "" && ag.Pane == j.Pane || j.Label != "" && ag.Name == j.Label {
			return "pane-live"
		}
	}
	return ""
}

// spawnStatusReader is a hub transport that can read one spawn operation's
// status (hubHTTP.SpawnStatus).
type spawnStatusReader interface {
	SpawnStatus(ctx context.Context, id string) (string, error)
}

// jobDirRecord reads the hk job record and names why it does not let the
// dir go: unreadable, missing, or not completed or lost (escalated stays).
// A submitter-side dir whose job hk never held may still go when the hub
// spawn operation failed; the record is then nil.
func (a *App) jobDirRecord(ctx context.Context, id string, submitter bool, request string) (map[string]any, string) {
	store, err := a.jobStore()
	if err != nil {
		return nil, "hk-unreadable"
	}
	rec, err := store.Get(ctx, id)
	if errors.Is(err, errJobNotFound) {
		if !submitter {
			return nil, "hk-missing"
		}
		status, err := a.spawnStatus(ctx, request)
		if err != nil {
			return nil, "spawn-status-unreadable"
		}
		if status != "failed" {
			return nil, "hk-missing"
		}
		return nil, ""
	}
	if err != nil || rec == nil {
		return nil, "hk-unreadable"
	}
	if got := str(rec, "job_id"); got != "" && got != id {
		return nil, "hk-unreadable"
	}
	if state := str(rec, "state"); state != "completed" && state != "lost" {
		return nil, "hk-not-terminal"
	}
	return rec, ""
}

func (a *App) spawnStatus(ctx context.Context, request string) (string, error) {
	if request == "" {
		return "", errors.New("submission names no request")
	}
	hub, err := a.hub()
	if err != nil {
		return "", err
	}
	reader, ok := hub.(spawnStatusReader)
	if !ok {
		return "", errors.New("hub transport cannot read a spawn status")
	}
	return reader.SpawnStatus(ctx, request)
}

// jobDirArtifacts names the first artifact in dir (root/<job>) hk does not
// confirm; recorded are the report paths the job recorded. A leftover
// pane-screen.txt is not confirmed here: it is an ordinary file of the dir.
func (a *App) jobDirArtifacts(ctx context.Context, root, dir string, j Job, rec map[string]any, recorded []any) string {
	reports, reason := jobDirReportFiles(root, dir, j.ID, recorded)
	if reason != "" {
		return reason
	}
	if reason = jobDirReports(ctx, reports, j.ID); reason != "" {
		return reason
	}
	for _, name := range []string{"findings.json", "result.json"} {
		if _, err := os.Lstat(filepath.Join(dir, name)); err == nil && !jobRecordHasResult(rec) || err != nil && !errors.Is(err, fs.ErrNotExist) {
			return "result-unconfirmed"
		}
	}
	return a.jobDirEvidence(ctx, dir, j)
}

// jobDirReportFiles lists the report files in dir (named id, under the real
// jobs root) that hk must confirm: every top-level report*.md and each
// recorded report path that lies in the dir and exists. A recorded path is
// judged by the file it really names, so any doubt keeps the dir
// (report-path-unreadable): a path that is not absolute or that
// filepath.Clean would change (a .. after a symlink names another file
// than the cleaned path), or one a symlink resolves to another path. Only
// the resolved path is tested against the dir; one outside it is not this
// pass's to delete and is skipped.
func jobDirReportFiles(root, dir, id string, recorded []any) ([]string, string) {
	real := filepath.Join(root, id)
	reports, err := filepath.Glob(filepath.Join(real, "report*.md"))
	if err != nil {
		return nil, "report-unconfirmed"
	}
	for _, value := range recorded {
		if value == nil {
			continue
		}
		text, ok := value.(string)
		if !ok || strings.ContainsRune(text, 0) || text != "" && (!filepath.IsAbs(text) || text != filepath.Clean(text)) {
			return nil, "report-path-unreadable"
		}
		if text == "" {
			continue
		}
		// The pass resolved the jobs root itself (Pass), so a path through
		// its alias is read under the real root.
		lexical := text
		if jobs := filepath.Dir(dir); within(jobs, text) {
			rel, _ := filepath.Rel(jobs, text)
			lexical = filepath.Join(root, rel)
		}
		resolved, err := resolveExisting(text)
		if err != nil || resolved != lexical && resolved != text {
			return nil, "report-path-unreadable"
		}
		if !within(real, resolved) {
			continue
		}
		if _, err := os.Lstat(resolved); errors.Is(err, fs.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, "report-unconfirmed"
		}
		if !slices.Contains(reports, resolved) {
			reports = append(reports, resolved)
		}
	}
	return reports, ""
}

// resolveExisting is filepath.EvalSymlinks of path, or for a path that does
// not exist, of its nearest existing parent joined with the rest. A path
// component that exists but cannot be resolved (a dangling symlink, an
// unreadable dir) is an error.
func resolveExisting(path string) (string, error) {
	rest := ""
	for cur := path; ; {
		resolved, err := filepath.EvalSymlinks(cur)
		if err == nil {
			return filepath.Join(resolved, rest), nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		if _, err := os.Lstat(cur); !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("resolve %s: %w", cur, fs.ErrInvalid)
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return "", err
		}
		rest = filepath.Join(filepath.Base(cur), rest)
		cur = parent
	}
}

// jobDirReports confirms each local report file by its own bytes: the key
// `panewire done` publishes them to is content-addressed
// (reportDocumentKey), so hk confirms a report when it holds a document at
// that key with the file's sha256. A report published before keys were
// content-addressed is confirmed at the legacy key reports/<job>/<basename>
// instead. The job record's pin and the job's events play no part: a report
// published but never pinned is still in hk.
func jobDirReports(ctx context.Context, reports []string, id string) string {
	for _, path := range reports {
		_, local, err := hashFile(path)
		if err != nil {
			return "report-unconfirmed"
		}
		want, reason := hkDocumentSHA(ctx, reportDocumentKey(id, local), "report-unconfirmed")
		if reason == "report-unconfirmed" {
			want, reason = hkDocumentSHA(ctx, legacyReportDocumentKey(id, path), "report-unconfirmed")
		}
		if reason != "" {
			return reason
		}
		if !strings.EqualFold(want, local) {
			return "report-sha-mismatch"
		}
	}
	return ""
}

// hkDocumentSHA is the sha256 hk holds for document key; missing is the
// reason when hk has no such document, hk-unreadable when hk cannot be read
// or its answer does not name the key and a sha256.
func hkDocumentSHA(ctx context.Context, key, missing string) (string, string) {
	doc, err := hkGetDocument(ctx, key)
	switch {
	case errors.Is(err, errHKNotFound):
		return "", missing
	case err != nil || doc.Key != key || doc.SHA256 == "":
		return "", "hk-unreadable"
	}
	return doc.SHA256, ""
}

// jobRecordHasResult reports whether the record's result is non-empty.
func jobRecordHasResult(rec map[string]any) bool {
	switch v := rec["result"].(type) {
	case nil:
		return false
	case string:
		v = strings.TrimSpace(v)
		return v != "" && v != "null" && v != "{}"
	case map[string]any:
		return len(v) > 0
	case []any:
		return len(v) > 0
	}
	return true
}

// jobDirEvidence confirms every file in dir the artifact policy classes
// evidence or final-build the way `artifacts sweep` would before deleting
// it: an artifact record of this job on this host lists it with the local
// bytes and sha256, and the archive is drive-confirmed (or uploaded, under
// that hatch, for a final build no mirror needs). A file under evidence/ or
// final/ the policy gives no such class keeps the dir
// (evidence-unclassified): its mirror requirement is unknown.
func (a *App) jobDirEvidence(ctx context.Context, dir string, j Job) string {
	var pol ArtifactPolicy
	if decode(defaultArtifactPolicy, &pol) != nil {
		return "evidence-unconfirmed"
	}
	pending, unclassified := map[string]bool{}, false
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		class, _, err := a.artifactClass(pol, j.ID, path)
		if err != nil {
			return err
		}
		top, _, _ := strings.Cut(filepath.ToSlash(rel), "/")
		if class == "evidence" || class == "final-build" {
			pending[path] = true
		} else if top == "evidence" || top == "final" {
			// No policy row classes it (a jobs root the policy's root
			// does not match), so neither the mirror requirement nor the
			// uploaded hatch can be decided for it.
			unclassified = true
		}
		return nil
	})
	if err != nil {
		return "evidence-unconfirmed"
	}
	if unclassified {
		return "evidence-unclassified"
	}
	if len(pending) == 0 {
		return ""
	}
	hatch, err := a.artifactDeleteHatch()
	if err != nil {
		return "evidence-unconfirmed"
	}
	if _, err = hkTaskID(j.Task); err != nil {
		return "evidence-unconfirmed"
	}
	body, err := hkTaskBody(ctx, j.Task)
	if errors.Is(err, errHKNotFound) {
		return "evidence-unconfirmed"
	}
	if err != nil {
		return "hk-unreadable"
	}
	var task struct{ Project string }
	if json.Unmarshal(body, &task) != nil {
		return "hk-unreadable"
	}
	if task.Project == "" {
		return "evidence-unconfirmed"
	}
	prefix := "artifacts/" + task.Project + "/" + j.Task + "/" + j.ID
	if body, err = hkDocumentListBody(ctx, prefix); err != nil {
		return "hk-unreadable"
	}
	var listing struct {
		Documents []struct{ Key string } `json:"documents"`
	}
	if json.Unmarshal(body, &listing) != nil {
		return "hk-unreadable"
	}
	for _, doc := range listing.Documents {
		// The prefix is a LIKE pattern: a _ or % in it matches more.
		if doc.Key != prefix && !strings.HasPrefix(doc.Key, prefix+"/") {
			continue
		}
		answer, err := hkDocumentBody(ctx, doc.Key)
		if err != nil {
			return "hk-unreadable"
		}
		rec, ok := a.jobArtifactRecord(answer, j)
		if !ok || rec.Archive.SourceHost != "" && rec.Archive.SourceHost != a.machine() {
			continue
		}
		// The walk names real paths, so the record's paths are resolved
		// before the join: a ./ or .. after a symlink, a symlinked
		// component, or an alias of the jobs root names the file the walk
		// sees, which a lexical filepath.Join would not (hk 3643).
		src := rec.Archive.Source
		if !filepath.IsAbs(src) {
			continue
		}
		if src, err = resolveExisting(src); err != nil || !within(dir, src) {
			continue
		}
		for _, row := range rec.Archive.Contents {
			if filepath.IsAbs(row.Path) {
				continue
			}
			path, err := resolveExisting(src + string(filepath.Separator) + row.Path)
			if err != nil || !within(src, path) || !pending[path] {
				continue
			}
			class, mirror, err := a.artifactClass(pol, j.ID, path)
			if err != nil {
				return "evidence-unconfirmed"
			}
			if !artifactArchived(rec, row, class, row.Mirror || mirror, hatch) {
				continue
			}
			if n, h, err := hashFile(path); err == nil && n == row.Bytes && h == row.SHA {
				delete(pending, path)
			}
		}
	}
	if len(pending) > 0 {
		return "evidence-unconfirmed"
	}
	return ""
}
