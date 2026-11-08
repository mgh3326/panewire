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
	remove       bool
	reason, note string
	shape        jobDirShape
}

// jobDirShape is what a re-check compares to see that a dir did not change
// between the scan and the delete: its file count, bytes and newest mtime,
// the operation lock left out.
type jobDirShape struct {
	files  int
	bytes  int64
	newest time.Time
}

func (s jobDirShape) same(o jobDirShape) bool {
	return s.files == o.files && s.bytes == o.bytes && s.newest.Equal(o.newest)
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
		v := p.decide(ctx, a, root, id, panes)
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
		note := ""
		if v.note != "" {
			note = " note=" + v.note
		}
		fmt.Fprintf(out, "job=%s action=%s reason=%s bytes=%d%s\n", id, action, v.reason, v.shape.bytes, note)
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
	if _, err = os.Lstat(filepath.Join(path, "events")); err == nil {
		if j, err = a.job(id); err != nil {
			return keep("events-unreadable")
		}
		for _, ev := range j.Events {
			if ev.when.After(newest) {
				newest = ev.when
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
			return keep("not-a-job")
		case err != nil:
			return keep("submission-unreadable")
		case s.Job != id:
			return keep("submission-mismatch")
		}
		submitter, request = true, sub.RequestID
		j = Job{ID: id, Task: s.Task, Keep: s.Keep}
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
	if !submitter {
		if reason := panes.check(ctx, a, j); reason != "" {
			return keep(reason)
		}
	}
	rec, reason := a.jobDirRecord(ctx, id, submitter, request)
	if reason != "" {
		return keep(reason)
	}
	if reason, v.note = a.jobDirArtifacts(ctx, path, j, rec); reason != "" {
		v.note = ""
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
// the dir only when it still passes and did not change since the scan.
func (p *JobDirTTL) expire(ctx context.Context, a *App, root, id string, scan jobDirVerdict) jobDirVerdict {
	if p.beforeRecheck != nil {
		p.beforeRecheck(id)
	}
	if p.Exclusive != nil {
		defer p.Exclusive()()
	}
	kept := func(reason string) jobDirVerdict {
		scan.remove, scan.reason, scan.note = false, reason, ""
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
	if !again.shape.same(scan.shape) {
		return kept("changed-during-pass")
	}
	if reason := confineJobDir(root, path, id); reason != "" {
		return kept(reason)
	}
	// RemoveAll unlinks a symlink found inside the dir; it never follows one.
	if err = os.RemoveAll(dir); err != nil {
		slog.Warn("job dir expiry failed", "job", id, "reason", again.reason)
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

// jobDirPanes caches herdr's agent list per session for one scan.
type jobDirPanes map[string]jobDirAgents

type jobDirAgents struct {
	agents []Agent
	err    error
}

// check names why j's pane may still exist: herdr cannot be read
// (pane-unreadable), the job recorded neither a pane nor a label
// (pane-unknown), or herdr lists the recorded pane id or label
// (pane-live). The job's own herdr session is asked.
func (panes jobDirPanes) check(ctx context.Context, a *App, j Job) string {
	if j.Pane == "" && j.Label == "" {
		return "pane-unknown"
	}
	route := a.laneRoute(j)
	session := route.herdrSession()
	list, ok := panes[session]
	if !ok {
		list.agents, list.err = route.agents(ctx)
		panes[session] = list
	}
	if list.err != nil {
		return "pane-unreadable"
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

// jobDirArtifacts names the first artifact in dir hk does not confirm; note
// records an accepted cut pane screen.
func (a *App) jobDirArtifacts(ctx context.Context, dir string, j Job, rec map[string]any) (reason, note string) {
	if reason = a.jobDirReports(ctx, dir, j.ID, rec); reason != "" {
		return reason, ""
	}
	for _, name := range []string{"findings.json", "result.json"} {
		if _, err := os.Lstat(filepath.Join(dir, name)); err == nil && !jobRecordHasResult(rec) || err != nil && !errors.Is(err, fs.ErrNotExist) {
			return "result-unconfirmed", ""
		}
	}
	if reason, note = jobDirScreen(ctx, dir, j.ID); reason != "" {
		return reason, ""
	}
	if reason = a.jobDirEvidence(ctx, dir, j); reason != "" {
		return reason, ""
	}
	return "", note
}

// jobDirReports confirms each local report*.md: the record names a
// report_doc, and the local bytes' sha256 equals the record's report_sha256
// pin for that doc, else the sha256 hk holds for reports/<job>/<file>.
func (a *App) jobDirReports(ctx context.Context, dir, id string, rec map[string]any) string {
	reports, err := filepath.Glob(filepath.Join(dir, "report*.md"))
	if err != nil {
		return "report-unconfirmed"
	}
	if len(reports) == 0 {
		return ""
	}
	pinned := str(rec, "report_doc")
	if pinned == "" {
		return "report-unconfirmed"
	}
	for _, path := range reports {
		_, local, err := hashFile(path)
		if err != nil {
			return "report-unconfirmed"
		}
		key := "reports/" + id + "/" + filepath.Base(path)
		want := ""
		if key == pinned {
			want = str(rec, "report_sha256")
		}
		if want == "" {
			var reason string
			if want, reason = hkDocumentSHA(ctx, key, "report-unconfirmed"); reason != "" {
				return reason
			}
		}
		if !strings.EqualFold(want, local) {
			return "report-sha-mismatch"
		}
	}
	return ""
}

// jobDirScreen confirms a local pane-screen.txt against
// screens/<job>/pane-screen.txt. A screen over hk's document cap was stored
// cut (paneScreenDocBody), so for it the doc's existence is accepted and
// noted.
func jobDirScreen(ctx context.Context, dir, id string) (reason, note string) {
	path := filepath.Join(dir, PaneScreenFile)
	if _, err := os.Lstat(path); errors.Is(err, fs.ErrNotExist) {
		return "", ""
	} else if err != nil {
		return "screen-unconfirmed", ""
	}
	n, local, err := hashFile(path)
	if err != nil {
		return "screen-unconfirmed", ""
	}
	want, reason := hkDocumentSHA(ctx, PaneScreenDocKey(id), "screen-unconfirmed")
	if reason != "" {
		return reason, ""
	}
	if n > PaneScreenDocMaxBytes {
		return "", "screen-cut"
	}
	if !strings.EqualFold(want, local) {
		return "screen-sha-mismatch", ""
	}
	return "", ""
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

// jobDirEvidence confirms every evidence/** and final/** file in dir (and any
// file the artifact policy classes evidence or final-build) the way
// `artifacts sweep` would before deleting it: an artifact record of this job
// on this host lists it with the local bytes and sha256, and the archive is
// drive-confirmed (or uploaded, under that hatch).
func (a *App) jobDirEvidence(ctx context.Context, dir string, j Job) string {
	var pol ArtifactPolicy
	if decode(defaultArtifactPolicy, &pol) != nil {
		return "evidence-unconfirmed"
	}
	pending := map[string]bool{}
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
		if top == "evidence" || top == "final" || class == "evidence" || class == "final-build" {
			pending[path] = true
		}
		return nil
	})
	if err != nil {
		return "evidence-unconfirmed"
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
		src := rec.Archive.Source
		if !filepath.IsAbs(src) || !within(dir, src) {
			continue
		}
		for _, row := range rec.Archive.Contents {
			path := filepath.Join(src, row.Path)
			if filepath.IsAbs(row.Path) || !within(src, path) || !pending[path] {
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
