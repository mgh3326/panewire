package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"time"
)

// laneEventMaxBytes bounds one events-lane record, as the relay scanner does.
const laneEventMaxBytes = 16 << 10

// laneTerminalMarker is the head of a lane event that ends a job. The optional
// [label] prefix is the form most workers use; a marker later in the text, such
// as "READY withheld" inside a progress note, does not count.
var laneTerminalMarker = regexp.MustCompile(`^(\[[^\]]*\]\s*)?(READY|DONE|ESC)\b`)

func (a *App) startWatch(job string) error {
	// Unit fakes never spawn real processes.
	if _, ok := a.Runner.(ExecRunner); !ok {
		return nil
	}
	exe, e := os.Executable()
	if e != nil {
		return e
	}
	dir := filepath.Join(a.Jobs, job)
	if e = os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	log, e := os.OpenFile(filepath.Join(dir, "panewire-watch.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer log.Close()
	cmd := exec.Command(exe, "_watch", job)
	cmd.Env = append(os.Environ(), "HERDR_SESSION="+a.herdrSession())
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.Stdin = nil
	if e = cmd.Start(); e != nil {
		return e
	}
	return cmd.Process.Release()
}
func (a *App) watch(ctx context.Context, args []string) error {
	if len(args) != 1 || !validID(args[0]) {
		return fail(exitUsage, "invalid watch job")
	}
	id := args[0]
	lock, e := os.OpenFile(filepath.Join(a.Jobs, id, ".panewire-watch.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if !tryLock(lock) {
		return nil
	}
	timeout := time.Hour
	if v := a.envOr("PANEWIRE_COMPLETION_TIMEOUT_S", "WRK_COMPLETION_TIMEOUT_S"); v != "" {
		d, ok := timeoutValue(v)
		if !ok {
			return fail(exitUsage, "invalid completion timeout")
		}
		timeout = d
	}
	deadline := a.Now().Add(timeout)
	var size int64 = -1
	var modified time.Time
	pending := false
	missing := 0
	latePoll := false
	for {
		j, e := a.job(id)
		if e != nil {
			return e
		}
		if j.Terminal || j.Reaped {
			return nil
		}
		if j.Report != "" {
			if info, e := os.Stat(j.Report); e == nil && info.Mode().IsRegular() {
				if pending && size == info.Size() && modified.Equal(info.ModTime()) && a.workerSignalledEnd(j) {
					return a.lifecycleWithWatchResult(ctx, "done", []string{id, "--report", j.Report}, true)
				}
				pending = true
				size = info.Size()
				modified = info.ModTime()
			} else {
				pending = false
			}
		}
		if a.Now().After(deadline) && (!pending || latePoll) {
			fmt.Fprintln(a.Err, "watch-expired: job stays live; quota retained")
			return nil
		}
		if a.Now().After(deadline) {
			latePoll = true
		}
		agents, e := a.agents(ctx)
		if e != nil {
			return e
		}
		present := false
		for _, ag := range agents {
			if ag.Pane == j.Pane {
				present = true
			}
		}
		if !present {
			missing++
		} else {
			missing = 0
		}
		if missing >= 2 {
			if e := a.putJobRecord(ctx, id, map[string]any{"state": "lost"}); e != nil {
				a.jobRecordFailed(ctx, id, "lost", e)
			}
			if e = a.event(ctx, id, "job.lost", map[string]any{"pane_id": j.Pane}); e != nil {
				return e
			}
			return a.releaseQuota(ctx, j)
		}
		if e = a.Sleep(ctx, 5*time.Second); e != nil {
			return e
		}
	}
}

// workerSignalledEnd reports whether the current attempt recorded an explicit
// end: a job.escalate record, or a READY, DONE or ESC lane event from its pane.
// A report file is never a signal, because a worker can write one early and keep
// working. A job.completed record needs no check here: it makes the job Terminal.
func (a *App) workerSignalledEnd(j Job) bool {
	var since time.Time
	escalated := false
	for _, ev := range j.Events {
		switch ev.Kind {
		case "job.spawned", "job.reclaim", "job.reprompted":
			since, escalated = ev.when, false
		case "job.escalate":
			escalated = true
		}
	}
	return escalated || a.paneEmittedMarker(j.Pane, since)
}

// paneEmittedMarker scans events-lane for a terminal marker the pane emitted at
// or after since. Lane events carry no job id, so the pane that `panewire emit`
// stamps is what ties an event to a job. An unreadable record is not a signal.
func (a *App) paneEmittedMarker(pane string, since time.Time) bool {
	if pane == "" {
		return false
	}
	dir := filepath.Join(filepath.Dir(a.Jobs), "events-lane")
	entries, e := os.ReadDir(dir)
	if e != nil {
		return false
	}
	// Records are stamped in whole seconds, so compare at that resolution.
	since = since.Truncate(time.Second)
	for _, entry := range entries {
		info, e := entry.Info()
		if e != nil || !info.Mode().IsRegular() || filepath.Ext(entry.Name()) != ".json" || info.Size() > laneEventMaxBytes || info.ModTime().Before(since) {
			continue
		}
		body, e := os.ReadFile(filepath.Join(dir, entry.Name()))
		if e != nil {
			continue
		}
		var record struct {
			Kind string `json:"kind"`
			Pane string `json:"pane_id"`
			Text string `json:"text"`
		}
		if json.Unmarshal(body, &record) == nil && record.Kind == "lane.event" && record.Pane == pane && laneTerminalMarker.MatchString(record.Text) {
			return true
		}
	}
	return false
}
