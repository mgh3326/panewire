package panewire

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// indexFixture is an inbox shaped like a long-running node's: mostly ended jobs
// older than every window, a few live ones, and the odd record the scanners
// must skip or special-case. Its directories are back-dated, as a quiet node's
// are, so an index may trust what it read.
type indexFixture struct {
	t     testing.TB
	root  string
	now   time.Time
	panes []string
	jobs  []string
}

const (
	indexFixtureOld    = 10 * 24 * time.Hour
	indexFixtureSettle = time.Hour
)

func newIndexFixture(t testing.TB, jobCount int) *indexFixture {
	t.Helper()
	f := &indexFixture{t: t, root: t.TempDir(), now: time.Now()}
	if err := os.MkdirAll(filepath.Join(f.root, "jobs"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(f.root, "events-lane"), 0o700); err != nil {
		t.Fatal(err)
	}
	for i := range jobCount {
		f.writeJob(i)
	}
	for i := range jobCount / 5 {
		f.writeLaneEvent(i, i%3 == 0)
	}
	f.settle()
	return f
}

func (f *indexFixture) stamp(age time.Duration) string {
	return f.now.Add(-age).UTC().Format(time.RFC3339)
}

// put writes one event file with the given age as its mtime.
func (f *indexFixture) put(job string, seq int, kind, body string, age time.Duration) {
	f.t.Helper()
	dir := filepath.Join(f.root, "jobs", job, "events")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		f.t.Fatal(err)
	}
	path := filepath.Join(dir, fmt.Sprintf("%05d-%s.json", seq, kind))
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		f.t.Fatal(err)
	}
	at := f.now.Add(-age)
	if err := os.Chtimes(path, at, at); err != nil {
		f.t.Fatal(err)
	}
}

func (f *indexFixture) claim(i int, epoch uint64, age time.Duration, pane string) string {
	payload := ""
	if pane != "" {
		payload = fmt.Sprintf(`,"payload":{"pane_id":%q,"herdr_session":"sess-%d"}`, pane, i%7)
	}
	return fmt.Sprintf(`{"type":"job.claimed","created_at":%q,"agent_label":"agent-%d","owner_lane":"lane-%d","epoch":%d,"t_level":"T2","role":"implementer","seq":1%s}`, f.stamp(age), i, i%9, epoch, payload)
}

func (f *indexFixture) spawned(i int, pane string, age time.Duration) string {
	return fmt.Sprintf(`{"type":"job.spawned","created_at":%q,"seq":2,"payload":{"pane_id":%q,"herdr_session":"sess-%d","label":"agent-%d"}}`, f.stamp(age), pane, i%7, i)
}

func (f *indexFixture) completed(i int, age time.Duration) string {
	return fmt.Sprintf(`{"type":"job.completed","created_at":%q,"agent_label":"agent-%d","owner_lane":"lane-%d","epoch":1,"label":"agent-%d","host":"host-a","report_path":"/tmp/report-%d.md","report_last_line":"VERDICT: DONE"}`, f.stamp(age), i, i%9, i, i)
}

// writeJob writes job i; the shape depends on i, so a fixture of any size has
// every shape in the same proportions.
func (f *indexFixture) writeJob(i int) {
	id := fmt.Sprintf("job-20260101-%04d", i)
	f.jobs = append(f.jobs, id)
	pane := fmt.Sprintf("w1:p%d", i)
	switch {
	case i%50 == 1: // two jobs, one pane: the later live one must win the pane lookup
		pane = "w1:pshared"
		f.put(id, 1, "job.claimed", f.claim(i, 1, 30*time.Minute, pane), 30*time.Minute)
		f.put(id, 2, "job.spawned", f.spawned(i, pane, 30*time.Minute), 30*time.Minute)
		f.panes = append(f.panes, pane)
	case i%20 == 2: // live with a pane
		f.put(id, 1, "job.claimed", f.claim(i, 1, 40*time.Minute, ""), 40*time.Minute)
		f.put(id, 2, "job.spawned", f.spawned(i, pane, 40*time.Minute), 40*time.Minute)
		f.put(id, 3, "job.progress", fmt.Sprintf(`{"type":"job.progress","created_at":%q}`, f.stamp(5*time.Minute)), 5*time.Minute)
		f.panes = append(f.panes, pane)
	case i%20 == 3: // live, no pane
		f.put(id, 1, "job.claimed", f.claim(i, 1, 20*time.Minute, ""), 20*time.Minute)
	case i%40 == 4: // no pane, silent past the stale age but inside the active age
		f.put(id, 1, "job.claimed", f.claim(i, 1, 30*time.Hour, ""), 30*time.Hour)
	case i%20 == 5: // ended a minute ago
		f.put(id, 1, "job.claimed", f.claim(i, 1, 2*time.Hour, ""), 2*time.Hour)
		f.put(id, 2, "job.spawned", f.spawned(i, pane, 2*time.Hour), 2*time.Hour)
		f.put(id, 3, "job.completed", f.completed(i, time.Minute), time.Minute)
		f.panes = append(f.panes, pane)
	case i%25 == 6: // lifecycle envelope, flat completion missing
		f.put(id, 1, "job.claimed", f.claim(i, 1, time.Hour, ""), time.Hour)
		f.put(id, 2, "job.completed", fmt.Sprintf(`{"created_at":%q,"job_id":%q,"kind":"job.completed","payload":{"agent_label":"agent-%d","owner_lane":"job-x","parent_lane":"lane-%d","role":"builder","report":"/tmp/envelope-%d.md"},"seq":2}`, f.stamp(10*time.Minute), id, i, i%9, i), 10*time.Minute)
	case i%25 == 7: // lifecycle envelope plus its flat completion
		f.put(id, 1, "job.claimed", f.claim(i, 1, time.Hour, ""), time.Hour)
		f.put(id, 2, "job.completed", fmt.Sprintf(`{"created_at":%q,"job_id":%q,"kind":"job.completed","payload":{"agent_label":"agent-%d","owner_lane":"job-x","parent_lane":"lane-%d","report":"/tmp/envelope-%d.md"},"seq":2}`, f.stamp(10*time.Minute), id, i, i%9, i), 10*time.Minute)
		f.put(id, 3, "job.completed", f.completed(i, 9*time.Minute), 9*time.Minute)
	case i%30 == 8: // escalation with a question and no report path
		f.put(id, 1, "job.claimed", f.claim(i, 1, 3*time.Hour, ""), 3*time.Hour)
		f.put(id, 2, "job.escalate", fmt.Sprintf(`{"type":"job.escalate","created_at":%q,"owner_lane":"lane-%d","epoch":1,"reason":"stuck","question":"which way?","pr":"https://example.test/pr/%d"}`, f.stamp(2*time.Hour), i%9, i), 2*time.Hour)
	case i%30 == 9: // quota stop with a pool
		f.put(id, 1, "job.claimed", f.claim(i, 1, 3*time.Hour, ""), 3*time.Hour)
		f.put(id, 2, "job.lost", fmt.Sprintf(`{"type":"job.lost","created_at":%q,"owner_lane":"lane-%d","epoch":1,"reason":"quota_stop","payload":{"pool":"pool-%d"}}`, f.stamp(time.Hour), i%9, i%3), time.Hour)
	case i%30 == 10: // records the scanners skip: corrupt, oversize, undated (mtime is the time)
		f.put(id, 1, "job.claimed", f.claim(i, 1, 5*time.Hour, ""), 5*time.Hour)
		f.put(id, 2, "job.progress", `{not json`, 4*time.Hour)
		f.put(id, 3, "job.progress", `{"type":"job.progress","question":"`+strings.Repeat("x", 17<<10)+`"}`, 3*time.Hour)
		f.put(id, 4, "job.progress", `{"type":"job.progress","created_at":"yesterday"}`, 2*time.Hour)
	case i%30 == 11: // revoked, then claimed again at a higher epoch
		f.put(id, 1, "job.claimed", f.claim(i, 1, 6*time.Hour, ""), 6*time.Hour)
		f.put(id, 2, "job.revoked", fmt.Sprintf(`{"type":"job.revoked","created_at":%q,"job_id":%q,"epoch":1}`, f.stamp(5*time.Hour), id), 5*time.Hour)
		f.put(id, 3, "job.claimed", f.claim(i, 2, 4*time.Hour, ""), 4*time.Hour)
		f.put(id, 4, "job.spawned", f.spawned(i, pane, 4*time.Hour), 4*time.Hour)
		f.panes = append(f.panes, pane)
	default: // ended long ago, outside every window
		f.put(id, 1, "job.claimed", f.claim(i, 1, indexFixtureOld, ""), indexFixtureOld)
		f.put(id, 2, "job.spawned", f.spawned(i, pane, indexFixtureOld), indexFixtureOld)
		f.put(id, 3, "job.progress", fmt.Sprintf(`{"type":"job.progress","created_at":%q}`, f.stamp(indexFixtureOld)), indexFixtureOld)
		f.put(id, 4, "job.completed", f.completed(i, indexFixtureOld), indexFixtureOld)
		if i%10 == 0 {
			f.panes = append(f.panes, pane)
		}
	}
}

func (f *indexFixture) writeLaneEvent(i int, valid bool) {
	body := fmt.Sprintf(`{"type":"lane.event","created_at":%q,"owner_lane":"lane-%d","event_id":"ev-%05d","text":"note %d","epoch":1}`, f.stamp(time.Duration(i%48)*time.Hour), i%9, i, i)
	if !valid {
		body = fmt.Sprintf(`{"type":"lane.event","owner_lane":"lane-%d","event_id":"ev-%05d","text":"bad\u0001text"}`, i%9, i)
	}
	path := filepath.Join(f.root, "events-lane", fmt.Sprintf("%05d-lane.event.json", i))
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		f.t.Fatal(err)
	}
	at := f.now.Add(-time.Duration(i%48) * time.Hour)
	if err := os.Chtimes(path, at, at); err != nil {
		f.t.Fatal(err)
	}
}

// settle back-dates every directory, as a node that has been quiet for an
// hour has: the index may then trust a read until the directory changes.
func (f *indexFixture) settle() {
	f.t.Helper()
	at := f.now.Add(-indexFixtureSettle)
	entries, err := os.ReadDir(filepath.Join(f.root, "jobs"))
	if err != nil {
		f.t.Fatal(err)
	}
	for _, entry := range entries {
		if err := os.Chtimes(filepath.Join(f.root, "jobs", entry.Name(), "events"), at, at); err != nil && !os.IsNotExist(err) {
			f.t.Fatal(err)
		}
	}
	for _, dir := range []string{filepath.Join(f.root, "jobs"), filepath.Join(f.root, "events-lane")} {
		if err := os.Chtimes(dir, at, at); err != nil {
			f.t.Fatal(err)
		}
	}
}
