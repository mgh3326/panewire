package fleet

import (
	"os"
	"path/filepath"

	"github.com/mgh3326/panewire/internal/durable"
)

// JobDoneMarker is the job-dir file that records the result line of the report
// a `done` completed: the report's sha256, a newline, the line `done` printed.
// Both the fleet's `done` and the node's job.done write it, so job.done answers
// already-done for a report whose file was deleted after the pin (hk 3685).
const JobDoneMarker = ".node-job-done"

// markJobDone records a completed report's result line beside the job's
// events. A job dir that is gone, or a write that fails, costs only that
// later answer; it never fails the completion.
func (a *App) markJobDone(id, digest, line string) {
	dir := filepath.Join(a.Jobs, id)
	if info, e := os.Lstat(dir); e != nil || !info.IsDir() {
		return
	}
	contents := []byte(digest + "\n" + line + "\n")
	if e := durable.ReplaceFileAtomically(filepath.Join(dir, JobDoneMarker), ".node-job-done-*", contents, 0600); e != nil {
		a.warn("job done marker write failed: %v", e)
	}
}
