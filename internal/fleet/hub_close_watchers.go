package fleet

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/durable"
)

// A node advertises close_ops_v1 only while the migration barrier was
// recorded and every live watcher on it is one that defers to hub-close
// intents and drops stale decisions (hk 4632; docs/node-commands.md,
// "Enabling close operations"). A watcher of this binary holds
// hubCloseWatchLock for its whole life; an older one never does.
//
// Launchers need no check of their own: a watcher an older launcher starts
// later either runs before a close is admitted (the per-job probe then
// refuses it) or after, when the node's fence on the job's watch lock
// (FenceWatchers) makes it exit at once.

// ProcessLister lists the host's processes as `ps -axo pid=,args=` prints
// them; tests replace it.
type ProcessLister func(ctx context.Context) ([]byte, error)

// psProcesses runs ps, bounded, on Linux and macOS alike.
func psProcesses(ctx context.Context) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "ps", "-axo", "pid=,args=").Output()
}

// HubCloseWatchersReady reports whether, for every live `_watch <job>`
// process of a job under the jobs root, the job's watcher holds
// hubCloseWatchLock (ProbeHubCloseWatcher answers aware). A job whose watcher
// is older, or caught between its two locks, an unreadable lock or a failed
// process listing is not ready: the caller then leaves close_ops_v1 out.
// list nil runs ps.
func HubCloseWatchersReady(ctx context.Context, jobs string, list ProcessLister) (bool, error) {
	if list == nil {
		list = psProcesses
	}
	out, err := list(ctx)
	if err != nil {
		return false, err
	}
	scanner := bufio.NewScanner(bytes.NewReader(out))
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		for i := 1; i+1 < len(fields); i++ {
			if fields[i] != "_watch" || !validID(fields[i+1]) {
				continue
			}
			dir := filepath.Join(jobs, fields[i+1])
			if info, err := os.Stat(dir); err != nil || !info.IsDir() {
				break // another jobs root's watcher
			}
			watcher, err := ProbeHubCloseWatcher(dir)
			if err != nil {
				return false, err
			}
			if watcher != HubCloseWatcherAware {
				return false, nil
			}
			break
		}
	}
	if err := scanner.Err(); err != nil {
		// A listing not scanned to its end proves nothing about the rest.
		return false, err
	}
	return true, nil
}

// closeOpsEnabledFile, under the jobs root, records that the migration
// barrier (EnableCloseOps) passed on this host.
const closeOpsEnabledFile = ".close-ops-enabled"

// ErrCloseOpsNotReady is EnableCloseOps refusing: an older panewire watcher
// still runs on the host, or the process listing failed.
var ErrCloseOpsNotReady = errors.New("close ops not enabled: an older panewire watcher still runs (or the process list is unreadable); let it finish or stop it, and retry")

// EnableCloseOps is the migration barrier (hk 4632): it records, durably
// under the jobs root, that no older-binary watcher runs on the host. The
// node advertises close_ops_v1 only once this record exists and, at every
// hello and re-check, HubCloseWatchersReady still holds. It is idempotent.
// list nil runs ps.
func EnableCloseOps(ctx context.Context, jobs string, list ProcessLister, now time.Time) error {
	ready, err := HubCloseWatchersReady(ctx, jobs, list)
	if err != nil {
		return errors.Join(ErrCloseOpsNotReady, err)
	}
	if !ready {
		return ErrCloseOpsNotReady
	}
	if e := os.MkdirAll(jobs, 0o700); e != nil {
		return e
	}
	return durable.ReplaceFileAtomically(filepath.Join(jobs, closeOpsEnabledFile), ".tmp-close-ops-*", []byte("enabled_at="+now.UTC().Format(time.RFC3339)+"\n"), 0o600)
}

// CloseOpsReady reports whether the node may advertise close_ops_v1 now: the
// barrier was recorded and every live watcher is aware.
func CloseOpsReady(ctx context.Context, jobs string, list ProcessLister) bool {
	if info, err := os.Stat(filepath.Join(jobs, closeOpsEnabledFile)); err != nil || !info.Mode().IsRegular() {
		return false
	}
	ready, err := HubCloseWatchersReady(ctx, jobs, list)
	return err == nil && ready
}
