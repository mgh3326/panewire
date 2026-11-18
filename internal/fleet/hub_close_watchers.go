package fleet

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io/fs"
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
			if info, err := os.Stat(dir); errors.Is(err, fs.ErrNotExist) {
				break // another jobs root's watcher
			} else if err != nil {
				return false, err
			} else if !info.IsDir() {
				return false, nil
			}
			watcher, err := ProbeHubCloseWatcher(dir)
			if err != nil {
				return false, err
			}
			if watcher != HubCloseWatcherAware && watcher != HubCloseWatcherDegraded {
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

const (
	// closeOpsEnabledFile, under the jobs root, records that the migration
	// barrier (EnableCloseOps) passed on this host.
	closeOpsEnabledFile = ".close-ops-enabled"
	// closeOpsAdvertiseFile, under the jobs root, is the explicit rollout
	// step (AdvertiseCloseOps) that turns close_ops_v1 on. It is off by
	// default: hk 4745's rollout runs it once the hub issuer is deployed.
	closeOpsAdvertiseFile = ".close-ops-advertise"
)

// ErrCloseOpsNoBarrier is AdvertiseCloseOps refusing on a host whose
// barrier was never recorded.
var ErrCloseOpsNoBarrier = errors.New("close ops not advertised: run `panewire node close-ops enable` first")

// AdvertiseCloseOps is the rollout step that turns close_ops_v1 on for the
// node (hk 4632, off by default): it requires the barrier record, and
// records durably under the jobs root that the node may advertise. Removing
// both files withdraws it (docs/node-commands.md, "Enabling close
// operations").
func AdvertiseCloseOps(jobs string, now time.Time) error {
	unlock, err := closeOpsLock(context.Background(), jobs, true)
	if err != nil {
		return err
	}
	defer unlock()
	if draining, err := closeOpsDraining(jobs); err != nil || draining {
		return ErrCloseOpsDraining
	}
	if info, err := os.Stat(filepath.Join(jobs, closeOpsEnabledFile)); err != nil || !info.Mode().IsRegular() {
		return ErrCloseOpsNoBarrier
	}
	return durable.ReplaceFileAtomically(filepath.Join(jobs, closeOpsAdvertiseFile), ".tmp-close-ops-*", []byte("advertised_at="+now.UTC().Format(time.RFC3339)+"\n"), 0o600)
}

// ErrCloseOpsNotReady is EnableCloseOps refusing: an older panewire watcher
// still runs on the host, or the process listing failed.
var ErrCloseOpsNotReady = errors.New("close ops not enabled: an older panewire watcher still runs (or the process list is unreadable); let it finish or stop it, and retry")

// EnableCloseOps is the migration barrier (hk 4632): it records, durably
// under the jobs root, that no older-binary watcher runs on the host. The
// node advertises close_ops_v1 only once this record exists and, at every
// hello and re-check, HubCloseWatchersReady still holds. It is idempotent.
// list nil runs ps.
func EnableCloseOps(ctx context.Context, jobs string, list ProcessLister, now time.Time) error {
	unlock, err := closeOpsLock(ctx, jobs, true)
	if err != nil {
		return err
	}
	defer unlock()
	if draining, err := closeOpsDraining(jobs); err != nil {
		return err
	} else if draining {
		pending, err := PendingHubCloseJobs(jobs)
		if err != nil || len(pending) != 0 {
			return errors.Join(ErrCloseOpsNotReady, err)
		}
		if err := os.Remove(filepath.Join(jobs, closeOpsDrainingFile)); err != nil {
			return err
		}
		durable.SyncDirectoryBestEffort(jobs)
	}
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

// CloseOpsReady reports whether the node may advertise close_ops_v1 and
// admit new closes now: the barrier and the advertise step were recorded,
// and every live watcher is aware.
func CloseOpsReady(ctx context.Context, jobs string, list ProcessLister) bool {
	if draining, err := closeOpsDraining(jobs); err != nil || draining {
		return false
	}
	for _, name := range []string{closeOpsEnabledFile, closeOpsAdvertiseFile} {
		if info, err := os.Stat(filepath.Join(jobs, name)); err != nil || !info.Mode().IsRegular() {
			return false
		}
	}
	ready, err := HubCloseWatchersReady(ctx, jobs, list)
	return err == nil && ready
}
