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
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/durable"
)

// A node advertises close_ops_v1 only while every live watcher process on it
// understands hub-close intents (hk 4652; docs/node-commands.md, "Hub close
// protocol"). A watcher of this binary records that at its very start, before
// any lock, as a capability token file named by its own pid. A watcher of an
// older binary never writes one, so it is seen as incompatible for as long as
// its process lives, whether or not it has taken a lock yet.
const (
	// WatcherCapsToken is the capability a watcher's token file carries.
	WatcherCapsToken = "hub_close_v1"
	watchCapsDir     = ".watch-caps"
)

// recordWatcherCaps writes this process's token for job and returns its
// removal; a stale file is harmless, since only a live pid is checked.
func (a *App) recordWatcherCaps(job string) (func(), error) {
	dir := filepath.Join(a.Jobs, job, watchCapsDir)
	if e := os.MkdirAll(dir, 0o700); e != nil {
		return nil, e
	}
	path := filepath.Join(dir, strconv.Itoa(os.Getpid()))
	if e := durable.ReplaceFileAtomically(path, ".tmp-caps-*", []byte(WatcherCapsToken+"\n"), 0o600); e != nil {
		return nil, e
	}
	return func() { os.Remove(path) }, nil
}

// ProcessLister lists the host's processes as `ps -axo pid=,args=` prints
// them; tests replace it.
type ProcessLister func(ctx context.Context) ([]byte, error)

// psProcesses runs ps, bounded, on Linux and macOS alike.
func psProcesses(ctx context.Context) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "ps", "-axo", "pid=,args=").Output()
}

// HubCloseWatchersReady reports whether every live panewire process that can
// end a job outside a hub close understands hub-close intents: every
// `_watch <job>` process for a job under the jobs root carries this binary's
// watcher token, and every launcher (`spawn`, `work`, `_spawn-stdin`), which
// can start a watcher later, carries this binary's launcher token (#572
// r4575956172, r4575962305). A launcher is not tied to a jobs root, so one
// without a token here is not ready either. Any process without a token (an
// older binary), an unreadable token or a failed process listing is not
// ready: the caller then leaves close_ops_v1 out. list nil runs ps.
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
		if len(fields) < 3 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}
		for i := 1; i+1 < len(fields); i++ {
			var tokenPath, want string
			switch {
			case fields[i] == "_watch" && validID(fields[i+1]):
				job := fields[i+1]
				if info, err := os.Stat(filepath.Join(jobs, job)); err != nil || !info.IsDir() {
					break // another jobs root's watcher
				}
				tokenPath, want = filepath.Join(jobs, job, watchCapsDir, strconv.Itoa(pid)), WatcherCapsToken
			case filepath.Base(fields[i]) == "panewire" && launcherCommands[fields[i+1]]:
				tokenPath, want = filepath.Join(jobs, launcherCapsDir, strconv.Itoa(pid)), LauncherCapsToken
			default:
				continue
			}
			if tokenPath == "" {
				break
			}
			token, err := os.ReadFile(tokenPath)
			if errors.Is(err, fs.ErrNotExist) || err == nil && strings.TrimSpace(string(token)) != want {
				return false, nil
			} else if err != nil {
				return false, err
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
	// LauncherCapsToken is the capability a launcher's token file carries:
	// any watcher it starts is one that defers to hub-close intents.
	LauncherCapsToken = "hub_close_launcher_v1"
	launcherCapsDir   = ".launcher-caps"
	// closeOpsEnabledFile, under the jobs root, records that the migration
	// barrier (EnableCloseOps) passed on this host.
	closeOpsEnabledFile = ".close-ops-enabled"
)

// launcherCommands are the panewire commands that can start a watcher.
var launcherCommands = map[string]bool{"spawn": true, "work": true, "_spawn-stdin": true}

// recordLauncherCaps writes this process's launcher token under the jobs root
// and returns its removal. A failure leaves no token, which only keeps the
// node from advertising close ops while this process lives.
func (a *App) recordLauncherCaps() func() {
	dir := filepath.Join(a.Jobs, launcherCapsDir)
	if e := os.MkdirAll(dir, 0o700); e != nil {
		return func() {}
	}
	path := filepath.Join(dir, strconv.Itoa(os.Getpid()))
	if e := durable.ReplaceFileAtomically(path, ".tmp-caps-*", []byte(LauncherCapsToken+"\n"), 0o600); e != nil {
		return func() {}
	}
	return func() { os.Remove(path) }
}

// ErrCloseOpsNotReady is EnableCloseOps refusing: an older panewire watcher
// or launcher still runs on the host, or the process listing failed.
var ErrCloseOpsNotReady = errors.New("close ops not enabled: an older panewire watcher or launcher still runs (or the process list is unreadable); finish or stop it and retry")

// EnableCloseOps is the migration barrier (hk 4632): it records, durably
// under the jobs root, that no old-binary panewire watcher or launcher runs
// on the host. The node advertises close_ops_v1 only once this record exists
// and, at every hello, a fresh HubCloseWatchersReady still holds. It is
// idempotent. list nil runs ps.
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
// barrier was recorded and no older watcher or launcher runs.
func CloseOpsReady(ctx context.Context, jobs string, list ProcessLister) bool {
	if info, err := os.Stat(filepath.Join(jobs, closeOpsEnabledFile)); err != nil || !info.Mode().IsRegular() {
		return false
	}
	ready, err := HubCloseWatchersReady(ctx, jobs, list)
	return err == nil && ready
}
