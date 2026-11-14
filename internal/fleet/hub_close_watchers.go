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

// HubCloseWatchersReady reports whether every live `_watch <job>` process for
// a job under the jobs root carries this binary's capability token. Any
// process without one (an older binary), an unreadable token or a failed
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
		if len(fields) < 3 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}
		for i := 1; i+1 < len(fields); i++ {
			if fields[i] != "_watch" || !validID(fields[i+1]) {
				continue
			}
			job := fields[i+1]
			if info, err := os.Stat(filepath.Join(jobs, job)); err != nil || !info.IsDir() {
				break // another jobs root's watcher
			}
			token, err := os.ReadFile(filepath.Join(jobs, job, watchCapsDir, strconv.Itoa(pid)))
			if errors.Is(err, fs.ErrNotExist) || err == nil && strings.TrimSpace(string(token)) != WatcherCapsToken {
				return false, nil
			} else if err != nil {
				return false, err
			}
			break
		}
	}
	return true, scanner.Err()
}
