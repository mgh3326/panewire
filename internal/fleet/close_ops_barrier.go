package fleet

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/durable"
)

// The close_ops_v1 migration barrier (hk 4632; docs/node-commands.md, "Hub
// close protocol"). A node advertises close_ops_v1 only after a recorded
// step, `panewire node close-ops enable`, confirmed that no process of an
// older binary that would ignore a hub close was left on the host: a watcher
// without the hub-close token, or a spawn launcher mid-launch. The step leaves
// CloseOpsMarker in the jobs root, and every hello re-checks the live watchers
// on top of it (HubCloseWatchersReady), so a watcher an older binary starts
// later takes close_ops_v1 away again until it ends.
const CloseOpsMarker = ".close-ops-v1"

// closeOpsLauncherVerb is the hidden verb a spawn launcher runs as.
const closeOpsLauncherVerb = "_spawn-stdin"

// CloseOpsRecord is the marker's content: who enabled close ops and when.
type CloseOpsRecord struct {
	Version string    `json:"version"`
	Enabled time.Time `json:"enabled"`
}

// ErrCloseOpsNotReady is EnableCloseOps's refusal: a process the barrier
// waits on is still live.
var ErrCloseOpsNotReady = errors.New("close ops not ready")

// launchersIdle reports whether no spawn launcher other than self runs.
func launchersIdle(out []byte, self int) (bool, error) {
	scanner := bufio.NewScanner(bytes.NewReader(out))
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil || pid == self {
			continue
		}
		for _, field := range fields[1:] {
			if field == closeOpsLauncherVerb {
				return false, nil
			}
		}
	}
	return true, scanner.Err()
}

// EnableCloseOps is the barrier step: it refuses with ErrCloseOpsNotReady
// while any watcher lacks the hub-close token or a spawn launcher runs, and
// otherwise records the marker, mode 0600, atomically. Repeating it rewrites
// the marker. list nil runs ps.
func EnableCloseOps(ctx context.Context, jobs, version string, now time.Time, list ProcessLister) error {
	if list == nil {
		list = psProcesses
	}
	if info, err := os.Stat(jobs); err != nil {
		return err
	} else if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", jobs)
	}
	ready, err := HubCloseWatchersReady(ctx, jobs, list)
	if err != nil {
		return err
	}
	if !ready {
		return fmt.Errorf("%w: a job watcher of an older binary is still running", ErrCloseOpsNotReady)
	}
	out, err := list(ctx)
	if err != nil {
		return err
	}
	if idle, err := launchersIdle(out, os.Getpid()); err != nil {
		return err
	} else if !idle {
		return fmt.Errorf("%w: a spawn launcher is still running", ErrCloseOpsNotReady)
	}
	body, err := json.Marshal(CloseOpsRecord{Version: version, Enabled: now.UTC()})
	if err != nil {
		return err
	}
	return durable.ReplaceFileAtomically(filepath.Join(jobs, CloseOpsMarker), ".tmp-close-ops-*", append(body, '\n'), 0o600)
}

// CloseOpsEnabled reports whether the barrier step recorded the marker in
// jobs. An unreadable or malformed marker is not enabled.
func CloseOpsEnabled(jobs string) bool {
	raw, err := os.ReadFile(filepath.Join(jobs, CloseOpsMarker))
	if err != nil {
		return false
	}
	var record CloseOpsRecord
	return json.Unmarshal(raw, &record) == nil && !record.Enabled.IsZero()
}

// CloseOpsReady is the hello's check: the marker is recorded and every live
// watcher carries the hub-close token now. Any failure is not ready.
func CloseOpsReady(ctx context.Context, jobs string, list ProcessLister) bool {
	if jobs == "" || !CloseOpsEnabled(jobs) {
		return false
	}
	ready, err := HubCloseWatchersReady(ctx, jobs, list)
	return err == nil && ready
}
