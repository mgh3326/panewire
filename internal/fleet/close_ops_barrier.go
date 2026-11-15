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
// step, `panewire node close-ops enable`, confirmed that nothing of an older
// binary that would ignore a hub close was left on the host, and only while
// that still holds:
//
//   - the marker names the node's own binary version, so a rollback or an
//     upgrade needs the step again, and an in-process launch of an older node
//     daemon (which a version change restarts) cannot outlive it;
//   - one process listing shows no watcher without the hub-close token and no
//     process of a verb that can start a watcher (spawn, work, lead, restore,
//     _spawn-stdin), so a launcher between its receipt and its watcher, of any
//     binary, holds the barrier closed.
//
// The same check gates every hello and every new close registration
// (CloseOpsReady), so a launcher started after the step fences the close it
// could race: it records its job.spawned receipt, which a close binds to,
// before it starts its watcher, and stays live until then.
const CloseOpsMarker = ".close-ops-v1"

// closeOpsLaunchVerbs are the panewire verbs whose process can start a job
// watcher (App.Spawn or restore).
var closeOpsLaunchVerbs = map[string]bool{"_spawn-stdin": true, "spawn": true, "work": true, "lead": true, "restore": true}

// CloseOpsRecord is the marker's content: which binary enabled close ops and
// when.
type CloseOpsRecord struct {
	Version string    `json:"version"`
	Enabled time.Time `json:"enabled"`
}

// ErrCloseOpsNotReady is the barrier's refusal: a process it waits on is
// still live.
var ErrCloseOpsNotReady = errors.New("close ops not ready")

// launchersIdle reports whether no process other than self runs a verb that
// can start a watcher: the first argument after a panewire executable.
func launchersIdle(out []byte, self int) (bool, error) {
	scanner := bufio.NewScanner(bytes.NewReader(out))
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 3 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil || pid == self {
			continue
		}
		for i := 1; i+1 < len(fields); i++ {
			if filepath.Base(fields[i]) == "panewire" && closeOpsLaunchVerbs[fields[i+1]] {
				return false, nil
			}
		}
	}
	return true, scanner.Err()
}

// closeOpsQuiet checks both process classes over one listing.
func closeOpsQuiet(ctx context.Context, jobs string, list ProcessLister) error {
	if list == nil {
		list = psProcesses
	}
	out, err := list(ctx)
	if err != nil {
		return err
	}
	snapshot := func(context.Context) ([]byte, error) { return out, nil }
	ready, err := HubCloseWatchersReady(ctx, jobs, snapshot)
	if err != nil {
		return err
	}
	if !ready {
		return fmt.Errorf("%w: a job watcher of an older binary is still running", ErrCloseOpsNotReady)
	}
	if idle, err := launchersIdle(out, os.Getpid()); err != nil {
		return err
	} else if !idle {
		return fmt.Errorf("%w: a panewire spawn, work, lead or restore is still running", ErrCloseOpsNotReady)
	}
	return nil
}

// EnableCloseOps is the barrier step for the binary version: it refuses with
// ErrCloseOpsNotReady, removing any earlier marker, while closeOpsQuiet does
// not hold, and otherwise records the marker, mode 0600, atomically. list nil
// runs ps.
func EnableCloseOps(ctx context.Context, jobs, version string, now time.Time, list ProcessLister) error {
	if info, err := os.Stat(jobs); err != nil {
		return err
	} else if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", jobs)
	}
	if err := closeOpsQuiet(ctx, jobs, list); err != nil {
		if removeErr := os.Remove(filepath.Join(jobs, CloseOpsMarker)); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return errors.Join(err, removeErr)
		}
		return err
	}
	body, err := json.Marshal(CloseOpsRecord{Version: version, Enabled: now.UTC()})
	if err != nil {
		return err
	}
	return durable.ReplaceFileAtomically(filepath.Join(jobs, CloseOpsMarker), ".tmp-close-ops-*", append(body, '\n'), 0o600)
}

// CloseOpsEnabled reports whether the barrier step recorded the marker in
// jobs for version. An unreadable or malformed marker is not enabled.
func CloseOpsEnabled(jobs, version string) bool {
	raw, err := os.ReadFile(filepath.Join(jobs, CloseOpsMarker))
	if err != nil {
		return false
	}
	var record CloseOpsRecord
	return json.Unmarshal(raw, &record) == nil && !record.Enabled.IsZero() && record.Version == version
}

// CloseOpsReady is the hello's and the close admission's check: the marker is
// recorded for version and closeOpsQuiet holds now. Any failure is not ready.
func CloseOpsReady(ctx context.Context, jobs, version string, list ProcessLister) bool {
	if jobs == "" || !CloseOpsEnabled(jobs, version) {
		return false
	}
	return closeOpsQuiet(ctx, jobs, list) == nil
}
