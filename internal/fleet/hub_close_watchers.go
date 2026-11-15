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
// removal. The token names the process's start time as ps reports it, so a
// file a killed watcher left behind never vouches for a later process that
// reuses its pid (#593 review). A start time ps cannot read is left empty,
// which no listing matches: that watcher holds close ops off, failing closed.
func (a *App) recordWatcherCaps(job string) (func(), error) {
	dir := filepath.Join(a.Jobs, job, watchCapsDir)
	if e := os.MkdirAll(dir, 0o700); e != nil {
		return nil, e
	}
	start, _ := processStartOf(context.Background(), os.Getpid())
	path := filepath.Join(dir, strconv.Itoa(os.Getpid()))
	if e := durable.ReplaceFileAtomically(path, ".tmp-caps-*", []byte(watcherCapsLine(start)), 0o600); e != nil {
		return nil, e
	}
	return func() { os.Remove(path) }, nil
}

// watcherCapsLine is a token file's content for a process started at start.
func watcherCapsLine(start string) string { return WatcherCapsToken + " " + start + "\n" }

// processStartOf reads a process's start time as `ps -o lstart=` prints it;
// tests replace it.
var processStartOf = func(ctx context.Context, pid int) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ps", "-o", "lstart=", "-p", strconv.Itoa(pid))
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	start := strings.Join(strings.Fields(string(out)), " ")
	if len(strings.Fields(start)) != lstartFields {
		return "", errors.New("unreadable process start time")
	}
	return start, nil
}

// lstartFields is the width of ps's lstart column ("Sun Nov 15 12:41:36 2026").
const lstartFields = 5

// ProcessLister lists the host's processes as `ps -axo pid=,lstart=,args=`
// prints them in the C locale; tests replace it.
type ProcessLister func(ctx context.Context) ([]byte, error)

// psProcesses runs ps, bounded, on Linux and macOS alike.
func psProcesses(ctx context.Context) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ps", "-axo", "pid=,lstart=,args=")
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	return cmd.Output()
}

// listedProcess is one line of a ProcessLister listing.
type listedProcess struct {
	pid   int
	start string
	args  []string
}

// parseProcessLine reads one listing line; ok is false for a line too short
// or without a numeric pid.
func parseProcessLine(line string) (listedProcess, bool) {
	fields := strings.Fields(line)
	if len(fields) < 2+lstartFields {
		return listedProcess{}, false
	}
	pid, err := strconv.Atoi(fields[0])
	if err != nil {
		return listedProcess{}, false
	}
	return listedProcess{pid: pid, start: strings.Join(fields[1:1+lstartFields], " "), args: fields[1+lstartFields:]}, true
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
		process, ok := parseProcessLine(scanner.Text())
		if !ok {
			continue
		}
		fields := process.args
		for i := 0; i+1 < len(fields); i++ {
			if fields[i] != "_watch" || !validID(fields[i+1]) {
				continue
			}
			job := fields[i+1]
			if info, err := os.Stat(filepath.Join(jobs, job)); err != nil || !info.IsDir() {
				break // another jobs root's watcher
			}
			// The token must name this process's start time: a file left
			// by a killed watcher whose pid was reused does not.
			token, err := os.ReadFile(filepath.Join(jobs, job, watchCapsDir, strconv.Itoa(process.pid)))
			if errors.Is(err, fs.ErrNotExist) || err == nil && string(token) != watcherCapsLine(process.start) {
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
