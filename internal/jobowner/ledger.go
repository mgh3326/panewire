// Package jobowner persists the hub's assignment and revocation for node job writers.
package jobowner

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"github.com/mgh3326/panewire/internal/durable"
)

var validID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

const assignmentFile = ".panewire-owner-epoch.json"

type assignment struct {
	Epoch uint64 `json:"epoch"`
}

// Epoch returns the most recent hub grant, or the initial hub epoch for a
// job that has not been reassigned. Malformed grants fail closed.
func Epoch(jobs, id string) (uint64, error) {
	if jobs == "" || !validID.MatchString(id) || id == "." || id == ".." {
		return 0, errors.New("invalid job assignment")
	}
	body, err := os.ReadFile(filepath.Join(jobs, id, assignmentFile))
	if errors.Is(err, os.ErrNotExist) {
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	var grant assignment
	if json.Unmarshal(body, &grant) != nil || grant.Epoch == 0 {
		return 0, errors.New("invalid job assignment")
	}
	return grant.Epoch, nil
}

// Assign durably records a monotonic hub grant before a node can use it.
func Assign(jobs, id string, epoch uint64) error {
	if epoch == 0 {
		return errors.New("invalid job assignment")
	}
	return withLock(jobs, id, func(dir string) error {
		old, err := Epoch(jobs, id)
		if err != nil || epoch <= old {
			return err
		}
		body, err := json.Marshal(assignment{Epoch: epoch})
		if err != nil {
			return err
		}
		return durable.ReplaceFileAtomically(filepath.Join(dir, assignmentFile), ".assignment-*", body, 0600)
	})
}

// Revoke appends the same local revocation record as a hub job.revoked frame.
func Revoke(jobs, id string, epoch uint64) error {
	if epoch == 0 {
		return errors.New("invalid job revocation")
	}
	return withLock(jobs, id, func(dir string) error {
		events := filepath.Join(dir, "events")
		if err := os.MkdirAll(events, 0700); err != nil {
			return err
		}
		entries, err := os.ReadDir(events)
		if err != nil {
			return err
		}
		var highest uint64
		for _, entry := range entries {
			prefix, _, found := strings.Cut(entry.Name(), "-")
			seq, err := strconv.ParseUint(prefix, 10, 64)
			if found && err == nil && seq > highest {
				highest = seq
			}
		}
		body, err := json.Marshal(struct {
			Type  string `json:"type"`
			JobID string `json:"job_id"`
			Epoch uint64 `json:"epoch"`
		}{"job.revoked", id, epoch})
		if err != nil {
			return err
		}
		path := filepath.Join(events, fmt.Sprintf("%05d-job.revoked.json", highest+1))
		return durable.ReplaceFileAtomically(path, ".revocation-*", body, 0600)
	})
}

func withLock(jobs, id string, fn func(string) error) error {
	if jobs == "" || !validID.MatchString(id) || id == "." || id == ".." {
		return errors.New("invalid job id")
	}
	dir := filepath.Join(jobs, id)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(dir, ".wrk-events.lock"), os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	return fn(dir)
}
