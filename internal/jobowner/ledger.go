// Package jobowner persists the hub's assignment and revocation for node job writers.
package jobowner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/mgh3326/panewire/internal/durable"
	"github.com/mgh3326/panewire/internal/filelock"
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

// Revoke appends a hub revocation unless a later local grant superseded it.
func Revoke(jobs, id string, epoch uint64) error {
	return RevokeWithin(context.Background(), jobs, id, epoch)
}

// RevokeWithin is Revoke with the wait for the job's events lock bounded by
// ctx. A lock still held when ctx ends writes nothing and returns ctx's error,
// so the caller can refuse to acknowledge the revocation.
func RevokeWithin(ctx context.Context, jobs, id string, epoch uint64) error {
	if epoch == 0 {
		return errors.New("invalid job revocation")
	}
	return withLockContext(ctx, jobs, id, func(dir string) error {
		current, err := Epoch(jobs, id)
		if err != nil || current >= epoch {
			return err
		}
		return appendRevocation(dir, id, epoch)
	})
}

// RevokeIfEpoch only revokes the grant that made the refused write. Assignment
// and revocation share this lock, so an old response cannot revoke a new grant.
func RevokeIfEpoch(jobs, id string, writerEpoch, revocationEpoch uint64) error {
	if writerEpoch == 0 || revocationEpoch == 0 {
		return errors.New("invalid job revocation")
	}
	return withLock(jobs, id, func(dir string) error {
		current, err := Epoch(jobs, id)
		if err != nil || current != writerEpoch {
			return err
		}
		return appendRevocation(dir, id, revocationEpoch)
	})
}

func appendRevocation(dir, id string, epoch uint64) error {
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
}

func withLock(jobs, id string, fn func(string) error) error {
	return withLockContext(context.Background(), jobs, id, fn)
}

func withLockContext(ctx context.Context, jobs, id string, fn func(string) error) error {
	if jobs == "" || !validID.MatchString(id) || id == "." || id == ".." {
		return errors.New("invalid job id")
	}
	dir := filepath.Join(jobs, id)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	release, err := filelock.Acquire(ctx, filepath.Join(dir, ".wrk-events.lock"))
	if err != nil {
		return err
	}
	defer release()
	return fn(dir)
}

// JobEnv and EpochEnv name the launch environment that binds a worker process
// tree to one attempt: the job it was launched for and that job's grant epoch
// on this node at launch. The node sets them once; nothing rewrites them, so a
// producer call from the tree presents the attempt that launched it even after
// the hub regrants the job (docs/producer-attempt-grant.md).
const (
	JobEnv   = "ARBITER_JOB"
	EpochEnv = "ARBITER_JOB_EPOCH"
)

// ValidID reports whether id can carry a hub grant at all. Legacy-only ids
// cannot, and are always epoch 1.
func ValidID(id string) bool {
	return validID.MatchString(id) && id != "." && id != ".."
}

// Producer refusals. Each is wrapped with the epochs it compared.
var (
	ErrStaleAttempt     = errors.New("stale attempt")
	ErrAttemptAhead     = errors.New("attempt epoch ahead of the node's grant")
	ErrIdentityRequired = errors.New("attempt identity required")
	ErrEpochConflict    = errors.New("--epoch conflicts with the epoch bound at launch")
	ErrRevoked          = errors.New("attempt revoked")
)

// Revoked returns the highest hub revocation recorded for the job, or 0. A
// revocation at epoch R takes the job away from the attempt granted below R,
// but Revoke leaves that grant file as it was, so the grant alone cannot say
// the attempt lost ownership. A later Assign at R or above supersedes it.
func Revoked(jobs, id string) (uint64, error) {
	if jobs == "" || !ValidID(id) {
		return 0, errors.New("invalid job assignment")
	}
	events := filepath.Join(jobs, id, "events")
	entries, err := os.ReadDir(events)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var highest uint64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "-job.revoked.json") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(events, entry.Name()))
		if err != nil {
			return 0, err
		}
		var marker struct {
			Type  string `json:"type"`
			Epoch uint64 `json:"epoch"`
		}
		if json.Unmarshal(body, &marker) != nil {
			return 0, errors.New("invalid job revocation")
		}
		// A local end also writes job.revoked, with a kind and no epoch.
		if marker.Type == "job.revoked" && marker.Epoch > highest {
			highest = marker.Epoch
		}
	}
	return highest, nil
}

// ProducerEpoch resolves the epoch a producer call (job done|escalate|joined,
// emit) may record for job id, from the epoch bound into the caller's launch
// environment (0 when absent) and an explicit --epoch (0 when absent).
//
// A presented epoch must equal the node's current grant: an older one is a
// worker from a superseded attempt (A1→B2→A3 leaves the A/1 worker alive), a
// newer one names a grant this node has not persisted. With no identity at
// all the call is accepted only while the grant is still the initial epoch 1,
// so a regrant never lets an unidentified caller pick up the new epoch. A grant
// the hub has revoked (A/1 → B/2 leaves A's file at 1) is refused whatever is
// presented, until a newer grant supersedes the revocation.
func ProducerEpoch(jobs, id string, bound, explicit uint64) (uint64, error) {
	if bound != 0 && explicit != 0 && bound != explicit {
		return 0, fmt.Errorf("%w: bound %d, --epoch %d", ErrEpochConflict, bound, explicit)
	}
	presented := bound
	if presented == 0 {
		presented = explicit
	}
	current, err := Epoch(jobs, id)
	if err != nil {
		return 0, err
	}
	revoked, err := Revoked(jobs, id)
	if err != nil {
		return 0, err
	}
	if revoked > current {
		return 0, fmt.Errorf("%w: the hub revoked this attempt at epoch %d (grant %d)", ErrRevoked, revoked, current)
	}
	switch {
	case presented == 0 && current == 1:
		return 1, nil
	case presented == 0:
		return 0, fmt.Errorf("%w: job %s is at grant epoch %d; pass --epoch %d from an operator or parent call", ErrIdentityRequired, id, current, current)
	case presented < current:
		return 0, fmt.Errorf("%w: attempt epoch %d, current grant %d", ErrStaleAttempt, presented, current)
	case presented > current:
		return 0, fmt.Errorf("%w: attempt epoch %d, current grant %d", ErrAttemptAhead, presented, current)
	}
	return presented, nil
}
