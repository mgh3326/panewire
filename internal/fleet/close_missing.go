package fleet

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/mgh3326/panewire/internal/durable"
)

// A hub close of a job whose directory is gone (hk 5296). The close journal
// lives inside the job directory, so a directory the job-dir TTL removed
// takes its release tombstone with it, and a delayed pane.close_op could meet
// a directory recreated for a later attempt under the same id. The node
// therefore keeps one durable tombstone per request outside every job
// directory, and writes it only once it has proof that the directory is gone:
// the jobs root is readable, the directory is definitely absent, and the
// job's identity record (.job-ids, which outlives the directory) shows the
// job was claimed on this node. docs/node-commands.md, "A missing job".

// closeMissingDir holds the tombstones. A dot name is never a valid job id,
// so no job scan reads it as one (as .job-ids and .expiring-*).
const closeMissingDir = ".close-missing"

// MissingJobTombstone is the node's durable record that a hub close request
// was fenced for a job whose directory was gone. Epoch is the hub's ownership
// epoch the request was issued at.
type MissingJobTombstone struct {
	RequestID string    `json:"request_id"`
	Job       string    `json:"job"`
	Epoch     uint64    `json:"epoch"`
	Created   time.Time `json:"created"`
}

func closeMissingPath(jobsRoot, requestID string) (string, error) {
	if !ValidCloseOpID(requestID) {
		return "", errors.New("invalid close operation id")
	}
	return filepath.Join(jobsRoot, closeMissingDir, requestID+".json"), nil
}

// JobDirMissingProof reports whether the node can prove that job id's
// directory is gone: the jobs root is a readable directory, no entry of that
// name (in any letter case) exists, and the job's identity record names
// exactly id. Anything unreadable is an error and never proof: a missing or
// unmounted jobs root, an entry that is not a directory, an unreadable or
// foreign identity record. A job that never recorded an identity (one older
// than hk 4824, or never claimed here) is not proved either; the caller
// keeps its hold.
func JobDirMissingProof(jobsRoot, id string) (bool, error) {
	if !ValidJobID(id) {
		return false, errors.New("invalid job id")
	}
	if info, err := os.Stat(jobsRoot); errors.Is(err, fs.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, err
	} else if !info.IsDir() {
		return false, nil
	}
	if _, err := os.Lstat(filepath.Join(jobsRoot, id)); err == nil {
		return false, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return false, err
	}
	if conflict, err := jobDirCaseConflict(jobsRoot, id); err != nil || conflict != "" {
		return false, err
	}
	path, err := jobDirIdentityPath(jobsRoot, id)
	if err != nil {
		return false, err
	}
	owner, claimed, err := readJobDirIdentity(path)
	if err != nil {
		return false, err
	}
	return claimed && owner == id, nil
}

// ReadMissingJobTombstone reads the tombstone of one request. A record that
// does not parse, or names another request, is an error: callers fail closed.
func ReadMissingJobTombstone(jobsRoot, requestID string) (MissingJobTombstone, bool, error) {
	path, err := closeMissingPath(jobsRoot, requestID)
	if err != nil {
		return MissingJobTombstone{}, false, err
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return MissingJobTombstone{}, false, nil
	} else if err != nil {
		return MissingJobTombstone{}, false, err
	}
	var tombstone MissingJobTombstone
	if err := json.Unmarshal(raw, &tombstone); err != nil || tombstone.RequestID != requestID || tombstone.Epoch == 0 || !ValidJobID(tombstone.Job) {
		return MissingJobTombstone{}, false, fmt.Errorf("unreadable missing-job tombstone %s", requestID)
	}
	return tombstone, true, nil
}

// WriteMissingJobTombstone durably records the fence for one request, at mode
// 0600 and fsynced before it is visible. A tombstone already recorded for the
// request is kept (its job must match; the epoch only ever rises), and the
// stored record is returned.
func WriteMissingJobTombstone(jobsRoot string, tombstone MissingJobTombstone) (MissingJobTombstone, error) {
	if tombstone.Epoch == 0 || !ValidJobID(tombstone.Job) {
		return MissingJobTombstone{}, errors.New("invalid missing-job tombstone")
	}
	path, err := closeMissingPath(jobsRoot, tombstone.RequestID)
	if err != nil {
		return MissingJobTombstone{}, err
	}
	existing, found, err := ReadMissingJobTombstone(jobsRoot, tombstone.RequestID)
	if err != nil {
		return MissingJobTombstone{}, err
	}
	if found {
		if existing.Job != tombstone.Job {
			return MissingJobTombstone{}, errors.New("missing-job tombstone names another job")
		}
		if existing.Epoch >= tombstone.Epoch {
			return existing, nil
		}
		tombstone.Created = existing.Created
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return MissingJobTombstone{}, err
	}
	raw, err := json.Marshal(tombstone)
	if err != nil {
		return MissingJobTombstone{}, err
	}
	if err := durable.ReplaceFileAtomically(path, ".tombstone-*.tmp", raw, 0600); err != nil {
		return MissingJobTombstone{}, err
	}
	durable.SyncDirectoryBestEffort(filepath.Dir(dir))
	return tombstone, nil
}
