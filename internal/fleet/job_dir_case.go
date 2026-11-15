package fleet

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/mgh3326/panewire/internal/durable"
)

// A jobs-inbox directory is named by exactly one job id. Two ids that differ
// only in letter case would name one directory on a case-insensitive volume
// (macOS APFS), where one job's brief-delivered.md, events and report overwrite
// the other's (hk 4824). Scanning the root alone cannot keep them apart: two
// launchers that start at once both find no directory. So the jobs root holds
// one no-clobber identity record per case-folded id, and the os.Link that
// publishes the record is the decision. The record outlives the job dir (a TTL
// pass may delete the dir), so one spelling of a job id stays reserved.

const (
	// jobDirIdentityDir holds the records; validID requires a leading letter or
	// digit, so no job dir can ever be named .job-ids.
	jobDirIdentityDir = ".job-ids"
	// jobDirIdentityMax is validID's maximum id length; a record is one id and
	// a newline.
	jobDirIdentityMax = 128
	// jobDirIdentityTemporary names the unpublished record os.Link publishes.
	jobDirIdentityTemporary = ".claim-*.tmp"
)

// jobDirCaseConflict names the jobs-root entry that matches id only in letter
// case, or "" when the root has no such entry. Entries are compared as the
// volume stores them (os.ReadDir), never through a path lookup, so the answer
// is the same on a case-insensitive volume and on a case-sensitive one. An
// entry whose name is exactly id is not a conflict: the launcher reuses that
// dir as before.
func jobDirCaseConflict(jobsRoot, id string) (string, error) {
	entries, err := os.ReadDir(jobsRoot)
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		if name := entry.Name(); name != id && strings.EqualFold(name, id) {
			return name, nil
		}
	}
	return "", nil
}

// physicalJobsRoot resolves the launcher's view of the jobs root to the
// volume's path: two launchers reach one root through different paths (a
// symlinked inbox, another mount), and their identity records must still be the
// same files.
func physicalJobsRoot(jobsRoot string) (string, error) {
	resolved, err := filepath.EvalSymlinks(jobsRoot)
	if err == nil {
		return resolved, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return filepath.Clean(jobsRoot), nil
	}
	return "", err
}

// jobDirIdentityKey is a job id's case-folded identity. validID admits ASCII
// only, so folding to lower case is exactly EqualFold's equivalence class for
// every id a launcher accepts; a case-sensitive volume then refuses the same
// ids as a case-insensitive one (hk 4824).
func jobDirIdentityKey(id string) string { return strings.ToLower(id) }

// jobDirIdentityPath is the record naming the id that owns id's identity.
func jobDirIdentityPath(jobsRoot, id string) (string, error) {
	root, err := physicalJobsRoot(jobsRoot)
	if err != nil {
		return "", err
	}
	return filepath.Join(root, jobDirIdentityDir, jobDirIdentityKey(id)), nil
}

// readJobDirIdentity returns the exact id a record names. claimed is false when
// no record exists; an unreadable or malformed record is an error, and every
// caller fails closed on it.
func readJobDirIdentity(path string) (id string, claimed bool, err error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if len(b) < 2 || len(b) > jobDirIdentityMax+1 || b[len(b)-1] != '\n' {
		return "", false, fmt.Errorf("malformed job-dir identity record %s", path)
	}
	id = string(b[:len(b)-1])
	if !validID(id) {
		return "", false, fmt.Errorf("malformed job-dir identity record %s", path)
	}
	return id, true, nil
}

// jobDirCaseRefusal names both ids: the requested one and the entry that holds
// the name on this volume.
func jobDirCaseRefusal(id, existing string) error {
	return fail(exitUsage, "job id %s differs only in letter case from the existing jobs-inbox entry %s; the two ids would share one directory and their files would overwrite each other. Choose a job id that differs by more than letter case.", id, existing)
}

// checkJobDirCase is the read-only half of the guard: a jobs-root entry that
// names id only in letter case, or a record another spelling owns, is the
// refusal. It writes nothing, so --dry-run runs it too.
func checkJobDirCase(jobsRoot, id string) error {
	existing, err := jobDirCaseConflict(jobsRoot, id)
	if err != nil {
		return fail(exitStateConflict, "jobs root %s unreadable: %v", jobsRoot, err)
	}
	if existing != "" {
		return jobDirCaseRefusal(id, existing)
	}
	path, err := jobDirIdentityPath(jobsRoot, id)
	if err != nil {
		return fail(exitStateConflict, "job-dir identity path: %v", err)
	}
	owner, claimed, err := readJobDirIdentity(path)
	if err != nil {
		return fail(exitStateConflict, "job-dir identity for %s is unreadable: %v", id, err)
	}
	if claimed && owner != id {
		return jobDirCaseRefusal(id, owner)
	}
	return nil
}

// claimJobDirIdentity publishes id's identity record no-clobber. Two launchers
// that ran the read-only check at once both saw no directory and no record;
// os.Link is the one atomic decision between them, so exactly one wins and the
// other reads the winner's exact id and refuses. An id that already owns its
// record (a retry of the same spawn) passes.
func claimJobDirIdentity(jobsRoot, id string) error {
	path, err := jobDirIdentityPath(jobsRoot, id)
	if err != nil {
		return fail(exitStateConflict, "job-dir identity path: %v", err)
	}
	dir := filepath.Dir(path)
	if err = os.MkdirAll(dir, 0700); err != nil {
		return fail(exitStateConflict, "job-dir identity dir: %v", err)
	}
	temporary, err := durable.WriteSyncedTemporaryFile(dir, jobDirIdentityTemporary, []byte(id+"\n"), 0600)
	if err != nil {
		return fail(exitStateConflict, "job-dir identity write: %v", err)
	}
	defer os.Remove(temporary)
	err = os.Link(temporary, path)
	switch {
	case errors.Is(err, fs.ErrExist):
		owner, claimed, readErr := readJobDirIdentity(path)
		if readErr != nil {
			return fail(exitStateConflict, "job-dir identity for %s is unreadable: %v", id, readErr)
		}
		if !claimed {
			return fail(exitStateConflict, "job-dir identity for %s disappeared while claiming it", id)
		}
		if owner == id {
			return nil
		}
		return jobDirCaseRefusal(id, owner)
	case err != nil:
		return fail(exitStateConflict, "job-dir identity link: %v", err)
	}
	// The record's bytes are synced before it is visible; sync the link too.
	durable.SyncDirectoryBestEffort(dir)
	return nil
}
