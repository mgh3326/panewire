package fleet

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// Save before sending. A response can be lost after the hub commits; rerunning
// the same command must replay the same key and exact payload, not launch again.
func (a *App) saveSubmission(request Submission) (Submission, error) {
	spawn, err := ValidateSubmission(request)
	if err != nil {
		return Submission{}, err
	}
	if err = os.MkdirAll(a.Jobs, 0700); err != nil {
		return Submission{}, err
	}
	root, err := filepath.EvalSymlinks(a.Jobs)
	if err != nil {
		return Submission{}, err
	}
	path := filepath.Join(root, spawn.Job, "submission.json")
	if err = noSymlinks(path); err != nil {
		return Submission{}, fail(exitStateConflict, "unsafe submission receipt; inspect the job directory before retrying")
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return Submission{}, err
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return Submission{}, err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		info, e := os.Lstat(path)
		if e != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 1<<20 {
			return Submission{}, fail(exitStateConflict, "submission receipt is not a private regular file; inspect the existing hub operation before repairing it")
		}
		old, e := os.ReadFile(path)
		var saved Submission
		if e != nil || json.Unmarshal(old, &saved) != nil {
			return Submission{}, fail(exitStateConflict, "submission receipt is incomplete; inspect the existing hub job before repairing it")
		}
		request.RequestID = saved.RequestID
		normalized, _ := json.Marshal(request)
		canonical, _ := json.Marshal(saved)
		if !bytes.Equal(normalized, canonical) {
			return Submission{}, fail(exitDuplicate, "saved submission differs from this command; replay the original flags and brief, or use a new job id for independent work")
		}
		return saved, nil
	}
	if err != nil {
		return Submission{}, err
	}
	if _, err = file.Write(raw); err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return Submission{}, err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return Submission{}, err
	}
	err = errors.Join(dir.Sync(), dir.Close())
	if err != nil {
		return Submission{}, err
	}
	return request, nil
}
