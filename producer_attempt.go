package panewire

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/mgh3326/panewire/internal/jobowner"
)

var errMalformedAttemptBinding = errors.New("malformed attempt binding")

// boundAttemptEpoch is the grant epoch the node bound into this process tree's
// launch environment for job, or 0 when the tree carries no identity for it.
// Identity for another job (a worker that runs a command about a sibling job)
// counts as none. A present but malformed value is an error, never "absent":
// a damaged identity must not fall back to the weaker no-identity rule.
func boundAttemptEpoch(job string) (uint64, error) {
	value := os.Getenv(jobowner.EpochEnv)
	if value == "" || os.Getenv(jobowner.JobEnv) != job {
		return 0, nil
	}
	epoch, err := strconv.ParseUint(value, 10, 64)
	if err != nil || epoch == 0 {
		return 0, fmt.Errorf("%w: %s is not a grant epoch", errMalformedAttemptBinding, jobowner.EpochEnv)
	}
	return epoch, nil
}

// producerEpoch is the one place a producer call learns which epoch it records
// (docs/producer-attempt-grant.md). explicit is a --epoch value or 0.
func producerEpoch(jobsRoot, job string, explicit uint64) (uint64, error) {
	if !hubJobIDPattern.MatchString(job) {
		// Legacy-only job IDs cannot receive a hub grant.
		if explicit > 1 {
			return 0, fmt.Errorf("%w: job %s has no hub grant", jobowner.ErrAttemptAhead, job)
		}
		return 1, nil
	}
	bound, err := boundAttemptEpoch(job)
	if err != nil {
		return 0, err
	}
	return jobowner.ProducerEpoch(jobsRoot, job, bound, explicit)
}

// producerRefusal reports whether err is a refusal of the caller's attempt
// identity, as opposed to an unreadable grant.
func producerRefusal(err error) bool {
	return errors.Is(err, jobowner.ErrStaleAttempt) || errors.Is(err, jobowner.ErrAttemptAhead) ||
		errors.Is(err, jobowner.ErrIdentityRequired) || errors.Is(err, jobowner.ErrRevoked) || errors.Is(err, jobowner.ErrEpochConflict) ||
		errors.Is(err, errMalformedAttemptBinding)
}
