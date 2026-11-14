//go:build darwin

package panewire

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// nodeReadAddProcess reads pid's identity from ps: the wall-clock start time of
// that pid. ok is false when ps cannot answer at all; a pid that no longer
// exists reports ok true with an empty Start. Pid reuse inside the same second
// is the only way a stranger could read as the original add, and the kernel does
// not recycle a pid that fast.
func nodeReadAddProcess(pid int) (nodeAddProcess, bool) {
	if pid <= 0 {
		return nodeAddProcess{}, true
	}
	cmd := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "lstart=")
	// UTC and the C locale keep the recorded string stable across a restart that
	// runs under another locale or time zone.
	cmd.Env = append(os.Environ(), "LC_ALL=C", "TZ=UTC")
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && len(bytes.TrimSpace(out)) == 0 {
			return nodeAddProcess{}, true
		}
		return nodeAddProcess{}, false
	}
	start := strings.Join(strings.Fields(string(out)), " ")
	if start == "" {
		return nodeAddProcess{}, true
	}
	return nodeAddProcess{PID: pid, Start: start}, true
}
