//go:build linux

package panewire

import (
	"errors"
	"os"
	"strconv"
	"strings"
)

// nodeReadAddProcess reads pid's identity from /proc: the kernel's start time for
// that pid, in clock ticks since boot. ok is false when /proc cannot be read at
// all; a pid that no longer exists reports ok true with an empty Start. A start
// time recorded before a reboot is not comparable afterwards, but a rebooted
// host cannot still be running the recorded add, and a coincidence only makes
// recovery refuse (the safe direction).
func nodeReadAddProcess(pid int) (nodeAddProcess, bool) {
	if pid <= 0 {
		return nodeAddProcess{}, true
	}
	contents, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		if _, statErr := os.Stat("/proc"); statErr != nil {
			return nodeAddProcess{}, false
		}
		if errors.Is(err, os.ErrNotExist) {
			return nodeAddProcess{}, true
		}
		return nodeAddProcess{}, false
	}
	// The command is parenthesized and may itself hold spaces or ')', so the
	// remaining fields start after its last ')'.
	end := strings.LastIndexByte(string(contents), ')')
	if end < 0 {
		return nodeAddProcess{}, false
	}
	fields := strings.Fields(string(contents[end+1:]))
	// state is field 3 of proc_pid_stat(5), so starttime (field 22) is fields[19].
	if len(fields) < 20 {
		return nodeAddProcess{}, false
	}
	return nodeAddProcess{PID: pid, Start: fields[19]}, true
}
