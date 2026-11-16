package fleet

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestWatcherOutlivesDaemonGroupStop runs a stand-in daemon in its own process
// group, has it start a real detached watcher, then stops the daemon by
// signalling its whole group — what launchd does without AbandonProcessGroup.
// The watcher keeps running; a plain child of the daemon does not.
func TestWatcherOutlivesDaemonGroupStop(t *testing.T) {
	dir := t.TempDir()
	watcherPID := filepath.Join(dir, "watcher.pid")
	script := filepath.Join(dir, "fake-panewire")
	// The stand-in watcher records its pid and idles like a watcher waiting for a report.
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho $$ > "+watcherPID+"\nexec sleep 60\n"), 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestWatchDetachDaemonHelper$", "--", dir, script)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	daemon := cmd.Process.Pid
	t.Cleanup(func() { _ = syscall.Kill(-daemon, syscall.SIGKILL); _ = cmd.Wait() })
	// The helper prints the pid of its plain child once the watcher is started.
	// Read the whole line: one short read could carry only a prefix of the pid.
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil {
		t.Fatalf("daemon helper did not start its children: %v", err)
	}
	plain, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		t.Fatalf("helper output %q", line)
	}
	var watcher int
	deadline := time.Now().Add(10 * time.Second)
	for watcher == 0 && time.Now().Before(deadline) {
		if raw, err := os.ReadFile(watcherPID); err == nil {
			watcher, _ = strconv.Atoi(strings.TrimSpace(string(raw)))
		}
		time.Sleep(5 * time.Millisecond) // polling for the stand-in watcher's pid file
	}
	if watcher == 0 {
		t.Fatal("watcher never started")
	}
	t.Cleanup(func() { _ = syscall.Kill(watcher, syscall.SIGKILL) })
	if pgid, _ := syscall.Getpgid(watcher); pgid == daemon {
		t.Fatalf("watcher shares the daemon's process group %d", daemon)
	}

	// Stop the daemon the way a group-wide stop does: the plain child dies with
	// the group. The observation that ends the wait is the one asserted, never a
	// fresh read of it — a reaped task is reported as dead (state X) after it was
	// a zombie (state Z), so a second read can disagree with the first although
	// the child never ran again (hk 4877).
	if err := syscall.Kill(-daemon, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	stopped := !alive(plain)
	deadline = time.Now().Add(10 * time.Second)
	for !stopped && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond) // the group signal is asynchronous
		stopped = !alive(plain)
	}
	if !stopped {
		t.Fatal("the harness did not stop the daemon's group: the plain child still runs")
	}
	if !alive(watcher) {
		t.Fatal("the detached watcher died with the daemon's process group")
	}
}

// alive reports a process that can still run. A task that has exited counts as
// gone: /proc reports it first as a zombie (state Z) and, while it is reaped, as
// dead (state X), before its entry disappears. Neither state can run, so reading
// only "Z" as dead made a finished wait look unfinished (hk 4877).
func alive(pid int) bool {
	if syscall.Kill(pid, 0) != nil {
		return false
	}
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		// The entry can go between the two calls while the pid is still found,
		// and without /proc (macOS) the signal-0 answer is all there is. Only a
		// failed signal comes back as gone, so a read race cannot cut a live
		// wait short.
		return true
	}
	return taskRunnable(raw)
}

// taskRunnable reports whether a /proc/<pid>/stat body describes a task that can
// still run. A dead task ("X") is in the reap window that follows its zombie
// state ("Z"). A body with no state letter is no evidence of death, so it keeps
// counting as running.
func taskRunnable(stat []byte) bool {
	switch taskState(stat) {
	case "Z", "X":
		return false
	}
	return true
}

// taskState returns the state letter of a /proc/<pid>/stat body: the field after
// the parenthesised command name. A body with no such field returns "".
func taskState(stat []byte) string {
	body := string(stat)
	i := strings.LastIndex(body, ")")
	if i < 0 {
		return ""
	}
	fields := strings.Fields(body[i+1:])
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

// TestTaskStateDeadIsNotRunning pins the state letters the harness reads as
// stopped. A killed task stays a zombie (Z) until its parent or init reaps it,
// and /proc then reports the task as dead (X) for one read before the entry
// disappears; treating X as running made a finished wait fail (hk 4877).
func TestTaskStateDeadIsNotRunning(t *testing.T) {
	const tail = " 1 1234 1234 0 -1 4194560 79 0 0 0 0 0 0 0 20 0 1 0 425 4243 0"
	for _, c := range []struct {
		stat string
		want bool
	}{
		{"1234 (sleep) S" + tail, true},
		{"1234 (sleep) R" + tail, true},
		{"1234 (sleep) D" + tail, true},
		{"1234 (sleep) Z" + tail, false},
		{"1234 (sleep) X" + tail, false},
		{"", true},
	} {
		if got := taskRunnable([]byte(c.stat)); got != c.want {
			t.Errorf("taskRunnable(%q) = %v, want %v", c.stat, got, c.want)
		}
	}
}

// TestWatchDetachDaemonHelper is the stand-in daemon; it does nothing in a normal run.
func TestWatchDetachDaemonHelper(t *testing.T) {
	args := flag.Args()
	if len(args) != 2 {
		t.Skip("helper for TestWatcherOutlivesDaemonGroupStop")
	}
	dir, exe := args[0], args[1]
	watchExecutable = func() (string, error) { return exe, nil }
	a := &App{Runner: ExecRunner{}, Jobs: filepath.Join(dir, "jobs"), HerdrSession: "s"}
	if err := a.startWatch("job-x"); err != nil {
		t.Fatal(err)
	}
	plain := exec.Command("sleep", "60")
	if err := plain.Start(); err != nil {
		t.Fatal(err)
	}
	fmt.Println(plain.Process.Pid)
	_ = os.Stdout.Sync()
	time.Sleep(time.Minute) // the daemon idles until the test stops its group
}
