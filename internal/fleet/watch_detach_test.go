package fleet

import (
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
	buf := make([]byte, 64)
	n, err := out.Read(buf)
	if err != nil {
		t.Fatalf("daemon helper did not start its children: %v", err)
	}
	plain, err := strconv.Atoi(strings.TrimSpace(string(buf[:n])))
	if err != nil {
		t.Fatalf("helper output %q", buf[:n])
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

	// Stop the daemon the way a group-wide stop does.
	if err := syscall.Kill(-daemon, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	gone := func(pid int) bool {
		// A killed child of the helper is reparented and reaped by init; poll until it is gone or a zombie.
		return !alive(pid)
	}
	deadline = time.Now().Add(10 * time.Second)
	for !gone(plain) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond) // the group signal is asynchronous
	}
	if !gone(plain) {
		t.Fatal("the harness did not stop the daemon's group: the plain child still runs")
	}
	if !alive(watcher) {
		t.Fatal("the detached watcher died with the daemon's process group")
	}
}

// alive reports a running process; a zombie counts as gone.
func alive(pid int) bool {
	if syscall.Kill(pid, 0) != nil {
		return false
	}
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		// The process may have been reaped between the two calls; without
		// /proc (macOS) the signal-0 answer stands.
		return syscall.Kill(pid, 0) == nil && !procAvailable()
	}
	fields := strings.Fields(string(raw[strings.LastIndex(string(raw), ")")+1:]))
	return len(fields) == 0 || fields[0] != "Z"
}

func procAvailable() bool {
	_, err := os.Stat("/proc/self/stat")
	return err == nil
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
