package panewire

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func newTaskPaneSweepClient(t *testing.T, interval time.Duration) *HubClient {
	t.Helper()
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: t.TempDir(), AllowInsecureForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	client.taskPaneSweepInterval = interval
	return client
}

func TestTaskPaneSweepRunsOnEveryTickUntilCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newTaskPaneSweepClient(t, time.Minute)
	ticks := make(chan time.Time)
	var passes atomic.Int32
	client.taskPaneSweepTicks = ticks
	client.taskPaneSweepPass = func(context.Context) { passes.Add(1) }
	done := make(chan struct{})
	go func() { client.runTaskPaneSweep(ctx); close(done) }()
	ticks <- time.Now()
	ticks <- time.Now()
	eventually(t, time.Second, "two sweep passes", func() bool { return passes.Load() == 2 })
	cancel()
	eventually(t, time.Second, "sweep loop shutdown", func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
}

func TestTaskPaneSweepStaysOffWithoutAnIntervalOrJobsRoot(t *testing.T) {
	for name, build := range map[string]func() *HubClient{
		"zero interval": func() *HubClient { return newTaskPaneSweepClient(t, 0) },
		"no jobs root": func() *HubClient {
			client := newTaskPaneSweepClient(t, time.Minute)
			client.jobsInboxRoot = ""
			return client
		},
	} {
		t.Run(name, func(t *testing.T) {
			client := build()
			client.taskPaneSweepTicks = make(chan time.Time)
			client.taskPaneSweepPass = func(context.Context) { t.Error("sweep ran") }
			finished := make(chan struct{})
			go func() { client.runTaskPaneSweep(context.Background()); close(finished) }()
			eventually(t, time.Second, "disabled sweep returns at once", func() bool {
				select {
				case <-finished:
					return true
				default:
					return false
				}
			})
		})
	}
}

func TestTaskPaneSweepDaemonFlags(t *testing.T) {
	root := t.TempDir()
	envPath := filepath.Join(root, "node.env")
	if err := os.WriteFile(envPath, []byte("HUB_MACHINE_ID=node-a\nHUB_TOKEN="+r6NodeAToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	base := []string{"--socket", filepath.Join(root, "daemon.sock"), "--hub-url", "ws://fixture.invalid", "--hub-token-env", envPath}
	daemon, code, err := newDaemonForCLI(base, daemonCLIDeps{AllowInsecureForTests: true})
	if err != nil || code != ExitOK || daemon.cfg.Hub.Client.taskPaneSweepInterval != 5*time.Minute || daemon.cfg.Hub.Client.taskPaneSweepGrace != 10*time.Minute {
		t.Fatalf("defaults: daemon=%v code=%d err=%v", daemon, code, err)
	}
	daemon, code, err = newDaemonForCLI(append(append([]string(nil), base...), "--task-pane-sweep-interval", "1m", "--task-pane-sweep-grace", "20m"), daemonCLIDeps{AllowInsecureForTests: true})
	if err != nil || code != ExitOK || daemon.cfg.Hub.Client.taskPaneSweepInterval != time.Minute || daemon.cfg.Hub.Client.taskPaneSweepGrace != 20*time.Minute {
		t.Fatalf("configured: daemon=%v code=%d err=%v", daemon, code, err)
	}
	daemon, code, err = newDaemonForCLI(append(append([]string(nil), base...), "--task-pane-sweep-interval", "0"), daemonCLIDeps{AllowInsecureForTests: true})
	if err != nil || code != ExitOK || daemon.cfg.Hub.Client.taskPaneSweepInterval != 0 {
		t.Fatalf("off: daemon=%v code=%d err=%v", daemon, code, err)
	}
	for _, bad := range [][]string{{"--task-pane-sweep-interval", "-1m"}, {"--task-pane-sweep-interval", "1s"}, {"--task-pane-sweep-grace", "-1m"}} {
		if daemon, code, err = newDaemonForCLI(append(append([]string(nil), base...), bad...), daemonCLIDeps{AllowInsecureForTests: true}); daemon != nil || code != ExitConditionInvalid || err == nil {
			t.Fatalf("%v accepted: daemon=%v code=%d err=%v", bad, daemon, code, err)
		}
	}
}
