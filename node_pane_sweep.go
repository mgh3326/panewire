package panewire

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The task pane sweep closes the pane of an implementer once its bound hk
// task is merged, done or dropped and the pane has stayed not working for a
// grace (docs/pane-sweep.md). The rules and the close live in
// fleet.TaskPaneSweeper; this file is the daemon loop around it.
const defaultTaskPaneSweepInterval = 5 * time.Minute

// minTaskPaneSweepInterval keeps a misconfigured loop from hammering herdr and
// hk. The grace has no minimum: a test or an operator may want none.
const minTaskPaneSweepInterval = 10 * time.Second

// jobDirTTLEvery spaces the job dir TTL passes the sweep loop runs: each one
// reads hk for every dir past the TTL, which a 5-minute cadence does not
// need.
const jobDirTTLEvery = time.Hour

func (client *HubClient) runTaskPaneSweep(ctx context.Context) {
	if client.jobsInboxRoot == "" || client.taskPaneSweepInterval <= 0 {
		return
	}
	sweeper := &fleet.TaskPaneSweeper{
		Grace: client.taskPaneSweepGrace,
		Info:  slog.Info,
		Exclusive: func() func() {
			// One acting command at a time per node, so a sweep close never
			// interleaves with a pane.close or job.done from the hub.
			client.nodeCommandMu.Lock()
			return client.nodeCommandMu.Unlock
		},
	}
	expiry := &fleet.JobDirTTL{Every: jobDirTTLEvery}
	pass := client.taskPaneSweepPass
	if pass == nil {
		pass = func(ctx context.Context) { client.sweepTaskPanes(ctx, sweeper, expiry) }
	}
	ticks := client.taskPaneSweepTicks
	if ticks == nil {
		ticker := time.NewTicker(client.taskPaneSweepInterval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			pass(ctx)
		}
	}
}

// sweepTaskPanes runs one pass over this node's jobs. It builds the fleet app
// afresh, as the node's job.done does, so a changed config or hk credential is
// picked up; the sweeper keeps the idle times between passes.
func (client *HubClient) sweepTaskPanes(ctx context.Context, sweeper *fleet.TaskPaneSweeper, expiry *fleet.JobDirTTL) {
	app, err := fleet.New(slogWarnWriter{}, slogWarnWriter{})
	if err != nil {
		slog.Warn("task pane sweep skipped", "reason", "app-unavailable")
		return
	}
	app.Jobs = filepath.Join(client.jobsInboxRoot, "jobs")
	app.HerdrSession = client.nodeSpawnSession()
	if app.JobStore, err = client.nodeJobStoreForCommand(); err != nil {
		slog.Warn("task pane sweep skipped", "reason", "hk-unavailable")
		return
	}
	sweeper.TaskState, sweeper.PR = client.nodeTaskState, client.nodePR
	if workRoot, err := client.nodeWorkRoot(); err == nil {
		// Only a worktree of a clone under this root is the node's to remove
		// (sweep_worktree.go); without the root nothing proves one is.
		sweeper.WorkRoot = workRoot
	}
	if sweeper.TaskState == nil || sweeper.PR == nil {
		env, err := client.nodeHandoffkeepEnv()
		if err == nil {
			if sweeper.TaskState == nil {
				sweeper.TaskState, err = fleet.NewTaskStateReader(env.URL, env.Token, env.CFID, env.CFSecret)
			}
			if err == nil && sweeper.PR == nil {
				sweeper.PR, err = fleet.NewPRReader(env.URL, env.Token, env.CFID, env.CFSecret)
			}
		}
		if err != nil {
			slog.Warn("task pane sweep skipped", "reason", "hk-unavailable")
			return
		}
	}
	if app.BenchStore == nil {
		if env, err := client.nodeHandoffkeepEnv(); err == nil {
			app.BenchStore, _ = fleet.NewBenchStore(env.URL, env.Token, env.CFID, env.CFSecret)
		}
	}
	if err := sweeper.Pass(ctx, app); err != nil {
		slog.Warn("task pane sweep failed", "reason", "jobs-unreadable")
	}
	client.expireJobDirs(ctx, app, expiry)
}

// expireJobDirs runs the job dir TTL (docs/job-dir-ttl.md) after the pane
// sweep, deleting, only when the node's hosts config sets [reap]
// job_dir_ttl. Unset, it does nothing: a deploy changes no dir until the key
// is set. Each locked re-check and delete holds the node's command lock, as
// a sweep close does.
func (client *HubClient) expireJobDirs(ctx context.Context, app *fleet.App, expiry *fleet.JobDirTTL) {
	ttl, ok := fleet.ConfiguredJobDirTTL(app.Config)
	if !ok {
		return
	}
	expiry.TTL, expiry.Apply = ttl, true
	expiry.Exclusive = func() func() {
		client.nodeCommandMu.Lock()
		return client.nodeCommandMu.Unlock
	}
	if _, err := expiry.Pass(ctx, app); err != nil {
		slog.Warn("job dir ttl failed", "reason", "jobs-unreadable")
	}
}

// slogWarnWriter sends the fleet app's warning lines to the daemon's log.
type slogWarnWriter struct{}

var _ io.Writer = slogWarnWriter{}

func (slogWarnWriter) Write(p []byte) (int, error) {
	for _, line := range bytes.Split(bytes.TrimRight(p, "\n"), []byte("\n")) {
		slog.Warn("task pane sweep", "detail", string(line))
	}
	return len(p), nil
}
