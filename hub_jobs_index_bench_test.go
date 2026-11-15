package panewire

import (
	"context"
	"errors"
	"testing"
)

// BenchmarkNodeHotPathJobScan times what the node daemon does per heartbeat and
// per quota-stop pane lookup on a quiet node with 3,000 retained job
// directories: the heartbeat (active-job scan), the relay outbox scan that
// follows it, and jobForPane. It uses only the daemon's own entry points, so
// the same file runs unchanged on the commit before the index
// (docs/job-event-index.md records both runs). Each sub-benchmark is one
// steady-state pass; the first pass, which fills the index, is not timed.
func BenchmarkNodeHotPathJobScan(b *testing.B) {
	f := newIndexFixture(b, 3000)
	client := &HubClient{
		jobsInboxRoot: f.root, assignedJobs: map[string]uint64{}, completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{},
		hostLoadCollector:   func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, errors.New("unavailable") },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, errors.New("unavailable") },
	}
	q := &quotaStopManager{root: f.root}
	ctx := context.Background()
	const pane = "w1:p2" // job 2 of the fixture is live and bound to it
	if job, ok := q.jobForPane(ctx, pane); !ok || job.JobID != f.jobs[2] {
		b.Fatalf("fixture pane lookup: %+v %v", job, ok)
	}
	heartbeat := func() { client.heartbeatEvent(ctx) }
	relay := func() { client.jobCompletionEvents() }
	lookup := func() { q.jobForPane(ctx, pane) }
	binding := func() { nodeJobHerdrBindingByPaneFromRoot(f.root, pane) }
	for _, bench := range []struct {
		name string
		pass func()
	}{
		{"heartbeat", heartbeat},
		{"relay_outbox", relay},
		{"jobForPane", lookup},
		{"pane_session", binding},
		{"heartbeat+relay_outbox+jobForPane", func() { heartbeat(); relay(); lookup() }},
	} {
		b.Run(bench.name, func(b *testing.B) {
			bench.pass()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				bench.pass()
			}
		})
	}
}
