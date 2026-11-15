package panewire

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 4625: a pool that publishes no quota reading of its own still starts the
// exhausted-pool sweep. Two triggers do it: every catalog row of the pool
// retired, or a job pane showing the provider's credit-exhausted rejection.

const (
	poolTriggerJobAccount = "anthropic-api:0123456789abcdef"
	poolOpenAIJobAccount  = "openai-api:fedcba9876543210"
)

// retiredCatalog is an App whose catalog has every anthropic-api row retired
// and a live claude row, so only the retired pool can fire.
func retiredCatalog(f *sweepFixture) func(io.Writer, io.Writer) (*fleet.App, error) {
	retired := "2026-11-13T00:00:00Z"
	return func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: f.jobs, Out: out, Err: errout, BenchCatalog: func(context.Context) ([]fleet.CatalogRow, error) {
			return []fleet.CatalogRow{
				{Profile: "omp-luna", Effort: "high", Grade: "A", ModelID: "claude-opus", Pool: "anthropic-api", RetiredAt: &retired},
				{Profile: "omp-sol", Effort: "high", Grade: "A", ModelID: "claude-sonnet", Pool: "anthropic-api", RetiredAt: &retired},
				{Profile: "sonnet", Effort: "high", Grade: "A", ModelID: "claude-sonnet", Pool: "claude"},
			}, nil
		}}, nil
	}
}

// liveCatalog is an App whose catalog keeps every pool's rows usable.
func liveCatalog(f *sweepFixture) func(io.Writer, io.Writer) (*fleet.App, error) {
	return func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: f.jobs, Out: out, Err: errout, BenchCatalog: func(context.Context) ([]fleet.CatalogRow, error) {
			return []fleet.CatalogRow{
				{Profile: "omp-luna", Effort: "high", Grade: "A", ModelID: "claude-opus", Pool: "anthropic-api"},
				{Profile: "omp-sol", Effort: "high", Grade: "A", ModelID: "claude-sonnet", Pool: "anthropic-api"},
			}, nil
		}}, nil
	}
}

// setScreen makes the pane of job show screen.
func (f *sweepFixture) setScreen(t *testing.T, job, screen string) {
	t.Helper()
	f.node2.mu.Lock()
	for pane, row := range f.node2.panes {
		if row.Name == job {
			f.node2.screens[pane] = screen
			f.node2.mu.Unlock()
			return
		}
	}
	f.node2.mu.Unlock()
	t.Fatalf("job %s has no pane on the holder node", job)
}

// noticeJobs counts the notices each job received. Every notice names its job
// as "for job <id>;".
func noticeJobs(events []hubRelayInjectEvent) map[string]int {
	counts := map[string]int{}
	for _, event := range events {
		at := strings.Index(event.Text, "for job ")
		if at < 0 {
			continue
		}
		rest := event.Text[at+len("for job "):]
		if end := strings.IndexByte(rest, ';'); end > 0 {
			counts[rest[:end]]++
		}
	}
	return counts
}

func TestPoolExhaustedRetiredCatalogNotifiesLiveJobs(t *testing.T) {
	f := newSweepFixture(t)
	f.hub.spawnNewApp = retiredCatalog(f)
	jobs := []string{
		f.addNamedJob(t, "omp-luna", "implementer", "anthropic-api", poolTriggerJobAccount, "idle", "sweep-1", 1),
		f.addNamedJob(t, "omp-sol", "reviewer", "anthropic-api", poolTriggerJobAccount, "idle", "sweep-2", 2),
		f.addNamedJob(t, "omp-luna", "consult", "anthropic-api", poolTriggerJobAccount, "working", "sweep-3", 3),
	}
	other := f.addNamedJob(t, "sonnet", "implementer", "claude", "claude:0123456789abcdef", "idle", "sweep-4", 4)

	f.hub.checkPoolTriggers(t.Context())
	eventually(t, 5*time.Second, "the retired pool's sweep", func() bool { return f.sweeps() == 1 })
	events := spawnFailedInjections(f.destination)
	if counts := noticeJobs(events); len(counts) != len(jobs) {
		t.Fatalf("notices=%+v, want one per job %v", events, jobs)
	} else {
		for _, job := range jobs {
			if counts[job] != 1 {
				t.Fatalf("job %s notices=%d, want one: %+v", job, counts[job], events)
			}
		}
	}
	if counts := noticeJobs(events); counts[other] != 0 {
		t.Fatalf("a live pool's job was notified: %+v", events)
	}
	for _, event := range events {
		if !strings.Contains(event.Text, "hand it over or end it") || !strings.Contains(event.Text, "no live catalog row left") {
			t.Fatalf("notice does not name the retirement and ask the owner to act: %+v", event)
		}
	}

	// The job stays live: the sweep only notifies.
	for _, job := range jobs {
		if state := f.state(job); state != "spawned" {
			t.Fatalf("job %s state=%s, want spawned", job, state)
		}
		if closed := f.node2.count("pane.close " + job); closed != 0 {
			t.Fatalf("job %s pane closed %d times", job, closed)
		}
	}

	// A second pass over the same retirement notifies nobody again.
	f.hub.checkPoolTriggers(t.Context())
	if extra := spawnFailedInjections(f.destination); len(extra) != 0 {
		t.Fatalf("duplicate notices: %+v", extra)
	}
}

func TestPoolExhaustedCreditScreenNotifiesLiveJobs(t *testing.T) {
	f := newSweepFixture(t)
	jobs := []string{
		f.addNamedJob(t, "omp-luna", "implementer", "anthropic-api", poolTriggerJobAccount, "idle", "sweep-1", 1),
		f.addNamedJob(t, "omp-sol", "reviewer", "anthropic-api", poolTriggerJobAccount, "idle", "sweep-2", 2),
	}
	other := f.addNamedJob(t, "gpt", "implementer", "openai-api", poolOpenAIJobAccount, "idle", "sweep-3", 3)
	f.setScreen(t, "sweep-1", "⏺ API Error: 400 Your credit balance is too low to access the Anthropic API\n\n❯ \n")

	f.hub.checkPoolTriggers(t.Context())
	eventually(t, 5*time.Second, "the credit-exhausted pool's sweep", func() bool { return f.sweeps() == 1 })
	events := spawnFailedInjections(f.destination)
	counts := noticeJobs(events)
	for _, job := range jobs {
		if counts[job] != 1 {
			t.Fatalf("job %s notices=%d, want one: %+v", job, counts[job], events)
		}
	}
	if f.node2.count("pane.read sweep-1") == 0 {
		t.Fatal("the sweep never read the pane showing the rejection")
	}
	if counts[other] != 0 {
		t.Fatalf("another pool's job was notified: %+v", events)
	}
	for _, event := range events {
		if !strings.Contains(event.Text, "no provider credit left") {
			t.Fatalf("notice does not name the credit exhaustion: %+v", event)
		}
	}

	f.hub.checkPoolTriggers(t.Context())
	if extra := spawnFailedInjections(f.destination); len(extra) != 0 {
		t.Fatalf("duplicate notices: %+v", extra)
	}
}

// The OpenAI equivalent of the Anthropic rejection (the quota-stop fixtures'
// openai provider error) names the openai-api pool.
func TestPoolExhaustedCreditScreenNamesOpenAIPool(t *testing.T) {
	f := newSweepFixture(t)
	f.addNamedJob(t, "gpt", "implementer", "openai-api", poolOpenAIJobAccount, "idle", "sweep-1", 1)
	f.addNamedJob(t, "omp-luna", "implementer", "anthropic-api", poolTriggerJobAccount, "idle", "sweep-2", 2)
	f.setScreen(t, "sweep-1", "⏺ API Error: 429 {\"error\":{\"code\":\"insufficient_quota\",\"message\":\"You exceeded your current quota\"}}\n\n❯ \n")

	f.hub.checkPoolTriggers(t.Context())
	eventually(t, 5*time.Second, "the openai-api sweep", func() bool { return f.sweeps() == 1 })
	events := spawnFailedInjections(f.destination)
	counts := noticeJobs(events)
	if counts["sweep-1"] != 1 {
		t.Fatalf("openai-api job notices=%+v, want one", events)
	}
	if counts["sweep-2"] != 0 {
		t.Fatalf("anthropic-api job was notified: %+v", events)
	}
}

// A pool with a useful catalog row and no credit text is not an exhausted pool:
// neither trigger fires.
func TestPoolExhaustedTriggersStayQuietForLivePool(t *testing.T) {
	f := newSweepFixture(t)
	f.hub.spawnNewApp = liveCatalog(f)
	f.addNamedJob(t, "omp-luna", "implementer", "anthropic-api", poolTriggerJobAccount, "idle", "sweep-1", 1)
	f.setScreen(t, "sweep-1", "I pushed the fix and opened the PR.\n\n❯ \n")

	f.hub.checkPoolTriggers(t.Context())
	f.hub.checkPoolTriggers(t.Context())
	// The window proves nothing more happens; the passes above are synchronous.
	time.Sleep(200 * time.Millisecond)
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("a live pool's owner was notified: %+v", events)
	}
	if sweeps := f.sweeps(); sweeps != 0 {
		t.Fatalf("a live pool's sweep ran %d times", sweeps)
	}
}

// A rejection quoted by an answer is not a stop: the credit line must be the
// last agent activity, the quota-stop reader's rule.
func TestPoolExhaustedCreditScreenQuotedInAnswerDoesNotFire(t *testing.T) {
	f := newSweepFixture(t)
	f.addNamedJob(t, "omp-luna", "implementer", "anthropic-api", poolTriggerJobAccount, "idle", "sweep-1", 1)
	f.setScreen(t, "sweep-1", "API Error: 400 Your credit balance is too low to access the Anthropic API\nI will retry the request with a smaller prompt.\n")

	f.hub.checkPoolTriggers(t.Context())
	time.Sleep(200 * time.Millisecond)
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("a quoted rejection was taken for a stop: %+v", events)
	}
}

// A pool-level cause reaches every account's jobs on the pool: the trigger is
// about the pool, not one account's bucket.
func TestPoolExhaustedPoolWideCauseReachesEveryAccount(t *testing.T) {
	f := newSweepFixture(t)
	jobs := []string{
		f.addJob(t, "implementer", "devin", sweepDevin, "idle"),
		f.addJob(t, "reviewer", "devin", "devin:1111111111111111", "idle"),
	}
	f.hub.sweepExhaustedPool(t.Context(), poolExhaustion{Pool: "devin", Cause: poolCauseRetired})
	counts := noticeJobs(spawnFailedInjections(f.destination))
	for _, job := range jobs {
		if counts[job] != 1 {
			t.Fatalf("job %s notices=%d, want one; counts=%+v", job, counts[job], counts)
		}
	}
}
