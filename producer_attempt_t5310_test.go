package panewire

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/jobowner"
)

// hk 5310: a producer call presents the attempt its process tree was launched
// as (ARBITER_JOB_EPOCH) or an explicit --epoch, and the node's current grant
// must equal it (docs/producer-attempt-grant.md).

const producerJob = "attempt-job"

type producerFixture struct {
	root, jobs, report, socket string
	pushes                     *r20t4Pushes
}

// newProducerFixture is a builder job whose current grant is grant. The launch
// environment starts with no identity; a test binds one with bindAttempt.
func newProducerFixture(t *testing.T, grant uint64) *producerFixture {
	t.Helper()
	root := t.TempDir()
	f := &producerFixture{root: root, jobs: filepath.Join(root, "jobs"), report: filepath.Join(root, "report.md"), pushes: &r20t4Pushes{}}
	events := filepath.Join(f.jobs, producerJob, "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	claim := `{"kind":"job.claim","job_id":"` + producerJob + `","payload":{"agent_label":"worker-a","owner_lane":"lane-a","parent_lane":"lane-p","role":"builder","pane_id":"w:p1"}}`
	if err := os.WriteFile(filepath.Join(events, "00001-job.claim.json"), []byte(claim), 0600); err != nil {
		t.Fatal(err)
	}
	if grant > 1 {
		if err := jobowner.Assign(f.jobs, producerJob, grant); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(f.report, []byte("result\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ARBITER_INBOX_ROOT", f.jobs)
	t.Setenv("HANDOFFKEEP_URL", "")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "")
	t.Setenv("PANEWIRE_HK_ENV", filepath.Join(root, "no-service-credential"))
	t.Setenv(jobowner.JobEnv, "")
	t.Setenv(jobowner.EpochEnv, "")
	f.socket = r20t4Socket(t, f.pushes.add)
	return f
}

func bindAttempt(t *testing.T, job string, epoch uint64) {
	t.Helper()
	t.Setenv(jobowner.JobEnv, job)
	t.Setenv(jobowner.EpochEnv, strconv.FormatUint(epoch, 10))
}

// eventFiles lists the job's event files: the claim plus whatever a call wrote.
func (f *producerFixture) eventFiles(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(f.jobs, producerJob, "events"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func (f *producerFixture) pushCount() int {
	f.pushes.mu.Lock()
	defer f.pushes.mu.Unlock()
	return len(f.pushes.records)
}

// produce runs one producer command and returns its status and stderr.
func (f *producerFixture) produce(t *testing.T, command string, extra ...string) (int, string) {
	t.Helper()
	var stderr bytes.Buffer
	cfg := CLIConfig{SocketPath: f.socket}
	switch command {
	case "done":
		return runJobCLI(append([]string{"done", producerJob, "--report", f.report}, extra...), &bytes.Buffer{}, &stderr, cfg), stderr.String()
	case "escalate":
		return runJobCLI(append([]string{"escalate", producerJob, "--question", "which branch?"}, extra...), &bytes.Buffer{}, &stderr, cfg), stderr.String()
	case "joined":
		return runJobCLI(append([]string{"joined", producerJob, "--pr", "o/r#1", "--head", "abc123", "--report", f.report}, extra...), &bytes.Buffer{}, &stderr, cfg), stderr.String()
	case "emit":
		args := append([]string{"--kind", "job.completed", "--job", producerJob, "--report", f.report, "--owner-lane", "lane-a", "--inbox-root", f.root}, extra...)
		return runEmitCLI(args, &bytes.Buffer{}, &stderr, cfg), stderr.String()
	}
	t.Fatalf("unknown command %q", command)
	return 0, ""
}

func TestProducerCallsRefuseSupersededAttemptAndWriteNothing_t5310(t *testing.T) {
	for _, command := range []string{"done", "escalate", "joined", "emit"} {
		t.Run(command, func(t *testing.T) {
			f := newProducerFixture(t, 3)
			bindAttempt(t, producerJob, 1)
			before := f.eventFiles(t)
			rc, stderr := f.produce(t, command)
			if rc == ExitOK || !strings.Contains(stderr, "stale attempt") || !strings.Contains(stderr, "attempt epoch 1, current grant 3") {
				t.Fatalf("rc=%d stderr=%q, want a refusal naming the stale attempt", rc, stderr)
			}
			if after := f.eventFiles(t); strings.Join(after, ",") != strings.Join(before, ",") {
				t.Fatalf("a refused call wrote %v", after)
			}
			if got := f.pushCount(); got != 0 {
				t.Fatalf("a refused call pushed %d records", got)
			}
		})
	}
}

func TestProducerCallsRecordTheBoundEpoch_t5310(t *testing.T) {
	for _, command := range []string{"done", "escalate", "joined", "emit"} {
		t.Run(command, func(t *testing.T) {
			f := newProducerFixture(t, 3)
			bindAttempt(t, producerJob, 3)
			if rc, stderr := f.produce(t, command); rc != ExitOK {
				t.Fatalf("rc=%d stderr=%q", rc, stderr)
			}
			pushed := f.pushes.await(t, 1)
			if pushed[0].Epoch != 3 {
				t.Fatalf("pushed epoch=%d, want the bound 3", pushed[0].Epoch)
			}
		})
	}
}

func TestProducerCallWithoutIdentity_t5310(t *testing.T) {
	for _, command := range []string{"done", "escalate", "joined", "emit"} {
		t.Run(command, func(t *testing.T) {
			// Epoch 1: a worker launched before the binding existed keeps working.
			f := newProducerFixture(t, 1)
			if rc, stderr := f.produce(t, command); rc != ExitOK {
				t.Fatalf("no identity at epoch 1: rc=%d stderr=%q", rc, stderr)
			}
			if got := f.pushes.await(t, 1)[0].Epoch; got != 1 {
				t.Fatalf("epoch=%d, want 1", got)
			}

			// Epoch 3: refused, naming the missing identity, and nothing is written.
			f = newProducerFixture(t, 3)
			before := f.eventFiles(t)
			rc, stderr := f.produce(t, command)
			if rc == ExitOK || !strings.Contains(stderr, "attempt identity required") || !strings.Contains(stderr, "--epoch 3") {
				t.Fatalf("no identity at epoch 3: rc=%d stderr=%q", rc, stderr)
			}
			if after := f.eventFiles(t); strings.Join(after, ",") != strings.Join(before, ",") || f.pushCount() != 0 {
				t.Fatalf("a refused call wrote %v and pushed %d", after, f.pushCount())
			}

			// An operator or parent call names the current grant.
			if rc, stderr := f.produce(t, command, "--epoch", "3"); rc != ExitOK {
				t.Fatalf("--epoch 3 at epoch 3: rc=%d stderr=%q", rc, stderr)
			}
			if got := f.pushes.await(t, 1)[0].Epoch; got != 3 {
				t.Fatalf("epoch=%d, want 3", got)
			}
		})
	}
}

func TestProducerExplicitEpochMustBeTheCurrentGrant_t5310(t *testing.T) {
	for _, command := range []string{"done", "escalate", "joined", "emit"} {
		t.Run(command, func(t *testing.T) {
			f := newProducerFixture(t, 3)
			for _, tc := range []struct{ epoch, reason string }{
				{"1", "stale attempt"},
				{"4", "ahead of the node's grant"},
			} {
				rc, stderr := f.produce(t, command, "--epoch", tc.epoch)
				if rc == ExitOK || !strings.Contains(stderr, tc.reason) {
					t.Fatalf("--epoch %s: rc=%d stderr=%q, want %q", tc.epoch, rc, stderr, tc.reason)
				}
			}
			if len(f.eventFiles(t)) != 1 || f.pushCount() != 0 {
				t.Fatalf("a refused call wrote %v and pushed %d", f.eventFiles(t), f.pushCount())
			}
		})
	}
}

// The bound epoch is immutable for the tree: a worker cannot lift itself to the
// new grant with --epoch.
func TestProducerExplicitEpochCannotOverrideTheBoundOne_t5310(t *testing.T) {
	for _, command := range []string{"done", "emit"} {
		t.Run(command, func(t *testing.T) {
			f := newProducerFixture(t, 3)
			bindAttempt(t, producerJob, 1)
			rc, stderr := f.produce(t, command, "--epoch", "3")
			if rc == ExitOK || !strings.Contains(stderr, "conflicts with the epoch bound at launch") {
				t.Fatalf("rc=%d stderr=%q", rc, stderr)
			}
			if len(f.eventFiles(t)) != 1 || f.pushCount() != 0 {
				t.Fatalf("a refused call wrote %v and pushed %d", f.eventFiles(t), f.pushCount())
			}
		})
	}
}

func TestProducerBindingForAnotherJobIsNoIdentity_t5310(t *testing.T) {
	f := newProducerFixture(t, 3)
	bindAttempt(t, "sibling-job", 3)
	rc, stderr := f.produce(t, "done")
	if rc == ExitOK || !strings.Contains(stderr, "attempt identity required") {
		t.Fatalf("rc=%d stderr=%q, want the sibling's binding ignored", rc, stderr)
	}
}

func TestProducerMalformedBindingFailsClosed_t5310(t *testing.T) {
	for _, value := range []string{"abc", "0", "-1", "1.5"} {
		t.Run(value, func(t *testing.T) {
			f := newProducerFixture(t, 1)
			t.Setenv(jobowner.JobEnv, producerJob)
			t.Setenv(jobowner.EpochEnv, value)
			rc, stderr := f.produce(t, "done")
			if rc == ExitOK || !strings.Contains(stderr, "malformed attempt binding") {
				t.Fatalf("rc=%d stderr=%q", rc, stderr)
			}
			if len(f.eventFiles(t)) != 1 {
				t.Fatalf("a refused call wrote %v", f.eventFiles(t))
			}
		})
	}
}

// The node's socket handler refuses a superseded epoch from a client that did
// not go through the CLI's own check.
func TestDaemonRefusesSupersededProducerPush_t5310(t *testing.T) {
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	if err := jobowner.Assign(jobs, producerJob, 3); err != nil {
		t.Fatal(err)
	}
	daemon := t22NamespaceDaemon(root, &bytes.Buffer{})
	request := localRequest{Op: "emit", Kind: "job.completed", JobID: producerJob, ReportPath: "report.md", OwnerLane: "lane-a", InboxRoot: root, Epoch: 1}
	err := daemon.emitRelayEvent(request)
	if err == nil || ExitCode(err) != ExitUsage || !strings.Contains(err.Error(), "stale attempt") {
		t.Fatalf("epoch 1 under grant 3: err=%v", err)
	}
	request.Epoch = 0 // an absent epoch is epoch 1
	if err := daemon.emitRelayEvent(request); err == nil || !strings.Contains(err.Error(), "stale attempt") {
		t.Fatalf("absent epoch under grant 3: err=%v", err)
	}
	request.Epoch = 3
	if err := daemon.emitRelayEvent(request); err != nil && strings.Contains(err.Error(), "stale attempt") {
		t.Fatalf("epoch 3 under grant 3: err=%v", err)
	}
}

// A/1 -> B/2 leaves A's grant file at 1 and appends a revocation marker. The
// still-running A/1 worker must be refused even though its bound epoch equals
// the grant file, and the regranted A/3 worker accepted (mergewake finding on
// #720).
func TestProducerCallsRefuseRevokedAttempt_t5310(t *testing.T) {
	for _, command := range []string{"done", "escalate", "joined", "emit"} {
		t.Run(command, func(t *testing.T) {
			f := newProducerFixture(t, 1)
			if err := jobowner.Revoke(f.jobs, producerJob, 2); err != nil {
				t.Fatal(err)
			}
			before := f.eventFiles(t)
			for _, bound := range []uint64{1, 0} {
				if bound != 0 {
					bindAttempt(t, producerJob, bound)
				} else {
					t.Setenv(jobowner.JobEnv, "")
					t.Setenv(jobowner.EpochEnv, "")
				}
				rc, stderr := f.produce(t, command)
				if rc == ExitOK || !strings.Contains(stderr, "attempt revoked") {
					t.Fatalf("bound %d: rc=%d stderr=%q, want a refusal naming the revocation", bound, rc, stderr)
				}
			}
			if after := f.eventFiles(t); strings.Join(after, ",") != strings.Join(before, ",") || f.pushCount() != 0 {
				t.Fatalf("a refused call wrote %v and pushed %d", after, f.pushCount())
			}
			if err := jobowner.Assign(f.jobs, producerJob, 3); err != nil {
				t.Fatal(err)
			}
			bindAttempt(t, producerJob, 3)
			if rc, stderr := f.produce(t, command); rc != ExitOK {
				t.Fatalf("regranted attempt: rc=%d stderr=%q", rc, stderr)
			}
		})
	}
}

func TestDaemonRefusesRevokedProducerPush_t5310(t *testing.T) {
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	if err := jobowner.Revoke(jobs, producerJob, 2); err != nil {
		t.Fatal(err)
	}
	daemon := t22NamespaceDaemon(root, &bytes.Buffer{})
	request := localRequest{Op: "emit", Kind: "job.completed", JobID: producerJob, ReportPath: "report.md", OwnerLane: "lane-a", InboxRoot: root, Epoch: 1}
	if err := daemon.emitRelayEvent(request); err == nil || !strings.Contains(err.Error(), "attempt revoked") {
		t.Fatalf("epoch 1 after revocation at 2: err=%v", err)
	}
	request.Epoch = 2
	if err := daemon.emitRelayEvent(request); err == nil || !strings.Contains(err.Error(), "attempt revoked") {
		t.Fatalf("the revoking epoch itself: err=%v", err)
	}
	if err := jobowner.Assign(jobs, producerJob, 3); err != nil {
		t.Fatal(err)
	}
	request.Epoch = 3
	if err := daemon.emitRelayEvent(request); err != nil && (strings.Contains(err.Error(), "attempt revoked") || strings.Contains(err.Error(), "stale attempt")) {
		t.Fatalf("regranted epoch 3: err=%v", err)
	}
}

// A client of the socket cannot pick an epoch the node has not persisted: after
// A/2 -> B/3 the grant file reads 2 and the revocation marker 3, and a push at 3
// (B's epoch) or any later one is refused, as is a regrant not yet persisted.
func TestDaemonRefusesFutureProducerPush_t5310(t *testing.T) {
	for _, tc := range []struct {
		name          string
		grant, revoke uint64
		epoch         uint64
		want          string
	}{
		{name: "revocation epoch", grant: 2, revoke: 3, epoch: 3, want: "attempt revoked"},
		{name: "after the revocation", grant: 2, revoke: 3, epoch: 4, want: "attempt revoked"},
		{name: "unpersisted regrant", grant: 1, epoch: 3, want: "ahead of the node's grant"},
		{name: "ahead of a persisted grant", grant: 3, epoch: 4, want: "ahead of the node's grant"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			jobs := filepath.Join(root, "jobs")
			if tc.grant > 1 {
				if err := jobowner.Assign(jobs, producerJob, tc.grant); err != nil {
					t.Fatal(err)
				}
			}
			if tc.revoke > 0 {
				if err := jobowner.Revoke(jobs, producerJob, tc.revoke); err != nil {
					t.Fatal(err)
				}
			}
			daemon := t22NamespaceDaemon(root, &bytes.Buffer{})
			request := localRequest{Op: "emit", Kind: "job.completed", JobID: producerJob, ReportPath: "report.md", OwnerLane: "lane-a", InboxRoot: root, Epoch: tc.epoch}
			err := daemon.emitRelayEvent(request)
			if err == nil || ExitCode(err) != ExitUsage || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("epoch %d (grant %d, revoked %d): err=%v, want %q", tc.epoch, tc.grant, tc.revoke, err, tc.want)
			}
		})
	}
}

// With no inbox namespace configured the daemon has no authoritative grant: a
// job.* push is refused, not checked against a path under the working
// directory or the request's own root (mergewake finding on #720).
func TestDaemonRefusesProducerPushWithoutNamespace_t5310(t *testing.T) {
	root := t.TempDir()
	if err := jobowner.Assign(filepath.Join(root, "jobs"), producerJob, 3); err != nil {
		t.Fatal(err)
	}
	daemon := t22NamespaceDaemon("", &bytes.Buffer{})
	for _, epoch := range []uint64{1, 3} {
		request := localRequest{Op: "emit", Kind: "job.completed", JobID: producerJob, ReportPath: "report.md", OwnerLane: "lane-a", InboxRoot: root, Epoch: epoch}
		err := daemon.emitRelayEvent(request)
		if err == nil || ExitCode(err) != ExitUsage || !strings.Contains(err.Error(), "no inbox namespace configured") {
			t.Fatalf("epoch %d: err=%v", epoch, err)
		}
	}
}

// The daemon's configuration supplies the ledger to trust and the request only
// names a namespace to validate: an omitted root inherits the configured one,
// and an unconfigured daemon refuses with the queue left empty (consult 5310).
func TestDaemonResolvesOneRootForProducerPushes_t5310(t *testing.T) {
	root := t.TempDir()
	if err := jobowner.Assign(filepath.Join(root, "jobs"), producerJob, 3); err != nil {
		t.Fatal(err)
	}
	push := func(daemon *Daemon, requestRoot string, epoch uint64) (error, int) {
		request := localRequest{Op: "emit", Kind: "job.completed", JobID: producerJob, ReportPath: "report.md", OwnerLane: "lane-a", InboxRoot: requestRoot, Epoch: epoch}
		err := daemon.emitRelayEvent(request)
		return err, len(daemon.cfg.Hub.Client.events)
	}
	fallback := func() *Daemon { // the hub client names no jobs root: the inbox root is the namespace
		daemon := t22NamespaceDaemon("", &bytes.Buffer{})
		daemon.cfg.InboxRoot = root
		return daemon
	}
	for _, tc := range []struct {
		name   string
		daemon func() *Daemon
		root   string
	}{
		{"configured, matching root", func() *Daemon { return t22NamespaceDaemon(root, &bytes.Buffer{}) }, root},
		{"configured, omitted root", func() *Daemon { return t22NamespaceDaemon(root, &bytes.Buffer{}) }, ""},
		{"inbox-root fallback, matching root", fallback, root},
		{"inbox-root fallback, omitted root", fallback, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A stale epoch is refused from the configured ledger and queues nothing.
			if err, queued := push(tc.daemon(), tc.root, 1); err == nil || !strings.Contains(err.Error(), "stale attempt") || queued != 0 {
				t.Fatalf("epoch 1 under grant 3: err=%v queued=%d", err, queued)
			}
			// The current grant is admitted and queued.
			if err, queued := push(tc.daemon(), tc.root, 3); err != nil || queued != 1 {
				t.Fatalf("epoch 3 under grant 3: err=%v queued=%d", err, queued)
			}
		})
	}
	for _, requestRoot := range []string{root, ""} {
		daemon := t22NamespaceDaemon("", &bytes.Buffer{})
		if err, queued := push(daemon, requestRoot, 3); err == nil || !strings.Contains(err.Error(), "no inbox namespace configured") || queued != 0 {
			t.Fatalf("unconfigured daemon, request root %q: err=%v queued=%d", requestRoot, err, queued)
		}
	}
}

// A completion recorded at one attempt's epoch is not a duplicate of the same
// report at another: the A/1 record cannot close the A/3 assignment, so A/3's
// call must write and push its own (mergewake outside-diff findings on #720).
func TestJobDoneDuplicateSuppressionIsScopedToTheEpoch_t5310(t *testing.T) {
	for _, tc := range []struct {
		name  string
		bound bool
		flag  []string
	}{
		{name: "bound attempt", bound: true},
		{name: "operator recovery", flag: []string{"--epoch", "3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newProducerFixture(t, 1)
			if rc, stderr := f.produce(t, "done"); rc != ExitOK {
				t.Fatalf("epoch 1 done: rc=%d stderr=%q", rc, stderr)
			}
			f.pushes.await(t, 1)
			// The same report, completed again at the same epoch, is still a duplicate.
			if rc, stderr := f.produce(t, "done"); rc != ExitOK || !strings.Contains(stderr, "suppressed duplicate") {
				t.Fatalf("same-epoch repeat: rc=%d stderr=%q", rc, stderr)
			}
			if err := jobowner.Assign(f.jobs, producerJob, 3); err != nil {
				t.Fatal(err)
			}
			if tc.bound {
				bindAttempt(t, producerJob, 3)
			}
			rc, stderr := f.produce(t, "done", tc.flag...)
			if rc != ExitOK || strings.Contains(stderr, "suppressed duplicate") {
				t.Fatalf("epoch 3 done: rc=%d stderr=%q, want a record of its own", rc, stderr)
			}
			pushed := f.pushes.await(t, 2)
			if pushed[1].Epoch != 3 {
				t.Fatalf("second push epoch=%d, want 3", pushed[1].Epoch)
			}
			completions := 0
			for _, name := range f.eventFiles(t) {
				if strings.HasSuffix(name, "-job.completed.json") {
					completions++
				}
			}
			if completions != 2 {
				t.Fatalf("completion records=%d, want one per epoch", completions)
			}
		})
	}
}

// A regrant that lands after the first check and before the record is written
// must stop the call: the check is repeated under the events lock.
func TestProducerCallRechecksTheAttemptUnderTheLock_t5310(t *testing.T) {
	for _, command := range []string{"done", "escalate", "joined"} {
		t.Run(command, func(t *testing.T) {
			f := newProducerFixture(t, 1)
			bindAttempt(t, producerJob, 1)
			// A/1 -> B/2 -> A/3 in the window: the grant file now reads 3.
			jobBeforeRecordWrite = func() {
				if err := jobowner.Assign(f.jobs, producerJob, 3); err != nil {
					t.Error(err)
				}
			}
			t.Cleanup(func() { jobBeforeRecordWrite = nil })
			before := f.eventFiles(t)
			rc, stderr := f.produce(t, command)
			if rc == ExitOK || !strings.Contains(stderr, "stale attempt") {
				t.Fatalf("rc=%d stderr=%q, want the old attempt refused", rc, stderr)
			}
			if after := f.eventFiles(t); strings.Join(after, ",") != strings.Join(before, ",") || f.pushCount() != 0 {
				t.Fatalf("a refused call wrote %v and pushed %d", after, f.pushCount())
			}
		})
	}
}
