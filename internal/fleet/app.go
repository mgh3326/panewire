package fleet

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/mgh3326/panewire/internal/fleetevent"
	"github.com/mgh3326/panewire/internal/hubretry"

	"github.com/mgh3326/panewire/internal/arbiter"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// BuildVersion and BuildCommit are set by the binary's main from its link-time
// values.
var BuildVersion, BuildCommit = "dev", "unknown"

type App struct {
	EnforceTrust bool
	Runner       Runner
	Quota        QuotaGate
	Config       Config
	ConfigPath   string
	Policy       Policy
	Out, Err     io.Writer
	// hubRetries tallies the idempotent hub calls this command sent again; it
	// is printed once when the command ends (reportHubRetries).
	hubRetries hubretry.Stats

	// planOnly marks a dry run that measures nothing: a quota refresh writes
	// presence, lease and observation to the shared quota store. planFromHub
	// marks a receipt the hub planned; dryRunLine labels its receipt with it.
	// lastPlan is the placement the dry run chose.
	planOnly, planFromHub bool
	lastPlan              *SpawnPlan

	Jobs, Home   string
	Sleep        func(context.Context, time.Duration) error
	Now          func() time.Time
	Bins         map[string]string
	HerdrSession string // frozen for the lifetime of a spawn
	// Arbiter is the local job ledger (leases, job events, gc). Nil means the
	// one under Home and Jobs; tests inject a store over a temp dir.
	Arbiter        *arbiter.Store
	warned         map[string]bool
	ompModelNames  map[string]string // omp's footer names, read once per spawn
	PlacementHosts func(context.Context, string) ([]HostView, error)
	// PlacementTry is called with the host the moment placement decides to
	// try it for a spawn, before any gate or upload that takes time, so the
	// owner of the host view can count the job at once. A later call for the
	// same spawn names the next candidate. Nil does nothing.
	PlacementTry func(host string)
	// PlacementGate admits one placement decision at a time: it returns when
	// this decision may read the host view, and its release is called once the
	// decision has told PlacementTry its host (or ended). Nil does not gate.
	PlacementGate func(context.Context) (release func(), err error)
	// ScheduleQuotaRefresh is set by the hub coordinator. It queues a node
	// measurement without waiting; ordinary CLI spawns keep their synchronous
	// refresh path.
	ScheduleQuotaRefresh func(machine, pool string, timeout time.Duration)
	// SharedLatest reads one account's latest stored quota reading for the
	// admission gate; nil reads handoffkeep (admission_native.go).
	SharedLatest func(context.Context, string) (shared.Latest, error)
	// BenchCatalog reads the bench catalog for the admission gate; nil reads
	// handoffkeep through catalog.go.
	BenchCatalog func(context.Context) ([]CatalogRow, error)
	// modelFamilies substitutes the HK policy reader in tests.
	modelFamilies func(context.Context) (ModelFamiliesPolicy, error)
	Source        PlacementSource
	Wake          Waker
	LoadRatio     func(context.Context) (float64, error)
	JobStore      JobStore
	// TaskState reads one hk task; nil builds it from [quota] / the
	// handoffkeep env on first use (TaskStatus).
	TaskState TaskStateReader
	// PR lists the PRs hk links to one task; nil builds it the same way on
	// first use (PRs). Reap reads it to keep a completed author's pane.
	PR PRReader
	// BenchStore reads and writes the shared bench reps. Nil builds it from
	// [quota] / the handoffkeep env on first use; a node sets the store built
	// from its own spawn.json credential.
	BenchStore *BenchStore
	// Hub carries via="hub" spawns and quota refreshes; nil builds the HTTP
	// client from [hub] on first use.
	Hub              HubTransport
	Repository       *Repository
	ObserveStage     func(string, time.Duration)
	ObserveRetryable func(bool)
	ObserveReceipt   func(ExecutionReceipt)
	// RequestID is the hub spawn request this App executes; the placement fleet
	// event carries it so Loki joins the decision to the spawn and its Sentry
	// event. Empty outside a hub spawn.
	RequestID string
	// EventSink receives fleet events instead of the state directory's file;
	// tests only.
	EventSink func(fleetevent.Event)
	// shadowProbe runs at the start of each placement shadow arm; tests only
	// (placement_shadow.go).
	shadowProbe func(arm string)
	// shadowSnapProbe receives the waste_first arm's pass-1 order before the arm
	// reads it; tests only (placement_shadow_waste.go).
	shadowSnapProbe func(order []*selCand)
	// SentryWarning sends one warning-level Sentry event. Tests substitute it;
	// nil uses SENTRY_DSN_PANEWIRE (sentry_warning.go).
	SentryWarning func(fingerprint []string, message string, tags map[string]string)
	// LiveSpawnProfiles counts live hub spawns by placed profile for the
	// [quota].pool_max_concurrent cap, leaving out the hub request named by
	// its second argument (the spawn being placed). The hub's own App reads
	// its operations; nil reads GET /v1/spawns/live with the [hub] operator
	// credential.
	LiveSpawnProfiles func(context.Context, string) (map[string]int, error)
	// State reads the hub's fleet state snapshot; nil reads GET /v1/state
	// with the [hub] operator credential. fleetLog records the sources used.
	DecisionReply func(context.Context, string, string, time.Time) (bool, error)
	// QuotaHandOffGrace is how long a reviewer watcher leaves a quota-stopped
	// job to the hub before ending it without a replacement; zero ends it at
	// once. New sets QuotaStopHandOffGrace.
	QuotaHandOffGrace time.Duration
	State             FleetStateReader
	fleetLog          *fleetStateLog
	// spawnFn replaces Spawn where delegate starts its pane; tests only.
	spawnFn func(context.Context, *App, Spawn) error
	// Version is the build line `panewire --version` prints, compared with the
	// remote hosts' by the preflight.
	Version string
	// StateDir holds panewire's local state (the routing policy cache); empty
	// means ~/.local/state/panewire under Home.
	StateDir string
}

func Main(ctx context.Context, args []string, out, errout io.Writer) int {
	a, e := New(out, errout)
	if e == nil {
		e = a.Run(ctx, args)
	}
	if e != nil {
		fmt.Fprintln(errout, "panewire:", e)
		if len(args) > 0 && (args[0] == "spawn" || args[0] == "work") {
			fmt.Fprintln(errout, "Fix:", spawnRefusalFix(e))
		}
		var x *ExitError
		if errors.As(e, &x) {
			return x.Code
		}
		return 1
	}
	return 0
}
func New(out, errout io.Writer) (*App, error) {
	path := configPath(errout)
	c, e := LoadConfig(path)
	if e != nil {
		return nil, e
	}
	warnRetiredQuotaKeys(c, errout)
	warnIgnoredHostsTables(c, path, errout)
	p, e := LoadPolicy(os.Getenv("PANEWIRE_POLICY"))
	if e != nil {
		return nil, e
	}
	h, _ := os.UserHomeDir()
	a := &App{Runner: ExecRunner{}, Config: c, ConfigPath: path, Policy: p, Out: out, Err: errout, Home: h, Jobs: filepath.Join(h, "work", "herdr-inbox", "jobs"), Now: time.Now, Sleep: sleepContext, Bins: map[string]string{}, QuotaHandOffGrace: QuotaStopHandOffGrace}
	if v := os.Getenv("ARBITER_INBOX_ROOT"); v != "" {
		a.Jobs = v
	}
	for bin, env := range map[string]string{"herdr": "HERDR_BIN", "handoffkeep": "HANDOFFKEEP_BIN", "panewire": "PANEWIRE_BIN"} {
		a.Bins[bin] = bin
		if v := os.Getenv(env); v != "" {
			a.Bins[bin] = v
		}
	}
	if os.Getenv("PANEWIRE_QUOTA_BIN") != "" {
		fmt.Fprintln(errout, "panewire: 경고: PANEWIRE_QUOTA_BIN 설정은 폐기되어 무시됩니다")
	}
	cliQuota := CLIQuota{Runner: a.Runner, Now: a.Now, CreditTotalUSD: creditTotals(c.Quota), Accounts: quotaAccounts(c.Quota)}
	cliQuota.SharedLatest = a.sharedLatest
	a.Quota = admissionQuota{cliQuota, a}
	a.Source = stateSource{a: a, inner: fleetSource{a}}
	a.LoadRatio = a.loadRatio
	a.Version = BuildVersion
	a.EnforceTrust = true
	if v := os.Getenv("XDG_STATE_HOME"); v != "" {
		a.StateDir = filepath.Join(v, "panewire")
	}
	return a, nil
}
func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// ledger is the local job ledger: state.db under $XDG_DATA_HOME (default
// ~/.local/share) and the events inbox under Jobs, the paths the arbiter
// script used.
func (a *App) ledger() *arbiter.Store {
	if a.Arbiter != nil {
		return a.Arbiter
	}
	data := os.Getenv("XDG_DATA_HOME")
	if data == "" {
		data = filepath.Join(a.Home, ".local", "share")
	}
	return &arbiter.Store{DB: arbiter.StatePath(data), Inbox: a.Jobs, Now: a.Now}
}

// ledgerFail turns a ledger error into the CLI error with the same exit
// status the arbiter script gave it.
func ledgerFail(e error, what string) error {
	if e == nil {
		return nil
	}
	return fail(arbiter.Code(e), "arbiter %s: %v", what, e)
}

func (a *App) call(ctx context.Context, bin string, args ...string) Result {
	name := a.Bins[bin]
	if name == "" {
		name = bin
	}
	c := Command{Name: name, Args: args}
	if bin == "herdr" {
		c.Env = []string{"HERDR_SESSION=" + a.herdrSession()}
	}
	return a.Runner.Run(ctx, c)
}

func (a *App) herdrSession() string {
	return firstNonempty(a.HerdrSession, os.Getenv("HERDR_SESSION"), "default")
}
func (a *App) must(ctx context.Context, bin string, args ...string) error {
	return check(a.call(ctx, bin, args...), bin)
}

// envOr reads a renamed environment variable. For one release the retired
// name still applies, with a one-line warning to switch, emitted once per
// name so selection loops do not repeat it.
func (a *App) envOr(name, retired string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	if v := os.Getenv(retired); v != "" {
		if a.warned == nil {
			a.warned = map[string]bool{}
		}
		if !a.warned[retired] {
			a.warned[retired] = true
			a.warn("%s is retired; set %s instead", retired, name)
		}
		return v
	}
	return ""
}
func (a *App) warn(format string, args ...any) {
	fmt.Fprintf(a.Err, "panewire: warning: "+format+"\n", args...)
}
func flags(name string) *flag.FlagSet {
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(io.Discard)
	return f
}

func flagPassed(f *flag.FlagSet, name string) bool {
	passed := false
	f.Visit(func(p *flag.Flag) {
		if p.Name == name {
			passed = true
		}
	})
	return passed
}

var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

func validID(s string) bool { return idPattern.MatchString(s) && s != "." && s != ".." }
func (a *App) Run(ctx context.Context, args []string) error {
	a.flushPlacements()
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		fmt.Fprintln(a.Out, "Usage: panewire <spawn|delegate|done|escalate|restore|reap|prune|lanes-missing|hosts|profiles|find|name-sync|artifacts|simulate|plan|resolve|preflight> [options]")
		return nil
	}
	if args[0] == "lead" {
		return fail(exitUsage, "%s", retiredLeadSeatPointer)
	}
	if len(args) > 1 && (args[1] == "--help" || args[1] == "-h") {
		return a.help(args[0])
	}
	if a.EnforceTrust && (args[0] == "spawn" || args[0] == "work" || args[0] == "delegate" || args[0] == "plan" || args[0] == "resolve" || args[0] == "preflight" || args[0] == "_spawn-stdin" || args[0] == "done" || args[0] == "escalate" || args[0] == "restore" || args[0] == "reap" || args[0] == "prune" || args[0] == "artifacts" || args[0] == "_watch") {
		if e := a.trust(); e != nil {
			return e
		}
	}
	switch args[0] {
	case "work":
		s, e := a.workPreset(ctx, args[1:])
		if e != nil {
			return e
		}
		if s.DryRun {
			return a.previewSpawn(ctx, s)
		}
		return a.SubmitSpawn(ctx, s)
	case "spawn":
		s, e := parseSpawn(args[1:])
		if e != nil {
			return e
		}
		if s.DryRun {
			return a.previewSpawn(ctx, s)
		}
		return a.SubmitSpawn(ctx, s)
	case "done", "escalate", "restore":
		return a.lifecycle(ctx, args[0], args[1:])
	case "reap", "prune":
		return a.hygiene(ctx, args[0], args[1:])
	case "hosts", "profiles", "find", "name-sync", "lanes-missing":
		return a.diagnostics(ctx, args[0], args[1:])
	case "artifacts":
		return a.artifacts(ctx, args[1:])
	case "simulate":
		return a.simulate(ctx, args[1:])
	case "plan":
		return a.planCmd(ctx, args[1:])
	case "resolve":
		return a.resolveCmd(ctx, args[1:])
	case "delegate":
		return a.delegate(ctx, args[1:])
	case "preflight":
		return a.preflightCmd(ctx, args[1:])
	case "_artifact-inventory":
		return a.artifactInventory()
	case "_watch":
		return a.watch(ctx, args[1:])
	case "_spawn-stdin":
		return a.spawnStdin(ctx)
	default:
		return fail(exitUsage, "unknown command %q", args[0])
	}
}
func (a *App) help(cmd string) error {
	switch cmd {
	case "work":
		fmt.Fprintln(a.Out, "Usage: panewire work TASK [spawn flags override task defaults]")
	case "spawn":
		fmt.Fprintln(a.Out, "Usage: panewire spawn -c CWD [-m PROFILE|auto] -p BRIEF|hk:doc/KEY -w WORKSPACE -l LABEL --t T0..T3 [--task ID|--parent-job JOB] [--effort E] [--role worker|tester|reviewer|counsel] [--review-pass 1..9 (reviewer only)] [--routing-role consult|implementer|verifier|reviewer|judge] [--lane L] [--parent L] [--owner L] [--job ID] [--host auto|local|NAME] [--tags a,b,c] [--exclude-host a,b] [--exclude-pool P,P] [--report PATH] [-L live|mock] [--keep] [--job-dup-ok] [--task-hk-bypass] [--allow-escalation] [--override-reason TEXT] [--containment hk:ID] [--operator-request REF] [--requested-by NAME] [--purpose PURPOSE] [--report-relative PATH] [--wait] [--wait-timeout DUR] [--dry-run] (pins require --containment hk:ID or --operator-request REF)")
	case "done":
		fmt.Fprintln(a.Out, "Usage: panewire done JOB [--report PATH] [--result FILE]")
	case "escalate":
		fmt.Fprintln(a.Out, "Usage: panewire escalate JOB --question TEXT [--report PATH]")
	case "restore":
		fmt.Fprintln(a.Out, "Usage: panewire restore JOB (rebind a released job to its verified existing pane)")
	case "reap", "prune":
		fmt.Fprintf(a.Out, "Usage: panewire %s [--job ID[,ID]] [--lane L] [--grace 10m] [--include-builders] [--worktrees] [--apply]\n", cmd)
		if cmd == "reap" {
			fmt.Fprintln(a.Out, "       panewire reap --expire-job-dirs [--job ID[,ID]] [--apply] (job dir TTL; a dry run without --apply; docs/job-dir-ttl.md)")
		}
	case "artifacts":
		fmt.Fprintln(a.Out, "Usage: panewire artifacts move SRC --dest DEST [--source-host HOST] [--apply] | sweep [--job JOB] [--age 7d] [--policy FILE] [--apply]")
	case "simulate":
		fmt.Fprintln(a.Out, "Usage: panewire simulate --selection FILE [--now RFC3339] [--policy FILE] [--policy-set key=value] ... [--set pool:used=N,reset=RFC3339|+Nh[,window=D][,projected=N]] ... (rank a recorded selection input; no quota reads, no spawn)")
	case "delegate":
		fmt.Fprintln(a.Out, "Usage: panewire delegate --role reader|verifier|reviewer --prompt FILE [--grade G] [--cwd DIR] [--timeout D] [--headless] [--patch] [--panel 2..4] [--family F] [--workspace W] [--json] (hand one bounded reading, verifying or cross-checking task to a free or spare-quota model; the answer is the last thing printed)")
	case "plan":
		fmt.Fprintln(a.Out, "Usage: panewire plan [--grade G] --t T0..T3 [--role R] [--tags a,b] [--effort E] [--urgency normal|urgent] [--exclude-host a,b] [--exclude-pool P,P] [--builder-family F] [--routing-role R] [--allow-escalation] [--credit-at TIME] [--expected DURATION] [--json] (live, read-only: the host and model a spawn would get now; no task, spawn or job write; --expected 6h or 2d keys subscription pools by whether their shortest window fits the job)")
	case "resolve":
		fmt.Fprintln(a.Out, "Usage: panewire resolve --role consult|implementer|verifier|reviewer|judge [--grade G] [--t T0..T3] [--builder-family F] [--lane L (consult asker lane)] [--harness KIND] [--allow-escalation] [--json] (read-only: the profile and effort role routing and quota ordering pick now, for an in-session subagent; no host placement, spawn or job write; the answer is the last thing printed)")
	case "preflight":
		fmt.Fprintln(a.Out, "Usage: panewire preflight [--host NAME] [--pool P] [--json] (read-only: per remote host, hub acceptance, shared quota observation age, quota transport setting, hub load/memory, mergewake review config and panewire/panewire version skew, each with a named reason; exit 0 when all good, 78 otherwise)")
	default:
		fmt.Fprintf(a.Out, "Usage: panewire %s [options]; see README and docs/config.md\n", cmd)
	}
	return nil
}
