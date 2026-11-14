package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// testdata/omp holds captures from a live omp 18.14.2 launched the way the
// launcher builds it, with only home, repository and branch names replaced.
func ompCapture(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "omp", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

const ompOtherScript = `{"process_info":{"shell_pid":1,"foreground_processes":[{"pid":2,"argv0":"bun","name":"bun","argv":["/home/user/.bun/bin/bun","--no-env-file","/home/user/other/server.js"]}]}}`

// ompSeat overrides what the spawn fixture answers once omp has started.
func ompSeat(fr *fakeRunner, processInfo string, screen func(read int) string) {
	base := fr.Do
	started := false
	reads := 0
	fr.Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		r := base(c)
		switch {
		case strings.HasPrefix(args, "agent start"):
			started = true
		case started && processInfo != "" && strings.HasPrefix(args, "pane process-info"):
			return Result{Out: processInfo}
		case started && screen != nil && strings.HasPrefix(args, "pane read"):
			reads++
			return Result{Out: screen(reads)}
		}
		return r
	}
}

func ompWrapperInfo(t *testing.T, cwd string) string {
	return strings.ReplaceAll(ompCapture(t, "process_info_wrapper.json"), "/home/user/work/tree", cwd)
}

func ompPrompts(fr *fakeRunner) int {
	n := 0
	for _, c := range fr.Calls {
		if strings.HasPrefix(strings.Join(c.Args, " "), "agent prompt") {
			n++
		}
	}
	return n
}

// Both cases failed on the code this was written against, with the refusals
// the fleet saw: "agent not in foreground; brief withheld" for the wrapper and
// "omp model/effort not proven by visible session" for the footer.
func TestOMPSpawnProvesCapturedSeat(t *testing.T) {
	t.Run("bun wrapper foreground", func(t *testing.T) {
		a, s, fr := ompSpawnFixture(t)
		ompSeat(fr, ompWrapperInfo(t, s.CWD), nil)
		if err := a.Spawn(context.Background(), s); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("18.x footer", func(t *testing.T) {
		a, s, _ := ompSpawnFixture(t)
		if err := a.Spawn(context.Background(), s); err != nil {
			t.Fatal(err)
		}
	})
}

func TestOMPForegroundArgv(t *testing.T) {
	const cli = "/home/user/.bun/install/global/node_modules/@oh-my-pi/pi-coding-agent/dist/cli.js"
	var captured processInfo
	var wrapped struct {
		Result struct {
			Info processInfo `json:"process_info"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(ompCapture(t, "process_info_wrapper.json")), &wrapped); err != nil {
		t.Fatal(err)
	}
	captured = wrapped.Result.Info
	if len(captured.Processes) != 1 {
		t.Fatalf("capture has %d processes", len(captured.Processes))
	}
	proc := captured.Processes[0]
	if !foregroundAgent("omp", proc.Argv0, proc.Name, proc.Argv) {
		t.Fatalf("captured wrapper foreground refused: %+v", proc)
	}
	for _, tc := range []struct {
		name, kind, argv0, proc string
		argv                    []string
		want                    bool
	}{
		{"omp binary", "omp", "omp", "", nil, true},
		{"bun without the env flag", "omp", "bun", "bun", []string{"bun", cli}, true},
		{"linux reports no argv0", "omp", "", "bun", []string{"/usr/local/bin/bun", "--no-env-file", cli}, true},
		{"bare bun", "omp", "bun", "bun", []string{"bun"}, false},
		{"bun with another script", "omp", "bun", "bun", []string{"bun", "--no-env-file", "/home/user/other/server.js"}, false},
		{"another script naming omp as its argument", "omp", "bun", "bun", []string{"bun", "/home/user/other/server.js", cli}, false},
		{"bun preloading another flag", "omp", "bun", "bun", []string{"bun", "--preload", "/home/user/x.js", cli}, false},
		{"lookalike package path", "omp", "bun", "bun", []string{"bun", "/home/user/x@oh-my-pi/pi-coding-agent/dist/cli.js"}, false},
		{"node running the script", "omp", "node", "node", []string{"node", cli}, false},
		{"shell", "omp", "zsh", "zsh", []string{"zsh"}, false},
		{"bun wrapper is omp only", "cmd", "bun", "bun", []string{"bun", cli}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := foregroundAgent(tc.kind, tc.argv0, tc.proc, tc.argv); got != tc.want {
				t.Fatalf("got %t", got)
			}
		})
	}
}

func TestOMPSpawnRefusesAnotherBunScript(t *testing.T) {
	a, s, fr := ompSpawnFixture(t)
	ompSeat(fr, ompOtherScript, nil)
	err := a.Spawn(context.Background(), s)
	if err == nil || !strings.Contains(err.Error(), "agent not in foreground") {
		t.Fatalf("got %v", err)
	}
	if n := ompPrompts(fr); n != 0 {
		t.Fatalf("brief sent %d times", n)
	}
}

func TestOMPFooterProof(t *testing.T) {
	worktree := ompCapture(t, "screen_opus_high_worktree.txt")
	folder := ompCapture(t, "screen_sonnet_medium_folder.txt")
	working := ompCapture(t, "screen_working_footer.txt")
	clamped := ompCapture(t, "screen_folder_clamped.txt")
	const longCWD = "/home/user/work/plain-fixture-directory/desk-for-the-clamped-footer"
	// clampTail is what omp keeps of a long label ending in end.
	clampTail := func(end string) string {
		long := strings.Repeat("d", ompPathClampTail) + end
		return long[len(long)-ompPathClampTail:]
	}
	const (
		home    = "/home/user"
		tree    = "/home/user/work/tree"
		desk    = "/home/user/work/plain/desk"
		opus    = "Opus 5.5"
		sonnet  = "Sonnet 5.5"
		footer  = " π > ◒ GPT-6.1 Terra > 📁 %s ▶─1%%──1M─"
		rowGit  = " π > ◒ Opus 5.5 > 🌳 %s > ⑂ %s ▶─1%%──1M─"
		rowPlan = " π > ◒ Opus 5.5 > plan > 🌳 panewire/tree > ⑂ wk/fixture ▶─1%%──1M─"
	)
	row := func(format string, args ...any) string {
		return "history\n" + fmt.Sprintf(format, args...) + "\n╰─  hint"
	}
	for _, tc := range []struct {
		name, screen, model, effort, cwd, brief, label, branch string
		want                                                   bool
	}{
		{name: "captured worktree", screen: worktree, model: opus, effort: "high", cwd: tree, label: "panewire/tree", branch: "wk/fixture", want: true},
		{name: "captured folder", screen: folder, model: sonnet, effort: "medium", cwd: desk, want: true},
		{name: "captured working footer refuses detached branch", screen: working, model: opus, effort: "high", cwd: tree, label: "panewire/tree", branch: "wk/fixture"},
		{name: "working footer: different model", screen: working, model: sonnet, effort: "high", cwd: tree},
		{name: "working footer: different cwd", screen: working, model: opus, effort: "high", cwd: "/home/user/work/other", label: "panewire/other", branch: "wk/other"},
		{name: "working footer quoted by the brief", screen: working, model: opus, effort: "high", cwd: tree, brief: working, label: "panewire/tree", branch: "detached"},
		{name: "minutes elapsed", screen: row(" ⠙ 1m 4s > ◒ Opus 5.5 > 📁 ~/work/plain/desk ▶─1%%──1M─"), model: opus, effort: "high", cwd: desk, want: true},
		{name: "spinner without elapsed time", screen: row(" ⠙ > ◒ Opus 5.5 > 📁 ~/work/plain/desk ▶─1%%──1M─"), model: opus, effort: "high", cwd: desk},
		{name: "elapsed time without spinner", screen: row(" 16s > ◒ Opus 5.5 > 📁 ~/work/plain/desk ▶─1%%──1M─"), model: opus, effort: "high", cwd: desk},
		{name: "other lead segment", screen: row(" plan > ◒ Opus 5.5 > 📁 ~/work/plain/desk ▶─1%%──1M─"), model: opus, effort: "high", cwd: desk},

		{name: "worktree: different model", screen: worktree, model: sonnet, effort: "high", cwd: tree},
		{name: "folder: different model", screen: folder, model: opus, effort: "medium", cwd: desk},
		{name: "worktree: different effort", screen: worktree, model: opus, effort: "medium", cwd: tree},
		{name: "folder: different effort", screen: folder, model: sonnet, effort: "high", cwd: desk},
		{name: "worktree: different cwd", screen: worktree, model: opus, effort: "high", cwd: "/home/user/work/other", label: "panewire/other", branch: "wk/other"},
		{name: "folder: different cwd", screen: folder, model: sonnet, effort: "medium", cwd: "/home/user/work/plain/other"},
		{name: "folder: cwd only shares a prefix", screen: folder, model: sonnet, effort: "medium", cwd: desk + "-extra"},
		{name: "unmapped effort", screen: worktree, model: opus, effort: "auto", cwd: tree},
		{name: "empty model name", screen: worktree, model: "", effort: "high", cwd: tree},
		{name: "footer quoted by the brief", screen: worktree, model: opus, effort: "high", cwd: tree, brief: worktree},
		{name: "before first paint", screen: ompCapture(t, "screen_before_first_paint.txt"), model: opus, effort: "high", cwd: tree},
		{name: "setup wizard", screen: ompCapture(t, "screen_setup_wizard.txt"), model: opus, effort: "high", cwd: tree},

		{name: "longer model name is another model", screen: row(" π > ◒ GPT-5.6 Terra Pro > 📁 ~/work/plain/desk ▶─1%%──1M─"), model: "GPT-5.6 Terra", effort: "high", cwd: desk},
		{name: "usage badge keeps the model", screen: row(" π > ◒ Opus 5.5 · Wrap-up > 📁 ~/work/plain/desk ▶─1%%──1M─"), model: opus, effort: "high", cwd: desk, want: true},
		{name: "mode segment before the path", screen: row(rowPlan), model: opus, effort: "high", cwd: tree, label: "panewire/tree", branch: "wk/fixture", want: true},
		{name: "no path segment", screen: row(" π > ◒ Opus 5.5 > $0.04 ▶─1%%──1M─"), model: opus, effort: "high", cwd: tree},
		{name: "older footer", screen: "── editor ──\n\n────────\n~/work/tree\n12%/100k (anthropic) claude-opus-5-5 • high", model: opus, effort: "high", cwd: tree},

		{name: "tilde exact", screen: row(footer, "~/work/plain/desk"), model: "GPT-6.1 Terra", effort: "high", cwd: desk, want: true},
		{name: "tilde is not a suffix", screen: row(footer, "~/desk"), model: "GPT-6.1 Terra", effort: "high", cwd: desk},
		{name: "absolute exact", screen: row(footer, "/srv/plain/desk"), model: "GPT-6.1 Terra", effort: "high", cwd: "/srv/plain/desk", want: true},
		{name: "absolute other", screen: row(footer, "/srv/other/desk"), model: "GPT-6.1 Terra", effort: "high", cwd: "/srv/plain/desk"},
		{name: "display root stripped", screen: row(footer, "plain/desk"), model: "GPT-6.1 Terra", effort: "high", cwd: "/home/user/Projects/plain/desk", want: true},
		{name: "display root stops at a path boundary", screen: row(footer, "lain/desk"), model: "GPT-6.1 Terra", effort: "high", cwd: "/home/user/Projects/plain/desk"},
		{name: "captured clamp", screen: clamped, model: opus, effort: "high", cwd: longCWD, want: true},
		{name: "captured clamp names another directory", screen: clamped, model: opus, effort: "high", cwd: "/home/user/work/plain-fixture-directory/desk-for-another-footer"},
		{name: "short tail is not omp's clamp", screen: row(footer, "…ain/desk"), model: "GPT-6.1 Terra", effort: "high", cwd: "/home/user/plain/desk"},

		{name: "worktree dir equals branch", screen: row(rowGit, "panewire", "tree"), model: opus, effort: "high", cwd: tree, label: "panewire", branch: "tree", want: true},
		{name: "worktree branch mismatch", screen: row(rowGit, "panewire/tree", "wk/other"), model: opus, effort: "high", cwd: tree, label: "panewire/tree", branch: "wk/fixture"},
		{name: "worktree label clamped", screen: row(rowGit, "…"+clampTail("project/tree"), "wk/x"), model: opus, effort: "high", cwd: tree, label: strings.Repeat("d", 39) + "project/tree", branch: "wk/x", want: true},
		{name: "worktree label clamped to other", screen: row(rowGit, "…"+clampTail("project/other"), "wk/x"), model: opus, effort: "high", cwd: tree, label: "project/tree", branch: "wk/x"},
		{name: "worktree clamp allows a partial worktree name", screen: row(rowGit, "…"+clampTail("d"), "wk/x"), model: opus, effort: "high", cwd: "/home/user/work/" + strings.Repeat("d", 45), label: "project/" + strings.Repeat("d", 45), branch: "wk/x", want: true},
		{name: "worktree clamp shorter than 16 runes", screen: row(rowGit, "…ire/tree", "wk/x"), model: opus, effort: "high", cwd: tree, label: "panewire/tree", branch: "wk/x"},
		{name: "worktree footer refuses missing cwd branch", screen: row(rowGit, "panewire/tree", "wk/fixture"), model: opus, effort: "high", cwd: tree, label: "panewire/tree"},
		{name: "worktree footer requires branch segment", screen: row(" π > ◒ Opus 5.5 > 🌳 panewire/tree ▶─1%%──1M─"), model: opus, effort: "high", cwd: tree, label: "panewire/tree", branch: "wk/fixture"},
		{name: "worktree clamp rejects a tail outside omp label", screen: row(rowGit, "…other/panewire/tree", "wk/fixture"), model: opus, effort: "high", cwd: tree, label: "panewire/tree", branch: "wk/fixture"},
		{name: "Muse Spark footer clamp", screen: row(" π > ◒ Muse Spark 1.3 Contributor > 🌳 …/mergewake.exp4523-musespark13c-3539 > ⑂ exp/4523-musespark13c-3539 ▶─0.9%%─┃1M─"), model: "Muse Spark 1.3 Contributor", effort: "high", cwd: "/Users/robin/work/mergewake.exp4523-musespark13c-3539", label: "mergewake/mergewake.exp4523-musespark13c-3539", branch: "exp/4523-musespark13c-3539", want: true},
		{name: "ParkingArcade footer clamp", screen: row(" π > ◒ DeepSeek V4.1 Flash > 🌳 …cade/ParkingArcade.ci-evidence-4556 > ⑂ wk/ci-evidence-cleanup-4556 > ⤴ #637 ▶─1%%─┃1M─"), model: "DeepSeek V4.1 Flash", effort: "high", cwd: "/Users/robin/work/ParkingArcade.ci-evidence-4556", label: "ParkingArcade/ParkingArcade.ci-evidence-4556", branch: "wk/ci-evidence-cleanup-4556", want: true},
		{name: "role rename footer clamp", screen: row(" π > ◔ DeepSeek V4.1 Flash > 🌳 …ewire/panewire.role-rename-no-aliases > ⑂ wk/role-rename-no-aliases > ⤴ #518 ▶─1%%─┃1M─"), model: "DeepSeek V4.1 Flash", effort: "low", cwd: "/Users/robin/work/panewire.role-rename-no-aliases", label: "panewire/panewire.role-rename-no-aliases", branch: "wk/role-rename-no-aliases", want: true},
		{name: "parent job binding footer clamp", screen: row(" π > ◔ DeepSeek V4.1 Flash > 🌳 …wire.parent-job-binding-takeover > ⑂ wk/parent-job-binding-takeover > ⤴ #520 ▶─1%%─┃1M─"), model: "DeepSeek V4.1 Flash", effort: "low", cwd: "/Users/robin/work/panewire.parent-job-binding-takeover", label: "panewire/panewire.parent-job-binding-takeover", branch: "wk/parent-job-binding-takeover", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := OMPFooterProof(tc.screen, tc.model, tc.effort, tc.cwd, home, tc.brief, tc.label, tc.branch); got != tc.want {
				t.Fatalf("got %t for %q", got, tc.screen)
			}
		})
	}
}

// The bottom row is the seat's; a footer an earlier turn printed proves nothing.
func TestOMPFooterProofReadsTheBottomRow(t *testing.T) {
	screen := " π > ◒ Sonnet 5.5 > 🌳 panewire/tree ▶─1%──1M─\n" + ompCapture(t, "screen_opus_high_worktree.txt")
	if !OMPFooterProof(screen, "Opus 5.5", "high", "/home/user/work/tree", "/home/user", "", "panewire/tree", "wk/fixture") {
		t.Fatal("bottom row refused")
	}
	if OMPFooterProof(screen, "Sonnet 5.5", "high", "/home/user/work/tree", "/home/user", "", "panewire/tree", "wk/fixture") {
		t.Fatal("quoted row proved")
	}
}

func TestOMPWorktreeIdentityUsesLinkedCheckoutNames(t *testing.T) {
	a, s, _ := ompSpawnFixture(t)
	label, branch := a.ompWorktreeIdentity(context.Background(), s.CWD)
	if label != "panewire/tree" || branch != "wk/fixture" {
		t.Fatalf("OMP worktree identity = (%q, %q), want (%q, %q)", label, branch, "panewire/tree", "wk/fixture")
	}
}

func TestOMPModelNameFromCatalog(t *testing.T) {
	for _, tc := range []struct {
		name, model, catalog string
		code                 int
		want, errPart        string
	}{
		{name: "claude prefix dropped", model: "anthropic/claude-opus-5-5", catalog: ompCapture(t, "models.json"), want: "Opus 5.5"},
		{name: "other names kept", model: "openai/gpt-5.6-terra-pro", catalog: ompCapture(t, "models.json"), want: "GPT-5.6 Terra Pro"},
		{name: "follows the catalog", model: "anthropic/claude-opus-5-5", catalog: `{"models":[{"id":"claude-opus-5-5","selector":"anthropic/claude-opus-5-5","name":"Renamed"}]}`, want: "Renamed"},
		{name: "unnamed falls back to id", model: "x/y", catalog: `{"models":[{"id":"y","selector":"x/y"}]}`, want: "y"},
		{name: "not listed", model: "anthropic/none", catalog: ompCapture(t, "models.json"), errPart: "does not list"},
		{name: "unreadable", model: "anthropic/claude-opus-5-5", catalog: "not json", errPart: "unreadable"},
		{name: "command fails despite readable output", model: "anthropic/claude-opus-5-5", catalog: ompCapture(t, "models.json"), code: 1, errPart: "omp models"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := testApp(t)
			a.Runner = &fakeRunner{Do: func(c Command) Result {
				if c.Name != "omp" || strings.Join(c.Args, " ") != "models --json" {
					t.Fatalf("unexpected call %v", c)
				}
				return Result{Out: tc.catalog, Code: tc.code}
			}}
			got, err := a.ompModelName(context.Background(), tc.model)
			if tc.errPart != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errPart) {
					t.Fatalf("got %q, %v", got, err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %q, %v", got, err)
			}
		})
	}
}

func TestOMPSpawnSkipsSetupWizard(t *testing.T) {
	a, s, fr := ompSpawnFixture(t)
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	for _, c := range fr.Calls {
		if strings.HasPrefix(strings.Join(c.Args, " "), "tab create") {
			if !slices.Contains(c.Args, "OMP_SKIP_SETUP=1") {
				t.Fatalf("seat env lacks the setup skip: %v", c.Args)
			}
			return
		}
	}
	t.Fatal("no tab created")
}

func TestOMPSpawnRefusesSetupWizardScreen(t *testing.T) {
	a, s, fr := ompSpawnFixture(t)
	wizard := ompCapture(t, "screen_setup_wizard.txt")
	ompSeat(fr, "", func(int) string { return wizard })
	err := a.Spawn(context.Background(), s)
	if err == nil || !strings.Contains(err.Error(), "OMP_SETUP_WIZARD") {
		t.Fatalf("got %v", err)
	}
	if n := ompPrompts(fr); n != 0 {
		t.Fatalf("brief sent %d times", n)
	}
	if fix := spawnRefusalFix(err); !strings.Contains(fix, "OMP_SKIP_SETUP") {
		t.Fatalf("fix text: %s", fix)
	}
}

// A transcript quoting the hint is not the wizard: only the last row counts.
func TestOMPSetupWizardReadsTheLastRow(t *testing.T) {
	if !ompSetupWizard(ompCapture(t, "screen_setup_wizard.txt")) {
		t.Fatal("captured wizard not detected")
	}
	quoted := "the hint ⌃C exit setup was on screen\n" + ompCapture(t, "screen_opus_high_worktree.txt")
	if ompSetupWizard(quoted) {
		t.Fatal("quoted hint read as the wizard")
	}
}

// herdr reports omp ready well before its first paint, so a blank screen must
// wait, and the brief must wait with it.
func TestOMPSpawnWaitsForFirstPaint(t *testing.T) {
	a, s, fr := ompSpawnFixture(t)
	blank := ompCapture(t, "screen_before_first_paint.txt")
	painted := ompCapture(t, "screen_opus_high_worktree.txt")
	var promptAfterRead int
	reads := 0
	ompSeat(fr, "", func(read int) string {
		reads = read
		if read <= 45 {
			return blank
		}
		return painted
	})
	base := fr.Do
	fr.Do = func(c Command) Result {
		if strings.HasPrefix(strings.Join(c.Args, " "), "agent prompt") {
			promptAfterRead = reads
		}
		return base(c)
	}
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if promptAfterRead <= 45 {
		t.Fatalf("brief sent at read %d, before the footer painted", promptAfterRead)
	}
}

func TestOMPSpawnRefusesScreenThatNeverPaints(t *testing.T) {
	a, s, fr := ompSpawnFixture(t)
	blank := ompCapture(t, "screen_before_first_paint.txt")
	ompSeat(fr, "", func(int) string { return blank })
	err := a.Spawn(context.Background(), s)
	if err == nil || !strings.Contains(err.Error(), "not proven") {
		t.Fatalf("got %v", err)
	}
	if n := ompPrompts(fr); n != 0 {
		t.Fatalf("brief sent %d times", n)
	}
}

func TestOMPSpawnRefusesCatalogFailure(t *testing.T) {
	a, s, fr := ompSpawnFixture(t)
	base := fr.Do
	fr.Do = func(c Command) Result {
		if c.Name == "omp" {
			return Result{Err: "omp: not found", Code: 127}
		}
		return base(c)
	}
	err := a.Spawn(context.Background(), s)
	if err == nil || !strings.Contains(err.Error(), "omp models") {
		t.Fatalf("got %v", err)
	}
	if n := ompPrompts(fr); n != 0 {
		t.Fatalf("brief sent %d times", n)
	}
}

// The footer abbreviates a worktree to its directory name; a seat in another
// directory of that name is caught by the process's own cwd.
func TestOMPSpawnRefusesProcessInAnotherDirectory(t *testing.T) {
	a, s, fr := ompSpawnFixture(t)
	other := filepath.Join(a.Home, "elsewhere", filepath.Base(s.CWD))
	if err := os.MkdirAll(other, 0700); err != nil {
		t.Fatal(err)
	}
	ompSeat(fr, ompWrapperInfo(t, other), nil)
	err := a.Spawn(context.Background(), s)
	if err == nil || !strings.Contains(err.Error(), "omp cwd not proven") {
		t.Fatalf("got %v", err)
	}
	if n := ompPrompts(fr); n != 0 {
		t.Fatalf("brief sent %d times", n)
	}
}

// TestMain clears the environment, so the live check takes a flag:
//
//	go test -run '^TestOMPLiveProof$' ./internal/fleet/ -args -omp-live PANE,MODEL,EFFORT,CWD
var ompLive = flag.String("omp-live", "", "check a live omp pane: PANE,MODEL,EFFORT,CWD")

// TestOMPLiveProof runs the proofs against a real pane: a scratch omp the caller
// launched the way the launcher does. It never sends the pane any input.
func TestOMPLiveProof(t *testing.T) {
	if *ompLive == "" {
		t.Skip("pass -args -omp-live PANE,MODEL,EFFORT,CWD to check a live omp pane")
	}
	spec := strings.SplitN(*ompLive, ",", 4)
	if len(spec) != 4 {
		t.Fatalf("-omp-live wants PANE,MODEL,EFFORT,CWD, got %q", *ompLive)
	}
	pane, cwd := spec[0], spec[3]
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	l := Launch{ModelID: spec[1], Effort: spec[2]}
	a := &App{Runner: ExecRunner{}, Home: home, Out: &bytes.Buffer{}, Err: &bytes.Buffer{}, Now: time.Now, Sleep: sleepContext, Bins: map[string]string{}}
	ctx := context.Background()
	p, err := a.pinfo(ctx, pane)
	if err != nil {
		t.Fatal(err)
	}
	foreground := false
	for _, proc := range p.Processes {
		foreground = foreground || foregroundAgent("omp", proc.Argv0, proc.Name, proc.Argv)
		t.Logf("process name=%s argv0=%s foreground_ok=%t", proc.Name, proc.Argv0, foregroundAgent("omp", proc.Argv0, proc.Name, proc.Argv))
	}
	if !foreground {
		t.Fatal("omp not in foreground")
	}
	name, err := a.ompModelName(ctx, l.ModelID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("catalog name for %s: %q", l.ModelID, name)
	if err := a.ompProof(ctx, l, false, cwd, pane, "", 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := a.ompProcessCWD(ctx, cwd, pane); err != nil {
		t.Fatal(err)
	}
	t.Logf("proof ok: model=%s effort=%s", l.ModelID, l.Effort)
	other := "anthropic/claude-sonnet-5-5"
	if l.ModelID == other {
		other = "anthropic/claude-opus-5-5"
	}
	if err := a.ompProof(ctx, Launch{ModelID: other, Effort: l.Effort}, false, cwd, pane, "", 0); err == nil {
		t.Fatal("a different model was proven")
	}
	if err := a.ompProof(ctx, l, false, filepath.Join(filepath.Dir(cwd), "other"), pane, "", 0); err == nil {
		t.Fatal("a different cwd was proven")
	}
	t.Log("negative controls refused: different model, different cwd")
}
