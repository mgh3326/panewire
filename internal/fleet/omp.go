package fleet

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// ompCLIScript is the script the omp wrapper execs under bun, so a running omp
// reports bun as its process. Any other script under bun is not omp.
const ompCLIScript = "/@oh-my-pi/pi-coding-agent/dist/cli.js"

func ompBunScript(argv []string) bool {
	if len(argv) < 2 {
		return false
	}
	args := argv[1:]
	if args[0] == "--no-env-file" {
		args = args[1:]
	}
	return len(args) > 0 && strings.HasSuffix(args[0], ompCLIScript)
}

// ompEffortGlyphs is omp's default (unicode preset, compact) thinking-level
// mark, the only place its footer shows effort. Other presets are refused.
var ompEffortGlyphs = map[string]string{"minimal": "○", "low": "◔", "medium": "◑", "high": "◒", "xhigh": "◕", "max": "◉"}

// ompWorkingLead is the first footer segment while omp is working: a braille
// spinner frame and the elapsed time ("⠸ 16s", "⠙ 1m 4s").
var ompWorkingLead = regexp.MustCompile(`^[\x{2800}-\x{28FF}] (?:\d+[hms] ?)*\d+[hms]$`)

// ompFooterLead reports whether the first " > " segment is omp's: "π" at rest,
// the spinner and elapsed time at work. Any other lead is not the footer.
func ompFooterLead(segment string) bool {
	return segment == "π" || ompWorkingLead.MatchString(segment)
}

// OMPFooterProof binds the seat to omp's bottom status row:
//
//	π > ◒ Opus 5.5 > 🌳 project/tree > ⑂ branch ▶─1%──…─1M─
//	π > ◑ Sonnet 5.5 > 📁 ~/work/desk ▶─1%──…─1M─
//	⠸ 16s > ◉ GPT-6.1 Sol > 🌳 project/tree > ⑂ detached > $0.03 ▶─1%──…─1M─
//
// modelName is omp's footer name for the launched model (see ompModelName).
func OMPFooterProof(screen, modelName, effort, cwd, home, brief string, noThinkingRung ...bool) bool {
	glyph, ok := ompEffortGlyphs[effort]
	noThinking := effort == "off" && len(noThinkingRung) > 0 && noThinkingRung[0]
	if !noThinking && !ok || modelName == "" {
		return false
	}
	row := ""
	for _, line := range cleanScreen(screen) {
		if lead, _, found := strings.Cut(strings.TrimSpace(line), " > "); found && ompFooterLead(lead) {
			row = line
		}
	}
	if row == "" || strings.Contains(normalized(brief), normalized(row)) {
		return false
	}
	left, _, _ := strings.Cut(row, "▶")
	segments := strings.Split(strings.TrimSpace(left), " > ")
	if len(segments) < 3 {
		return false
	}
	// A usage-limit badge follows " · "; any other tail is another model's name.
	modelPrefix := modelName
	if !noThinking {
		modelPrefix = glyph + " " + modelName
	}
	tail, ok := strings.CutPrefix(segments[1], modelPrefix)
	if !ok || tail != "" && !strings.HasPrefix(tail, " · ") {
		return false
	}
	paths := []string{cwd}
	if real, e := filepath.EvalSymlinks(cwd); e == nil && real != cwd {
		paths = append(paths, real)
	}
	for i, segment := range segments[2:] {
		worktree, isWorktree := strings.CutPrefix(segment, "🌳 ")
		folder, isFolder := strings.CutPrefix(segment, "📁 ")
		if !isFolder {
			folder, isFolder = strings.CutPrefix(segment, "🗑 ")
		}
		if !isWorktree && !isFolder {
			continue
		}
		branch := ""
		if next := segments[2:][i+1:]; len(next) > 0 {
			branch = next[0]
		}
		for _, p := range paths {
			if isWorktree && ompWorktreeShown(worktree, branch, p) || isFolder && ompFolderShown(folder, p, home) {
				return true
			}
		}
		return false
	}
	return false
}

// ompPathClampTail is how much of a long path omp keeps behind "…". The cut can
// fall inside a directory name, so this length, not a path boundary, is what
// tells a real clamp from a short tail that many directories would end with.
const ompPathClampTail = 39

func ompClampedTail(tail string) bool {
	return utf8.RuneCountInString(tail) == ompPathClampTail
}

// ompFolderShown follows omp's path segment: a ~ form, a path relative to one
// of its display roots, or a tail clamped behind "…".
func ompFolderShown(shown, cwd, home string) bool {
	if tail, ok := strings.CutPrefix(shown, "…"); ok {
		return ompClampedTail(tail) && strings.HasSuffix(cwd, tail)
	}
	if rest, ok := strings.CutPrefix(shown, "~"); ok {
		return strings.TrimRight(home, "/")+rest == cwd
	}
	if strings.HasPrefix(shown, "/") {
		return shown == cwd
	}
	return shown != "" && strings.HasSuffix(cwd, "/"+shown)
}

// ompWorktreeShown follows omp's linked-worktree label: project/dir, or project
// alone when the worktree dir equals the branch, so the branch segment names it.
func ompWorktreeShown(label, branch, cwd string) bool {
	dir := filepath.Base(cwd)
	if tail, ok := strings.CutPrefix(label, "…"); ok {
		return ompClampedTail(tail) && (strings.HasSuffix(tail, "/"+dir) || strings.HasSuffix(dir, tail))
	}
	if strings.Contains(label, "/") {
		return strings.HasSuffix(label, "/"+dir)
	}
	fields := strings.Fields(branch)
	return len(fields) >= 2 && fields[0] == "⑂" && fields[1] == dir
}

// ompModelName derives the footer's name for a launched model from omp's own
// catalog, applying the one rewrite its status line makes.
func (a *App) ompModelName(ctx context.Context, model string) (string, error) {
	if name, ok := a.ompModelNames[model]; ok {
		return name, nil
	}
	r := a.call(ctx, "omp", "models", "--json")
	if e := check(r, "omp models"); e != nil {
		return "", e
	}
	var catalog struct {
		Models []struct {
			ID       string `json:"id"`
			Selector string `json:"selector"`
			Name     string `json:"name"`
		} `json:"models"`
	}
	if e := json.Unmarshal([]byte(r.Out), &catalog); e != nil {
		return "", fail(exitConfig, "omp models output unreadable: %v", e)
	}
	for _, m := range catalog.Models {
		if m.Selector != model {
			continue
		}
		name := strings.TrimPrefix(firstNonEmpty(m.Name, m.ID), "Claude ")
		if a.ompModelNames == nil {
			a.ompModelNames = map[string]string{}
		}
		a.ompModelNames[model] = name
		return name, nil
	}
	return "", fail(exitConfig, "omp model catalog does not list %s", model)
}

// ompSetupWizard reads the hint on the wizard's last row; a transcript quoting
// it higher up is not the wizard.
func ompSetupWizard(screen string) bool {
	lines := cleanScreen(screen)
	return len(lines) > 0 && strings.Contains(lines[len(lines)-1], "⌃C exit setup")
}

// ompProof re-reads the screen because omp paints well after herdr reports it
// ready; a blank screen right after launch is not a refusal yet.
func (a *App) ompProof(ctx context.Context, l Launch, cwd, pane, brief string, wait time.Duration) error {
	name, e := a.ompModelName(ctx, l.ModelID)
	if e != nil {
		return e
	}
	deadline := a.Now().Add(wait)
	for {
		screen, e := a.visible(ctx, pane)
		if e != nil {
			return e
		}
		if ompSetupWizard(screen) {
			return fail(exitConfig, "OMP_SETUP_WIZARD: omp is in its setup wizard despite OMP_SKIP_SETUP; seat withheld")
		}
		if OMPFooterProof(screen, name, l.Effort, cwd, a.Home, brief, l.NoThinking) {
			return nil
		}
		if !a.Now().Before(deadline) {
			return fail(exitConfig, "omp model/effort not proven by visible session")
		}
		if e = a.Sleep(ctx, time.Second); e != nil {
			return e
		}
	}
}

// ompProcessCWD binds the exact directory: the footer can abbreviate it, the
// process cannot. herdr omitting the field leaves the footer as the only proof.
func (a *App) ompProcessCWD(ctx context.Context, cwd, pane string) error {
	p, e := a.pinfo(ctx, pane)
	if e != nil {
		return e
	}
	want := resolvedPath(cwd)
	for _, proc := range p.Processes {
		if foregroundAgent("omp", proc.Argv0, proc.Name, proc.Argv) && proc.CWD != "" && resolvedPath(proc.CWD) != want {
			return fail(exitConfig, "omp cwd not proven: process runs in %s, seat asked for %s", proc.CWD, cwd)
		}
	}
	return nil
}

func resolvedPath(p string) string {
	if real, e := filepath.EvalSymlinks(p); e == nil {
		return real
	}
	return p
}

// ompAgent reads only user-agent frontmatter, never OMP auth or settings.
// Glob order makes duplicate model matches deterministic.
func (a *App) ompAgent(model string) string {
	paths, _ := filepath.Glob(filepath.Join(a.Home, ".omp", "agent", "agents", "*.md"))
	for _, path := range paths {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		name, agentModel := ompAgentFrontmatter(path)
		if name != "" && agentModel == model {
			return name
		}
	}
	return ""
}

func ompAgentFrontmatter(path string) (name, model string) {
	f, err := os.Open(path)
	if err != nil {
		return "", ""
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	if !s.Scan() || strings.TrimSpace(s.Text()) != "---" {
		return "", ""
	}
	seen := map[string]bool{}
	for s.Scan() {
		line := s.Text()
		if strings.TrimSpace(line) == "---" {
			return name, model
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok || key != "name" && key != "model" {
			continue
		}
		if seen[key] {
			return "", ""
		}
		seen[key] = true
		value, ok = ompAgentScalar(strings.TrimSpace(value))
		if !ok {
			return "", ""
		}
		if key == "name" {
			name = value
		} else {
			model = value
		}
	}
	return "", ""
}

func ompAgentScalar(value string) (string, bool) {
	if value == "" || value[0] != '"' && value[0] != '\'' {
		value, _, _ = strings.Cut(value, " #")
		return strings.TrimSpace(value), true
	}
	quote := value[0]
	for i := 1; i < len(value); i++ {
		if quote == '"' && value[i] == '\\' {
			i++
			continue
		}
		if value[i] != quote {
			continue
		}
		if quote == '\'' && i+1 < len(value) && value[i+1] == '\'' {
			i++
			continue
		}
		if rest := strings.TrimSpace(value[i+1:]); rest != "" && !strings.HasPrefix(rest, "#") {
			return "", false
		}
		if quote == '\'' {
			return strings.ReplaceAll(value[1:i], "''", "'"), true
		}
		parsed, err := strconv.Unquote(value[:i+1])
		return parsed, err == nil
	}
	return "", false
}
