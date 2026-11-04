package fleet

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

func (a *App) launchArgs(s Seat, l Launch) ([]string, []string, error) {
	m, e := l.ModelID, l.Effort
	var env []string
	switch s.Kind {
	case "claude":
		argv := []string{"--model", m, "--dangerously-skip-permissions"}
		if e != "" && s.Route == "" {
			argv = append(argv, "--effort", e)
		}
		if s.Route != "" {
			env, err := a.routeEnv(s)
			return argv, env, err
		}
		return argv, env, nil
	case "codex":
		return []string{"--yolo", "-m", m, "-c", "model_reasoning_effort=" + e}, env, nil
	case "devin":
		return []string{"--model", m, "--permission-mode", "dangerous", "--respect-workspace-trust", "false"}, env, nil
	case "grok":
		return []string{"--always-approve", "-m", m, "--effort", e}, env, nil
	case "pi":
		return []string{"--no-mcp", "--model", m + ":" + e}, env, nil
	case "omp":
		return []string{"--model", m + ":" + e}, []string{"OMP_SKIP_SETUP=1"}, nil
	case "cmd":
		return []string{"-m", m, "--effort", e, "--yolo", "--skip-onboarding", "--no-session"}, env, nil
	case "kimi":
		home := os.Getenv("KIMI_CODE_HOME")
		if home == "" {
			home = filepath.Join(a.Home, ".kimi-code")
		}
		if s.Pin {
			key := "KIMI_CODE_" + strings.ToUpper(e) + "_HOME"
			home = os.Getenv(key)
			if home == "" {
				home = filepath.Join(a.Home, ".local", "share", "kimi-code-"+e)
			}
		}
		c, err := readThinking(home)
		if err != nil || c != e {
			return nil, nil, fail(exitConfig, "Kimi clone must attest thinking effort %s", e)
		}
		return []string{"--auto", "-m", m}, []string{"KIMI_CODE_HOME=" + home}, nil
	case "kiro":
		return []string{"chat", "--v3", "--trust-all-tools", "--require-mcp-startup", "--model", m, "--effort", e}, env, nil
	case "opencode":
		return []string{"--auto", "--model", m}, env, nil
	case "agy":
		return []string{"--model", m, "--dangerously-skip-permissions"}, env, nil
	default:
		return nil, nil, fail(exitConfig, "unsupported agent kind %s", s.Kind)
	}
}

// launchProfile names the launched rung; a rung-less launch has no @effort.
func launchProfile(canon, effort string) string {
	if effort == "" {
		return canon
	}
	return canon + "@" + effort
}

// route describes where a routed claude harness sends its model traffic. The
// endpoint and the key file come from the environment; the key is read at
// spawn time and handed only to the spawned pane.
type route struct{ baseURLEnv, defaultURL, keyFileEnv, defaultKeyFile string }

var routes = map[string]route{
	"clinepass": {"CLINEPASS_GATE_URL", "http://127.0.0.1:8317", "CLINEPASS_GATE_KEY_FILE", ".config/cliproxyapi-key.txt"},
}

func (a *App) routeEnv(s Seat) ([]string, error) {
	r := routes[s.Route]
	keyFile := os.Getenv(r.keyFileEnv)
	if keyFile == "" {
		keyFile = filepath.Join(a.Home, r.defaultKeyFile)
	} else if keyFile == "~" || strings.HasPrefix(keyFile, "~/") {
		keyFile = filepath.Join(a.Home, strings.TrimPrefix(keyFile, "~"))
	}
	b, err := os.ReadFile(keyFile)
	key := strings.TrimSpace(string(b))
	if err != nil || key == "" {
		return nil, fail(exitConfig, "%s route key file is missing or empty", s.Route)
	}
	env := []string{"ANTHROPIC_BASE_URL=" + firstNonempty(os.Getenv(r.baseURLEnv), r.defaultURL), "ANTHROPIC_AUTH_TOKEN=" + key}
	if s.ContextTokens > 0 {
		env = append(env, "CLAUDE_CODE_MAX_CONTEXT_TOKENS="+strconv.Itoa(s.ContextTokens))
	}
	return env, nil
}
func readThinking(home string) (string, error) {
	b, e := os.ReadFile(filepath.Join(home, "config.toml"))
	if e != nil {
		return "", e
	}
	var t struct {
		Thinking struct {
			Effort string `toml:"effort"`
		} `toml:"thinking"`
	}
	if e = unmarshalTOML(b, &t); e != nil {
		return "", e
	}
	return t.Thinking.Effort, nil
}

type Agent struct {
	Pane           string `json:"pane_id"`
	Tab            string `json:"tab_id"`
	Name           string `json:"name"`
	Kind           string `json:"agent"`
	Status         string `json:"agent_status"`
	CWD            string `json:"cwd"`
	StateChangeSeq *int64 `json:"state_change_seq,omitempty"`
	Focused        bool   `json:"focused,omitempty"`
}

func (a *App) agents(ctx context.Context) ([]Agent, error) {
	r := a.call(ctx, "herdr", "agent", "list")
	if e := check(r, "herdr agent list"); e != nil {
		return nil, e
	}
	var m map[string]json.RawMessage
	if e := json.Unmarshal([]byte(r.Out), &m); e != nil {
		return nil, e
	}
	if v, ok := m["result"]; ok {
		if e := json.Unmarshal(v, &m); e != nil {
			return nil, e
		}
	}
	v, ok := m["agents"]
	if !ok {
		return nil, fail(exitConfig, "agent list missing agents")
	}
	var agents []Agent
	e := json.Unmarshal(v, &agents)
	return agents, e
}
func (a *App) status(ctx context.Context, pane string) (string, error) {
	agents, e := a.agents(ctx)
	if e != nil {
		return "", e
	}
	for _, ag := range agents {
		if ag.Pane == pane {
			return ag.Status, nil
		}
	}
	return "", fail(exitConfig, "pane absent from agent list")
}

var ansi = regexp.MustCompile("\x1b\\[[0-?]*[ -/]*[@-~]")

func cleanScreen(s string) []string {
	lines := strings.Split(ansi.ReplaceAllString(s, ""), "\n")
	for i := range lines {
		lines[i] = strings.TrimRight(lines[i], " \r\t")
	}
	for len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}
func normalized(s string) string { return strings.Join(strings.Fields(s), " ") }
func (a *App) visible(ctx context.Context, pane string) (string, error) {
	r := a.call(ctx, "herdr", "pane", "read", pane, "--source", "visible", "--lines", "80")
	return r.Out, check(r, "herdr visible read")
}
func CmdProof(screen, model, effort, cwd, home, brief string) bool {
	lines := cleanScreen(screen)
	if _, name, ok := strings.Cut(model, "/"); ok {
		model = name
	}
	if !strings.HasSuffix(model, "-(latest)") {
		model += "-(latest)"
	}
	want := "# models: " + model + " with " + effort + " effort · "
	count := 0
	for i, line := range lines {
		if !strings.HasPrefix(line, "# models: ") {
			continue
		}
		if !strings.HasPrefix(line, want) || len(line) == len(want) || i == 0 || i+1 >= len(lines) || !regexp.MustCompile(`^# Command Code v\S+$`).MatchString(lines[i-1]) {
			return false
		}
		shown := []string{"# " + cwd}
		if cwd == home {
			shown = append(shown, "# ~")
		} else if strings.HasPrefix(cwd, strings.TrimRight(home, "/")+"/") {
			shown = append(shown, "# ~"+strings.TrimPrefix(cwd, home))
		}
		if !slices.Contains(shown, lines[i+1]) {
			return false
		}
		block := normalized(strings.Join(lines[i-1:i+2], "\n"))
		if strings.Contains(normalized(brief), block) {
			return false
		}
		count++
	}
	return count == 1
}
func PiProof(screen, model, effort, cwd, home, brief string) bool {
	lines := cleanScreen(screen)
	bottom := -1
	for i, line := range lines {
		if regexp.MustCompile(`^(─{3,}|─+ ↓ \d+ more ─+)$`).MatchString(line) {
			bottom = i
		}
	}
	if bottom < 2 || len(lines)-bottom < 3 || len(lines)-bottom > 4 {
		return false
	}
	top := -1
	for i := 0; i < bottom; i++ {
		if strings.HasPrefix(lines[i], "──") {
			top = i
		}
	}
	if top < 0 || bottom-top < 2 {
		return false
	}
	footer := lines[bottom+1:]
	paths := []string{cwd}
	if real, e := filepath.EvalSymlinks(cwd); e == nil {
		paths = append(paths, real)
	}
	for _, p := range slices.Clone(paths) {
		if p == home {
			paths = append(paths, "~")
		} else if strings.HasPrefix(p, home+"/") {
			paths = append(paths, "~"+p[len(home):])
		}
	}
	cwdOK := false
	for _, p := range paths {
		if regexp.MustCompile(`^` + regexp.QuoteMeta(p) + `(?: \([^\r\n]*\))?(?: • [^\r\n]+)?$`).MatchString(footer[0]) {
			cwdOK = true
		}
	}
	provider, name, ok := strings.Cut(model, "/")
	if !ok || !cwdOK {
		return false
	}
	pattern := `^(?s).*(?:\d+(?:\.\d+)?%|\?)/\S+(?: \(auto\))?\s+(?:\(` + regexp.QuoteMeta(provider) + `\) )?` + regexp.QuoteMeta(name) + ` • ` + regexp.QuoteMeta(effort) + `$`
	return regexp.MustCompile(pattern).MatchString(footer[1]) && !strings.Contains(normalized(brief), normalized(strings.Join(footer[:2], "\n")))
}
func (a *App) modelProof(ctx context.Context, s Seat, l Launch, cwd, pane, brief string) error {
	if s.Kind == "omp" {
		return a.ompProof(ctx, l, cwd, pane, brief, ompLandedProofWait)
	}
	screen, e := a.visible(ctx, pane)
	if e != nil {
		return e
	}
	ok := true
	if s.Kind == "cmd" {
		ok = CmdProof(screen, l.ModelID, l.Effort, cwd, a.Home, brief)
	}
	if s.Kind == "pi" {
		ok = PiProof(screen, l.ModelID, l.Effort, cwd, a.Home, brief)
	}
	if !ok {
		return fail(exitConfig, "%s model/effort not proven by visible session", s.Kind)
	}
	return nil
}

// ompLandedProofWait bounds the re-check after the brief lands: the seat already
// painted its footer once, so only a redraw can hide it.
const ompLandedProofWait = 15 * time.Second

type processInfo struct {
	ShellPID  int `json:"shell_pid"`
	Processes []struct {
		PID     int      `json:"pid"`
		Argv0   string   `json:"argv0"`
		Argv    []string `json:"argv"`
		Name    string   `json:"name"`
		Cmdline string   `json:"cmdline"`
		CWD     string   `json:"cwd"`
	} `json:"foreground_processes"`
}

func (a *App) pinfo(ctx context.Context, pane string) (processInfo, error) {
	r := a.call(ctx, "herdr", "pane", "process-info", "--pane", pane)
	var p processInfo
	if e := check(r, "process-info"); e != nil {
		return p, e
	}
	var m map[string]json.RawMessage
	if e := json.Unmarshal([]byte(r.Out), &m); e != nil {
		return p, e
	}
	if v, ok := m["result"]; ok {
		if e := json.Unmarshal(v, &m); e != nil {
			return p, e
		}
	}
	v, ok := m["process_info"]
	if !ok {
		return p, fail(exitConfig, "missing process_info")
	}
	if e := json.Unmarshal(v, &p); e != nil {
		return p, e
	}
	if p.Processes == nil {
		return p, fail(exitConfig, "missing foreground_processes")
	}
	return p, nil
}
func (a *App) start(ctx context.Context, s Spawn, seat Seat, l Launch, pane string, argv []string) error {
	timeout := time.Duration(integer(a.Config.Local, "shell_ready_timeout_ms", 120000)) * time.Millisecond
	deadline := a.Now().Add(timeout)
	ready := false
	for n := 0; n < 120 && a.Now().Before(deadline); n++ {
		p, e := a.pinfo(ctx, pane)
		if e == nil && p.ShellPID != 0 && len(p.Processes) == 1 && p.Processes[0].PID == p.ShellPID {
			ready = true
			break
		}
		if e = a.Sleep(ctx, time.Second); e != nil {
			return e
		}
	}
	if !ready {
		return fail(exitConfig, "shell not ready")
	}
	// Exact output-line proof avoids typing launch argv into an rc-file read.
	var nonce [12]byte
	if _, e := rand.Read(nonce[:]); e != nil {
		return e
	}
	token := "panewire-ready-" + hex.EncodeToString(nonce[:])
	if e := a.must(ctx, "herdr", "pane", "run", pane, "echo", token); e != nil {
		return e
	}
	ready = false
	for n := 0; n < 20 && a.Now().Before(deadline); n++ {
		r := a.call(ctx, "herdr", "pane", "read", pane, "--source", "visible", "--lines", "40")
		for _, line := range cleanScreen(r.Out) {
			if line == token {
				ready = true
			}
		}
		if ready {
			break
		}
		if e := a.Sleep(ctx, time.Second); e != nil {
			return e
		}
	}
	if !ready {
		return fail(exitConfig, "shell command execution not proven")
	}
	var args []string
	if seat.Kind == "devin" || seat.Kind == "cmd" {
		args = append([]string{"pane", "run", pane, seat.Kind}, argv...)
	} else {
		args = append([]string{"agent", "start", s.Label, "--kind", seat.Kind, "--pane", pane, "--timeout", fmt.Sprint(timeout.Milliseconds()), "--"}, argv...)
	}
	if e := a.must(ctx, "herdr", args...); e != nil {
		if trustErr := a.checkTrustPrompt(ctx, seat.Kind, pane, s.CWD); trustErr != nil {
			return trustErr
		}
		return e
	}
	if seat.Kind == "devin" || seat.Kind == "cmd" {
		if e := a.waitAgentIdle(ctx, pane, timeout); e != nil {
			return e
		}
		if e := a.must(ctx, "herdr", "agent", "rename", pane, s.Label); e != nil {
			return e
		}
	}
	foreground := false
	for n := 0; n < 30; n++ {
		p, e := a.pinfo(ctx, pane)
		if e != nil {
			return e
		}
		for _, proc := range p.Processes {
			if foregroundAgent(seat.Kind, proc.Argv0, proc.Name, proc.Argv) {
				foreground = true
			}
		}
		if foreground {
			break
		}
		if e = a.Sleep(ctx, time.Second); e != nil {
			return e
		}
	}
	if !foreground {
		if e := a.checkTrustPrompt(ctx, seat.Kind, pane, s.CWD); e != nil {
			return e
		}
		return fail(exitConfig, "agent not in foreground; brief withheld")
	}
	if e := a.checkTrustPrompt(ctx, seat.Kind, pane, s.CWD); e != nil {
		return e
	}
	if seat.Kind == "devin" {
		r := a.call(ctx, "herdr", "agent", "explain", pane)
		if e := check(r, "Devin identity proof"); e != nil {
			return e
		}
		if !strings.Contains(r.Out, "welcome_prompt_footer") {
			return fail(exitConfig, "Devin identity not proven")
		}
	}
	if seat.Kind == "pi" || seat.Kind == "cmd" {
		if e := a.modelProof(ctx, seat, l, s.CWD, pane, ""); e != nil {
			return e
		}
	}
	if seat.Kind == "omp" {
		if e := a.ompProof(ctx, l, s.CWD, pane, "", timeout); e != nil {
			return e
		}
		if e := a.ompProcessCWD(ctx, s.CWD, pane); e != nil {
			return e
		}
	}
	if seat.Proof == "codex-footer" {
		// Codex draws its footer (model, effort, cwd) a moment after start; a
		// single read right after launch sees only the banner. Re-read until
		// the footer proves the launch or the launch deadline passes.
		proven := false
		for deadline := a.Now().Add(timeout); ; {
			screen, e := a.visible(ctx, pane)
			if e != nil {
				return e
			}
			if CodexFooterProof(screen, l.ModelID, l.Effort, s.CWD, a.Home) {
				proven = true
				break
			}
			if codexTrustPrompt(screen) {
				return trustPromptError(s.CWD)
			}
			if codexHooksReviewPrompt(screen) {
				return hooksReviewError()
			}
			if !a.Now().Before(deadline) {
				break
			}
			if e := a.Sleep(ctx, time.Second); e != nil {
				return e
			}
		}
		if !proven {
			return fail(exitConfig, "Codex model/effort footer not proven")
		}
	}
	if e := a.waitAgentIdle(ctx, pane, timeout); e != nil {
		if trustErr := a.checkTrustPrompt(ctx, seat.Kind, pane, s.CWD); trustErr != nil {
			return trustErr
		}
		return e
	}
	return nil
}

// waitAgentIdle waits for the pane's agent to go idle. herdr answers
// agent_not_found at once for a pane whose agent it has not detected yet
// (a CLI started with pane run is recognised a moment later), so that one
// answer is retried until the deadline instead of failing the spawn.
func (a *App) waitAgentIdle(ctx context.Context, pane string, timeout time.Duration) error {
	deadline := a.Now().Add(timeout)
	for {
		left := deadline.Sub(a.Now())
		if left < time.Second {
			left = time.Second
		}
		r := a.call(ctx, "herdr", "agent", "wait", pane, "--timeout", fmt.Sprint(left.Milliseconds()))
		if r.Code == 0 || !strings.Contains(r.Out+r.Err, "agent_not_found") || !a.Now().Before(deadline) {
			return check(r, "herdr")
		}
		if e := a.Sleep(ctx, time.Second); e != nil {
			return e
		}
	}
}
func CodexFooterProof(screen, model, effort, cwd, home string) bool {
	lines := cleanScreen(screen)
	expected := strings.ToLower(strings.ReplaceAll(model, "-", " "))
	for i := len(lines) - 1; i >= 0; i-- {
		line := lines[i]
		cwdShown := strings.Contains(line, cwd)
		if strings.HasPrefix(cwd, home+"/") {
			cwdShown = cwdShown || strings.Contains(line, "~"+strings.TrimPrefix(cwd, home))
		}
		// The footer is the last row that shows the cwd. Older Codex also
		// printed "Context N%" there; 0.166 prints only "<model> <effort> · <cwd>".
		if !cwdShown && !strings.Contains(strings.ToLower(line), "context") {
			continue
		}
		shown := strings.ToLower(strings.ReplaceAll(line, "-", " "))
		return cwdShown && strings.Contains(shown, expected) && regexp.MustCompile(`(?i)\b`+regexp.QuoteMeta(effort)+`\b`).MatchString(line)
	}
	return false
}
func foregroundAgent(kind, argv0, name string, argv []string) bool {
	exe := procName(firstNonEmpty(argv0, firstArg(argv), name))
	if slices.Contains(executables(kind), exe) {
		return true
	}
	return kind == "omp" && exe == "bun" && ompBunScript(argv)
}

func executables(kind string) []string {
	switch kind {
	case "claude":
		return []string{"claude", "claude-code"}
	case "devin":
		return []string{"devin", "devin-cli"}
	case "grok":
		return []string{"grok", "grok-build"}
	case "kimi":
		return []string{"kimi-code", "kimi"}
	case "cmd":
		return []string{"cmd", "command-code"}
	case "kiro":
		return []string{"kiro", "kiro-cli"}
	case "opencode":
		return []string{"opencode", "open-code"}
	}
	return []string{kind}
}
func (a *App) deliver(ctx context.Context, kind, pane, brief string) error {
	before, e := a.status(ctx, pane)
	if e != nil {
		return e
	}
	if before != "idle" && before != "done" {
		return fail(exitSubmitUnconfirmed, "agent must be idle before initial brief")
	}
	initial, e := a.visible(ctx, pane)
	if e != nil {
		return e
	}
	if kind == "cmd" {
		if e = a.must(ctx, "herdr", "pane", "send-text", pane, brief); e != nil {
			return e
		}
		if e = a.must(ctx, "herdr", "pane", "send-keys", pane, "return"); e != nil {
			return e
		}
	} else {
		if e = a.must(ctx, "herdr", "agent", "prompt", pane, brief); e != nil {
			return e
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		residue := false
		for n := 0; n < 15; n++ {
			status, e := a.status(ctx, pane)
			if e != nil {
				return e
			}
			visible, e := a.visible(ctx, pane)
			if e != nil {
				return e
			}
			// A pasted chip or the brief in the composer is not proof. Require the
			// cold idle pane to start working, corroborated by no unsubmitted chip.
			residue = briefInComposer(kind, visible, brief)
			if !residue && (status == "working" || submittedBrief(kind, visible, brief) && !submittedBrief(kind, initial, brief)) {
				return nil
			}
			if e = a.Sleep(ctx, time.Second); e != nil {
				return e
			}
		}
		// Retry the submit key, never paste a second copy of an ambiguous payload.
		if attempt == 0 && (kind != "codex" || residue) {
			scope := "agent"
			if kind == "cmd" {
				scope = "pane"
			}
			if e = a.must(ctx, "herdr", scope, "send-keys", pane, "return"); e != nil {
				return e
			}
		}
	}
	return fail(exitSubmitUnconfirmed, "BRIEF_SUBMISSION_UNCONFIRMED: session=%s pane=%s; no working transition or submitted brief turn", a.herdrSession(), pane)
}

// A prompt marker alone can be the editor. Require the exact payload followed
// by an assistant turn; this also proves a brief that finished between reads.
func submittedBrief(kind, screen, brief string) bool {
	if kind != "codex" {
		return false
	}
	lines := cleanScreen(screen)
	for i, line := range lines {
		if !strings.HasPrefix(strings.TrimSpace(line), "› ") {
			continue
		}
		for j := i + 1; j < len(lines); j++ {
			if strings.HasPrefix(strings.TrimSpace(lines[j]), "› ") {
				break
			}
			if strings.HasPrefix(strings.TrimSpace(lines[j]), "• ") {
				payload := strings.TrimPrefix(strings.TrimSpace(line), "› ") + strings.Join(lines[i+1:j], "")
				if compact(payload) == compact(brief) {
					return true
				}
				break
			}
		}
	}
	return false
}

func compact(s string) string { return strings.Join(strings.Fields(s), "") }

func briefInComposer(kind, screen, brief string) bool {
	if strings.Contains(screen, "[Pasted text") {
		return true
	}
	if kind != "codex" {
		return false
	}
	lines := cleanScreen(screen)
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.HasPrefix(strings.TrimSpace(lines[i]), "› ") {
			return strings.Contains(compact(strings.Join(lines[i:], "")), compact(brief)) && !submittedBrief(kind, strings.Join(lines[i:], "\n"), brief)
		}
	}
	return false
}

func codexTrustPrompt(screen string) bool {
	s := strings.ToLower(normalized(screen))
	legacy := strings.Contains(s, "do you trust the contents of this directory?") && strings.Contains(s, "yes, continue") && strings.Contains(s, "no, quit")
	current := strings.Contains(s, "folder access") && strings.Contains(s, "trust this folder?") && strings.Contains(s, "trust and continue") && strings.Contains(s, "2. quit")
	return legacy || current
}

func trustPromptError(cwd string) error {
	return fail(exitTrustPrompt, "WORKSPACE_TRUST_REQUIRED: Codex directory trust prompt for %s; brief withheld; approve only this worktree in the executing CODEX_HOME", cwd)
}

// codexHooksReviewPrompt reports Codex's startup "Hooks need review" screen,
// shown before the composer when hooks are new or changed. It matches the title
// and two of the three choices, which every width renders (a narrow terminal
// wraps the explanatory line and the third choice, so those are not matched
// whole). The strings come from codex-rs/tui/src/startup_hooks_review.rs at tag
// rust-v0.166.0 (selection_view_params) and its rendered snapshots
// codex-rs/tui/src/snapshots/codex_tui__startup_hooks_review__tests__startup_hooks_review_prompt*.snap
// (testdata/codex-hooks-review-fixtures.md). panewire only detects the screen;
// hook trust is the host owner's decision.
func codexHooksReviewPrompt(screen string) bool {
	s := strings.ToLower(normalized(screen))
	return strings.Contains(s, "hooks need review") && strings.Contains(s, "trust all and continue") && strings.Contains(s, "continue without trusting")
}

func hooksReviewError() error {
	return fail(exitTrustPrompt, "HOOKS_REVIEW_REQUIRED: Codex is waiting at its Hooks need review screen; brief withheld; the host owner reviews the hooks in Codex on this host (the executing CODEX_HOME), then respawn")
}

func (a *App) checkTrustPrompt(ctx context.Context, kind, pane, cwd string) error {
	if kind == "codex" {
		if screen, e := a.visible(ctx, pane); e == nil {
			if codexTrustPrompt(screen) {
				return trustPromptError(cwd)
			}
			if codexHooksReviewPrompt(screen) {
				return hooksReviewError()
			}
		}
	}
	return nil
}
func (a *App) registerLane(ctx context.Context, lane, parent, pane, role, machine string) error {
	if str(a.Config.Hub, "hub_url") == "" {
		return fail(exitConfig, "hub lane config missing")
	}
	args := []string{"lanes", "add", lane, "--machine", machine, "--pane", pane, "--parent", parent}
	args = append(args, a.laneHubArgs(role)...)
	for i, arg := range args {
		if arg != "--hub-token-env" || i+1 == len(args) {
			continue
		}
		args[i+1] = expand(args[i+1])
		env, e := readEnvFile(args[i+1])
		if id := env["HUB_MACHINE_ID"]; e == nil && id != "" && id != "operator" && id != machine {
			return fail(exitConfig, "LANE_SESSION_MACHINE_MISMATCH: session=%s route=%s credential_machine=%s; configure hub.session_machine_ids and hub.session_lanes_token_envs together", a.herdrSession(), machine, id)
		}
	}
	return a.must(ctx, "panewire", args...)
}

func (a *App) laneHubArgs(role string) []string {
	c := a.Config
	c.Hub = maps.Clone(a.Config.Hub)
	if tokens, ok := c.Hub["session_lanes_token_envs"].(map[string]any); ok {
		if token := str(tokens, a.herdrSession()); token != "" {
			c.Hub["lanes_token_env"] = token
		}
	}
	return c.hubArgs(role)
}
func (a *App) hubGate(ctx context.Context, cwd, pool string) error {
	if str(a.Config.Hub, "hub_token_env") == "" {
		return nil
	}
	if !validID(pool) {
		return fail(exitConfig, "quota gate response lacks pool")
	}
	args := []string{"place", "--class", "worker", "--cwd", cwd, "--pool", pool, "--hub-url", str(a.Config.Hub, "hub_url"), "--hub-token-env", str(a.Config.Hub, "hub_token_env")}
	if v := str(a.Config.Hub, "hub_cf_env"); v != "" {
		args = append(args, "--hub-cf-env", v)
	}
	r := a.call(ctx, "panewire", args...)
	switch r.Code {
	case 0:
		return nil
	case 4:
		a.warn("hub quota unavailable; using local gate")
		return nil
	case 2, 5, 6, 70:
		return check(r, "hub quota policy")
	default:
		return fail(exitConfig, "hub quota policy failed closed")
	}
}

// procName is a process's lowercased executable name without a Windows-style
// ".exe" suffix (in any case): Claude Code's native installer links claude to claude.exe, so the
// foreground process can report either name.
func procName(argv0 string) string {
	name := argv0
	if i := strings.LastIndexAny(name, `/\`); i >= 0 {
		name = name[i+1:]
	}
	return strings.TrimSuffix(strings.ToLower(name), ".exe")
}

// herdr on Linux reports argv and name but no argv0, so the foreground check
// falls back to argv[0], then the process name.
func firstArg(argv []string) string {
	if len(argv) == 0 {
		return ""
	}
	return argv[0]
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}
