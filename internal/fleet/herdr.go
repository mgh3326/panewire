package fleet

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
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
		// check_for_update_on_startup=false: an "Update now / Skip" prompt at
		// startup holds the pane before it reads the brief (agent_not_ready),
		// whatever the host's ~/.codex/config.toml says. Upgrades are deploys.
		return []string{"--yolo", "-m", m, "-c", "model_reasoning_effort=" + e, "-c", "check_for_update_on_startup=false"}, env, nil
	case "devin":
		return []string{"--model", m, "--permission-mode", "dangerous", "--respect-workspace-trust", "false"}, env, nil
	case "omp":
		if s.NoThinking && e == "off" {
			return []string{"--model", m}, []string{"OMP_SKIP_SETUP=1"}, nil
		}
		return []string{"--model", m + ":" + e}, []string{"OMP_SKIP_SETUP=1"}, nil
	case "cmd":
		return []string{"-m", m, "--effort", e, "--yolo", "--skip-onboarding", "--no-session"}, env, nil
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

// withSessionName names a Claude session after its job, so ListAgents,
// /list-agents and `@name` address it by the id the fleet already uses
// (hk 4258). Only claude takes --name; every other kind's argv is returned
// unchanged. panewire starts no design, desk or policy session (those are
// operator-opened and carry their lane name through /rename), so the job id
// is the one name a panewire-started session gets.
func withSessionName(kind string, argv []string, job string) []string {
	if kind != "claude" || job == "" {
		return argv
	}
	return append(argv, "--name", job)
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

type Agent struct {
	Pane           string          `json:"pane_id"`
	Tab            string          `json:"tab_id"`
	Name           string          `json:"name"`
	Kind           string          `json:"agent"`
	Status         string          `json:"agent_status"`
	CWD            string          `json:"cwd"`
	StateChangeSeq *int64          `json:"state_change_seq,omitempty"`
	Focused        bool            `json:"focused,omitempty"`
	Session        json.RawMessage `json:"agent_session,omitempty"`
}

func (a *App) agents(ctx context.Context) ([]Agent, error) {
	r := a.call(ctx, "herdr", "agent", "list")
	if e := check(r, "herdr agent list"); e != nil {
		return nil, e
	}
	return ParseAgents([]byte(r.Out))
}

// ParseAgents decodes herdr agent.list metadata.
func ParseAgents(data []byte) ([]Agent, error) {
	return parseHerdrList[Agent](data, "agent", "agents")
}

// Pane is one `herdr pane list` row. The pane list names plain shells as
// well as agents, and carries a pane's label where the agent list has its
// name.
type Pane struct {
	Pane  string `json:"pane_id"`
	Label string `json:"label"`
	Name  string `json:"name"`
}

// panes reads `herdr pane list`, the full pane inventory. As the node's
// pane list reader does, an answer with no rows array is an error.
func (a *App) panes(ctx context.Context) ([]Pane, error) {
	r := a.call(ctx, "herdr", "pane", "list")
	if e := check(r, "herdr pane list"); e != nil {
		return nil, e
	}
	return parsePaneList([]byte(r.Out))
}

// parseHerdrList decodes the rows under key of a herdr list answer, bare or
// in its result envelope; list names the command in the error.
func parseHerdrList[T any](data []byte, list, key string) ([]T, error) {
	var m map[string]json.RawMessage
	if e := json.Unmarshal(data, &m); e != nil {
		return nil, e
	}
	if v, ok := m["result"]; ok {
		if e := json.Unmarshal(v, &m); e != nil {
			return nil, e
		}
	}
	v, ok := m[key]
	if !ok {
		return nil, fail(exitConfig, "%s list missing %s", list, key)
	}
	var rows []T
	e := json.Unmarshal(v, &rows)
	return rows, e
}

// parsePaneList decodes a `herdr pane list` answer, bare or in its result
// envelope, more strictly than parseHerdrList: its absence of a pane is
// evidence a job dir may go. An answer that carries an error (an "error"
// member or "ok":false, at either level), one with no panes array, and any
// row that is not an object with a pane id is an error: a row panewire
// cannot read must not read as a pane that is not there.
func parsePaneList(data []byte) ([]Pane, error) {
	var m map[string]json.RawMessage
	if e := json.Unmarshal(data, &m); e != nil {
		return nil, e
	}
	if e := herdrListError(m, "pane"); e != nil {
		return nil, e
	}
	if v, ok := m["result"]; ok {
		m = nil
		if e := json.Unmarshal(v, &m); e != nil {
			return nil, e
		}
		if e := herdrListError(m, "pane"); e != nil {
			return nil, e
		}
	}
	var raw []json.RawMessage
	if v, ok := m["panes"]; ok {
		if e := json.Unmarshal(v, &raw); e != nil {
			return nil, e
		}
	}
	if raw == nil {
		return nil, fail(exitConfig, "pane list missing panes")
	}
	panes := make([]Pane, 0, len(raw))
	for i, r := range raw {
		if !bytes.HasPrefix(bytes.TrimSpace(r), []byte("{")) {
			return nil, fail(exitConfig, "pane list row %d is not an object", i)
		}
		var p Pane
		if e := json.Unmarshal(r, &p); e != nil {
			return nil, e
		}
		if strings.TrimSpace(p.Pane) == "" {
			return nil, fail(exitConfig, "pane list row %d has no pane_id", i)
		}
		panes = append(panes, p)
	}
	return panes, nil
}

// herdrListError names the error a herdr list answer (or its result)
// carries: a non-null "error" member, or "ok":false.
func herdrListError(m map[string]json.RawMessage, list string) error {
	if v, ok := m["error"]; ok && string(bytes.TrimSpace(v)) != "null" {
		return fail(exitConfig, "%s list answered an error", list)
	}
	if v, ok := m["ok"]; ok && string(bytes.TrimSpace(v)) == "false" {
		return fail(exitConfig, "%s list answered ok:false", list)
	}
	return nil
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
func (a *App) modelProof(ctx context.Context, s Seat, l Launch, cwd, pane, brief string) error {
	if s.Kind == "omp" {
		return a.ompProof(ctx, l, s.NoThinking, cwd, pane, brief, ompLandedProofWait)
	}
	screen, e := a.visible(ctx, pane)
	if e != nil {
		return e
	}
	ok := true
	if s.Kind == "cmd" {
		ok = CmdProof(screen, l.ModelID, l.Effort, cwd, a.Home, brief)
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
	if seat.Kind == "cmd" {
		if e := a.modelProof(ctx, seat, l, s.CWD, pane, ""); e != nil {
			return e
		}
	}
	if seat.Kind == "omp" {
		if e := a.ompProof(ctx, l, seat.NoThinking, s.CWD, pane, "", timeout); e != nil {
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
			return &launchProofError{&ExitError{Code: exitConfig, Message: "Codex model/effort footer not proven"}}
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
	case "cmd":
		return []string{"cmd", "command-code"}
	case "kiro":
		return []string{"kiro", "kiro-cli"}
	case "opencode":
		return []string{"opencode", "open-code"}
	}
	return []string{kind}
}

const (
	defaultBriefSubmitTimeoutMS = 120000
	defaultBriefSubmitRetryMS   = 15000
	briefSubmitWaitSlice        = 5 * time.Second
	// briefSubmitReserve leaves the node time to capture the pane and finish
	// cleanup (10s bound plus the 2s settle) inside its spawn budget.
	briefSubmitReserve = 20 * time.Second
)

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
	timeout, retry := a.briefSubmitWindow()
	clamped := false
	// A node runs the spawn under its hub_wait budget. Refuse with 76 and the
	// capture before that budget expires, or the node reports a bare timeout.
	if until, ok := ctx.Deadline(); ok {
		if left := time.Until(until) - briefSubmitReserve; left < timeout {
			timeout, clamped = max(left, 0), true
		}
	}
	deadline := a.Now().Add(timeout)
	polls := int(timeout / time.Second)
	nextReturn := a.Now().Add(retry)
	residue := false
	useWait := kind != "cmd"
	for n := 1; n <= polls && a.Now().Before(deadline); n++ {
		status := ""
		wake := ""
		if useWait {
			left := min(briefSubmitWaitSlice, deadline.Sub(a.Now()), nextReturn.Sub(a.Now()))
			if left >= time.Millisecond {
				r := a.call(ctx, "herdr", "agent", "wait", pane, "--until", "working", "--until", "blocked", "--timeout", fmt.Sprint(left.Milliseconds()))
				wake = briefWaitOutcome(r)
				if wake == "" {
					useWait = false
				} else if wake == "working" {
					status = wake
				}
			}
		}
		if status == "" {
			status, e = a.status(ctx, pane)
			if e != nil {
				return e
			}
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
		if !useWait || wake != "timeout" {
			if e = a.Sleep(ctx, time.Second); e != nil {
				return e
			}
		}
		// Retry the submit key, never paste a second copy of an ambiguous payload.
		// The cadence is wall-clock: one poll can take seconds on a loaded host.
		if !a.Now().Before(nextReturn) && n < polls && a.Now().Before(deadline) && submitKeyAllowed(kind, status, residue) {
			nextReturn = a.Now().Add(retry)
			scope := "agent"
			if kind == "cmd" {
				scope = "pane"
			}
			if e = a.must(ctx, "herdr", scope, "send-keys", pane, "return"); e != nil {
				return e
			}
		}
	}
	note := ""
	if residue {
		note = "; brief still in composer"
	}
	if clamped {
		note += fmt.Sprintf("; window cut to %ds by the node budget, raise hub_wait for this host", int(timeout/time.Second))
	}
	if note != "" {
		return fail(exitSubmitUnconfirmed, "BRIEF_SUBMISSION_UNCONFIRMED: session=%s pane=%s; no working transition or submitted brief turn%s", a.herdrSession(), pane, note)
	}
	return fail(exitSubmitUnconfirmed, "BRIEF_SUBMISSION_UNCONFIRMED: session=%s pane=%s; no working transition or submitted brief turn", a.herdrSession(), pane)
}

// briefWaitOutcome accepts only the lifecycle responses understood by deliver.
func briefWaitOutcome(r Result) string {
	var reply struct {
		Result struct {
			Type  string `json:"type"`
			Agent *Agent `json:"agent"`
		} `json:"result"`
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(firstNonempty(r.Out, r.Err)), &reply) != nil {
		return ""
	}
	if r.Code != 0 && reply.Error.Code == "timeout" {
		return "timeout"
	}
	if r.Code == 0 && reply.Result.Type == "agent_info" && reply.Result.Agent != nil {
		switch reply.Result.Agent.Status {
		case "working", "blocked":
			return reply.Result.Agent.Status
		}
	}
	return ""
}

// briefSubmitWindow is how long deliver waits for a turn to start and how often
// it repeats the submit key. A loaded host starts turns late; see docs/config.md.
func (a *App) briefSubmitWindow() (timeout, retry time.Duration) {
	return time.Duration(integer(a.Config.Local, "brief_submit_timeout_ms", defaultBriefSubmitTimeoutMS)) * time.Millisecond,
		time.Duration(integer(a.Config.Local, "brief_submit_retry_ms", defaultBriefSubmitRetryMS)) * time.Millisecond
}

// submitKeyAllowed keeps Return away from a pane that could take it as an
// answer. Return confirms a dialog's default, so only an idle pane gets one;
// where the composer is readable, it must visibly still hold the brief.
func submitKeyAllowed(kind, status string, residue bool) bool {
	if status != "idle" && status != "done" {
		return false
	}
	if kind == "codex" || kind == "claude" {
		return residue
	}
	return true
}

// A prompt marker alone can be the editor. Require the exact payload followed
// by an assistant turn; this also proves a brief that finished between reads.
func submittedBrief(kind, screen, brief string) bool {
	if kind == "claude" {
		return claudeSubmittedBrief(cleanScreen(screen), brief)
	}
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
	if kind == "claude" {
		return claudeBriefInComposer(cleanScreen(screen), brief)
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

// claudePromptLines lists the rows carrying Claude Code's prompt marker; the
// last one is the composer and any earlier one is a submitted turn.
func claudePromptLines(lines []string) []int {
	var rows []int
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "❯") {
			rows = append(rows, i)
		}
	}
	return rows
}

func claudeRule(line string) bool {
	line = strings.TrimSpace(line)
	return line != "" && strings.Trim(line, "─") == ""
}

// claudeBriefInComposer also accepts a prefix: a loaded pane draws the typed
// text a piece at a time, and part of the brief is still unsubmitted text.
func claudeBriefInComposer(lines []string, brief string) bool {
	rows := claudePromptLines(lines)
	if len(rows) == 0 {
		return false
	}
	var body []string
	for _, line := range lines[rows[len(rows)-1]:] {
		if claudeRule(line) {
			break
		}
		body = append(body, line)
	}
	text := compact(strings.TrimPrefix(compact(strings.Join(body, "")), "❯"))
	return text != "" && strings.HasPrefix(compact(brief), text)
}

// A turn shows as the brief under a prompt marker above the composer.
func claudeSubmittedBrief(lines []string, brief string) bool {
	rows := claudePromptLines(lines)
	if len(rows) < 2 {
		return false
	}
	composer := rows[len(rows)-1]
	for _, row := range rows[:len(rows)-1] {
		if strings.HasPrefix(strings.TrimPrefix(compact(strings.Join(lines[row:composer], "")), "❯"), compact(brief)) {
			return true
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

// laneRegisterRetryDelay is the pause before the one retry of a failed hub lane
// registration; a hub that dropped one request usually answers the next.
const laneRegisterRetryDelay = time.Second

// lanePutRequest is one lane registration. Takeover names the predecessor job
// whose lane row this write moves to the requested pane: a --parent-job
// successor keeps the lane the task's findings are addressed to, which may be
// named differently from the successor's own label.
type lanePutRequest struct {
	Lane     string
	Machine  string
	Pane     string
	Parent   string
	Takeover string
	Role     string
}

// laneRegistration is the row a successful registration holds. A takeover can
// move a row named differently from the requested lane, so the name comes from
// the hub's answer, not from the request.
type laneRegistration struct {
	Lane    string
	Machine string
	Pane    string
	Parent  string
}

// answer reads the published row from a hub lane projection. An answer with no
// readable route falls back to the requested row: a write without a takeover
// always lands under the requested name.
func (req lanePutRequest) answer(raw []byte) laneRegistration {
	fallback := laneRegistration{Lane: req.Lane, Machine: req.Machine, Pane: req.Pane, Parent: req.Parent}
	var projection struct {
		Lane    string `json:"lane"`
		Machine string `json:"machine"`
		Pane    string `json:"pane"`
		Parent  string `json:"parent"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &projection) != nil {
		return fallback
	}
	if projection.Lane == "" || len(projection.Lane) > 128 || projection.Machine == "" || projection.Pane == "" {
		return fallback
	}
	return laneRegistration{Lane: projection.Lane, Machine: projection.Machine, Pane: projection.Pane, Parent: projection.Parent}
}

// laneNamePattern mirrors the hub's LaneNamePattern for the lane a task names:
// the fleet package cannot import the root, and a name the hub would refuse is
// not offered as a takeover hint.
var laneNamePattern = regexp.MustCompile(`^[a-z0-9_-]{1,32}$`)

// registerLaneWithRetry registers the lane and, when the hub refuses or is
// unreachable, tries once more. A local configuration refusal (exitConfig) is
// deterministic, so it is not retried. Each failed attempt is logged with its
// cause; the published row of the last attempt is returned.
func (a *App) registerLaneWithRetry(ctx context.Context, req lanePutRequest) (laneRegistration, error) {
	registration, e := a.registerLane(ctx, req)
	if e == nil {
		return registration, nil
	}
	a.warn("lane registration failed (session=%s machine=%s pane=%s): %v", a.herdrSession(), req.Machine, req.Pane, e)
	if ErrorCode(e) == exitConfig {
		return laneRegistration{}, e
	}
	if a.Sleep(ctx, laneRegisterRetryDelay) != nil {
		return laneRegistration{}, e
	}
	registration, e = a.registerLane(ctx, req)
	if e != nil {
		a.warn("lane registration failed again (session=%s machine=%s pane=%s): %v", a.herdrSession(), req.Machine, req.Pane, e)
		return laneRegistration{}, e
	}
	return registration, nil
}

// laneRefused reports whether a lane registration error is the hub's refusal
// rather than a transport or daemon failure. Both routes keep the CLI's
// refusal line, so the code reads the same either way.
func laneRefused(e error) bool {
	text := e.Error()
	return strings.Contains(text, "lanes rejected by hub") || strings.Contains(text, "lanes rejected: ")
}

// announceLaneUnregistered tells the lane that owns the spawn that this pane's
// findings and escalations have no hub route: a lane.event to the parent lane
// (the owner for a worker, the parent for a builder). The text carries neither
// the word READY nor ESC, which the wait loop reads as job outcomes.
func (a *App) announceLaneUnregistered(ctx context.Context, s Spawn, lane, parent, pane, machine string) {
	text := fmt.Sprintf("[%s] LANE_UNREGISTERED | lane=%s machine=%s pane=%s | hub lane registration failed after one retry; this pane's findings are not routed | fix: panewire lanes add %s --machine %s --pane %s --parent %s", s.Job, lane, machine, pane, lane, machine, pane, parent)
	if s.laneError != "" {
		text += " | reason=" + s.laneError
	}
	args := []string{"emit", "--kind", "lane.event", "--lane", parent, "--type", "blocked", "--event-id", s.Job + "-lane-unregistered", "--label", s.Label, "--host", machine, "--pane", pane, "--inbox-root", filepath.Dir(a.Jobs), "--text", text}
	if r := a.call(ctx, "panewire", args...); r.Code != 0 {
		a.warn("LANE_UNREGISTERED event to %s not delivered (rc=%d): %s", parent, r.Code, strings.TrimSpace(r.Err))
		_ = a.event(ctx, s.Job, "job.relay_failed", map[string]any{"kind": "lane.event", "exit": r.Code})
	}
}

// laneSecretRE matches a credential-looking key=value pair in a CLI error.
var laneSecretRE = regexp.MustCompile(`(?i)\b(token|secret|password|authorization|client_secret)\s*[=:]\s*\S+`)

// laneFailureReason turns a lane registration error into the bounded,
// single-line, redacted reason recorded in the receipt and the notice.
func laneFailureReason(e error) string {
	return SanitizeFailureDetail(e.Error())
}

// SanitizeFailureDetail bounds and redacts a diagnostic before it leaves the node.
func SanitizeFailureDetail(text string) string {
	msg := strings.Join(strings.Fields(text), " ")
	msg = laneSecretRE.ReplaceAllString(msg, "$1=<redacted>")
	return strings.ToValidUTF8(clip(msg, 240), "")
}

// registerLane registers a spawned lane with the hub. On a node the daemon
// already holds the hub credential, so the socket route goes first; the
// fleet-config `lanes add` command stays for hosts where no daemon answers.
func (a *App) registerLane(ctx context.Context, req lanePutRequest) (laneRegistration, error) {
	if registration, e := a.registerLaneThroughDaemon(ctx, req); !errors.Is(e, errLaneDaemonAbsent) {
		return registration, e
	}
	if str(a.Config.Hub, "hub_url") == "" {
		return laneRegistration{}, fail(exitConfig, "hub lane config missing")
	}
	args := []string{"lanes", "add", req.Lane, "--machine", req.Machine, "--pane", req.Pane, "--parent", req.Parent}
	if req.Takeover != "" {
		args = append(args, "--takeover", req.Takeover)
	}
	args = append(args, a.laneHubArgs(req.Role)...)
	for i, arg := range args {
		if arg != "--hub-token-env" || i+1 == len(args) {
			continue
		}
		args[i+1] = expand(args[i+1])
		env, e := readEnvFile(args[i+1])
		if id := env["HUB_MACHINE_ID"]; e == nil && id != "" && id != "operator" && id != req.Machine {
			return laneRegistration{}, fail(exitConfig, "LANE_SESSION_MACHINE_MISMATCH: session=%s route=%s credential_machine=%s; configure hub.session_machine_ids and hub.session_lanes_token_envs together", a.herdrSession(), req.Machine, id)
		}
	}
	r := a.call(ctx, "panewire", args...)
	if e := check(r, "lanes add"); e != nil {
		return laneRegistration{}, e
	}
	return req.answer([]byte(r.Out)), nil
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
