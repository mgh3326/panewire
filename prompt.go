package panewire

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode"
)

type PromptRequest struct {
	Sender, Target, Path, Uptake string
	StorePromptBody              bool
}

type PromptResult struct {
	DeliveryID, PreflightResult, SubmissionResult, UptakeResult string
	PreflightRevision, SendRevision, EvidenceRevision           int64
	Cached                                                      bool `json:"cached,omitempty"`
}

type paneIdentity struct {
	PaneID, WorkspaceID, TabID, Agent, Name, Label, Title, CWD, Harness, Status, AgentSession string
	Revision, StateChangeSeq                                                                  int64
	InteractiveReady                                                                          *bool
}

type expectFields struct {
	Name, Label, CWD, TitleContains, RecentContains string
}

type readEvidence struct {
	Text     string
	Revision int64
	// Source and Rule record which read and which classifySubmission rule
	// produced the final submission result (#547 AC5); pollSubmission fills
	// them, readPane leaves them empty.
	Source, Rule string
}

var pasteChipRE = regexp.MustCompile(`\[Pasted text #[^\]]+\]`)
var codexToolRE = regexp.MustCompile(`•\s*Ran\b`)

func Prompt(ctx context.Context, store *Store, client *HerdrClient, req PromptRequest, caps GuardResult) (PromptResult, error) {
	if store == nil || client == nil {
		return PromptResult{}, &codedError{ExitInternal, fmt.Errorf("prompt requires store and herdr client")}
	}
	if strings.TrimSpace(req.Sender) == "" || strings.TrimSpace(req.Target) == "" || req.Path == "" {
		return PromptResult{}, &codedError{ExitConditionInvalid, fmt.Errorf("sender, target, and file are required")}
	}
	if req.Uptake != "" && req.Uptake != "tool" && req.Uptake != "status-transition" {
		return PromptResult{}, &codedError{ExitConditionInvalid, fmt.Errorf("invalid uptake mode")}
	}
	bodyBytes, err := os.ReadFile(req.Path)
	if err != nil {
		return PromptResult{}, &codedError{ExitConditionInvalid, fmt.Errorf("read prompt file: %w", err)}
	}
	expect, body, err := parsePromptFile(string(bodyBytes))
	if err != nil {
		return PromptResult{}, &codedError{ExitConditionInvalid, err}
	}
	sum := sha256.Sum256([]byte(body))
	promptHash := hex.EncodeToString(sum[:])
	id := correlationID(req.Sender, req.Target, req.Path, promptHash, req.Uptake)
	if old, ok, e := store.GetDelivery(ctx, id); e != nil {
		return PromptResult{}, &codedError{ExitInternal, e}
	} else if ok {
		if old.CompletedAtMS == 0 {
			return cachedDeliveryResult(old), deliveryError(old)
		}
		// A preflight rejection never reached herdr, so retrying after the
		// operator corrects expect/identity cannot create a duplicate injection.
		if old.HerdrAcceptance == "" {
			if e := store.DeleteDelivery(ctx, id); e != nil {
				return PromptResult{}, &codedError{ExitInternal, e}
			}
		} else {
			return cachedDeliveryResult(old), deliveryError(old)
		}
	}

	if !caps.Prompt || !caps.AgentRead {
		return recordNewFailure(ctx, store, Delivery{DeliveryID: id, Sender: req.Sender, TargetInput: req.Target, SourcePath: req.Path, PromptSHA256: promptHash, RequestedAtMS: time.Now().UnixMilli(), PreflightResult: "ambiguous", ErrorCode: "daemon_unavailable"}, body, req.StorePromptBody, ExitDaemonUnavailable, "prompt capability unavailable")
	}
	pane, err := resolveTarget(ctx, client, req.Target)
	if err != nil {
		if ctx.Err() != nil {
			err = &codedError{ExitTimeout, fmt.Errorf("timeout resolving target")}
		}
		return recordFailureForRequest(ctx, store, id, req, promptHash, body, req.StorePromptBody, "ambiguous", err)
	}
	pre, err := readPane(ctx, client, pane, "recent_unwrapped")
	if err != nil {
		if ctx.Err() != nil {
			err = &codedError{ExitTimeout, fmt.Errorf("timeout reading target")}
		}
		return recordFailureForRequest(ctx, store, id, req, promptHash, body, req.StorePromptBody, "ambiguous", err)
	}
	pre.Source = "recent_unwrapped"
	if pre.Text == "" {
		pre, err = readPane(ctx, client, pane, "visible")
		pre.Source = "visible"
		if err != nil {
			if ctx.Err() != nil {
				err = &codedError{ExitTimeout, fmt.Errorf("timeout reading target")}
			}
			return recordFailureForRequest(ctx, store, id, req, promptHash, body, req.StorePromptBody, "ambiguous", err)
		}
	}
	if !matchesExpect(expect, pane, pre.Text) {
		failed := expectFailures(expect, pane, pre.Text)
		return recordFailureForRequestWithPaneDetail(ctx, store, id, req, promptHash, body, req.StorePromptBody, pane, pre.Revision, digest(pre.Text), "mismatch", "expect_failed="+strings.Join(failed, ","), &codedError{ExitConditionInvalid, fmt.Errorf("recipient identity does not match expect")})
	}
	if req.Uptake != "" && pane.Status == "working" {
		return recordFailureForRequestWithPane(ctx, store, id, req, promptHash, body, req.StorePromptBody, pane, pre.Revision, digest(pre.Text), "passed", &codedError{ExitDeliveryFailure, fmt.Errorf("uptake_unproven: target is already working")})
	}

	// Resolve again after the read. Revision drift is recorded; identity drift
	// is a hard stop before the durable preflight commit/send boundary.
	sendPane, err := resolveTarget(ctx, client, req.Target)
	if err != nil {
		if ctx.Err() != nil {
			err = &codedError{ExitTimeout, fmt.Errorf("timeout resolving target")}
		}
		return recordFailureForRequestWithPane(ctx, store, id, req, promptHash, body, req.StorePromptBody, pane, pre.Revision, digest(pre.Text), "identity_changed", err)
	}
	if identityChanged(pane, sendPane) {
		return recordFailureForRequestWithPane(ctx, store, id, req, promptHash, body, req.StorePromptBody, pane, pre.Revision, digest(pre.Text), "identity_changed", &codedError{ExitConditionInvalid, fmt.Errorf("target identity changed during preflight")})
	}
	if sendPane.Revision == 0 {
		sendPane.Revision = pre.Revision
	}
	if pre.Revision == 0 {
		pre.Revision = pane.Revision
	}
	d := Delivery{DeliveryID: id, Sender: req.Sender, TargetInput: req.Target, ResolvedPaneID: pane.PaneID, ResolvedWorkspaceID: pane.WorkspaceID,
		SourcePath: req.Path, PromptSHA256: promptHash, BodyStored: req.StorePromptBody, RequestedAtMS: time.Now().UnixMilli(),
		PreflightRevision: pre.Revision, SendRevision: sendPane.Revision, PreflightReadSHA256: digest(pre.Text), PreflightResult: "passed",
		UptakeMode: req.Uptake, UptakeResult: "not_requested"}
	inserted, err := store.InsertDelivery(ctx, d, body)
	if err != nil {
		return PromptResult{}, &codedError{ExitInternal, fmt.Errorf("persist preflight: %w", err)}
	}
	if !inserted {
		old, ok, e := store.GetDelivery(ctx, id)
		if e != nil || !ok {
			return PromptResult{}, &codedError{ExitInternal, fmt.Errorf("correlation id already exists but cannot be read")}
		}
		return cachedDeliveryResult(old), deliveryError(old)
	}

	var statusEvents <-chan HerdrEvent
	if req.Uptake == "status-transition" {
		if !caps.Events {
			return finishPrompt(ctx, store, d, &codedError{ExitDaemonUnavailable, fmt.Errorf("events capability unavailable")})
		}
		statusEvents, _, err = client.Subscribe(ctx)
		if err != nil {
			return finishPrompt(ctx, store, d, &codedError{ExitDaemonUnavailable, err})
		}
	}
	acceptedTarget := pane.PaneID
	if acceptedTarget == "" {
		acceptedTarget = pane.Agent
	}
	_, err = client.Call(ctx, "agent.prompt", map[string]any{"target": acceptedTarget, "text": body})
	d.HerdrAcceptance = "accepted"
	if err != nil {
		d.HerdrAcceptance = "rejected"
		code := ExitDeliveryFailure
		if ctx.Err() != nil {
			code = ExitTimeout
		}
		return finishPrompt(ctx, store, d, &codedError{code, fmt.Errorf("herdr prompt rejected: %w", err)})
	}

	post, submission, postErr := pollSubmission(ctx, client, pane, body, pre)
	if postErr != nil {
		d.SubmissionResult = "unproven"
		code := ExitDeliveryFailure
		if ctx.Err() != nil {
			code = ExitTimeout
		}
		return finishPrompt(ctx, store, d, &codedError{code, postErr})
	}
	d.SubmissionResult = submission
	d.SubmissionEvidence = submissionEvidence(post)
	d.EvidenceRevision = post.Revision
	if post.Revision == 0 {
		d.EvidenceRevision = sendPane.Revision
	}
	if d.SubmissionResult == "composer_residue" {
		return finishPrompt(ctx, store, d, &codedError{ExitDeliveryFailure, fmt.Errorf("prompt remains in composer")})
	}
	if d.SubmissionResult == "unproven" {
		return finishPrompt(ctx, store, d, &codedError{ExitDeliveryFailure, fmt.Errorf("submission evidence unproven")})
	}

	if req.Uptake == "status-transition" {
		d.UptakeResult = "unproven"
		for {
			select {
			case ev, ok := <-statusEvents:
				if !ok {
					return finishPrompt(ctx, store, d, &codedError{ExitDeliveryFailure, fmt.Errorf("uptake_unproven: status stream closed")})
				}
				if ev.PaneID == pane.PaneID && ev.AgentStatus == "working" {
					d.UptakeResult = "confirmed"
					d.EvidenceRevision = ev.Revision
					return finishPrompt(ctx, store, d, nil)
				}
			case <-ctx.Done():
				return finishPrompt(ctx, store, d, &codedError{ExitTimeout, fmt.Errorf("uptake_unproven: status transition not observed")})
			}
		}
	}
	if req.Uptake == "tool" {
		if !toolReceipt(pane.Harness, post.Text, markerFor(body), post.Revision, sendPane.Revision) {
			d.UptakeResult = "unproven"
			return finishPrompt(ctx, store, d, &codedError{ExitDeliveryFailure, fmt.Errorf("tool uptake evidence unproven")})
		}
		d.UptakeResult = "confirmed"
	}
	return finishPrompt(ctx, store, d, nil)
}

func parsePromptFile(raw string) (expectFields, string, error) {
	s := strings.ReplaceAll(raw, "\r\n", "\n")
	scanner := bufio.NewScanner(strings.NewReader(s))
	var line string
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) != "" {
			line = strings.TrimSpace(scanner.Text())
			break
		}
	}
	if !strings.HasPrefix(line, "expect:") {
		return expectFields{}, "", fmt.Errorf("prompt file must begin with expect metadata")
	}
	e := expectFields{}
	for _, token := range strings.Fields(strings.TrimSpace(strings.TrimPrefix(line, "expect:"))) {
		parts := strings.SplitN(token, "=", 2)
		if len(parts) != 2 || parts[1] == "" {
			return expectFields{}, "", fmt.Errorf("invalid expect field: %s", token)
		}
		switch parts[0] {
		case "name":
			e.Name = parts[1]
		case "label":
			e.Label = parts[1]
		case "cwd":
			e.CWD = parts[1]
		case "title~":
			e.TitleContains = parts[1]
		case "recent~":
			e.RecentContains = parts[1]
		default:
			return expectFields{}, "", fmt.Errorf("unknown expect field: %s", parts[0])
		}
	}
	if e.Name == "" && e.CWD == "" {
		return expectFields{}, "", fmt.Errorf("expect requires name or cwd")
	}
	idx := strings.Index(s, "\n")
	if idx < 0 {
		return e, "", nil
	}
	body := strings.TrimLeft(s[idx+1:], "\n")
	if strings.TrimSpace(body) == "" {
		return expectFields{}, "", fmt.Errorf("prompt body is empty")
	}
	return e, body, nil
}

// tabLabels joins tab.list into a tab_id→label map. L8(2026-08-29 라이브): herdr 의
// agent 레코드에는 label 필드가 없다 — 라벨의 정본은 tab.list 다. agent.list 만 읽는
// 라벨 폴백은 실데이터에서 항상 실패했다(fixture 가 agent 레코드에 label 을 넣어 줘서
// 통과해 보였을 뿐). tab.list 실패는 라벨 폴백만 잃고 이름 해석은 계속한다(fail-open —
// 라벨은 폴백 경로라서다).
func tabLabels(ctx context.Context, c *HerdrClient) map[string]string {
	raw, err := c.Call(ctx, "tab.list", map[string]any{})
	if err != nil {
		return nil
	}
	var top struct {
		Tabs []map[string]any `json:"tabs"`
	}
	if json.Unmarshal(raw, &top) != nil {
		return nil
	}
	labels := make(map[string]string, len(top.Tabs))
	for _, tab := range top.Tabs {
		id, label := aString(tab, "tab_id"), aString(tab, "label")
		if id != "" && label != "" {
			labels[id] = label
		}
	}
	return labels
}

func identityFromMap(a map[string]any) paneIdentity {
	return paneIdentity{PaneID: aString(a, "pane_id"), WorkspaceID: aString(a, "workspace_id"), TabID: firstString(a, "tab_id", "tab"), Agent: aString(a, "agent"), Name: aString(a, "name"), Label: firstString(a, "label", "tab_label", "display_agent"), Title: aString(a, "title"), CWD: firstString(a, "cwd", "workdir", "working_dir"), Harness: firstString(a, "harness", "harness_kind", "kind", "agent"), Status: aString(a, "agent_status"), Revision: aInt(a, "revision"), StateChangeSeq: aInt(a, "state_change_seq"), InteractiveReady: aBoolPtr(a, "interactive_ready"), AgentSession: agentSessionValue(a["agent_session"])}
}
func aString(a map[string]any, key string) string { v, _ := a[key].(string); return v }
func aBoolPtr(a map[string]any, key string) *bool {
	v, ok := a[key].(bool)
	if !ok {
		return nil
	}
	return &v
}
func firstString(a map[string]any, keys ...string) string {
	for _, k := range keys {
		if v := aString(a, k); v != "" {
			return v
		}
	}
	return ""
}
func aInt(a map[string]any, key string) int64 {
	if v, ok := a[key].(float64); ok {
		return int64(v)
	}
	return 0
}
func identityChanged(a, b paneIdentity) bool {
	return a.PaneID != b.PaneID || a.Agent != b.Agent || a.Name != b.Name || a.CWD != b.CWD
}

func readPane(ctx context.Context, c *HerdrClient, p paneIdentity, source string) (readEvidence, error) {
	target := p.PaneID
	if target == "" {
		target = p.Agent
	}
	raw, err := c.Call(ctx, "agent.read", map[string]any{"target": target, "source": source})
	if err != nil {
		raw, err = c.Call(ctx, "pane.read", map[string]any{"pane_id": p.PaneID, "source": source})
	}
	if err != nil {
		if ctx.Err() != nil {
			return readEvidence{}, &codedError{ExitTimeout, fmt.Errorf("timeout reading target")}
		}
		return readEvidence{}, &codedError{ExitDaemonUnavailable, err}
	}
	return decodeRead(raw), nil
}
func decodeRead(raw json.RawMessage) readEvidence {
	var v map[string]any
	_ = json.Unmarshal(raw, &v)
	return readEvidence{Text: findText(v), Revision: findInt(v, "revision")}
}
func findText(v map[string]any) string {
	for _, k := range []string{"text", "content", "output", "screen", "read"} {
		if s, ok := v[k].(string); ok {
			return s
		}
		if m, ok := v[k].(map[string]any); ok {
			if s := findText(m); s != "" {
				return s
			}
		}
	}
	return ""
}
func findInt(v map[string]any, key string) int64 {
	if n, ok := v[key].(float64); ok {
		return int64(n)
	}
	for _, child := range v {
		if m, ok := child.(map[string]any); ok {
			if n := findInt(m, key); n != 0 {
				return n
			}
		}
	}
	return 0
}
func digest(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func markerFor(body string) string {
	body = strings.TrimSpace(body)
	if body == "" {
		return ""
	}
	r := []rune(body)
	if len(r) > 24 {
		r = r[:24]
	}
	return string(r)
}

func matchesExpect(e expectFields, p paneIdentity, recent string) bool {
	return len(expectFailures(e, p, recent)) == 0
}
func expectFailures(e expectFields, p paneIdentity, recent string) []string {
	failed := make([]string, 0, 5)
	if e.Name != "" && e.Name != p.Agent && e.Name != p.Name {
		failed = append(failed, "name")
	}
	if e.Label != "" && e.Label != p.Label && e.Label != p.Title {
		failed = append(failed, "label")
	}
	if e.CWD != "" && e.CWD != p.CWD {
		failed = append(failed, "cwd")
	}
	if e.TitleContains != "" && !strings.Contains(p.Title, e.TitleContains) {
		failed = append(failed, "title~")
	}
	if e.RecentContains != "" && !strings.Contains(recent, e.RecentContains) {
		failed = append(failed, "recent~")
	}
	return failed
}

// devin shares claude's fixed two-divider composer layout (verified live on
// 2026-09-16 across devin's idle, working, and queued screens), so its residue
// check uses the same composerRegion: it locates the live input row by position
// (between the last two divider lines) instead of by matching one of devin's
// several placeholder strings ("Ask Devin to build features...", "Guide Devin
// while it works", "Press Enter to send queued messages now"), which sidesteps
// the open unknown of whether those strings are stable across devin CLI
// versions. A placeholder-text rule (marker absent from screen == unproven,
// placeholder absent == residue) was tried first and rejected: a live capture
// showed devin's idle placeholder stays absent for the entire multi-second
// "Thinking"/"Running tools" duration of a real turn, so that rule would
// misclassify an already-submitted, still-processing prompt as
// composer_residue for the whole turn -- not just the brief post-submit
// render-lag window -- which would make the relay's return-once re-read land
// on composer_residue again and report unconfirmed, driving a hub retry that
// re-injects into the still-live pane.
func classifySubmission(harness, screen, marker string) string {
	result, _ := classifySubmissionEvidence(harness, screen, marker)
	return result
}

// classifySubmissionEvidence is classifySubmission plus the name of the rule
// that decided it, so a caller can record why a delivery was judged the way
// it was (#547 AC5). The claude and codex arms are unchanged; devin has its
// own arm (see devinSubmission).
func classifySubmissionEvidence(harness, screen, marker string) (string, string) {
	if pasteChipRE.MatchString(screen) {
		return "composer_residue", "paste_chip"
	}
	if strings.EqualFold(harness, "devin") {
		return devinSubmission(screen, marker)
	}
	if strings.EqualFold(harness, "claude") && claudeComposerContains(screen, marker) {
		return "composer_residue", "composer_divider"
	}
	if relayQueuedBanner(harness, screen) && !pasteChipRE.MatchString(screen) {
		return "queued", "queued_banner"
	}
	if (strings.EqualFold(harness, "claude") || strings.EqualFold(harness, "codex")) && marker != "" && strings.Contains(screen, marker) {
		return "marker_observed", "marker_echo"
	}
	return "unproven", "none"
}

// devinQueueBannerRE matches devin's queue header line, captured live as
// "── 1 queued ── ↑ edit · ↵ send now ──". devin draws it at column 0, while
// every continuation line of a transcript echo is indented, so a submitted
// message whose body contains a header-shaped line cannot match. The composer
// hint "Press Enter to send queued messages now" is the other half of the
// same state, and counts only inside the live composer.
var devinQueueBannerRE = regexp.MustCompile(`^─+[ \t]*\d+[ \t]+queued\b`)

// devinQueued reports whether devin holds at least one message it has not
// submitted: the composer hint, or a column-0 queue header that sits below
// the last transcript echo ("❭ ...") and above the composer.
func devinQueued(screen string) bool {
	if region, ok := composerRegionWith(screen, isDevinDividerLine); ok && strings.Contains(region, "send queued messages now") {
		return true
	}
	lines := strings.Split(screen, "\n")
	end := len(lines)
	if start, ok := composerStartWith(lines, isDevinDividerLine); ok {
		end = start
	}
	for i := end - 1; i >= 0; i-- {
		if strings.HasPrefix(lines[i], "❭") {
			return false
		}
		if devinQueueBannerRE.MatchString(lines[i]) {
			return true
		}
	}
	return false
}

// devinSubmission orders devin's rules so that a message that is only in
// devin's queue can never read as submitted: residue in the composer first,
// then the queue banner, and only then the transcript echo. Its queued row
// ("○ <message>") carries the marker too, which is why the banner has to win
// over the echo. Marker matching ignores whitespace because a wrapped screen
// can split the marker across lines.
func devinSubmission(screen, marker string) (string, string) {
	if region, ok := composerRegionWith(screen, isDevinDividerLine); ok && marker != "" && strings.Contains(compactWhitespace(region), compactWhitespace(marker)) {
		return "composer_residue", "composer_divider"
	}
	if devinQueued(screen) {
		return "queued", "devin_queue_banner"
	}
	if marker != "" && strings.Contains(compactWhitespace(screen), compactWhitespace(marker)) {
		return "marker_observed", "marker_echo"
	}
	return "unproven", "none"
}

func compactWhitespace(value string) string {
	return strings.Join(strings.Fields(value), "")
}

// Claude echoes submitted prompts in the transcript with a leading ❯. Only
// text between the final two horizontal divider lines is the live composer;
// an echo above those dividers is submission evidence, not residue.
func claudeComposerContains(screen, marker string) bool {
	if marker == "" {
		return false
	}
	region, ok := composerRegion(screen)
	return ok && strings.Contains(region, marker)
}

// composerRegion returns the text between the final two divider lines --
// claude's live composer, and devin's, whose layout is the same.
func composerRegion(screen string) (string, bool) {
	return composerRegionWith(screen, isDividerLine)
}

func composerRegionWith(screen string, isDivider func(string) bool) (string, bool) {
	lines := strings.Split(screen, "\n")
	start, ok := composerStartWith(lines, isDivider)
	if !ok {
		return "", false
	}
	end := start + 1
	for end < len(lines) && !isDivider(lines[end]) {
		end++
	}
	return strings.Join(lines[start+1:end], "\n"), true
}

// composerStartWith is the index of the second-to-last divider line, the top
// edge of the live composer.
func composerStartWith(lines []string, isDivider func(string) bool) (int, bool) {
	dividers := make([]int, 0, 2)
	for i, line := range lines {
		if isDivider(line) {
			dividers = append(dividers, i)
		}
	}
	if len(dividers) < 2 {
		return 0, false
	}
	return dividers[len(dividers)-2], true
}

func isDividerLine(line string) bool {
	return strings.Contains(line, "─") && strings.Trim(line, " \t─") == ""
}

// isDevinDividerLine also accepts devin's decorated composer top edge,
// captured live as "──────── (bypass permissions on) ─". Its long leading run
// of ─ tells it apart from the queue header, which starts "── 1 queued".
func isDevinDividerLine(line string) bool {
	return isDividerLine(line) || strings.HasPrefix(strings.TrimSpace(line), strings.Repeat("─", 8))
}

func toolReceipt(harness, screen, marker string, evidenceRevision, sendRevision int64) bool {
	if evidenceRevision <= sendRevision || marker == "" || !strings.Contains(screen, marker) {
		return false
	}
	if strings.EqualFold(harness, "codex") {
		return codexToolRE.MatchString(screen)
	}
	if strings.EqualFold(harness, "claude") {
		return strings.Contains(screen, "⏺") || strings.Contains(strings.ToLower(screen), "tool")
	}
	return false
}

// promptReturnSettle is how long a direct prompt's own text must stay in the
// composer before pollSubmission sends its one return (#646). herdr writes
// the paste and its Enter as one ordered submission, and a claude pane
// renders the echo about 0.4s later, so text still in the composer after
// this long is an Enter the harness dropped, not a render in progress.
const promptReturnSettle = 600 * time.Millisecond

// promptReturnRecheck is the time a return must leave for the reads that
// prove it; with less, the call would report residue for text it submitted.
const promptReturnRecheck = 500 * time.Millisecond

// pollSubmission reads pane until the prompt's submission is proven, it is
// queued, or ctx ends, and returns the last read and its classification.
// pre is the preflight read: a marker counts as echoed only when it is new
// since then (see promptFreshEcho). Residue of this call's own text gets one
// return keypress, and only when the composer holds nothing but that text
// (#646, the #626 part B rule).
func pollSubmission(ctx context.Context, c *HerdrClient, p paneIdentity, body string, pre readEvidence) (readEvidence, string, error) {
	var last readEvidence
	lastResult := "unproven"
	first, second := "recent_unwrapped", "visible"
	if pre.Source == "visible" {
		first, second = second, first
	}
	var residueSince time.Time
	returnAttempted, returnRule := false, ""
	for {
		source := first
		post, err := readPane(ctx, c, p, source)
		if err == nil && post.Text == "" {
			source = second
			post, err = readPane(ctx, c, p, source)
		}
		if err != nil {
			if ctx.Err() != nil {
				return last, lastResult, nil
			}
			return last, "unproven", err
		}
		result, rule := classifyPromptSubmission(p.Harness, pre.Text, post.Text, body)
		if result == "marker_observed" && source != pre.Source {
			// The two reads cover different windows, so a marker that is
			// only above the preflight window would read as new.
			result, rule = "unproven", "marker_unanchored"
		}
		if returnRule != "" {
			rule += "+return_once:" + returnRule
		}
		post.Source, post.Rule = source, rule
		last = post
		lastResult = result
		if result == "marker_observed" || result == "submitted" || result == "queued" {
			return post, result, nil
		}
		// composer_residue is expected during the paste→submit transition;
		// retain it as the possible final result but keep polling for proof.
		if result != "composer_residue" {
			residueSince = time.Time{}
		} else if residueSince.IsZero() {
			residueSince = time.Now()
		} else if !returnAttempted && time.Since(residueSince) >= promptReturnSettle && promptBudgetLeft(ctx) >= promptReturnRecheck {
			if safe, why := relayComposerReturnSafe(p.Harness, post.Text, body, pre.Text); safe && (why == "composer_self" || why == "composer_self_chip") {
				returnAttempted = true
				target := p.PaneID
				if target == "" {
					target = p.Agent
				}
				if _, err := c.Call(ctx, "agent.send_keys", map[string]any{"target": target, "keys": []string{"return"}}); err == nil {
					returnRule = why
				}
			}
		}
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return last, lastResult, nil
		case <-timer.C:
		}
	}
}

func promptBudgetLeft(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return promptReturnRecheck
	}
	return time.Until(deadline)
}

// promptTailMarkerRunes is the length of the tail marker. A brief longer than
// the read window scrolls its head (and markerFor's marker) out of the
// window by the first read, while its tail stays next to the composer.
const promptTailMarkerRunes = 48

func promptTailMarker(body string) string {
	r := []rune(strings.TrimSpace(body))
	if len(r) > promptTailMarkerRunes {
		r = r[len(r)-promptTailMarkerRunes:]
	}
	return strings.TrimSpace(string(r))
}

// classifyPromptSubmission is the direct-prompt classifier (#646). It keeps
// classifySubmissionEvidence's queued rules and replaces the one that reports
// a submission, which there only asks whether the marker is anywhere on
// screen. That answer is wrong in real cases:
//
//   - the marker is in the composer, but the composer was not found or the
//     wrong lines were taken for it -- a brief with a line of ─ in it moves
//     composerRegion to that line (fixture f3);
//   - the marker is an earlier message's. markerFor's 24 runes of a fleet
//     brief are its boilerplate header, which every relay message and most
//     briefs start with (fixture f5-pre: #623's delta brief).
//
// So the composer must be located (promptComposer; unlocated proves
// nothing), this call's text in it is residue, and a submission needs the
// brief's tail echoed as new output since pre, the preflight read -- and its
// head too while the head is still in the transcript. The tail identifies
// the brief where the head is shared boilerplate, and it stays on screen
// when a brief taller than the read window scrolls its head out. Residue is
// judged before the echo, and a paste chip only after it: a chip that
// appears once this call's text has been echoed is someone else's.
func classifyPromptSubmission(harness, pre, screen, body string) (string, string) {
	head, tail := markerFor(body), promptTailMarker(body)
	result, rule := classifySubmissionEvidence(harness, screen, head)
	if result == "queued" || !harnessHasSubmissionEvidence(harness) {
		return result, rule
	}
	lines := strings.Split(screen, "\n")
	top, end, ok := promptComposer(harness, lines)
	if !ok {
		return "unproven", "composer_unlocated"
	}
	composer := compactWhitespace(strings.Join(lines[top:end], "\n"))
	if strings.Contains(composer, compactWhitespace(head)) || strings.Contains(composer, compactWhitespace(tail)) {
		return "composer_residue", "composer_divider"
	}
	transcript := strings.Join(lines[:top], "\n")
	if promptFreshEcho(harness, pre, transcript, tail) {
		if promptFreshEcho(harness, pre, transcript, head) {
			return "marker_observed", "marker_echo"
		}
		if !strings.Contains(compactWhitespace(transcript), compactWhitespace(head)) {
			return "marker_observed", "marker_echo_tail"
		}
	}
	if result == "composer_residue" {
		return result, rule
	}
	if result == "marker_observed" {
		return "unproven", "marker_stale"
	}
	return "unproven", "none"
}

// promptComposer locates the live composer in lines: [top, end) is the
// composer and what is drawn under it, and lines[:top] is the transcript,
// where a submitted prompt is echoed. Every edge is found at column 0,
// because every line of text inside a composer or an echo is prefixed
// (the ❯/❭/› prompt, or a two-space continuation indent): a brief's own
// "────" or "❯ ..." lines cannot pose as an edge.
//
//   - claude and devin draw the composer between two divider lines, the
//     top one followed by the ❯ (claude) or ❭ (devin) prompt;
//   - codex draws no divider; its composer is the last line starting
//     with the › prompt, down to the footer. An echo starts with › too,
//     but a new composer is always drawn below it.
//
// Other harnesses have no locatable composer.
func promptComposer(harness string, lines []string) (top, end int, ok bool) {
	isEdge, glyph := isDividerLine, "❯"
	switch {
	case strings.EqualFold(harness, "claude"):
	case strings.EqualFold(harness, "devin"):
		isEdge, glyph = isDevinDividerLine, "❭"
	case strings.EqualFold(harness, "codex"):
		for i := len(lines) - 1; i >= 0; i-- {
			if strings.HasPrefix(lines[i], "›") {
				return i, len(lines), true
			}
		}
		return 0, 0, false
	default:
		return 0, 0, false
	}
	var edges []int
	for i, line := range lines {
		if line != "" && !unicode.IsSpace([]rune(line)[0]) && isEdge(line) {
			edges = append(edges, i)
		}
	}
	if len(edges) < 2 {
		return 0, 0, false
	}
	top, bottom := edges[len(edges)-2], edges[len(edges)-1]
	for i := top + 1; i < bottom; i++ {
		if strings.TrimSpace(lines[i]) != "" {
			return top, bottom, strings.HasPrefix(lines[i], glyph)
		}
	}
	return 0, 0, false
}

// promptAnchorMinRunes keeps a short or blank last line (a lone ⏺) from
// serving as the anchor: it would be found again in almost any new output.
const promptAnchorMinRunes = 12

// promptTranscript is screen above its live composer (see promptComposer),
// or all of it when no composer is located.
func promptTranscript(harness, screen string) string {
	lines := strings.Split(screen, "\n")
	if top, _, ok := promptComposer(harness, lines); ok {
		return strings.Join(lines[:top], "\n")
	}
	return screen
}

// promptFreshEcho reports whether marker is echoed in transcript -- the part
// of the current screen above its composer -- as new output since pre, the
// preflight read of the same source. Either the marker occurs more often
// than in pre's transcript, or it occurs after pre's last transcript lines
// (the anchor). The anchor covers an old echo scrolling out of the window
// while the new one scrolls in, which leaves the count equal. A missing
// anchor proves nothing: the window may have moved past it, or it was a
// spinner line that has since changed. Whitespace is ignored because claude
// indents and hard-wraps the lines it echoes.
func promptFreshEcho(harness, pre, transcript, marker string) bool {
	m := compactWhitespace(marker)
	if m == "" {
		return false
	}
	before := promptTranscript(harness, pre)
	after := compactWhitespace(transcript)
	if strings.Count(after, m) > strings.Count(compactWhitespace(before), m) {
		return true
	}
	anchor := promptAnchor(before)
	if anchor == "" {
		return false
	}
	i := strings.LastIndex(after, anchor)
	return i >= 0 && strings.Contains(after[i+len(anchor):], m)
}

// promptAnchor is the compacted last non-blank lines of transcript, as many
// of the final three as it takes to reach promptAnchorMinRunes.
func promptAnchor(transcript string) string {
	lines := strings.Split(transcript, "\n")
	anchor := ""
	taken := 0
	for i := len(lines) - 1; i >= 0 && taken < 3; i-- {
		line := compactWhitespace(lines[i])
		if line == "" {
			continue
		}
		anchor = line + anchor
		taken++
		if len([]rune(anchor)) >= promptAnchorMinRunes {
			return anchor
		}
	}
	return ""
}

// submissionEvidence is the deliveries-row record of how a submission was
// judged: which read source, then which classifySubmission rule.
func submissionEvidence(post readEvidence) string {
	if post.Source == "" {
		return ""
	}
	return post.Source + ":" + post.Rule
}

func correlationID(sender, target, path, hash, uptake string) string {
	return digest(sender + "\x00" + target + "\x00" + path + "\x00" + hash + "\x00" + uptake)
}
func deliveryResult(d Delivery) PromptResult {
	return PromptResult{DeliveryID: d.DeliveryID, PreflightResult: d.PreflightResult, SubmissionResult: d.SubmissionResult, UptakeResult: d.UptakeResult, PreflightRevision: d.PreflightRevision, SendRevision: d.SendRevision, EvidenceRevision: d.EvidenceRevision}
}
func cachedDeliveryResult(d Delivery) PromptResult {
	r := deliveryResult(d)
	r.Cached = true
	return r
}
func deliveryError(d Delivery) error {
	if d.CompletedAtMS == 0 {
		return &codedError{ExitDeliveryFailure, fmt.Errorf("delivery %s is still in progress", d.DeliveryID)}
	}
	if d.ErrorCode == "" || d.UptakeResult == "confirmed" || d.SubmissionResult == "marker_observed" || d.SubmissionResult == "queued" && d.UptakeMode == "" {
		return nil
	}
	code := ExitDeliveryFailure
	if d.ErrorCode == "condition_invalid" {
		code = ExitConditionInvalid
	}
	if d.ErrorCode == "daemon_unavailable" {
		code = ExitDaemonUnavailable
	}
	if d.ErrorCode == "timeout" {
		code = ExitTimeout
	}
	return &codedError{code, fmt.Errorf("delivery %s: %s", d.DeliveryID, d.ErrorCode)}
}
func recordNewFailure(ctx context.Context, s *Store, d Delivery, body string, storeBody bool, code int, msg string) (PromptResult, error) {
	d.BodyStored = storeBody
	d.CompletedAtMS = time.Now().UnixMilli()
	d.ErrorCode = errorCodeFor(code)
	if _, err := s.InsertDelivery(context.WithoutCancel(ctx), d, body); err != nil {
		return PromptResult{}, &codedError{ExitInternal, err}
	}
	return deliveryResult(d), &codedError{code, fmt.Errorf("%s", msg)}
}
func recordFailureForRequest(ctx context.Context, s *Store, id string, req PromptRequest, hash, body string, storeBody bool, result string, err error) (PromptResult, error) {
	return recordFailureForRequestWithPaneDetail(ctx, s, id, req, hash, body, storeBody, paneIdentity{}, 0, "", result, "", err)
}
func recordFailureForRequestWithPane(ctx context.Context, s *Store, id string, req PromptRequest, hash, body string, storeBody bool, p paneIdentity, revision int64, readHash, result string, err error) (PromptResult, error) {
	return recordFailureForRequestWithPaneDetail(ctx, s, id, req, hash, body, storeBody, p, revision, readHash, result, "", err)
}
func recordFailureForRequestWithPaneDetail(ctx context.Context, s *Store, id string, req PromptRequest, hash, body string, storeBody bool, p paneIdentity, revision int64, readHash, result, detail string, err error) (PromptResult, error) {
	d := Delivery{DeliveryID: id, Sender: req.Sender, TargetInput: req.Target, ResolvedPaneID: p.PaneID, ResolvedWorkspaceID: p.WorkspaceID, SourcePath: req.Path, PromptSHA256: hash, BodyStored: storeBody, RequestedAtMS: time.Now().UnixMilli(), CompletedAtMS: time.Now().UnixMilli(), PreflightRevision: revision, PreflightReadSHA256: readHash, PreflightResult: result, UptakeMode: req.Uptake, UptakeResult: "not_requested", ErrorCode: errorCodeFor(ExitCode(err))}
	d.ErrorDetail = detail
	if _, e := s.InsertDelivery(context.WithoutCancel(ctx), d, body); e != nil {
		return PromptResult{}, &codedError{ExitInternal, e}
	}
	return deliveryResult(d), err
}
func finishPrompt(ctx context.Context, s *Store, d Delivery, err error) (PromptResult, error) {
	d.CompletedAtMS = time.Now().UnixMilli()
	if err != nil && d.ErrorCode == "" {
		d.ErrorCode = errorCodeFor(ExitCode(err))
	}
	if e := s.UpdateDelivery(context.WithoutCancel(ctx), d); e != nil {
		return PromptResult{}, &codedError{ExitInternal, e}
	}
	return deliveryResult(d), err
}
func errorCodeFor(code int) string {
	switch code {
	case ExitConditionInvalid:
		return "condition_invalid"
	case ExitDaemonUnavailable:
		return "daemon_unavailable"
	case ExitDeliveryFailure:
		return "delivery_failure"
	case ExitTimeout:
		return "timeout"
	default:
		return "internal"
	}
}

func recordUnavailablePrompt(ctx context.Context, store *Store, req PromptRequest, code int, msg string) (PromptResult, error) {
	raw, readErr := os.ReadFile(req.Path)
	body := string(raw)
	if _, parsed, parseErr := parsePromptFile(body); parseErr == nil {
		body = parsed
	}
	hash := digest(body)
	d := Delivery{DeliveryID: correlationID(req.Sender, req.Target, req.Path, hash, req.Uptake), Sender: req.Sender, TargetInput: req.Target, SourcePath: req.Path, PromptSHA256: hash, BodyStored: req.StorePromptBody, RequestedAtMS: time.Now().UnixMilli(), CompletedAtMS: time.Now().UnixMilli(), PreflightResult: "ambiguous", UptakeMode: req.Uptake, UptakeResult: "not_requested", ErrorCode: errorCodeFor(code)}
	if readErr != nil {
		d.ErrorCode = errorCodeFor(ExitConditionInvalid)
	}
	if _, err := store.InsertDelivery(ctx, d, body); err != nil {
		return PromptResult{}, &codedError{ExitInternal, err}
	}
	if readErr != nil {
		return deliveryResult(d), &codedError{ExitConditionInvalid, readErr}
	}
	return deliveryResult(d), &codedError{code, fmt.Errorf("%s", msg)}
}
