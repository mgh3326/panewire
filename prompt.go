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
	PaneID, WorkspaceID, TabID, Agent, Name, Label, Title, CWD, Harness, Status string
	Revision                                                                    int64
}

type expectFields struct {
	Name, Label, CWD, TitleContains, RecentContains string
}

type readEvidence struct {
	Text     string
	Revision int64
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
	pane, err := resolvePane(ctx, client, req.Target)
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
	if pre.Text == "" {
		pre, err = readPane(ctx, client, pane, "visible")
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
	sendPane, err := resolvePane(ctx, client, req.Target)
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
		statusEvents, err = client.Subscribe(ctx)
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

	post, submission, postErr := pollSubmission(ctx, client, pane, markerFor(body))
	if postErr != nil {
		d.SubmissionResult = "unproven"
		code := ExitDeliveryFailure
		if ctx.Err() != nil {
			code = ExitTimeout
		}
		return finishPrompt(ctx, store, d, &codedError{code, postErr})
	}
	d.SubmissionResult = submission
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

func resolvePane(ctx context.Context, c *HerdrClient, target string) (paneIdentity, error) {
	raw, err := c.Call(ctx, "agent.list", map[string]any{})
	if err != nil {
		return paneIdentity{}, &codedError{ExitDaemonUnavailable, err}
	}
	var top struct {
		Agents []map[string]any `json:"agents"`
	}
	if json.Unmarshal(raw, &top) != nil {
		return paneIdentity{}, &codedError{ExitConditionInvalid, fmt.Errorf("invalid herdr agent list")}
	}
	var named, labeled []paneIdentity
	for _, a := range top.Agents {
		p := identityFromMap(a)
		if p.Agent == target || p.Name == target {
			named = append(named, p)
		} else if p.Label == target || p.TabID == target || p.Title == target || aString(a, "tab_label") == target {
			labeled = append(labeled, p)
		}
	}
	if len(named) == 1 {
		return named[0], nil
	}
	if len(named) > 1 {
		return paneIdentity{}, &codedError{ExitConditionInvalid, fmt.Errorf("ambiguous agent target: %s", target)}
	}
	if len(labeled) == 1 {
		return labeled[0], nil
	}
	if len(labeled) > 1 {
		return paneIdentity{}, &codedError{ExitConditionInvalid, fmt.Errorf("ambiguous label target: %s", target)}
	}
	return paneIdentity{}, &codedError{ExitConditionInvalid, fmt.Errorf("agent target not found: %s", target)}
}

func identityFromMap(a map[string]any) paneIdentity {
	return paneIdentity{PaneID: aString(a, "pane_id"), WorkspaceID: aString(a, "workspace_id"), TabID: firstString(a, "tab_id", "tab"), Agent: aString(a, "agent"), Name: aString(a, "name"), Label: firstString(a, "label", "tab_label", "display_agent"), Title: aString(a, "title"), CWD: firstString(a, "cwd", "workdir", "working_dir"), Harness: firstString(a, "harness", "harness_kind", "kind", "agent"), Status: aString(a, "agent_status"), Revision: aInt(a, "revision")}
}
func aString(a map[string]any, key string) string { v, _ := a[key].(string); return v }
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
func classifySubmission(harness, screen, marker string) string {
	if pasteChipRE.MatchString(screen) {
		return "composer_residue"
	}
	if strings.EqualFold(harness, "claude") && claudeComposerContains(screen, marker) {
		return "composer_residue"
	}
	if (strings.EqualFold(harness, "claude") || strings.EqualFold(harness, "codex")) && strings.Contains(screen, "Press up to edit queued messages") && !pasteChipRE.MatchString(screen) {
		return "queued"
	}
	if (strings.EqualFold(harness, "claude") || strings.EqualFold(harness, "codex")) && marker != "" && strings.Contains(screen, marker) {
		return "marker_observed"
	}
	return "unproven"
}

// Claude echoes submitted prompts in the transcript with a leading ❯. Only
// text between the final two horizontal divider lines is the live composer;
// an echo above those dividers is submission evidence, not residue.
func claudeComposerContains(screen, marker string) bool {
	if marker == "" {
		return false
	}
	lines := strings.Split(screen, "\n")
	dividers := make([]int, 0, 2)
	for i, line := range lines {
		if strings.Contains(line, "─") && strings.Trim(line, " \t─") == "" {
			dividers = append(dividers, i)
		}
	}
	if len(dividers) < 2 {
		return false
	}
	start, end := dividers[len(dividers)-2], dividers[len(dividers)-1]
	return strings.Contains(strings.Join(lines[start+1:end], "\n"), marker)
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

func pollSubmission(ctx context.Context, c *HerdrClient, p paneIdentity, marker string) (readEvidence, string, error) {
	var last readEvidence
	lastResult := "unproven"
	for {
		post, err := readPane(ctx, c, p, "recent_unwrapped")
		if err == nil && post.Text == "" {
			post, err = readPane(ctx, c, p, "visible")
		}
		if err != nil {
			if ctx.Err() != nil {
				return last, lastResult, nil
			}
			return last, "unproven", err
		}
		last = post
		result := classifySubmission(p.Harness, post.Text, marker)
		lastResult = result
		if result == "marker_observed" || result == "submitted" || result == "queued" {
			return post, result, nil
		}
		// composer_residue is expected during the paste→submit transition;
		// retain it as the possible final result but keep polling for proof.
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
