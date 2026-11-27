package fleet

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// hk 6886: SUBMIT_WINDOW_UNAVAILABLE is a host configuration failure (hub_wait
// too short for this host), so it gets its own refusal code. Before the row
// existed, its text said "brief" and the broad brief row classified it as
// brief_invalid, which the hub does not send to Sentry as an error.

// The three launch-window refusals, from the real fail sites, each map to
// submit_window and keep the raise-hub_wait hint.
func TestSubmitWindowRefusalsMapToTheirOwnCode(t *testing.T) {
	a := testApp(t)

	tight, cancelTight := context.WithTimeout(t.Context(), briefSubmitReserve+defaultFloor-time.Second)
	defer cancelTight()
	_, _, reserveErr := a.reserveSubmitWindow(tight)

	expired, endExpired := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer endExpired()
	budgetErr := a.launchBudgetSpent(expired, t.Context(), os.ErrDeadlineExceeded)
	cutoffErr := a.launchCutoff(expired, t.Context(), "before tab create")

	// A launch error that itself names another row's text must not steal the
	// refusal: the submit_window row sits ahead of every substring row.
	wrapped := a.launchBudgetSpent(expired, t.Context(), fail(exitUsage, "workspace workers not found"))

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"reserve refusal before launch", reserveErr},
		{"launch used the budget", budgetErr},
		{"launch cutoff", cutoffErr},
		{"launch error names another row", wrapped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.err == nil || !strings.Contains(tc.err.Error(), "SUBMIT_WINDOW_UNAVAILABLE") {
				t.Fatalf("err=%v, want the SUBMIT_WINDOW_UNAVAILABLE refusal", tc.err)
			}
			if code(tc.err) != exitConfig {
				t.Fatalf("exit=%d, want configuration %d: a host configuration failure", code(tc.err), exitConfig)
			}
			if got := SpawnRefusalCode(tc.err.Error(), code(tc.err)); got != "submit_window" {
				t.Fatalf("code=%q, want submit_window", got)
			}
			if fix := spawnRefusalFix(tc.err); !strings.Contains(fix, "hub_wait") {
				t.Fatalf("fix=%q, want the raise-hub_wait hint", fix)
			}
		})
	}
}

// The brief row is a broad substring match. Every refusal text this package
// emits that contains "brief" must land on the code it names. Texts whose code
// is brief_invalid are requester input errors; the rest are listed with the code
// they get today and why that is wrong, so a fix flips its row here.
//
// Texts are the fail-site formats with sample values. rc is the exit status the
// fail site returns (exitCode of the error).
func TestBriefTextRefusalsKeepTheirOwnCode(t *testing.T) {
	for _, tc := range []struct {
		name string
		text string
		rc   int
		code string
		// trap is set when code is the wrong answer today (hk 6886 RISK): a
		// host configuration, hk availability or duplicate refusal that the
		// brief row misnames as brief_invalid.
		trap string
	}{
		// The launch-window texts (submit_window.go). They are the same
		// refusals TestSubmitWindowRefusalsMapToTheirOwnCode drives through the
		// real fail sites; the literals keep the audit complete on its own.
		{name: "submit window before deliver", text: "SUBMIT_WINDOW_UNAVAILABLE: 12s of the node budget remain for the brief after the 30s reserve, below the 45s submit window; brief withheld, raise hub_wait for this host", rc: exitConfig, code: "submit_window"},
		{name: "submit window launch overrun", text: "SUBMIT_WINDOW_UNAVAILABLE: launch used the node budget up to the 45s submit window and the 30s reserve; no brief was sent, raise hub_wait for this host (launch error: context deadline exceeded)", rc: exitConfig, code: "submit_window"},
		{name: "submit window launch cutoff", text: "SUBMIT_WINDOW_UNAVAILABLE: the launch cutoff passed before tab create; no brief was sent, raise hub_wait for this host", rc: exitConfig, code: "submit_window"},

		// Requester input: brief_invalid is the right code.
		{name: "brief empty", text: "brief is empty", rc: exitUsage, code: "brief_invalid"},
		{name: "brief not utf8", text: "brief for hub spawn must be valid UTF-8 of at most 65536 bytes", rc: exitUsage, code: "brief_invalid"},
		{name: "brief flag missing", text: "pass -p with a nonempty UTF-8 brief of at most 64 KiB", rc: exitUsage, code: "brief_invalid"},
		{name: "hk brief key invalid", text: `invalid hk brief doc key "../x": -p hk:doc/KEY needs a nonempty key of at most 256 bytes with no leading slash, control character or ".."`, rc: exitUsage, code: "brief_invalid"},
		{name: "hk brief doc not found", text: "hk brief doc briefs/x not found; put the brief in hk under that key or pass -p with a file", rc: exitUsage, code: "brief_invalid"},
		{name: "hk brief doc empty", text: "hk brief doc briefs/x is empty", rc: exitUsage, code: "brief_invalid"},
		{name: "hk brief doc too large", text: "hk brief doc briefs/x is 99999 bytes; pass -p with a nonempty UTF-8 brief of at most 64 KiB", rc: exitUsage, code: "brief_invalid"},
		{name: "consult question too long", text: "question document exceeds the consult brief bound", rc: exitUsage, code: "brief_invalid"},

		// Refusals with their own rows ahead of the brief row.
		{name: "agent not in foreground", text: "agent not in foreground; brief withheld", rc: exitConfig, code: "foreground_missing"},
		{name: "agent not idle before brief", text: "agent must be idle before initial brief", rc: exitSubmitUnconfirmed, code: "submit_unconfirmed"},
		{name: "workspace trust prompt", text: "WORKSPACE_TRUST_REQUIRED: Codex directory trust prompt for /w/x; brief withheld; approve only this worktree in the executing CODEX_HOME", rc: exitTrustPrompt, code: "trust_prompt"},
		{name: "hooks review screen", text: "HOOKS_REVIEW_REQUIRED: Codex is waiting at its Hooks need review screen; brief withheld; the host owner reviews the hooks", rc: exitTrustPrompt, code: "hooks_review"},
		{name: "brief upload failed", text: "brief upload to briefs/job-x.md failed: handoffkeep unreachable", rc: exitPlacement, code: "brief_upload"},

		// Host configuration failures that the brief row misnames today. Each
		// is exitConfig, so the Sentry policy drops it as a requester refusal.
		{name: "brief policy version", text: "unsupported brief policy version", rc: exitConfig, code: "brief_invalid", trap: "host brief policy config"},
		{name: "brief preamble missing", text: `missing brief preamble "fleet"`, rc: exitConfig, code: "brief_invalid", trap: "host brief preamble config"},
		{name: "brief doc prefix", text: "hosts.mac-personal.brief_doc_prefix must be a key prefix ending in /", rc: exitConfig, code: "brief_invalid", trap: "host config"},
		{name: "brief submit timeout range", text: "brief_submit_timeout_ms must be 30000..600000", rc: exitConfig, code: "brief_invalid", trap: "host config"},
		{name: "brief submit retry range", text: "brief_submit_retry_ms must be at least 1000 and below brief_submit_timeout_ms", rc: exitConfig, code: "brief_invalid", trap: "host config"},
		{name: "hk brief doc read error", text: "hk brief doc briefs/x: connection refused", rc: exitConfig, code: "brief_invalid", trap: "hk unavailable"},
		{name: "hk brief doc unavailable", text: "hk brief doc briefs/x unavailable: connection refused", rc: exitStateConflict, code: "brief_invalid", trap: "hk unavailable"},

		// A duplicate-job refusal that names the saved brief.
		{name: "saved submission differs", text: "saved submission differs from this command; replay the original flags and brief, or use a new job id for independent work", rc: exitDuplicate, code: "brief_invalid", trap: "duplicate misnamed as brief; a duplicate code would start Sentry events"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(tc.text, "brief") {
				t.Fatalf("fixture %q does not contain brief", tc.text)
			}
			if got := SpawnRefusalCode(tc.text, tc.rc); got != tc.code {
				t.Fatalf("code=%q, want %q (trap=%q)", got, tc.code, tc.trap)
			}
		})
	}
}

// The rows that precede the brief row must not claim the brief-containing
// submit texts either; a submit_window text carrying any of their words still
// gets submit_window, so the audit is also a check of the new row's position.
func TestSubmitWindowTextWinsOverEarlierRowWords(t *testing.T) {
	for _, word := range []string{"workspace", "trust", "task does not exist", "cwd is not a directory", "quota catalog", "symlink path refused"} {
		text := fmt.Sprintf("SUBMIT_WINDOW_UNAVAILABLE: launch used the node budget; no brief was sent, raise hub_wait for this host (launch error: %s)", word)
		if got := SpawnRefusalCode(text, exitConfig); got != "submit_window" {
			t.Errorf("launch error %q: code=%q, want submit_window", word, got)
		}
	}
}
