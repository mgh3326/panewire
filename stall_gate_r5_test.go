package panewire

// Round-5 deterministic regression gate. These cells began as the r4
// adversarial probe set; they are now the standing contract for the
// journal-read boundary and the harness-gated banner rule, run by CI on
// every push. Instrumentation is independent of the detector's lstat seam:
// fifo canaries (an open blocks and the in-test watchdog fails the cell)
// plus store / inbox-tree content inspection.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const gateMarker = "GATE-CANARY-5ca1ab1e"

func gateResult(t *testing.T, axis, name, expect string, got any, pass bool) {
	t.Helper()
	verdict := "PASS"
	if !pass {
		verdict = "FAIL"
		t.Errorf("%s/%s: expect %s, got %v", axis, name, expect, got)
	}
	fmt.Printf("G5|%s|%s|expect=%s|got=%v|%s\n", axis, name, expect, got, verdict)
}

// gateScan runs one detector cycle with a watchdog: a detector that opens a
// fifo never returns, so a timeout is positive proof of an open-for-read.
// The timeout fires as an assertion failure in the cell, not as a
// go-test-timeout panic.
func gateScan(t *testing.T, fx *stallFixture, timeout time.Duration) bool {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); fx.scan() }()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// gateStoreLeaksMarker reports whether any durable stall row carries the
// canary body string.
func gateStoreLeaksMarker(t *testing.T, fx *stallFixture) string {
	t.Helper()
	jobs, err := fx.store.stallJobs(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	for _, job := range jobs {
		if strings.Contains(job.ReportPath, gateMarker) {
			return "stall_jobs.report_path"
		}
	}
	reports := []stallReportRow{}
	for _, job := range jobs {
		row, found, err := fx.store.stallReport(context.Background(), job.JobID, job.Attempt)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			reports = append(reports, row)
		}
	}
	for _, row := range reports {
		if strings.Contains(row.RefValue, gateMarker) || strings.Contains(row.Note, gateMarker) || strings.Contains(row.Path, gateMarker) {
			return "stall_reports"
		}
	}
	return ""
}

// gateInboxHasMarker walks the fixture inbox for canary bytes the detector
// may have copied in.
func gateInboxHasMarker(t *testing.T, fx *stallFixture) string {
	t.Helper()
	var hit string
	_ = filepath.Walk(fx.inbox, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !info.Mode().IsRegular() {
			return nil
		}
		contents, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(contents), gateMarker) {
			hit = path
		}
		return nil
	})
	return hit
}

// gateRefusals returns the journal_refused incidents recorded for a job.
func gateRefusals(t *testing.T, fx *stallFixture, jobID string) []stallIncidentRow {
	t.Helper()
	var out []stallIncidentRow
	for _, row := range stallIncidentsFor(t, fx, jobID) {
		if row.Cause == stallCauseJournalRefused {
			out = append(out, row)
		}
	}
	return out
}

// ---- H1 — file-access boundary ----

func TestR5GateH1Boundary(t *testing.T) {
	outside := t.TempDir()

	run := func(name, expect string, setup func(t *testing.T, fx *stallFixture), wantState string) {
		t.Run(name, func(t *testing.T) {
			fx := newStallFixture(t, false)
			claimJob(t, fx, "job-a", "", nil, fx.now.Add(-time.Hour))
			setup(t, fx)
			if !gateScan(t, fx, 20*time.Second) {
				gateResult(t, "H1", name, expect, "scan blocked (an outside path was opened for reading)", false)
				return
			}
			got := []string{}
			if leak := gateStoreLeaksMarker(t, fx); leak != "" {
				got = append(got, "store-leak:"+leak)
			}
			if hit := gateInboxHasMarker(t, fx); hit != "" {
				got = append(got, "inbox-copy:"+hit)
			}
			row, found, err := fx.store.stallReport(context.Background(), "job-a", 1)
			if err != nil {
				t.Fatal(err)
			}
			state := "missing"
			if found {
				state = row.State
			}
			pass := len(got) == 0 && state == wantState
			if len(got) == 0 {
				gateResult(t, "H1", name, expect, "state="+state, pass)
			} else {
				gateResult(t, "H1", name, expect, strings.Join(got, ",")+" state="+state, pass)
			}
		})
	}

	terminal := func(fx *stallFixture, raw string) {
		writeRawJobEvent(t, fx.inbox, "job-a", "00004-job.completed.json", raw)
	}

	// JSON escapes decode to a ../ chain that resolves outside the job
	// directory; a fifo sits exactly where the chain would land, so any
	// resolve+open hangs the scan and the watchdog fires.
	run("json_unicode_escape_parent", "state=no_report, scan returns", func(t *testing.T, fx *stallFixture) {
		fifo := filepath.Join(filepath.Dir(fx.inbox), "escape.md")
		if err := syscall.Mkfifo(fifo, 0600); err != nil {
			t.Fatal(err)
		}
		terminal(fx, `{"kind":"job.completed","job_id":"job-a","payload":{"note":"done report=..\/..\/..\/escape.md token=..\/..\/..\/escape.md"},"seq":4}`)
	}, "no_report")

	// A literal parent chain far deeper than the tree, resolving to a fifo.
	run("note_long_parent_chain", "state=no_report, scan returns", func(t *testing.T, fx *stallFixture) {
		fifo := filepath.Join(outside, "deep.md")
		if err := syscall.Mkfifo(fifo, 0600); err != nil {
			t.Fatal(err)
		}
		token := strings.Repeat("../", 12) + strings.TrimPrefix(fifo, "/")
		terminal(fx, fmt.Sprintf(`{"kind":"job.completed","job_id":"job-a","payload":{"note":"done report=%s"},"seq":4}`, token))
	}, "no_report")

	// Absolute-path token pointing at a fifo: an open would hang the scan.
	run("note_absolute_fifo", "state=no_report, scan returns", func(t *testing.T, fx *stallFixture) {
		fifo := filepath.Join(outside, "absfifo.md")
		if err := syscall.Mkfifo(fifo, 0600); err != nil {
			t.Fatal(err)
		}
		terminal(fx, fmt.Sprintf(`{"kind":"job.completed","job_id":"job-a","payload":{"note":"done report=%s"},"seq":4}`, fifo))
	}, "no_report")

	// Convention path is a symlink CHAIN (link -> link -> outside fifo).
	run("convention_symlink_chain", "state=symlink_refused, scan returns", func(t *testing.T, fx *stallFixture) {
		fifo := filepath.Join(outside, "chainfifo.md")
		if err := syscall.Mkfifo(fifo, 0600); err != nil {
			t.Fatal(err)
		}
		link2 := filepath.Join(outside, "link2.md")
		if err := os.Symlink(fifo, link2); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(link2, filepath.Join(fx.inbox, "jobs", "job-a", "report.md")); err != nil {
			t.Fatal(err)
		}
		terminal(fx, `{"kind":"job.completed","job_id":"job-a","seq":4}`)
	}, "symlink_refused")

	// Convention path is a symlink to an outside DIRECTORY.
	run("convention_symlink_to_dir", "state=symlink_refused", func(t *testing.T, fx *stallFixture) {
		if err := os.Symlink(outside, filepath.Join(fx.inbox, "jobs", "job-a", "report.md")); err != nil {
			t.Fatal(err)
		}
		terminal(fx, `{"kind":"job.completed","job_id":"job-a","seq":4}`)
	}, "symlink_refused")

	// Convention path exists but is a directory.
	run("convention_report_md_is_directory", "state=non_regular", func(t *testing.T, fx *stallFixture) {
		if err := os.MkdirAll(filepath.Join(fx.inbox, "jobs", "job-a", "report.md"), 0700); err != nil {
			t.Fatal(err)
		}
		terminal(fx, `{"kind":"job.completed","job_id":"job-a","seq":4}`)
	}, "non_regular")

	// jobs/<job> itself is a symlink to an outside job directory: the job
	// is never ingested, and the refusal is a recorded row, not silence.
	t.Run("jobdir_itself_symlink", func(t *testing.T) {
		fx := newStallFixture(t, false)
		realJob := filepath.Join(outside, "job-z")
		eventsDir := filepath.Join(realJob, "events")
		if err := os.MkdirAll(eventsDir, 0700); err != nil {
			t.Fatal(err)
		}
		eventPath := filepath.Join(eventsDir, "00001-job.completed.json")
		if err := os.WriteFile(eventPath, []byte(`{"kind":"job.completed","job_id":"job-z","payload":{"note":"done `+gateMarker+`"},"seq":1}`), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(realJob, filepath.Join(fx.inbox, "jobs", "job-z")); err != nil {
			t.Fatal(err)
		}
		if !gateScan(t, fx, 20*time.Second) {
			gateResult(t, "H1", "jobdir_itself_symlink", "job skipped, outside untouched", "scan blocked", false)
			return
		}
		jobs, err := fx.store.stallJobs(context.Background(), true)
		if err != nil {
			t.Fatal(err)
		}
		ingested := false
		for _, job := range jobs {
			if job.JobID == "job-z" {
				ingested = true
			}
		}
		refusals := gateRefusals(t, fx, "job-z")
		pass := !ingested && len(refusals) == 1
		gateResult(t, "H1", "jobdir_itself_symlink", "job skipped + journal_refused row",
			fmt.Sprintf("ingested=%v refusals=%d", ingested, len(refusals)), pass)
	})

	// jobs/<job>/events is a symlink to an outside directory: the whole
	// journal (claim + spawn + terminal) lives outside jobs/ and must never
	// be read — the refusal row records why.
	t.Run("events_dir_symlink", func(t *testing.T) {
		fx := newStallFixture(t, false)
		if err := os.MkdirAll(filepath.Join(fx.inbox, "jobs", "job-a"), 0700); err != nil {
			t.Fatal(err)
		}
		writeRawJobEvent(t, outside, "job-a", "00001-job.claim.json",
			`{"kind":"job.claim","job_id":"job-a","created_at":"2026-09-17T08:00:00Z","seq":1,"payload":{"owner_lane":"owner-1"}}`)
		writeRawJobEvent(t, outside, "job-a", "00003-job.spawned.json",
			`{"kind":"job.spawned","job_id":"job-a","created_at":"2026-09-17T08:00:00Z","seq":3,"payload":{"pane_id":"w1:p1"}}`)
		writeRawJobEvent(t, outside, "job-a", "00009-job.completed.json",
			`{"kind":"job.completed","job_id":"job-a","created_at":"2026-09-17T09:00:00Z","seq":9,"payload":{"note":"done report=/nonexistent/`+gateMarker+`.md"}}`)
		if err := os.Symlink(filepath.Join(outside, "jobs", "job-a", "events"), filepath.Join(fx.inbox, "jobs", "job-a", "events")); err != nil {
			t.Fatal(err)
		}
		if !gateScan(t, fx, 20*time.Second) {
			gateResult(t, "H1", "events_dir_symlink", "no outside open/read", "scan blocked", false)
			return
		}
		jobs, err := fx.store.stallJobs(context.Background(), true)
		if err != nil {
			t.Fatal(err)
		}
		terminalFromOutside := false
		for _, job := range jobs {
			if job.JobID == "job-a" && job.Terminal && job.TerminalKind == "job.completed" {
				terminalFromOutside = true
			}
		}
		leak := gateStoreLeaksMarker(t, fx)
		refusals := gateRefusals(t, fx, "job-a")
		pass := !terminalFromOutside && leak == "" && len(refusals) == 1
		gateResult(t, "H1", "events_dir_symlink", "no outside open/read + journal_refused row",
			fmt.Sprintf("terminal-from-outside=%v store-leak=%q refusals=%d", terminalFromOutside, leak, len(refusals)), pass)
	})

	// A journal event file that is a symlink to an outside fifo: refused
	// before open — the scan must return and the refusal must be recorded.
	t.Run("event_file_symlink_to_fifo", func(t *testing.T) {
		fx := newStallFixture(t, false)
		claimJob(t, fx, "job-a", "", nil, fx.now.Add(-time.Hour))
		fifo := filepath.Join(outside, "evfifo.json")
		if err := syscall.Mkfifo(fifo, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(fifo, filepath.Join(fx.inbox, "jobs", "job-a", "events", "00004-job.completed.json")); err != nil {
			t.Fatal(err)
		}
		completed := gateScan(t, fx, 15*time.Second)
		refusals := gateRefusals(t, fx, "job-a")
		gateResult(t, "H1", "event_file_symlink_to_fifo", "scan returns + journal_refused row",
			fmt.Sprintf("scan-completed=%v refusals=%d", completed, len(refusals)), completed && len(refusals) == 1)
	})

	// A journal event file that IS a fifo (no outside target at all): the
	// Lstat gate refuses it before any open, so the scan cannot wedge.
	t.Run("event_file_is_fifo", func(t *testing.T) {
		fx := newStallFixture(t, false)
		claimJob(t, fx, "job-a", "", nil, fx.now.Add(-time.Hour))
		if err := syscall.Mkfifo(filepath.Join(fx.inbox, "jobs", "job-a", "events", "00004-job.completed.json"), 0600); err != nil {
			t.Fatal(err)
		}
		completed := gateScan(t, fx, 15*time.Second)
		refusals := gateRefusals(t, fx, "job-a")
		gateResult(t, "H1", "event_file_is_fifo", "scan returns (journal reader bounded) + journal_refused row",
			fmt.Sprintf("scan-completed=%v refusals=%d", completed, len(refusals)), completed && len(refusals) == 1)
	})

	// A journal record over the byte cap is refused before open: the
	// oversize terminal record never enters job state, and the refusal row
	// names the cap as the reason.
	t.Run("event_file_oversize_refused", func(t *testing.T) {
		fx := newStallFixture(t, false)
		claimJob(t, fx, "job-a", "", nil, fx.now.Add(-time.Hour))
		pad := strings.Repeat("x", stallJournalMaxEventBytes)
		writeRawJobEvent(t, fx.inbox, "job-a", "00004-job.completed.json",
			`{"kind":"job.completed","job_id":"job-a","created_at":"2026-09-17T09:00:00Z","seq":4,"payload":{"note":"done `+pad+`"}}`)
		completed := gateScan(t, fx, 15*time.Second)
		jobs, err := fx.store.stallJobs(context.Background(), true)
		if err != nil {
			t.Fatal(err)
		}
		terminalFromOversize := false
		for _, job := range jobs {
			if job.JobID == "job-a" && job.Terminal {
				terminalFromOversize = true
			}
		}
		refusals := gateRefusals(t, fx, "job-a")
		oversizeReason := false
		for _, row := range refusals {
			if strings.Contains(string(row.Evidence), stallRejectFileOversize) {
				oversizeReason = true
			}
		}
		pass := completed && !terminalFromOversize && oversizeReason
		gateResult(t, "H1", "event_file_oversize_refused", "oversize record refused before open",
			fmt.Sprintf("scan-completed=%v terminal=%v oversize-row=%v", completed, terminalFromOversize, oversizeReason), pass)
	})

	// One job's refusal must not stop or skip another job's detection: a
	// fifo inside job-a's journal and an overdue job-b are observed in the
	// same scan.
	t.Run("rejected_job_does_not_starve_others", func(t *testing.T) {
		fx := newStallFixture(t, false)
		claimJob(t, fx, "job-a", "", nil, fx.now.Add(-time.Hour))
		if err := syscall.Mkfifo(filepath.Join(fx.inbox, "jobs", "job-a", "events", "00004-job.completed.json"), 0600); err != nil {
			t.Fatal(err)
		}
		claimJob(t, fx, "job-b", "", map[string]any{"deadline_at": fx.now.Add(-time.Minute).Format(time.RFC3339)}, fx.now.Add(-2*time.Hour))
		completed := gateScan(t, fx, 15*time.Second)
		refusals := gateRefusals(t, fx, "job-a")
		overdue := false
		for _, row := range stallIncidentsFor(t, fx, "job-b") {
			if row.Cause == stallCauseOverdue {
				overdue = true
			}
		}
		pass := completed && len(refusals) == 1 && overdue
		gateResult(t, "H1", "rejected_job_does_not_starve_others", "refusal row on job-a + overdue fires on job-b",
			fmt.Sprintf("scan-completed=%v refusals=%d overdue=%v", completed, len(refusals), overdue), pass)
	})
}

// Instrument control: the fifo watchdog's sensitivity is proven — an open
// without a writer must block.
func TestR5GateH1FifoInstrumentControl(t *testing.T) {
	fifo := filepath.Join(t.TempDir(), "control.fifo")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _, _ = os.ReadFile(fifo) }()
	select {
	case <-done:
		gateResult(t, "H1", "fifo_instrument_control", "open without writer blocks", "returned", false)
	case <-time.After(5 * time.Second):
		gateResult(t, "H1", "fifo_instrument_control", "open without writer blocks", "blocked (instrument sensitive)", true)
	}
}

// ---- H2 — quoted markers vs real waiting ----

func gateScreenJob(t *testing.T, harness string) (*stallFixture, string) {
	t.Helper()
	fx := newStallFixture(t, false)
	at := fx.now.Add(-time.Hour)
	pool, profile := harness, harness+"-x"
	writeJobEvent(t, fx.inbox, "job-a", 1, "job.claim", map[string]any{"owner_lane": "owner-1"}, at)
	writeJobEvent(t, fx.inbox, "job-a", 2, "quota_pool.record", map[string]any{"pool": pool, "profile": profile}, at)
	writeJobEvent(t, fx.inbox, "job-a", 3, "job.spawned", map[string]any{"pane_id": "w1:p1", "profile": profile}, at)
	fx.agents = append(fx.agents, paneIdentity{PaneID: "w1:p1", CWD: "/work/job-a", Harness: harness})
	fx.subscribed["w1:p1"] = true
	return fx, "w1:p1"
}

// gateDrive baselines on clean text, then plays the target screen for two
// reads (the unsubmitted-banner confirmation needs the second read).
func gateDrive(t *testing.T, fx *stallFixture, pane, screen string) []string {
	t.Helper()
	fx.reads[pane] = readEvidence{Text: "clean baseline output\n", Revision: 1}
	fx.scan()
	fx.reads[pane] = readEvidence{Text: screen, Revision: 2}
	fx.advance(time.Minute)
	fx.scan()
	fx.reads[pane] = readEvidence{Text: screen, Revision: 3}
	fx.advance(time.Minute)
	fx.scan()
	return stallCauses(stallIncidentsFor(t, fx, "job-a"))
}

const gateRealBanner = `── 2 queued ──────────────────────────── ↑ edit · ↵ send now ──
○ <message-one>
○ <message-two>
────────────────────────────────────────────────
❭ Press Enter to send queued messages now
────────────────────────────────────────────────
SWE-2 High · task status line
`

func TestR5GateH2QuotedNewShapes(t *testing.T) {
	cases := []struct {
		name   string
		screen string
	}{
		{"multiline_box_quote", "reviewing the worker pane now\n" +
			"│ ── 2 queued ──────────── ↑ edit · ↵ send now ── │\n" +
			"│ ○ <message-one>                                  │\n" +
			"│ ────────────────────────────────────────────     │\n" +
			"│ ❭ Press Enter to send queued messages now        │\n" +
			"│ ────────────────────────────────────────────     │\n" +
			"quote ends here\n"},
		{"multiline_blockquote", "the capture looked like this:\n" +
			"> ── 1 queued ──────────── ↑ edit · ↵ send now ──\n" +
			"> ○ <message>\n" +
			"> ────────────────────────────────────────\n" +
			"> ❭ Press Enter to send queued messages now\n" +
			"> ────────────────────────────────────────\n" +
			"and that was the whole quote\n"},
		{"banner_wholesale_in_prose", `the banner read "── 1 queued ── ↑ edit · ↵ send now ──" and then "❭ Press Enter to send queued messages now" — both quoted verbatim` + "\n"},
		{"prefix_quotes_inside_live_window", "❭ Press Enter to send queued messages now\n" +
			"> Error: [provider.auth_error] 403 {\"error\":{\"message\":\"usage limit reached\"}}\n" +
			"› ■ Your access token could not be refreshed because you signed in to another account. Please sign in again.\n" +
			"❯ Error: [provider.auth_error] 403 usage limit hit\n" +
			"- ■ Your access token could not be refreshed; please sign in again\n"},
		{"divider_mimicry_no_header", "ordinary output line\n" +
			"────────────────────────────────────\n" +
			"❭ Press Enter to send queued messages now\n" +
			"────────────────────────────────────\n" +
			"prompt sits here\n"},
		{"banner_not_between_last_dividers", "── 1 queued ──────────── ↑ edit · ↵ send now ──\n" +
			"○ <message>\n" +
			"────────────────────────────────────\n" +
			"❭ Press Enter to send queued messages now\n" +
			"────────────────────────────────────\n" +
			"SWE-2 status\n" +
			"────────────────────────────────────\n" +
			"❭ \n" +
			"────────────────────────────────────\n"},
		{"instruction_with_trailing_text", "── 1 queued ──────────── ↑ edit · ↵ send now ──\n" +
			"○ <message>\n" +
			"────────────────────────────────────\n" +
			"❭ Press Enter to send queued messages now.\n" +
			"────────────────────────────────────\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx, pane := gateScreenJob(t, "devin")
			causes := gateDrive(t, fx, pane, tc.screen)
			gateResult(t, "H2a", tc.name, "0 incidents", fmt.Sprintf("%v", causes), len(causes) == 0)
		})
	}

	// Verbatim full-structure quote: the exact devin banner, byte for byte,
	// sitting at the tail of a NON-devin pane (a claude worker that pasted a
	// devin screen capture into its output). Structurally indistinguishable
	// from the real thing except that a claude pane cannot be a devin
	// composer — the harness gate is what keeps this a quote.
	t.Run("verbatim_banner_on_claude_pane", func(t *testing.T) {
		fx, pane := gateScreenJob(t, "claude")
		causes := gateDrive(t, fx, pane, "pasting the devin screen I captured:\n"+gateRealBanner)
		gateResult(t, "H2a", "verbatim_banner_on_claude_pane", "0 incidents (harness mismatch: quote)", fmt.Sprintf("%v", causes), len(causes) == 0)
	})
	// Same bytes on a devin pane: this is the shape the contract calls real.
	t.Run("verbatim_banner_on_devin_pane_control", func(t *testing.T) {
		fx, pane := gateScreenJob(t, "devin")
		causes := gateDrive(t, fx, pane, gateRealBanner)
		count := 0
		for _, c := range causes {
			if c == stallCauseInputUnsubmitted {
				count++
			}
		}
		gateResult(t, "H2b", "verbatim_banner_on_devin_pane_control", "1 input_unsubmitted", fmt.Sprintf("%v", causes), count == 1)
	})
	// The same gate, fail-closed: a pane whose harness is unknown (empty)
	// must not classify the devin banner either.
	t.Run("verbatim_banner_on_unknown_harness_pane", func(t *testing.T) {
		fx, pane := gateScreenJob(t, "")
		causes := gateDrive(t, fx, pane, gateRealBanner)
		gateResult(t, "H2a", "verbatim_banner_on_unknown_harness_pane", "0 incidents (unknown harness: not a devin banner)", fmt.Sprintf("%v", causes), len(causes) == 0)
	})
}

func TestR5GateH2AboveLiveWindow(t *testing.T) {
	fx, pane := gateScreenJob(t, "devin")
	var sb strings.Builder
	sb.WriteString("boot\n")
	sb.WriteString("Error: [provider.auth_error] 403 {\"error\":{\"message\":\"usage limit hit\"}}\n")
	for i := 0; i < 13; i++ {
		fmt.Fprintf(&sb, "later output line %02d\n", i)
	}
	causes := gateDrive(t, fx, pane, sb.String())
	gateResult(t, "H2a", "head_match_above_live_window", "0 incidents (13 non-empty lines below)", fmt.Sprintf("%v", causes), len(causes) == 0)
}

func TestR5GateH2LiveStatesNewShapes(t *testing.T) {
	// Real devin banner on a devin pane — the live form the rule exists for.
	t.Run("real_devin_banner_new_shape", func(t *testing.T) {
		fx, pane := gateScreenJob(t, "devin")
		causes := gateDrive(t, fx, pane, "○ boot sequence complete\n"+gateRealBanner)
		count := 0
		for _, c := range causes {
			if c == stallCauseInputUnsubmitted {
				count++
			}
		}
		gateResult(t, "H2b", "real_devin_banner_new_shape", "1 input_unsubmitted", fmt.Sprintf("%v", causes), count == 1)
	})
	// limit/auth lines at line head inside the live region, with composer
	// lines below them (not at the very tail) — harness-agnostic rules.
	t.Run("limit_auth_mid_live_region", func(t *testing.T) {
		fx, pane := gateScreenJob(t, "kimi")
		screen := "work output\n" +
			"Error: [provider.auth_error] 403 {\"error\":{\"message\":\"You've reached your 5-hour usage limit\"}}\n" +
			"■ Your access token could not be refreshed because you have since logged out or signed in to another account. Please sign in again.\n"
		for i := 0; i < 8; i++ {
			screen += fmt.Sprintf("composer/status line %d\n", i)
		}
		causes := gateDrive(t, fx, pane, screen)
		var limit, auth int
		for _, c := range causes {
			if c == stallCauseLimitRefused {
				limit++
			}
			if c == stallCauseAuthRefused {
				auth++
			}
		}
		gateResult(t, "H2b", "limit_auth_mid_live_region", "1 limit_refused + 1 auth_refused",
			fmt.Sprintf("%v", causes), limit == 1 && auth == 1)
	})

	// 이것은 명세가 아니라 알려진 한계의 고정이다 — task 374 에서 의도적으로 뒤집는다.
	// The contract documents v1's queued-input detection as the devin
	// Press-Enter form only; these three real waiting states currently
	// produce zero events and are pinned at exactly that value.
	t.Run("known_limit_real_claude_queued_hint", func(t *testing.T) {
		fx, pane := gateScreenJob(t, "claude")
		screen := "⎿  Running…\n" +
			"────────────────────────────────────────\n" +
			"❯ \n" +
			"────────────────────────────────────────\n" +
			"  2 messages queued · Press up to edit queued messages\n"
		causes := gateDrive(t, fx, pane, screen)
		gateResult(t, "H2b", "known_limit_real_claude_queued_hint", "0 incidents (pinned known limit)", fmt.Sprintf("%v", causes), len(causes) == 0)
	})
	t.Run("known_limit_real_codex_pending_submit", func(t *testing.T) {
		fx, pane := gateScreenJob(t, "codex")
		screen := "working on task\n" +
			"────────────────────────────────────────\n" +
			"› \n" +
			"────────────────────────────────────────\n" +
			"Messages to be submitted after next tool call\n"
		causes := gateDrive(t, fx, pane, screen)
		gateResult(t, "H2b", "known_limit_real_codex_pending_submit", "0 incidents (pinned known limit)", fmt.Sprintf("%v", causes), len(causes) == 0)
	})
	t.Run("known_limit_devin_header_only_form", func(t *testing.T) {
		fx, pane := gateScreenJob(t, "devin")
		screen := "○ working\n" +
			"── 1 queued ──────────────────────────── ↑ edit · ↵ send now ──\n" +
			"○ <queued message>\n" +
			"────────────────────────────────────────────────\n" +
			"❭ \n" +
			"────────────────────────────────────────────────\n" +
			"SWE-2 High · status\n"
		causes := gateDrive(t, fx, pane, screen)
		gateResult(t, "H2b", "known_limit_devin_header_only_form", "0 incidents (pinned known limit)", fmt.Sprintf("%v", causes), len(causes) == 0)
	})
}

// ---- H3 — same-seq terminal+spawn ties under filename-order stress ----

func TestR5GateH3SameSeqFilenameStress(t *testing.T) {
	setup := func(t *testing.T, fx *stallFixture, jobID string, names [][2]string) {
		at := fx.now.Add(-time.Hour)
		writeJobEvent(t, fx.inbox, jobID, 1, "job.claim", map[string]any{
			"owner_lane": "owner-1", "deadline_at": at.Add(30 * time.Minute).Format(time.RFC3339)}, at)
		writeJobEvent(t, fx.inbox, jobID, 2, "quota_pool.record", map[string]any{"pool": "devin"}, at)
		writeJobEvent(t, fx.inbox, jobID, 3, "job.spawned", map[string]any{"pane_id": "w1:p1"}, at)
		for _, pair := range names {
			writeRawJobEvent(t, fx.inbox, jobID, pair[0], pair[1])
		}
		fx.agents = append(fx.agents, paneIdentity{PaneID: "w1:p2", CWD: "/work/" + jobID, Harness: "devin"})
		fx.subscribed["w1:p2"] = true
		fx.reads["w1:p2"] = readEvidence{Text: "fresh run\n", Revision: 1}
	}
	openAttempt := func(t *testing.T, fx *stallFixture, jobID string) (bool, bool) {
		jobs, err := fx.store.stallJobs(context.Background(), false)
		if err != nil {
			t.Fatal(err)
		}
		for _, job := range jobs {
			if job.JobID == jobID && job.PaneID == "w1:p2" && !job.Terminal {
				overdue := false
				for _, row := range stallIncidentsFor(t, fx, jobID) {
					if row.Cause == stallCauseOverdue {
						overdue = true
					}
				}
				return true, overdue
			}
		}
		return false, false
	}
	at := "2026-09-17T09:30:00Z"

	// Baseline direction, re-derived: job.lost sorts before job.spawned.
	t.Run("lost_then_spawn_same_seq", func(t *testing.T) {
		fx := newStallFixture(t, false)
		setup(t, fx, "job-a", [][2]string{
			{"00005-job.lost.json", `{"kind":"job.lost","job_id":"job-a","created_at":"` + at + `","seq":5}`},
			{"00005-job.spawned.json", `{"kind":"job.spawned","job_id":"job-a","created_at":"` + at + `","seq":5,"payload":{"pane_id":"w1:p2"}}`},
		})
		fx.scan()
		open, overdue := openAttempt(t, fx, "job-a")
		gateResult(t, "H3", "lost_then_spawn_same_seq", "new attempt open + overdue",
			fmt.Sprintf("open=%v overdue=%v", open, overdue), open && overdue)
	})
	// Width anomaly: at the fourth byte '0' (0x30) < '5' (0x35), so the
	// 5-digit "00005-job.lost.json" sorts BEFORE the 4-digit
	// "0005-job.spawned.json" — positionally the terminal lands first and the
	// position rule must treat the spawn as a new attempt.
	t.Run("width_anomaly_wide_terminal_first_spawn_reopens", func(t *testing.T) {
		fx := newStallFixture(t, false)
		setup(t, fx, "job-a", [][2]string{
			{"00005-job.lost.json", `{"kind":"job.lost","job_id":"job-a","created_at":"` + at + `","seq":5}`},
			{"0005-job.spawned.json", `{"kind":"job.spawned","job_id":"job-a","created_at":"` + at + `","seq":5,"payload":{"pane_id":"w1:p2"}}`},
		})
		fx.scan()
		open, overdue := openAttempt(t, fx, "job-a")
		gateResult(t, "H3", "width_anomaly_wide_terminal_first_spawn_reopens", "position rule: spawn after terminal = new attempt + overdue",
			fmt.Sprintf("open=%v overdue=%v", open, overdue), open && overdue)
	})
	// job.reaped / job.revoked also sort before job.spawned — same direction.
	t.Run("reaped_then_spawn_same_seq", func(t *testing.T) {
		fx := newStallFixture(t, false)
		setup(t, fx, "job-a", [][2]string{
			{"00005-job.reaped.json", `{"kind":"job.reaped","job_id":"job-a","created_at":"` + at + `","seq":5}`},
			{"00005-job.spawned.json", `{"kind":"job.spawned","job_id":"job-a","created_at":"` + at + `","seq":5,"payload":{"pane_id":"w1:p2"}}`},
		})
		fx.scan()
		open, overdue := openAttempt(t, fx, "job-a")
		gateResult(t, "H3", "reaped_then_spawn_same_seq", "new attempt open + overdue",
			fmt.Sprintf("open=%v overdue=%v", open, overdue), open && overdue)
	})
	// Reverse width anomaly: "00005-job.spawned.json" sorts before
	// "0005-job.lost.json", so positionally the terminal lands LAST even
	// though its seq string is narrower — the position rule must keep the
	// attempt closed.
	t.Run("width_anomaly_narrow_terminal_sorts_last_stays_closed", func(t *testing.T) {
		fx := newStallFixture(t, false)
		setup(t, fx, "job-a", [][2]string{
			{"0005-job.lost.json", `{"kind":"job.lost","job_id":"job-a","created_at":"` + at + `","seq":5}`},
			{"00005-job.spawned.json", `{"kind":"job.spawned","job_id":"job-a","created_at":"` + at + `","seq":5,"payload":{"pane_id":"w1:p2"}}`},
		})
		fx.scan()
		open, overdue := openAttempt(t, fx, "job-a")
		gateResult(t, "H3", "width_anomaly_narrow_terminal_sorts_last_stays_closed", "position rule: spawn before terminal = closed, no overdue",
			fmt.Sprintf("open=%v overdue=%v", open, overdue), !open && !overdue)
	})
	// True reverse with uniform widths: worker.complete outranks job.spawned,
	// so the same-seq attempt must stay closed (position rule, other side).
	t.Run("worker_complete_after_spawn_stays_closed", func(t *testing.T) {
		fx := newStallFixture(t, false)
		setup(t, fx, "job-a", [][2]string{
			{"00005-job.spawned.json", `{"kind":"job.spawned","job_id":"job-a","created_at":"` + at + `","seq":5,"payload":{"pane_id":"w1:p2"}}`},
			{"00005-worker.complete.json", `{"kind":"worker.complete","job_id":"job-a","created_at":"` + at + `","seq":5}`},
		})
		fx.scan()
		open, overdue := openAttempt(t, fx, "job-a")
		gateResult(t, "H3", "worker_complete_after_spawn_stays_closed", "attempt stays closed, no overdue",
			fmt.Sprintf("open=%v overdue=%v", open, overdue), !open && !overdue)
	})
}
