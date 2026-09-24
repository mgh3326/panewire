package panewire

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// #646: a direct prompt's classifier, run on screens captured from a real idle
// claude pane (Claude Code 2.1.281, herdr 0.9.1, recent_unwrapped reads,
// 2026-09-24). Each fixture pair is the preflight read and a read after the
// text reached the pane:
//
//	f1  chip residue      bracketed paste with no Enter; composer holds the chip
//	f3  divider residue   3-line paste with a line of ─ in it, no Enter
//	f4  queued            prompt sent while the pane ran `sleep 20`
//	f5  submitted         herdr agent prompt of a 35-line brief whose first line
//	                      is the fleet header an earlier message also started with
//	f6  submitted, long   a 90-line brief; its first line is above the window
//
// f5-pre doubles as the screen just after the send and before claude renders
// it: the header is already on screen, from the earlier message (#623's delta
// brief had that header too).

const task646Header = "(같은 내용이 두 번 보이면 재실행 금지)"

func task646Read(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "task646", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// task646Body is the text a fixture's paste carried: the stale-header brief
// with its run id replaced, as the capture script did.
func task646Body(t *testing.T, run string) string {
	t.Helper()
	raw := task646Read(t, "brief-stale-header.txt")
	_, body, err := parsePromptFile(raw)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(strings.ReplaceAll(body, "run 15", "run "+run))
}

type task646Case struct {
	name, pre, screen, body string
	want, wantRule          string
	baseWant                string // what classifySubmissionEvidence alone says (the pre-#646 verdict)
}

func task646Cases(t *testing.T) map[string]task646Case {
	return map[string]task646Case{
		"residue_divider": {
			pre: task646Read(t, "f3-pre.txt"), screen: task646Read(t, "f3-divider-residue.txt"),
			body: task646Header + "\n────────────────\nF3 residue with a divider line inside — do not submit.",
			want: "composer_residue", wantRule: "composer_divider", baseWant: "marker_observed",
		},
		"residue_chip": {
			pre: task646Read(t, "f1-pre.txt"), screen: task646Read(t, "f1-chip-residue.txt"), body: task646Body(t, "F1"),
			want: "composer_residue", wantRule: "paste_chip", baseWant: "composer_residue",
		},
		"stale_header_before_render": {
			pre: task646Read(t, "f5-pre.txt"), screen: task646Read(t, "f5-pre.txt"), body: task646Body(t, "F5"),
			want: "unproven", wantRule: "marker_stale", baseWant: "marker_observed",
		},
		"queued": {
			pre: task646Read(t, "f4-pre.txt"), screen: task646Read(t, "f4-queued.txt"),
			body: task646Header + "\ncomms-contract: v1\nF4 queued message — reply with ack-F4.",
			want: "queued", wantRule: "queued_banner", baseWant: "queued",
		},
		"submitted": {
			pre: task646Read(t, "f5-pre.txt"), screen: task646Read(t, "f5-submitted.txt"), body: task646Body(t, "F5"),
			want: "marker_observed", wantRule: "marker_echo", baseWant: "marker_observed",
		},
		"submitted_long": {
			pre: task646Read(t, "f6-pre.txt"), screen: task646Read(t, "f6-submitted-long.txt"), body: strings.TrimSpace(task646Read(t, "brief-long.txt")),
			want: "marker_observed", wantRule: "marker_echo_tail", baseWant: "unproven",
		},
	}
}

func TestTask646RealScreensClassify(t *testing.T) {
	for name, tc := range task646Cases(t) {
		t.Run(name, func(t *testing.T) {
			// The fixture must show what it claims before the classifier is
			// judged on it: the pre-#646 verdict on the same screen.
			if base, _ := classifySubmissionEvidence("claude", tc.screen, markerFor(tc.body)); base != tc.baseWant {
				t.Fatalf("premise: pre-#646 verdict %s, want %s", base, tc.baseWant)
			}
			got, rule := classifyPromptSubmission("claude", tc.pre, tc.screen, tc.body)
			if got != tc.want || rule != tc.wantRule {
				t.Fatalf("classifyPromptSubmission = %s/%s, want %s/%s", got, rule, tc.want, tc.wantRule)
			}
		})
	}
}

// The fixtures' premises, checked on the raw captures so a re-capture that
// loses them fails here rather than passing the classifier test vacuously.
func TestTask646FixturePremises(t *testing.T) {
	cases := task646Cases(t)
	divider := cases["residue_divider"]
	if region, _ := composerRegion(divider.screen); strings.Contains(region, task646Header) {
		t.Fatal("f3: composerRegion should have been misled to the in-brief divider")
	}
	lines := strings.Split(divider.screen, "\n")
	if top, end, ok := promptComposer("claude", lines); !ok || !strings.Contains(strings.Join(lines[top:end], "\n"), task646Header) {
		t.Fatalf("f3: claude's composer is the one under ❯, holding the whole brief; ok=%v", ok)
	}
	stale := cases["stale_header_before_render"]
	if !strings.Contains(stale.pre, task646Header) || strings.Contains(stale.pre, "run F5") {
		t.Fatal("f5-pre: needs the header from an earlier message and none of the F5 brief")
	}
	if markerFor(stale.body) != task646Header+"\n" {
		t.Fatalf("premise: markerFor is the boilerplate header, got %q", markerFor(stale.body))
	}
	long := cases["submitted_long"]
	if strings.Contains(compactWhitespace(long.screen), compactWhitespace(markerFor(long.body))) {
		t.Fatal("f6: the long brief's head should be above the read window")
	}
}

// A composer the screen does not show is no evidence either way, even with the
// marker elsewhere on screen.
func TestTask646ClaudeComposerUnlocatedIsNotSubmitted(t *testing.T) {
	body := "R2-MARKER brief body"
	screen := "❯ R2-MARKER brief body\n  (no composer drawn)\n"
	if got, rule := classifyPromptSubmission("claude", "idle\n", screen, body); got != "unproven" || rule != "composer_unlocated" {
		t.Fatalf("got %s/%s, want unproven/composer_unlocated", got, rule)
	}
}

// An earlier echo scrolls out of the window while the new one scrolls in: the
// count stays at one, and only the anchor (pre's last transcript lines) shows
// the echo is new.
func TestTask646EchoAfterAnchorWhenOldEchoScrollsOut(t *testing.T) {
	body := "R2-MARKER brief"
	composer := "────\n❯\n────\nstatus\n"
	pre := "❯ R2-MARKER brief (earlier)\n⏺ earlier answer\n✻ Worked for 3s · done 오후 3:12\n" + composer
	post := "✻ Worked for 3s · done 오후 3:12\n❯ R2-MARKER brief\n" + composer
	if got, rule := classifyPromptSubmission("claude", pre, post, body); got != "marker_observed" || rule != "marker_echo" {
		t.Fatalf("got %s/%s, want marker_observed/marker_echo", got, rule)
	}
	// The same screens without the new echo: the old one is gone and nothing
	// follows the anchor, so nothing is proven.
	if got, _ := classifyPromptSubmission("claude", pre, "✻ Worked for 3s · done 오후 3:12\n"+composer, body); got == "marker_observed" {
		t.Fatal("no echo after the anchor must not be a submission")
	}
}

// Harnesses without submission evidence keep their unproven verdict: freshness
// only narrows a positive verdict, it never creates one.
func TestTask646NoEvidenceHarnessStaysUnproven(t *testing.T) {
	for _, harness := range []string{"kimi", "agy", "grok", "mystery"} {
		if got, _ := classifyPromptSubmission(harness, "idle\n", "assistant saw R2-MARKER\n", "R2-MARKER"); got != "unproven" {
			t.Fatalf("%s: got %s, want unproven", harness, got)
		}
	}
}

// A marker split by claude's echo indentation (a newline inside the 24 runes)
// is still the same text; base missed it and reported unproven for a
// submitted short brief (AC1 run 12).
func TestTask646IndentedEchoIsProven(t *testing.T) {
	body := "comms-contract: v1\nThis is a panewire test message (run 12)."
	composer := "────\n❯\n────\nstatus\n"
	post := "❯ comms-contract: v1\n  This is a panewire test message (run 12).\n\n⏺ ack-12\n" + composer
	if base, _ := classifySubmissionEvidence("claude", post, markerFor(body)); base != "unproven" {
		t.Fatalf("premise: base verdict %s, want unproven", base)
	}
	if got, _ := classifyPromptSubmission("claude", "✻ Worked for 3s · done 오후 3:12\n"+composer, post, body); got != "marker_observed" {
		t.Fatalf("got %s, want marker_observed", got)
	}
}

// A divider line followed by a line shorter than the tail marker: taking the
// divider as the composer's top edge leaves a region that holds neither
// marker, and reads the brief's first line, just above it, as an echo. Shape
// from f3 with a shorter last line.
func TestTask646BriefEndingInDividerIsResidue(t *testing.T) {
	body := task646Header + "\nF3b brief\n────────────────\nshort tail"
	composer := "❯ " + task646Header + "\n  F3b brief\n  ────────────────\n  short tail"
	pre := task646Read(t, "f3-pre.txt")
	lines := strings.Split(pre, "\n")
	top, bottom, ok := promptComposer("claude", lines)
	if !ok {
		t.Fatal("premise: f3-pre has a composer")
	}
	screen := strings.Join(lines[:top+1], "\n") + "\n" + composer + "\n" + strings.Join(lines[bottom:], "\n")
	if base, _ := classifySubmissionEvidence("claude", screen, markerFor(body)); base != "marker_observed" {
		t.Fatalf("premise: pre-#646 verdict %s, want marker_observed", base)
	}
	if got, _ := classifyPromptSubmission("claude", pre, screen, body); got != "composer_residue" {
		t.Fatalf("got %s, want composer_residue", got)
	}
}

// Real codex screens (codex-cli, GPT-6-Sol, 2026-09-24): text pasted with no
// Enter sits on codex's input line -- the last line starting with › -- in full
// (c1) or as codex's own chip (c2). Neither is a submission.
func TestTask646RealCodexInputLineIsNotSubmitted(t *testing.T) {
	pre := task646Read(t, "c1-pre.txt")
	c1 := task646Header + "\ncomms-contract: v1\nC1 codex residue — do not submit."
	if base, _ := classifySubmissionEvidence("codex", task646Read(t, "c1-codex-residue.txt"), markerFor(c1)); base != "marker_observed" {
		t.Fatalf("premise: pre-#646 verdict on c1 %s, want marker_observed", base)
	}
	if got, rule := classifyPromptSubmission("codex", pre, task646Read(t, "c1-codex-residue.txt"), c1); got != "composer_residue" {
		t.Fatalf("c1: got %s/%s, want composer_residue", got, rule)
	}
	if got, rule := classifyPromptSubmission("codex", pre, task646Read(t, "c2-codex-chip-residue.txt"), task646Body(t, "C2")); got == "marker_observed" {
		t.Fatalf("c2: codex chip reported submitted (%s)", rule)
	}
}

// Fleet briefs share endings as well as the header (this ending is longer
// than the tail marker). A new message from
// someone else that ends like this brief is not this brief's echo while the
// brief's head is on screen only in an older message.
func TestTask646ForeignMessageSharingTheTailIsNotProof(t *testing.T) {
	ending := "완료 시 wrk done 으로 알리고 보고서 경로를 적어라. 승인 대기 없이 검증을 끝내라. pane 에 질문 금지."
	body := task646Header + "\nthis call's brief, still pending somewhere\n" + ending
	composer := "────\n❯\n────\nstatus\n"
	pre := "❯ " + task646Header + "\n  an older brief\n  " + ending + "\n⏺ done\n✻ Worked for 3s · done 오후 3:12\n" + composer
	post := "❯ " + task646Header + "\n  an older brief\n  " + ending + "\n⏺ done\n✻ Worked for 3s · done 오후 3:12\n❯ another sender's note\n  " + ending + "\n" + composer
	if got, rule := classifyPromptSubmission("claude", pre, post, body); got == "marker_observed" {
		t.Fatalf("a foreign message sharing the tail proved the brief (%s)", rule)
	}
}
