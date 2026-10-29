package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

// #687: relay inject confirmation by a per-row nonce token. The #683 design
// matched the injected text against the pane echo; four tester rounds each
// found a rendering variant that broke it. Now every injected relay text
// carries one bracketed ASCII nonce derived from the durable row, and only
// an exact-token appearance of that nonce -- on the visible screen or in
// the recent-unwrapped transcript -- proves THIS row landed.

// task687Text composes the text deliver() injects for one held row: the
// row's nonce token ahead of the body, the same shape relayBatchText
// produces for a single-item group.
func task687Text(eventID int64, body string) string {
	return relayBatchText([]relayHeld{{EventID: eventID, Text: body}}, false, time.Now())
}

// task687Batch composes one multi-member batch and returns the composed
// text plus each member's nonce.
func task687Batch(items ...relayHeld) (string, []string) {
	text := relayBatchText(items, false, time.Now())
	var nonces []string
	for _, item := range items {
		nonces = append(nonces, relayNonce(item))
	}
	return text, nonces
}

var task687NonceShape = regexp.MustCompile(`^\[r[0-9][0-9a-z]{2,}\]$`)

// AC1: the nonce format is one bracketed ASCII-alphanumeric token -- no
// underscores, no markdown-significant bytes, deterministic per row, and
// short enough that it rejoins under the wrap-join at every pane width.
func TestTask687NonceFormat(t *testing.T) {
	for _, id := range []int64{1, 42, 1807, 18074, 68301, 1 << 62} {
		nonce := relayNonce(relayHeld{EventID: id})
		if !task687NonceShape.MatchString(nonce) {
			t.Fatalf("event %d: nonce %q is not the bracketed token shape", id, nonce)
		}
		if strings.ContainsAny(nonce, "_*`~^\\") {
			t.Fatalf("event %d: nonce %q carries a markdown-significant byte", id, nonce)
		}
		if len(nonce) > 28 {
			t.Fatalf("event %d: nonce %q too long to stay whole inside a wrap row", id, nonce)
		}
		if got := relayNonce(relayHeld{EventID: id}); got != nonce {
			t.Fatalf("event %d: nonce is not deterministic: %q then %q", id, nonce, got)
		}
	}
	// Distinct rows get distinct nonces; an id prefix (1807 vs 18074) is
	// kept distinct by the check suffix.
	if relayNonce(relayHeld{EventID: 1807}) == relayNonce(relayHeld{EventID: 18074}) {
		t.Fatal("1807 and 18074 share a nonce")
	}
	// A fire-and-forget row derives its token from the text, deterministically.
	a := relayNonce(relayHeld{Lane: "lane-a", JobID: "j1", Text: "hello"})
	if !task687NonceShape.MatchString(a) || a != relayNonce(relayHeld{Lane: "lane-a", JobID: "j1", Text: "hello"}) {
		t.Fatalf("row-less nonce malformed or unstable: %q", a)
	}
}

// Tester F1: handoffkeep folds later job.* rounds into the first round's
// durable row, so different texts can share one event id. The check folds
// the text in, so a folded round with new text gets a new nonce -- an
// earlier round's echo can no longer "prove" it. A replay of the same
// message still derives the same token.
func TestTask687FoldedRoundsGetDistinctNonces(t *testing.T) {
	r1 := relayNonce(relayHeld{EventID: 101, Text: "VERDICT: FAIL"})
	r2 := relayNonce(relayHeld{EventID: 101, Text: "VERDICT: PASS @ae3f2c7"})
	r3 := relayNonce(relayHeld{EventID: 101, Text: "VERDICT: PASS @5120d3a"})
	if r1 == r2 || r2 == r3 || r1 == r3 {
		t.Fatalf("folded rounds share a nonce: %q %q %q", r1, r2, r3)
	}
	if r1 != relayNonce(relayHeld{EventID: 101, Text: "VERDICT: FAIL"}) {
		t.Fatal("same id+text nonce is not stable across calls")
	}
	if !strings.HasPrefix(r1, "[r101") || !strings.HasPrefix(r2, "[r101") {
		t.Fatalf("nonce digit prefix must still carry the event id: %q %q", r1, r2)
	}
}

// AC1: extraction pulls only the bracketed nonce tokens out of a composed
// text -- bracketed boilerplate and ordinary words are not nonces.
func TestTask687NonceExtraction(t *testing.T) {
	items := []relayHeld{{EventID: 201, Text: "first"}, {EventID: 202, Text: "second"}, {EventID: 203, Text: "third"}}
	text, nonces := task687Batch(items...)
	if got := relayNoncesIn(text); len(got) != 3 || got[0] != nonces[0] || got[1] != nonces[1] || got[2] != nonces[2] {
		t.Fatalf("relayNoncesIn(%q)=%v, want the three member nonces in order", text, got)
	}
	if got := relayNoncesIn("[batch 3건] 1) [report] x 2) [event] y [대기 만료 4분]"); len(got) != 0 {
		t.Fatalf("bracketed boilerplate extracted as nonces: %v", got)
	}
	// A nonce-shaped token inside a member's own text is extracted too --
	// it echoes with the rest, so proving it is harmless.
	withQuoted := relayBatchText([]relayHeld{{EventID: 204, Text: "see [r9999zz] for details"}}, false, time.Now())
	got := relayNoncesIn(withQuoted)
	if len(got) != 2 || got[0] != relayNonce(relayHeld{EventID: 204, Text: "see [r9999zz] for details"}) || got[1] != "[r9999zz]" {
		t.Fatalf("quoted nonce not extracted in order: %v", got)
	}
}

// AC1: exact-token matching -- the nonce's core must stand alone, never be
// a substring of a longer token. An underscore is a word byte, so
// changed_r1807xx does not contain the nonce.
func TestTask687NonceTokenBoundaries(t *testing.T) {
	nonce := relayNonce(relayHeld{EventID: 18074})
	other := relayNonce(relayHeld{EventID: 1807})
	core := relayNonceCore(nonce)
	// Both cores begin r1807 -- the decimal prefix is shared by design; the
	// check suffix and the token boundary are what keep them distinct.
	if !strings.HasPrefix(core, "r1807") || !strings.HasPrefix(relayNonceCore(other), "r1807") {
		t.Fatal("nonce format drifted: cores must start with r<event-id>")
	}
	yes := []string{
		"❯ " + nonce + " rest of the line",
		"**" + nonce + "** markdown emphasis around it",
		"`" + nonce + "` code span",
		"[" + core + "](https://example.invalid) link syntax",
		nonce + "-trailing punctuation",
		"prefix-" + nonce + "-suffix hyphens are boundaries",
		"「" + core + "」 corner brackets",
		" " + nonce + "  zero-width around it",
		"at end " + nonce,
		nonce + " at start",
		"standalone " + nonce + " mid-line",
		"(같은 내용) " + nonce + " (보이면)",
	}
	for _, line := range yes {
		if !relayNonceTokenIn(line, nonce) {
			t.Fatalf("nonce not found in %q", line)
		}
	}
	no := []string{
		// A prefix of this row's nonce core is a different row's territory.
		"❯ " + other + " the earlier row",
		// The nonce as a substring inside a longer token.
		"❯ x" + core,
		"❯ " + core + "9z",
		// Underscores are word bytes: the nonce fused into an identifier.
		"❯ changed_" + core,
		"❯ " + core + "_seq",
		"❯ state_" + core + "_seq",
		// Another row's nonce is not this row's.
		"❯ " + relayNonce(relayHeld{EventID: 99999}),
		// The core without brackets is still the token; a mangled variant is not.
		"❯ " + strings.TrimSuffix(core, core[len(core)-1:]) + "]",
	}
	for _, line := range no {
		if relayNonceTokenIn(line, nonce) {
			t.Fatalf("nonce matched inside %q", line)
		}
	}
	// The AC's literal pair: an unsuffixed [r1807] must not match inside
	// [r18074..], and neither real nonce matches the other's echo.
	if relayNonceTokenIn("❯ [r18074ab]", "[r1807]") {
		t.Fatal("[r1807] matched inside [r18074ab]")
	}
	if relayNonceTokenIn("❯ "+nonce, other) || relayNonceTokenIn("❯ "+other, nonce) {
		t.Fatal("r1807/r18074 nonce cores matched each other")
	}
}

// AC1: a wrap at any real width leaves every nonce recognizable -- either
// whole on a physical row or rejoined by the transcript-block join. The
// nonce is at most 24 bytes, so at widths >= 40 a wrap can split it but the
// joined block always re-forms it.
func TestTask687NonceSurvivesWraps(t *testing.T) {
	items := []relayHeld{
		{EventID: 301, Text: "(같은 내용이 두 번 보이면 재실행 금지) [event] director-1 :: {\"kind\":\"idle-wake\",\"pane\":\"w16:p1\",\"changed_at\":\"2026-09-24T19:35:59.581Z\",\"state_change_seq\":1}"},
		{EventID: 302, Text: "(같은 내용이 두 번 보이면 재실행 금지) [event] director-1 :: {\"kind\":\"idle-wake\",\"pane\":\"w16:p7\",\"changed_at\":\"2026-09-24T19:37:29.581Z\",\"state_change_seq\":9}"},
		{EventID: 303, Text: "(같은 내용이 두 번 보이면 재실행 금지) [report] b618 :: VERDICT: PASS @de41e7a -> jobs/618/report.md"},
	}
	text, nonces := task687Batch(items...)
	for width := 40; width <= 250; width++ {
		wrapped := task687Wrap("❯ "+text, width)
		for _, nonce := range nonces {
			if !relayNoncePresent(wrapped, nonce) {
				t.Fatalf("width %d: nonce %s lost in\n%s", width, nonce, wrapped)
			}
		}
	}
	// A nonce split at every internal position still rejoins in the block.
	nonce := nonces[0]
	for cut := 1; cut < len(nonce); cut++ {
		screen := "❯ text " + nonce[:cut] + "\n  " + nonce[cut:] + " more"
		if !relayNoncePresent(screen, nonce) {
			t.Fatalf("nonce split at %d not recognized:\n%s", cut, screen)
		}
	}
	// A split nonce under a deeper indent is not a wrap continuation and
	// must not rejoin (a detail row is not the echo's wrap).
	screen := "❯ text " + nonce[:4] + "\n      " + nonce[4:]
	if relayNoncePresent(screen, nonce) {
		t.Fatalf("nonce rejoined across a deep indent:\n%s", screen)
	}
}

// task687Wrap draws the echo the way claude wraps it: the first row fills to
// width, continuation rows are indented two columns and fill to width.
// Wraps may land mid-word (the #683 R4-1 failure shape).
func task687Wrap(line string, width int) string {
	if len(line) <= width {
		return line
	}
	var rows []string
	rows = append(rows, line[:width])
	rest := line[width:]
	for len(rest) > width-2 {
		rows = append(rows, "  "+rest[:width-2])
		rest = rest[width-2:]
	}
	if rest != "" {
		rows = append(rows, "  "+rest)
	}
	return strings.Join(rows, "\n")
}

// AC2: on a busy claude pane the echo scrolled past the small read window
// still proves the row -- by nonce, not by body text. One prompt, no
// keypress, delivered once.
func TestTask687BusyPaneScrolledNonceIsDeliveredOnce(t *testing.T) {
	text := task687Text(68701, task626Text)
	echo := "❯ " + text
	post := task626Claude(append(append(append([]string{}, task626Transcript...), echo), task683Filler("post", 200)...), "❯")
	postTranscript := strings.Join(append(append(append([]string{}, task626Transcript...), echo), task683Filler("post", 200)...), "\n")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{task626ClaudeEmpty, post},
		[]string{task626TranscriptOnly, postTranscript})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", text, nil)
	if result.Outcome != relayInjectDelivered || result.Evidence != "recent-unwrapped:nonce_echo" {
		t.Fatalf("result=%+v, want delivered recent-unwrapped:nonce_echo", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 || task683Count(got, "send-keys") != 0 {
		t.Fatalf("herdr calls = %q, want one prompt and no keypress", got)
	}
}

// AC2: the #683 R4-1 shape -- production relayBatchText with
// changed_at/state_change_seq members hard-wrapping mid-word at a real pane
// width -- is delivered per member, on one prompt, no second attempt.
// deliver() emits one relay.delivered per member whose nonce was seen.
func TestTask687R4BatchAllMembersDelivered(t *testing.T) {
	items := []relayHeld{
		{Pane: "w1:p1", Lane: "director-1", EventID: 18071, JobID: "relay-job-18071", Text: "(같은 내용이 두 번 보이면 재실행 금지) [event] director-1 :: {\"kind\":\"idle-wake\",\"pane\":\"w16:p1\",\"changed_at\":\"2026-09-25T07:31:02.481Z\",\"state_change_seq\":1}", HeldSince: time.Now(), DeliverPolicy: "idle"},
		{Pane: "w1:p1", Lane: "director-1", EventID: 18072, JobID: "relay-job-18072", Text: "(같은 내용이 두 번 보이면 재실행 금지) [event] director-1 :: {\"kind\":\"idle-wake\",\"pane\":\"w16:p7\",\"changed_at\":\"2026-09-25T07:33:44.918Z\",\"state_change_seq\":9}", HeldSince: time.Now(), DeliverPolicy: "idle"},
		{Pane: "w1:p1", Lane: "director-1", EventID: 18073, JobID: "relay-job-18073", Text: "(같은 내용이 두 번 보이면 재실행 금지) [event] director-1 :: {\"kind\":\"idle-wake\",\"pane\":\"w16:p2\",\"changed_at\":\"2026-09-25T07:34:10.102Z\",\"state_change_seq\":12}", HeldSince: time.Now(), DeliverPolicy: "idle"},
	}
	batch, _ := task687Batch(items...)
	// A 93-column pane: the wrap lands inside changed_at/state_change_seq,
	// the exact R4-1 failure shape.
	echo := task687Wrap("❯ "+batch, 93)
	post := task626Claude(append(append([]string{}, task626Transcript...), strings.Split(echo, "\n")...), "❯")
	postTranscript := strings.Join(append(append([]string{}, task626Transcript...), strings.Split(echo, "\n")...), "\n")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{task626ClaudeEmpty, post},
		[]string{task626TranscriptOnly, postTranscript})
	events := make(chan hubClientEvent, 16)
	client := &HubClient{}
	client.setRelayEmitter(func(event hubClientEvent) { events <- event })
	client.relayBusyManager().deliver(context.Background(), items, false)
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("R4-1 batch prompted %d times: %q", task683Count(got, "prompt"), got)
	}
	var deliveredIDs []int64
	for {
		select {
		case event := <-events:
			if event.Kind == "relay.delivered" {
				var ack relayAckPayload
				if err := json.Unmarshal(event.Payload, &ack); err != nil {
					t.Fatal(err)
				}
				deliveredIDs = append(deliveredIDs, ack.OriginalEventID)
			}
			if event.Kind == "relay.unconfirmed" {
				t.Fatalf("a wrapped member went unconfirmed: %s", event.Payload)
			}
		default:
			goto drained
		}
	}
drained:
	if len(deliveredIDs) != 3 {
		t.Fatalf("delivered=%v, want all three members", deliveredIDs)
	}
}

// AC2: a different row's nonce on the pane -- same boilerplate, same lane,
// earlier event -- proves nothing for this row. It is still typed, and when
// its own nonce never appears the verdict is may-be-in-pane, not delivered.
func TestTask687ForeignNonceIsNotThisRow(t *testing.T) {
	earlier := task687Text(18057, task626Text)
	later := task687Text(18074, task626Text)
	pre := task626Claude(append(append(append([]string{}, task626Transcript...), "❯ "+earlier), task683Filler("later", 30)...), "❯")
	preTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+earlier), "\n")
	post := task626Claude(append(append(append([]string{}, task626Transcript...), "❯ "+later), task683Filler("post", 40)...), "❯")
	postTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+later), "\n")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{pre, post},
		[]string{preTranscript, postTranscript})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", later, nil)
	if result.Outcome != relayInjectDelivered || strings.HasPrefix(result.Evidence, "presend:") {
		t.Fatalf("result=%+v, want typed and delivered on this row's nonce", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("a foreign nonce claimed the row without typing: %q", got)
	}
	// And when this row's nonce never appears, it is not delivered.
	_, calls = task683FakeHerdr(t, "claude",
		[]string{pre, pre},
		[]string{preTranscript, preTranscript})
	result = defaultHubRelayInjectVerdict(context.Background(), "w1:p1", later, nil)
	if result.Outcome != relayInjectMaybeInPane {
		t.Fatalf("result=%+v, want maybe_in_pane on only a foreign nonce", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("foreign nonce held the paste: %q", got)
	}
}

// AC2/AC5: a batch delivers per member. When the echo proves only two of
// three nonces, those two are delivered and the third is unconfirmed --
// never silently delivered on a sibling's proof.
func TestTask687BatchDeliversPerMemberNonce(t *testing.T) {
	items := []relayHeld{
		{Pane: "w1:p1", Lane: "lane-a", EventID: 401, JobID: "relay-job-401", Text: "first member", HeldSince: time.Now(), DeliverPolicy: "idle"},
		{Pane: "w1:p1", Lane: "lane-a", EventID: 402, JobID: "relay-job-402", Text: "second member", HeldSince: time.Now(), DeliverPolicy: "idle"},
		{Pane: "w1:p1", Lane: "lane-a", EventID: 403, JobID: "relay-job-403", Text: "third member", HeldSince: time.Now(), DeliverPolicy: "idle"},
	}
	batch, nonces := task687Batch(items...)
	if got := relayNoncesIn(batch); len(got) != 3 {
		t.Fatalf("composed batch %q carries %v nonces, want 3", batch, got)
	}
	// The pane echo shows members 1 and 3's nonces; member 2's never lands.
	partial := "❯ [batch 3건] 1) " + nonces[0] + " first member 2) [r402zz] 3) " + nonces[2] + " third member"
	post := task626Claude(append(append([]string{}, task626Transcript...), partial), "❯")
	postTranscript := strings.Join(append(append([]string{}, task626Transcript...), partial), "\n")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{task626ClaudeEmpty, post},
		[]string{task626TranscriptOnly, postTranscript})
	events := make(chan hubClientEvent, 16)
	client := &HubClient{}
	client.setRelayEmitter(func(event hubClientEvent) { events <- event })
	client.relayBusyManager().deliver(context.Background(), items, false)
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("calls=%q, want one prompt", got)
	}
	var delivered, unconfirmed []int64
	for {
		select {
		case event := <-events:
			switch event.Kind {
			case "relay.delivered":
				var ack relayAckPayload
				if err := json.Unmarshal(event.Payload, &ack); err != nil {
					t.Fatal(err)
				}
				delivered = append(delivered, ack.OriginalEventID)
			case "relay.unconfirmed":
				var ack relayAckPayload
				if err := json.Unmarshal(event.Payload, &ack); err != nil {
					t.Fatal(err)
				}
				unconfirmed = append(unconfirmed, ack.OriginalEventID)
			}
		default:
			goto drained
		}
	}
drained:
	if fmt.Sprint(delivered) != "[401 403]" || fmt.Sprint(unconfirmed) != "[402]" {
		t.Fatalf("delivered=%v unconfirmed=%v, want [401 403] / [402]", delivered, unconfirmed)
	}
}

// AC2: queued evidence counts only when the queued text carries the nonce.
// A fresh banner alone -- or a queue holding a different row's nonce -- is
// not proof this message landed.
func TestTask687QueuedNeedsTheNonce(t *testing.T) {
	text := task687Text(68711, task626Text)
	// Banner appears, and the queued row's text -- nonce included -- is in
	// the transcript.
	queued := task626Claude([]string{"❯ " + text, "Press up to edit queued messages", ""}, "❯")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{task626ClaudeEmpty, queued},
		[]string{task626TranscriptOnly})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", text, nil)
	if result.Outcome != relayInjectQueued {
		t.Fatalf("result=%+v, want queued on banner + nonce", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 || task683Count(got, "send-keys") != 0 {
		t.Fatalf("herdr calls = %q", got)
	}
	// Banner alone, no nonce anywhere: unproven, never queued.
	bare := task626Claude([]string{"Press up to edit queued messages", ""}, "❯")
	_, calls = task683FakeHerdr(t, "claude",
		[]string{task626ClaudeEmpty, bare},
		[]string{task626TranscriptOnly})
	result = defaultHubRelayInjectVerdict(context.Background(), "w1:p1", text, nil)
	if result.Outcome != relayInjectMaybeInPane {
		t.Fatalf("result=%+v, want maybe_in_pane on a nonce-less queue", result)
	}
	// A fresh banner plus only a foreign row's nonce: still not this row.
	foreign := task626Claude([]string{"❯ " + task687Text(68712, task626Text), "Press up to edit queued messages", ""}, "❯")
	_, calls = task683FakeHerdr(t, "claude",
		[]string{task626ClaudeEmpty, foreign},
		[]string{task626TranscriptOnly})
	result = defaultHubRelayInjectVerdict(context.Background(), "w1:p1", text, nil)
	if result.Outcome != relayInjectMaybeInPane {
		t.Fatalf("result=%+v, want maybe_in_pane on a foreign-nonce queue", result)
	}
}

// AC3: the PR 86 CodeRabbit Major -- devin's postsend echo check accepted a
// shared fragment of an earlier message -- is closed by the nonce rule: a
// head-identical row with a different nonce is not this row, and a bare
// text fragment is never proof.
func TestTask687DevinFragmentIsNotProof(t *testing.T) {
	body := "(같은 내용이 두 번 보이면 재실행 금지) [event] b534 :: review the migration plan and report"
	earlier := task687Text(7001, body)
	later := task687Text(7002, body)
	// The fragment that used to satisfy the check: the shared head of an
	// identical earlier message, no nonce of ours anywhere.
	fragment := strings.TrimPrefix(earlier, relayNonce(relayHeld{EventID: 7001, Text: body})+" ")
	calls := task547FakeDevin{
		before:    task547Both(task547IdleScreen),
		afterSend: task547Both(task547SubmittedScreen(fragment)),
	}.install(t)
	result := defaultHubRelayInjectVerdict(context.Background(), "devin-pane", later, nil)
	if result.Outcome == relayInjectDelivered || result.Outcome == relayInjectQueued {
		t.Fatalf("result=%+v, a nonce-less fragment must not prove the row", result)
	}
	// And a foreign nonce on the pane is not this row's proof either.
	calls = task547FakeDevin{
		before:    task547Both(task547IdleScreen),
		afterSend: task547Both(task547SubmittedScreen(earlier)),
	}.install(t)
	result = defaultHubRelayInjectVerdict(context.Background(), "devin-pane", later, nil)
	if result.Outcome != relayInjectMaybeInPane {
		t.Fatalf("result=%+v, a foreign nonce must not prove the row", result)
	}
	if got := task547Count(calls(), "prompt"); got != 1 {
		t.Fatalf("prompts=%d, want 1", got)
	}
}

// AC3/devin arm: the nonce echo delivers; the queued row's nonce plus the
// queue banner queues; a missing nonce is may-be-in-pane.
func TestTask687DevinNonceVerification(t *testing.T) {
	text := task687Text(7101, "(같은 내용이 두 번 보이면 재실행 금지) [event] b534 :: review the migration plan and report")
	t.Run("nonce echo is delivered", func(t *testing.T) {
		calls := task547FakeDevin{
			before:    task547Both(task547IdleScreen),
			afterSend: task547Both(task547SubmittedScreen(text)),
		}.install(t)
		result := defaultHubRelayInjectVerdict(context.Background(), "devin-pane", text, nil)
		if result.Outcome != relayInjectDelivered || !strings.Contains(result.Evidence, "nonce_echo") {
			t.Fatalf("result=%+v, want delivered on the nonce echo", result)
		}
		if got := task547Count(calls(), "prompt"); got != 1 {
			t.Fatalf("prompts=%d", got)
		}
	})
	t.Run("queued row carrying the nonce is queued", func(t *testing.T) {
		calls := task547FakeDevin{
			before:    task547Both(task547IdleScreen),
			afterSend: task547Both(task547QueuedScreen(text)),
			status:    "working",
		}.install(t)
		result := defaultHubRelayInjectVerdict(context.Background(), "devin-pane", text, nil)
		if result.Outcome != relayInjectQueued {
			t.Fatalf("result=%+v, want queued on the nonce-bearing queue row", result)
		}
		if got := task547Count(calls(), "send-keys"); got != 0 {
			t.Fatalf("send-keys=%d on a busy pane's queue", got)
		}
	})
	t.Run("queue without the nonce is not proof", func(t *testing.T) {
		calls := task547FakeDevin{
			before:    task547Both(task547IdleScreen),
			afterSend: task547Both(task547QueuedScreen("a foreign row")),
			status:    "working",
		}.install(t)
		result := defaultHubRelayInjectVerdict(context.Background(), "devin-pane", text, nil)
		if result.Outcome != relayInjectMaybeInPane {
			t.Fatalf("result=%+v, want maybe_in_pane on a nonce-less queue", result)
		}
		if got := task547Count(calls(), "send-keys"); got != 0 {
			t.Fatalf("send-keys=%d", got)
		}
	})
}

// AC4 harness (substring mutant): a nonce core must never match inside a
// longer token. This is the exact assertion the substring-match mutant
// turns RED.
func TestTask687SubstringIsNeverAToken(t *testing.T) {
	nonce := relayNonce(relayHeld{EventID: 42})
	core := relayNonceCore(nonce)
	if relayNonceTokenIn("❯ xx"+core+"yy", nonce) {
		t.Fatal("nonce matched as a substring of a longer token")
	}
	if relayNoncePresent("❯ xx"+core+"yy", nonce) {
		t.Fatal("nonce present inside a longer token on the pane")
	}
}
