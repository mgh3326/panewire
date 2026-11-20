package panewire

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 5454: an owner info notice about a job is decided by that job's saved
// --owner-notify mode, whichever producer sent it. These tests drive the real
// producers where their entry point is direct, and the guard keeps every other
// info producer on the same notice.

func TestOwnerInfoDecision_5454(t *testing.T) {
	for _, tc := range []struct {
		mode     string
		terminal bool
		want     string
	}{
		{"", false, ownerDecisionDeliver},
		{"", true, ownerDecisionDeliver},
		{fleet.OwnerNotifyNever, false, ownerDecisionCount},
		{fleet.OwnerNotifyNever, true, ownerDecisionCount},
		{fleet.OwnerNotifyFinal, false, ownerDecisionCount},
		{fleet.OwnerNotifyFinal, true, ownerDecisionDeliver},
	} {
		if got := ownerInfoDecision(tc.mode, tc.terminal); got != tc.want {
			t.Fatalf("mode %q terminal=%v: decision %q, want %q", tc.mode, tc.terminal, got, tc.want)
		}
	}
}

// The producers under test. Each fires one info notice about its own job, with
// a spawn that names the mode under test (or none).
type ownerInfoProducer struct {
	name     string
	cause    string
	terminal bool
	fire     func(t *testing.T, h *HubServer, task hubSpawnTask, now time.Time)
}

func ownerInfoProducers() []ownerInfoProducer {
	exhaustion := func(now time.Time) poolExhaustion {
		return poolExhaustion{Pool: "claude-max", Cause: "window:5h", Reset: now.Add(time.Hour)}
	}
	return []ownerInfoProducer{
		{name: "pool_exhausted", cause: "pool_exhausted", fire: func(t *testing.T, h *HubServer, task hubSpawnTask, now time.Time) {
			h.notifyPoolExhausted(t.Context(), exhaustion(now), task, "implementer", "")
		}},
		{name: "pool_wait", cause: "pool_wait", fire: func(t *testing.T, h *HubServer, task hubSpawnTask, now time.Time) {
			h.notifyPoolWait(t.Context(), exhaustion(now), task, "waiting")
		}},
		{name: "pool_action_stuck", cause: "pool_action_stuck", fire: func(t *testing.T, h *HubServer, task hubSpawnTask, now time.Time) {
			h.notifyStuckPoolAction(t.Context(), poolAction{ID: "row-" + task.Job, Job: task.Job, Node: "rpi", Data: poolActionData{Owner: task.Owner}}, "node unreachable")
		}},
		{name: "spawn_park", cause: "spawn_park", fire: func(t *testing.T, h *HubServer, task hubSpawnTask, now time.Time) {
			h.relayLaneEventContext(t.Context(), spawnParkNoticeEvent(task), nil)
		}},
		{name: "ladder_refusal", cause: "ladder_quota_refused", fire: func(t *testing.T, h *HubServer, task hubSpawnTask, now time.Time) {
			h.escalateAuthorRefusal(t.Context(), hubEscalateAuthorRequest{Job: task.Job, Round: 1, PR: "owner/repo#1"}, task.Owner, "quota_refused", "successor did not place", false)
		}},
		// The hand-over failure is the terminal notice. Its producer is inside
		// the hand-over settle path, so this sends the same event through the
		// relay with the notice that path attaches.
		{name: "hand_over_failed", cause: "hand_over_failed", terminal: true, fire: func(t *testing.T, h *HubServer, task hubSpawnTask, now time.Time) {
			h.relayLaneEventContext(t.Context(), hubJobEventPayload{EventID: "hand-over-failed:" + task.Job, JobID: laneEventTransportID(task.Owner, "hand-over-failed:"+task.Job), OwnerLane: task.Owner, NotifyType: "info", Text: "[panewire] hand-over of " + task.Job + " failed", Host: "hub", Epoch: 1, Notice: ownerNoticeFor(task.Job, "hand_over_failed", true)}, nil)
		}},
	}
}

// TestOwnerInfoNoticeFollowsJobMode_5454: an absent mode delivers every
// producer's notice; never counts all of them; final delivers only the terminal
// one. Each withheld notice lands in the daily row under its cause.
func TestOwnerInfoNoticeFollowsJobMode_5454(t *testing.T) {
	success := hubSpawnOutcome{RC: 0, Host: "rpi", Pane: "w1:p9", Profile: "haiku", Model: "m"}
	for _, mode := range []string{"", fleet.OwnerNotifyNever, fleet.OwnerNotifyFinal} {
		name := mode
		if name == "" {
			name = "absent"
		}
		t.Run("mode="+name, func(t *testing.T) {
			now := time.Date(2026, 11, 20, 3, 0, 0, 0, time.UTC)
			h, _ := s5360Hub(t, &now)
			var args []string
			if mode != "" {
				args = []string{fleet.OwnerNotifyArg, mode}
			}
			var sent, held []ownerInfoProducer
			for _, p := range ownerInfoProducers() {
				job := "pw-5454-" + p.name + "-" + name
				task := s5360Spawn(t, h, job, args, success)
				p.fire(t, h, task, now)
				withheld := mode == fleet.OwnerNotifyNever || (mode == fleet.OwnerNotifyFinal && !p.terminal)
				if withheld {
					held = append(held, p)
				} else {
					sent = append(sent, p)
				}
			}

			items := t4270Items(t, h, s5360Owner)
			if len(items) != len(sent) {
				t.Fatalf("mode %q: owner heard %d notices, want %d (%s)", mode, len(items), len(sent), producerNames(sent))
			}
			for _, p := range sent {
				job := "pw-5454-" + p.name + "-" + name
				if !slices.ContainsFunc(items, func(item laneInboxItem) bool { return strings.Contains(item.Event.Text, job) }) {
					t.Fatalf("mode %q: the %s notice did not reach the owner", mode, p.name)
				}
			}
			if n := s5360OwnerSilenced(t, h, now); n != int64(len(held)) {
				t.Fatalf("mode %q: silenced=%d, want %d (%s)", mode, n, len(held), producerNames(held))
			}
			causes := dailyNoticeCauses(t, h, s5360Owner, now)
			for _, p := range held {
				if !slices.Contains(causes, p.cause) {
					t.Fatalf("mode %q: daily row causes %v lack %s", mode, causes, p.cause)
				}
			}
		})
	}
}

func producerNames(ps []ownerInfoProducer) string {
	names := make([]string, 0, len(ps))
	for _, p := range ps {
		names = append(names, p.name)
	}
	return strings.Join(names, ",")
}

// dailyNoticeCauses are the causes the day's spawn_notices group lists for lane.
func dailyNoticeCauses(t *testing.T, h *HubServer, lane string, now time.Time) []string {
	t.Helper()
	doc := h.loopsRowDocument(loopDateOf(now), now)
	var causes []string
	for _, row := range doc.SpawnNotices {
		if row.Lane == lane {
			causes = append(causes, row.Cause)
		}
	}
	return causes
}

// A notice with no job, or a cause the loops row cannot group, is not withheld:
// it takes the ordinary path, so a bad producer cannot silence an owner's mail.
func TestOwnerInfoNoticeInvalidFailsOpen_5454(t *testing.T) {
	now := time.Date(2026, 11, 20, 3, 0, 0, 0, time.UTC)
	h, _ := s5360Hub(t, &now)
	s5360Spawn(t, h, "pw-5454-invalid", []string{fleet.OwnerNotifyArg, fleet.OwnerNotifyNever}, hubSpawnOutcome{RC: 0, Host: "rpi", Pane: "w1:p9", Profile: "haiku", Model: "m"})
	for _, notice := range []*ownerInfoNotice{nil, {Job: "", Cause: "pool_wait"}, {Job: "pw-5454-invalid", Cause: "Not A Cause"}} {
		event := hubJobEventPayload{EventID: fmt.Sprintf("invalid-%p", notice), JobID: laneEventTransportID(s5360Owner, "invalid"), OwnerLane: s5360Owner, NotifyType: "info", Text: "[panewire] invalid notice", Host: "hub", Epoch: 1, Notice: notice}
		if _, held := h.ownerInfoHeld(t.Context(), event, s5360Owner); held {
			t.Fatalf("notice %+v was withheld; an invalid notice must not silence", notice)
		}
	}
}

// ownerInfoBypasses lists the info lane events in one source file that name no
// job notice, so the hub cannot decide them by the job's mode. A literal is an
// info event when its NotifyType is the string "info" or a variable (which may
// hold it). An exempt function is one whose notice is not about a job.
func ownerInfoBypasses(t *testing.T, filename string, src []byte) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		exempt := ownerInfoExempt[filepath.Base(filename)+":"+fn.Name.Name]
		ast.Inspect(fn, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok || !ownerInfoLiteral(lit.Type) || !ownerInfoNotifyValue(lit) || exempt || literalHasKey(lit, "Notice") {
				return true
			}
			found = append(found, fmt.Sprintf("%s:%d %s", filepath.Base(filename), fset.Position(lit.Pos()).Line, fn.Name.Name))
			return true
		})
	}
	return found
}

// ownerInfoExempt names the functions whose info notice is not about a job: the
// daily loops notice names a mechanism, not a job.
var ownerInfoExempt = map[string]bool{"hub_loop_ledger.go:persistLoopNotice": true}

func ownerInfoLiteral(expr ast.Expr) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && (ident.Name == "hubJobEventPayload" || ident.Name == "hubScannedRelayEvent" || ident.Name == "emitRecord")
}

func ownerInfoNotifyValue(lit *ast.CompositeLit) bool {
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok || !isIdentNamed(kv.Key, "NotifyType") {
			continue
		}
		switch value := kv.Value.(type) {
		case *ast.BasicLit:
			return value.Value == `"info"`
		case *ast.Ident:
			return true
		}
	}
	return false
}

func literalHasKey(lit *ast.CompositeLit, key string) bool {
	for _, elt := range lit.Elts {
		if kv, ok := elt.(*ast.KeyValueExpr); ok && isIdentNamed(kv.Key, key) {
			return true
		}
	}
	return false
}

func isIdentNamed(expr ast.Expr, name string) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && ident.Name == name
}

// Every info lane event the root package builds carries its job's notice.
func TestOwnerInfoProducersCarryNotice_5454(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if found := ownerInfoBypasses(t, name, src); len(found) != 0 {
			t.Fatalf("info lane events without a job notice (decided by no mode): %v", found)
		}
	}
}

// The guard catches a producer that bypasses the decision, and passes one that
// does not. The mutant is the shape a new producer would take.
func TestOwnerInfoBypassGuardFlagsMutant_5454(t *testing.T) {
	mutant := []byte("package panewire\n\nfunc hubNewProducer(h *HubServer) {\n\th.relayLaneEventContext(ctx, hubJobEventPayload{EventID: \"x\", OwnerLane: \"l\", NotifyType: \"info\", Text: \"t\"}, nil)\n}\n")
	if got := ownerInfoBypasses(t, "hub_new_producer.go", mutant); len(got) != 1 || !strings.Contains(got[0], "hubNewProducer") {
		t.Fatalf("mutant producer: flagged %v, want one bypass in hubNewProducer", got)
	}
	variable := []byte("package panewire\n\nfunc hubNewVariable(h *HubServer, kind string) {\n\th.relayLaneEventContext(ctx, hubJobEventPayload{EventID: \"x\", OwnerLane: \"l\", NotifyType: kind, Text: \"t\"}, nil)\n}\n")
	if got := ownerInfoBypasses(t, "hub_new_variable.go", variable); len(got) != 1 {
		t.Fatalf("mutant with a variable notify type: flagged %v, want one bypass", got)
	}
	compliant := []byte("package panewire\n\nfunc hubNewProducer(h *HubServer) {\n\th.relayLaneEventContext(ctx, hubJobEventPayload{EventID: \"x\", OwnerLane: \"l\", NotifyType: \"info\", Text: \"t\", Notice: ownerNoticeFor(\"pw-x\", \"c\", false)}, nil)\n}\n")
	if got := ownerInfoBypasses(t, "hub_new_producer.go", compliant); len(got) != 0 {
		t.Fatalf("compliant producer flagged: %v", got)
	}
	decision := []byte("package panewire\n\nfunc hubNewDecision(h *HubServer) {\n\th.relayLaneEventContext(ctx, hubJobEventPayload{EventID: \"x\", OwnerLane: \"l\", NotifyType: \"decision\", Text: \"t\"}, nil)\n}\n")
	if got := ownerInfoBypasses(t, "hub_new_decision.go", decision); len(got) != 0 {
		t.Fatalf("a non-info event flagged: %v", got)
	}
	loops := []byte("package panewire\n\nfunc persistLoopNotice(h *HubServer) {\n\th.relayLaneEventContext(ctx, hubJobEventPayload{EventID: \"x\", OwnerLane: \"l\", NotifyType: \"info\", Text: \"t\"}, nil)\n}\n")
	if got := ownerInfoBypasses(t, "hub_loop_ledger.go", loops); len(got) != 0 {
		t.Fatalf("the exempt loops notice flagged: %v", got)
	}
}

// A node's quota-stop resume names the job it is about, so the hub can decide it.
func TestQuotaStopResumeNoticeNamesJob_5454(t *testing.T) {
	var sent []hubScannedRelayEvent
	q := &quotaStopManager{
		root:    t.TempDir(),
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		enqueue: func(event hubScannedRelayEvent) bool { sent = append(sent, event); return true },
	}
	q.infoToOwner("mergewake-design", "quota-stop-resumed:q-5454", "resumed pw-x (claude-max quota reset) in pane w1:p1", ownerNoticeFor("pw-x", "quota_stop_resumed", false), time.Now())
	if len(sent) != 1 || sent[0].Notice == nil || sent[0].Notice.Job != "pw-x" || sent[0].Notice.Cause != "quota_stop_resumed" {
		t.Fatalf("quota-stop resume enqueued %+v, want its notice naming pw-x", sent)
	}
}

// The notice travels from the node's payload through the strict hub decoder,
// and an invalid one is dropped rather than refusing the event.
func TestLaneNoticeWireDecodedAndDropsInvalid_5454(t *testing.T) {
	good := []byte(`{"owner_lane":"mergewake-design","event_id":"quota-stop-resumed:q-5454","text":"resumed pw-x","notify_type":"info","notice":{"job":"pw-x","cause":"quota_stop_resumed"}}`)
	event, ok := decodeHubLaneEventPayload(good)
	if !ok || event.Notice == nil || event.Notice.Job != "pw-x" || event.Notice.Cause != "quota_stop_resumed" {
		t.Fatalf("decoded %+v ok=%v, want the notice kept", event.Notice, ok)
	}
	bad := []byte(`{"owner_lane":"mergewake-design","event_id":"quota-stop-resumed:q-5454","text":"resumed pw-x","notify_type":"info","notice":{"job":"pw-x","cause":"Not A Cause"}}`)
	event, ok = decodeHubLaneEventPayload(bad)
	if !ok || event.Notice != nil {
		t.Fatalf("invalid notice: ok=%v notice=%+v, want the event kept and the notice dropped", ok, event.Notice)
	}
}

// A notice written by the node's emit record is read back from the lane file
// with its job, so the scan hands the hub the same notice the payload carries.
func TestLaneNoticeFileScanKeepsJob_5454(t *testing.T) {
	root := t.TempDir()
	path, err := writeLaneEmitRecord(root, emitRecord{Type: "lane.event", Epoch: 1, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), OwnerLane: "mergewake-design", EventID: "replaced-close-pw-x", Text: "[panewire] closed pw-x", NotifyType: "info", Notice: ownerNoticeFor("pw-x", "replaced_close", false), Ceiling: true})
	if err != nil {
		t.Fatal(err)
	}
	scanned, ok, _, ioErr := readLaneEventFile(path)
	if !ok || ioErr || scanned.Notice == nil || scanned.Notice.Job != "pw-x" || scanned.Notice.Cause != "replaced_close" {
		t.Fatalf("scanned %+v ok=%v ioErr=%v, want the notice kept", scanned.Notice, ok, ioErr)
	}
}

// --notice-job names an info lane event; any other kind or type is a usage error.
func TestEmitNoticeFlagsNeedInfoLaneEvent_5454(t *testing.T) {
	for _, args := range [][]string{
		{"--kind", "lane.event", "--lane", "mergewake-design", "--type", "decision", "--notice-job", "pw-x", "--notice-cause", "replaced_close", "--text", "t"},
		{"--kind", "lane.event", "--lane", "mergewake-design", "--type", "info", "--notice-job", "pw-x", "--text", "t"},
		{"--kind", "lane.event", "--lane", "mergewake-design", "--type", "info", "--notice-job", "not a job", "--notice-cause", "replaced_close", "--text", "t"},
	} {
		var stdout, stderr strings.Builder
		if code := runEmitCLI(args, &stdout, &stderr, CLIConfig{}); code != ExitUsage {
			t.Fatalf("emit %v: rc %d, want ExitUsage (stderr %q)", args, code, stderr.String())
		}
	}
}
