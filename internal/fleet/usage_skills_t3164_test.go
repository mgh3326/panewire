package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// writeClaudeSkillsFixture plants three Skill calls (alpha twice, beta once)
// plus the same first call written again, as claude does for a message with
// two content blocks, a non-Skill tool call, and a Skill name that is free
// text. Every Skill input carries arguments, which must never cross.
func writeClaudeSkillsFixture(t *testing.T, root string) string {
	t.Helper()
	call := func(msgID, toolID, skill, args string) string {
		input := fmt.Sprintf(`{"skill":%q`, skill)
		if args != "" {
			input += fmt.Sprintf(`,"args":%q`, args)
		}
		return fmt.Sprintf(`{"type":"assistant","message":{"id":%q,"role":"assistant","content":[{"type":"tool_use","id":%q,"name":"Skill","input":%s}]}}`, msgID, toolID, input+"}")
	}
	return writeUsageFile(t, claudeFixturePath(root, testClaudeID), jsonl(
		`{"type":"user","message":{"role":"user","content":"`+secretText+`"}}`,
		call("m1", "toolu_1", "alpha", secretText),
		call("m1", "toolu_1", "alpha", secretText),
		`{"type":"assistant","message":{"id":"m1","role":"assistant","content":[{"type":"text","text":"`+secretText+`"}]}}`,
		call("m2", "toolu_2", "alpha", ""),
		call("m3", "toolu_3", "beta", ""),
		call("m4", "toolu_4", secretText, ""),
		`{"type":"assistant","message":{"id":"m5","role":"assistant","content":[{"type":"tool_use","id":"toolu_5","name":"Bash","input":{"command":"`+secretText+`"}}]}}`,
	))
}

func TestReadClaudeSkillsCountsEachCallOnce(t *testing.T) {
	root := t.TempDir()
	writeClaudeSkillsFixture(t, root)
	got := UsageReader{Root: root}.ReadSkills(HarnessSession{Kind: "claude", ID: testClaudeID, Source: "herdr:claude"}, testCWD)
	if !got.Supported || !got.Known {
		t.Fatalf("supported/known = %v/%v", got.Supported, got.Known)
	}
	want := []SkillUse{
		{Name: "alpha", Source: skillSourceClaude, Calls: 2},
		{Name: "beta", Source: skillSourceClaude, Calls: 1},
	}
	if fmt.Sprint(got.Skills) != fmt.Sprint(want) {
		t.Fatalf("skills = %v, want %v", got.Skills, want)
	}
	for _, skill := range got.Skills {
		if strings.Contains(skill.Name, secretText) {
			t.Fatalf("skill name carries transcript text: %q", skill.Name)
		}
	}
}

func TestReadSkillsUnsupportedHarness(t *testing.T) {
	for _, kind := range []string{"codex", "omp", "devin", "agy", "cmd"} {
		got := UsageReader{Root: t.TempDir()}.ReadSkills(HarnessSession{Kind: kind, ID: testClaudeID}, testCWD)
		if got.Supported || got.Known || len(got.Skills) != 0 {
			t.Fatalf("%s: supported/known/skills = %v/%v/%v, want false/false/none", kind, got.Supported, got.Known, got.Skills)
		}
	}
}

// The completion PUT carries the skills and skills_supported in the same
// usage-only body, and no Skill argument or other transcript text.
func TestCompletionWritesClaudeSkills(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	writeClaudeSkillsFixture(t, a.Home)
	jobs, bodies := hkJobUpdateContract(t)
	usageJob(t, a, claudeSession())
	a.JobStore = jobs

	doneJob(t, a)

	type update struct {
		Skills          []SkillUse     `json:"skills"`
		SkillsSupported *bool          `json:"skills_supported"`
		Usage           map[string]any `json:"usage"`
	}
	var withSkills int
	for _, body := range bodies() {
		if strings.Contains(body, secretText) {
			t.Errorf("PUT body carries transcript text: %s", body)
		}
		var u update
		if json.Unmarshal([]byte(body), &u) != nil || len(u.Skills) == 0 {
			continue
		}
		withSkills++
		if u.SkillsSupported == nil || !*u.SkillsSupported {
			t.Errorf("skills without skills_supported:true: %s", body)
		}
		if u.Usage == nil {
			t.Errorf("skills not on the usage PUT: %s", body)
		}
		if len(u.Skills) != 2 || u.Skills[0].Name != "alpha" || u.Skills[0].Calls != 2 || u.Skills[1].Name != "beta" || u.Skills[1].Calls != 1 {
			t.Errorf("skills = %v", u.Skills)
		}
	}
	if withSkills != 1 {
		t.Fatalf("skill PUTs = %d, want 1; bodies=%v", withSkills, bodies())
	}
}

// writeClaudeSkillsOverlongFixture adds one normal Skill call and one whose
// args push its JSONL line past the reader's per-line bound, which the scan
// skips. No list can be complete while a Skill line is unread.
func writeClaudeSkillsOverlongFixture(t *testing.T, root string) {
	t.Helper()
	big := fmt.Sprintf(`{"type":"assistant","message":{"id":"big","role":"assistant","content":[{"type":"tool_use","id":"toolu_big","name":"Skill","input":{"skill":"beta","args":%q}}]}}`, strings.Repeat("x", maxUsageLineBytes))
	writeUsageFile(t, claudeFixturePath(root, testClaudeID), jsonl(
		`{"type":"assistant","message":{"id":"m1","role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"Skill","input":{"skill":"alpha"}}]}}`,
		big,
	))
}

func TestReadClaudeSkillsSkippedLineIsNotAPartialList(t *testing.T) {
	root := t.TempDir()
	writeClaudeSkillsOverlongFixture(t, root)
	got := UsageReader{Root: root}.ReadSkills(HarnessSession{Kind: "claude", ID: testClaudeID, Source: "herdr:claude"}, testCWD)
	if !got.Supported {
		t.Fatal("claude must be supported")
	}
	if got.Known || len(got.Skills) != 0 {
		t.Fatalf("known/skills = %v/%v, want no partial list", got.Known, got.Skills)
	}
}

func TestOverlongSkillLineWritesNoSkills(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	writeClaudeSkillsOverlongFixture(t, a.Home)
	jobs, bodies := hkJobUpdateContract(t)
	usageJob(t, a, claudeSession())
	a.JobStore = jobs

	doneJob(t, a)

	sawSupported := false
	for _, body := range bodies() {
		var doc map[string]any
		if json.Unmarshal([]byte(body), &doc) != nil {
			continue
		}
		if _, ok := doc["skills"]; ok {
			t.Errorf("over-long record wrote a partial skills list: %s", body)
		}
		if supported, ok := doc["skills_supported"].(bool); ok && supported {
			sawSupported = true
		}
	}
	if !sawSupported {
		t.Fatalf("no body carried skills_supported:true; bodies=%v", bodies())
	}
}

func TestNonClaudeHarnessReportsSkillsUnsupported(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	store := usageJob(t, a, map[string]any{"kind": "cmd", "id": "unknown", "source": "unavailable"})

	doneJob(t, a)

	rec := store.record(t, "j")
	if supported, ok := rec["skills_supported"].(bool); !ok || supported {
		t.Fatalf("skills_supported = %v, want false", rec["skills_supported"])
	}
	if skills, ok := rec["skills"]; ok && skills != nil {
		t.Fatalf("non-claude harness carried skills: %v", skills)
	}
}

// skillFailStore refuses the update while it carries skills, as an hk that
// rejects one bad element does.
type skillFailStore struct {
	JobStore
	skillPuts int
}

func (s *skillFailStore) Put(ctx context.Context, id string, fields map[string]any) error {
	if _, ok := fields["skills"]; ok {
		s.skillPuts++
		return &BackendError{Status: 400, Code: "invalid_context"}
	}
	return s.JobStore.Put(ctx, id, fields)
}

func TestSkillsRefusalLeavesCompletionUnchanged(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	writeClaudeSkillsFixture(t, a.Home)
	base := usageJob(t, a, claudeSession())
	failing := &skillFailStore{JobStore: base}
	a.JobStore = failing
	var stderr bytes.Buffer
	a.Err = &stderr

	doneJob(t, a) // fails the test if lifecycle returns an error

	if failing.skillPuts != 1 {
		t.Fatalf("skill writes = %d", failing.skillPuts)
	}
	rec := base.record(t, "j")
	if rec["state"] != "completed" || rec["skills"] != nil || rec["skills_supported"] != nil {
		t.Fatalf("record = %v", rec)
	}
	if !strings.Contains(stderr.String(), "job usage write failed (hk status 400, code invalid_context)") {
		t.Fatalf("no warning: %q", stderr.String())
	}
	if strings.Contains(stderr.String(), secretText) {
		t.Fatalf("warning carries transcript text: %q", stderr.String())
	}
}
