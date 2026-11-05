package fleet

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// secretText is distinctive prompt/response text planted in every fixture. It
// must never appear in anything the reader returns.
const secretText = "SECRET-TRANSCRIPT-TEXT-9f3a"

const (
	testClaudeID = "5b3f2c1a-8d4e-4f6b-9a7c-2e1d0f9b8a6c"
	testCodexID  = "0199aaaa-bbbb-7ccc-8ddd-eeeeffff0000"
	testCWD      = "/work/tree.one"
)

func jsonl(lines ...string) string { return strings.Join(lines, "\n") + "\n" }

func writeUsageFile(t *testing.T, path, body string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func claudeFixturePath(root, id string) string {
	return filepath.Join(root, ".claude", "projects", "-work-tree-one", id+".jsonl")
}

func codexFixturePath(root, id string) string {
	return filepath.Join(root, ".codex", "sessions", "2026", "11", "05", "rollout-2026-11-05T10-00-00-"+id+".jsonl")
}

// claudeFixture writes message msg_1 three times (one entry per content
// block) and msg_2 once, plus noise and a malformed line.
func claudeFixture(t *testing.T, root string) {
	t.Helper()
	block := func(id string, in, out, read, write int) string {
		return fmt.Sprintf(`{"type":"assistant","message":{"id":%q,"model":"claude-sonnet-5-5","role":"assistant","content":[{"type":"text","text":%q}],"usage":{"input_tokens":%d,"output_tokens":%d,"cache_read_input_tokens":%d,"cache_creation_input_tokens":%d}}}`, id, secretText, in, out, read, write)
	}
	writeUsageFile(t, claudeFixturePath(root, testClaudeID), jsonl(
		`{"type":"user","message":{"role":"user","content":"`+secretText+`"}}`,
		block("msg_1", 100, 10, 1000, 200),
		block("msg_1", 100, 10, 1000, 200),
		`{not json `+secretText,
		block("msg_1", 100, 10, 1000, 200),
		`{"type":"summary","summary":"`+secretText+`"}`,
		block("msg_2", 50, 20, 1500, 0),
	))
}

func codexFixture(t *testing.T, root string) {
	t.Helper()
	writeUsageFile(t, codexFixturePath(root, testCodexID), jsonl(
		`{"type":"session_meta","payload":{"id":"`+testCodexID+`","instructions":"`+secretText+`"}}`,
		`{"type":"turn_context","payload":{"model":"gpt-6.1-example","cwd":"/x"}}`,
		`{"type":"event_msg","payload":{"type":"user_message","message":"`+secretText+`"}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":null}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1000,"cached_input_tokens":400,"output_tokens":100,"reasoning_output_tokens":30,"total_tokens":1100},"last_token_usage":{"input_tokens":1000,"cached_input_tokens":400,"output_tokens":100,"reasoning_output_tokens":30,"total_tokens":1100}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":2500,"cached_input_tokens":1200,"output_tokens":300,"reasoning_output_tokens":80,"total_tokens":2800},"last_token_usage":{"input_tokens":1500,"cached_input_tokens":800,"output_tokens":200,"reasoning_output_tokens":50,"total_tokens":1700}}}}`,
		`{"type":"event_msg","payload":{"type":"token_count","info":null}}`,
	))
}

func ompFixture(t *testing.T, root string) string {
	t.Helper()
	return writeUsageFile(t, filepath.Join(root, ".omp", "agent", "sessions", "tree", "s1.jsonl"), jsonl(
		`{"type":"session","id":"s1","cwd":"/x"}`,
		`{"type":"message","id":"u1","parentId":null,"message":{"role":"user","content":"`+secretText+`"}}`,
		`{"type":"message","id":"a1","parentId":"u1","message":{"role":"assistant","model":"claude-opus-5-5","content":"`+secretText+`","usage":{"input":10,"output":5,"cacheRead":100,"cacheWrite":7,"totalTokens":122}}}`,
		`{"type":"message","id":"a2","parentId":"a1","message":{"role":"assistant","model":"claude-opus-5-5","usage":{"input":20,"output":15,"cacheRead":200,"cacheWrite":0,"totalTokens":235}}}`,
		`{"type":"model_usage","model":"claude-opus-5-5","input":999}`,
	))
}

func wantUsage(t *testing.T, got TokenUsage, source, model string, in, out, reasoning, read, write *int64) {
	t.Helper()
	eq := func(name string, g, w *int64) {
		if (g == nil) != (w == nil) || g != nil && *g != *w {
			t.Errorf("%s = %v, want %v", name, deref(g), deref(w))
		}
	}
	if got.Source != source || got.Model != model {
		t.Errorf("source/model = %q/%q, want %q/%q", got.Source, got.Model, source, model)
	}
	eq("input", got.Input, in)
	eq("output", got.Output, out)
	eq("reasoning", got.Reasoning, reasoning)
	eq("cache_read", got.CacheRead, read)
	eq("cache_write", got.CacheWrite, write)
}

func deref(p *int64) any {
	if p == nil {
		return "unknown"
	}
	return *p
}

func n(v int64) *int64 { return &v }

func TestUsageReaderClaudeCountsEachMessageIDOnce(t *testing.T) {
	root := t.TempDir()
	claudeFixture(t, root)
	got := UsageReader{Root: root}.Read(HarnessSession{Kind: "claude", ID: testClaudeID, Source: "herdr:claude"}, testCWD)
	wantUsage(t, got, "claude-session-jsonl", "claude-sonnet-5-5", n(150), n(30), nil, n(2500), n(200))
}

func TestUsageReaderCodexTakesLastCumulativeTotal(t *testing.T) {
	root := t.TempDir()
	codexFixture(t, root)
	got := UsageReader{Root: root}.Read(HarnessSession{Kind: "codex", ID: testCodexID, Source: "herdr:codex"}, testCWD)
	wantUsage(t, got, "codex-token-count", "gpt-6.1-example", n(2500), n(300), n(80), n(1200), nil)
}

func TestUsageReaderOMPSumsAssistantMessages(t *testing.T) {
	root := t.TempDir()
	path := ompFixture(t, root)
	got := UsageReader{Root: root}.Read(HarnessSession{Kind: "omp", ID: path, Source: "herdr:omp"}, testCWD)
	wantUsage(t, got, "omp", "claude-opus-5-5", n(30), n(20), nil, n(300), n(7))
}

func TestUsageReaderHarnessWithoutRecordIsSourceOnly(t *testing.T) {
	for _, kind := range []string{"devin", "agy", "cmd"} {
		got := UsageReader{Root: t.TempDir()}.Read(HarnessSession{Kind: kind, ID: "some-id", Source: "herdr:" + kind}, testCWD)
		wantUsage(t, got, kind, "", nil, nil, nil, nil, nil)
		if got.HasFigures() || len(got.Wire()) != 1 || got.Wire()["source"] != kind {
			t.Errorf("%s wire = %v", kind, got.Wire())
		}
	}
}

// Missing data is unknown, never 0: nothing reaches the wire object.
func TestUsageReaderMissingDataIsUnknownNotZero(t *testing.T) {
	big := func(t *testing.T, path string) {
		t.Helper()
		writeUsageFile(t, path, "")
		if err := os.Truncate(path, maxUsageFileBytes+1); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name   string
		kind   string
		source string
		setup  func(t *testing.T, root string) HarnessSession
	}{
		{"claude unknown id", "claude", "claude-session-jsonl", func(t *testing.T, root string) HarnessSession {
			claudeFixture(t, root)
			return HarnessSession{Kind: "claude", ID: "unknown", Source: "unavailable"}
		}},
		{"claude missing file", "claude", "claude-session-jsonl", func(t *testing.T, root string) HarnessSession {
			return HarnessSession{Kind: "claude", ID: testClaudeID, Source: "herdr:claude"}
		}},
		{"claude other session in same dir is never picked", "claude", "claude-session-jsonl", func(t *testing.T, root string) HarnessSession {
			claudeFixture(t, root)
			return HarnessSession{Kind: "claude", ID: "11111111-2222-3333-4444-555555555555", Source: "herdr:claude"}
		}},
		{"claude traversal id", "claude", "claude-session-jsonl", func(t *testing.T, root string) HarnessSession {
			claudeFixture(t, root)
			return HarnessSession{Kind: "claude", ID: "../" + testClaudeID, Source: "herdr:claude"}
		}},
		{"claude malformed file", "claude", "claude-session-jsonl", func(t *testing.T, root string) HarnessSession {
			writeUsageFile(t, claudeFixturePath(root, testClaudeID), "{not json "+secretText+"\n\x00\x01garbage\n")
			return HarnessSession{Kind: "claude", ID: testClaudeID, Source: "herdr:claude"}
		}},
		{"claude no usage entries", "claude", "claude-session-jsonl", func(t *testing.T, root string) HarnessSession {
			writeUsageFile(t, claudeFixturePath(root, testClaudeID), jsonl(`{"type":"user","message":{"role":"user","content":"x"}}`, `{"type":"assistant","message":{"id":"m","content":[]}}`))
			return HarnessSession{Kind: "claude", ID: testClaudeID, Source: "herdr:claude"}
		}},
		{"claude oversize file", "claude", "claude-session-jsonl", func(t *testing.T, root string) HarnessSession {
			big(t, claudeFixturePath(root, testClaudeID))
			return HarnessSession{Kind: "claude", ID: testClaudeID, Source: "herdr:claude"}
		}},
		{"claude symlinked file", "claude", "claude-session-jsonl", func(t *testing.T, root string) HarnessSession {
			real := writeUsageFile(t, filepath.Join(root, "elsewhere.jsonl"), "")
			claudeFixture(t, root)
			target := claudeFixturePath(root, testClaudeID)
			if err := os.Remove(target); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(real, target); err != nil {
				t.Skipf("symlinks unavailable: %v", err)
			}
			return HarnessSession{Kind: "claude", ID: testClaudeID, Source: "herdr:claude"}
		}},
		{"codex unknown id", "codex", "codex-token-count", func(t *testing.T, root string) HarnessSession {
			codexFixture(t, root)
			return HarnessSession{Kind: "codex", ID: "unknown", Source: "unavailable"}
		}},
		{"codex missing file", "codex", "codex-token-count", func(t *testing.T, root string) HarnessSession {
			return HarnessSession{Kind: "codex", ID: testCodexID, Source: "herdr:codex"}
		}},
		{"codex ambiguous rollouts", "codex", "codex-token-count", func(t *testing.T, root string) HarnessSession {
			codexFixture(t, root)
			other := filepath.Join(root, ".codex", "sessions", "2026", "11", "06", "rollout-2026-11-06T10-00-00-"+testCodexID+".jsonl")
			writeUsageFile(t, other, "{}\n")
			return HarnessSession{Kind: "codex", ID: testCodexID, Source: "herdr:codex"}
		}},
		{"codex token_count with null info only", "codex", "codex-token-count", func(t *testing.T, root string) HarnessSession {
			writeUsageFile(t, codexFixturePath(root, testCodexID), jsonl(`{"type":"event_msg","payload":{"type":"token_count","info":null}}`))
			return HarnessSession{Kind: "codex", ID: testCodexID, Source: "herdr:codex"}
		}},
		{"codex oversize file", "codex", "codex-token-count", func(t *testing.T, root string) HarnessSession {
			big(t, codexFixturePath(root, testCodexID))
			return HarnessSession{Kind: "codex", ID: testCodexID, Source: "herdr:codex"}
		}},
		{"omp relative path", "omp", "omp", func(t *testing.T, root string) HarnessSession {
			ompFixture(t, root)
			return HarnessSession{Kind: "omp", ID: ".omp/agent/sessions/tree/s1.jsonl", Source: "herdr:omp"}
		}},
		{"omp path outside root", "omp", "omp", func(t *testing.T, root string) HarnessSession {
			outside := ompFixture(t, t.TempDir())
			return HarnessSession{Kind: "omp", ID: outside, Source: "herdr:omp"}
		}},
		{"omp path with dotdot", "omp", "omp", func(t *testing.T, root string) HarnessSession {
			path := ompFixture(t, root)
			return HarnessSession{Kind: "omp", ID: filepath.Dir(path) + "/../tree/s1.jsonl", Source: "herdr:omp"}
		}},
		{"omp unknown", "omp", "omp", func(t *testing.T, root string) HarnessSession {
			ompFixture(t, root)
			return HarnessSession{Kind: "omp", ID: "unknown", Source: "unavailable"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			ref := tc.setup(t, root)
			got := UsageReader{Root: root}.Read(ref, testCWD)
			wantUsage(t, got, tc.source, "", nil, nil, nil, nil, nil)
			if wire := got.Wire(); len(wire) != 1 {
				t.Fatalf("wire carries more than the source: %v", wire)
			}
		})
	}
}

func TestUsageReaderSkipsOverlongLineAndKeepsLaterEntries(t *testing.T) {
	root := t.TempDir()
	long := `{"type":"user","message":{"content":"` + strings.Repeat("x", maxUsageLineBytes+10) + `"}}`
	writeUsageFile(t, claudeFixturePath(root, testClaudeID), jsonl(
		long,
		`{"type":"assistant","message":{"id":"m1","model":"claude-sonnet-5-5","usage":{"input_tokens":7,"output_tokens":3}}}`,
	))
	got := UsageReader{Root: root}.Read(HarnessSession{Kind: "claude", ID: testClaudeID, Source: "herdr:claude"}, testCWD)
	wantUsage(t, got, "claude-session-jsonl", "claude-sonnet-5-5", n(7), n(3), nil, nil, nil)
}

func TestUsageReaderRejectsNegativeAndNonIntegerCounts(t *testing.T) {
	root := t.TempDir()
	writeUsageFile(t, claudeFixturePath(root, testClaudeID), jsonl(
		`{"type":"assistant","message":{"id":"m1","usage":{"input_tokens":-5,"output_tokens":1.5,"cache_read_input_tokens":9}}}`,
	))
	got := UsageReader{Root: root}.Read(HarnessSession{Kind: "claude", ID: testClaudeID, Source: "herdr:claude"}, testCWD)
	wantUsage(t, got, "claude-session-jsonl", "", nil, nil, nil, n(9), nil)
}

// A hostile model field is dropped, not sent.
func TestUsageReaderDropsUnsafeModel(t *testing.T) {
	root := t.TempDir()
	writeUsageFile(t, claudeFixturePath(root, testClaudeID), jsonl(
		`{"type":"assistant","message":{"id":"m1","model":"`+secretText+` has spaces","usage":{"input_tokens":1}}}`,
	))
	got := UsageReader{Root: root}.Read(HarnessSession{Kind: "claude", ID: testClaudeID, Source: "herdr:claude"}, testCWD)
	wantUsage(t, got, "claude-session-jsonl", "", n(1), nil, nil, nil, nil)
}

func TestUsageReaderOutputHoldsNoTranscriptText(t *testing.T) {
	root := t.TempDir()
	claudeFixture(t, root)
	codexFixture(t, root)
	omp := ompFixture(t, root)
	for _, ref := range []HarnessSession{
		{Kind: "claude", ID: testClaudeID}, {Kind: "codex", ID: testCodexID}, {Kind: "omp", ID: omp},
		{Kind: "claude", ID: "unknown"},
	} {
		got := UsageReader{Root: root}.Read(ref, testCWD)
		wire, err := json.Marshal(got.Wire())
		if err != nil {
			t.Fatal(err)
		}
		all := string(wire) + got.Model + got.Source
		for _, leak := range []string{secretText, root, testCWD} {
			if strings.Contains(all, leak) {
				t.Errorf("%s output contains %q: %s", ref.Kind, leak, all)
			}
		}
	}
}
