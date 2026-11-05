package fleet

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Per-job token usage is read from the harness's own session record, named by
// the job's harness_session (docs/jobs-resilience.md#harness-session-identity).
// Only the record that reference names is opened: never the newest file for a
// cwd. Only integers and a validated model id leave the parser; prompt,
// response and tool text are never retained, logged or sent.
//
// Record formats, from the harnesses' public docs and source:
//   - claude: Agent SDK sessions + cost-tracking docs. Transcripts live at
//     <config>/projects/<cwd with every non-alphanumeric as "-">/<session>.jsonl;
//     assistant entries carry message.id, message.model and message.usage
//     {input_tokens, output_tokens, cache_read_input_tokens,
//     cache_creation_input_tokens}; one API response is written once per
//     content block, all with the same message.id, so ids are counted once.
//   - codex: codex-rs/protocol TokenUsage {input_tokens, cached_input_tokens,
//     output_tokens, reasoning_output_tokens, total_tokens}. A rollout line is
//     {type:"event_msg", payload:{type:"token_count", info:{total_token_usage}}};
//     total_token_usage is cumulative, so the last one is the answer.
//     input_tokens includes cached_input_tokens; both are recorded as reported.
//   - omp: oh-my-pi session docs. Usage is message.usage {input, output,
//     cacheRead, cacheWrite, totalTokens} on type:"message" entries whose
//     message.role is "assistant"; there is no reasoning field.
//   - devin, agy, cmd and every other kind: no documented usage record.

const (
	// maxUsageFileBytes bounds what one read takes from a record. A larger
	// file is not truncated: its usage is unknown.
	maxUsageFileBytes = 8 << 20
	// maxUsageLineBytes bounds one JSONL line; a longer line is skipped.
	maxUsageLineBytes = 2 << 20
	// maxUsageDirEntries bounds the codex sessions walk.
	maxUsageDirEntries = 100000
	// maxTokenCount rejects values that are not plausible counts.
	maxTokenCount = int64(1) << 50

	usageSourceClaude = "claude-session-jsonl"
	usageSourceCodex  = "codex-token-count"
	usageSourceOMP    = "omp"
)

var (
	usageSessionIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,127}$`)
	usageModelRE     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,127}$`)
	usageKindRE      = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)
)

// TokenUsage is one job's usage. A nil count is unknown, never zero.
type TokenUsage struct {
	Input, Output, Reasoning, CacheRead, CacheWrite *int64
	Source                                          string
	Model                                           string
}

// HasFigures reports whether any count is known.
func (u TokenUsage) HasFigures() bool {
	return u.Input != nil || u.Output != nil || u.Reasoning != nil || u.CacheRead != nil || u.CacheWrite != nil
}

// Wire is the hk `usage` object: unknown counts are absent, source always set.
func (u TokenUsage) Wire() map[string]any {
	out := map[string]any{"source": u.Source}
	for key, v := range map[string]*int64{
		"input_tokens": u.Input, "output_tokens": u.Output, "reasoning_tokens": u.Reasoning,
		"cache_read_tokens": u.CacheRead, "cache_write_tokens": u.CacheWrite,
	} {
		if v != nil {
			out[key] = *v
		}
	}
	return out
}

// UsageReader reads session records below Root, the home directory of the
// user the harness ran as. Tests inject a temporary Root.
type UsageReader struct{ Root string }

// Read returns the usage of the session ref names. Everything it cannot prove
// (unknown id, missing, unreadable, ambiguous or oversize record) is unknown;
// the source is still set so "tried, unknown" differs from "never written".
func (r UsageReader) Read(ref HarnessSession, cwd string) TokenUsage {
	kind := ref.Kind
	if !usageKindRE.MatchString(kind) {
		return TokenUsage{}
	}
	unknown := TokenUsage{Source: kind}
	switch kind {
	case "claude":
		unknown.Source = usageSourceClaude
	case "codex":
		unknown.Source = usageSourceCodex
	case "omp":
		unknown.Source = usageSourceOMP
	}
	switch kind {
	case "claude":
		if path, ok := r.claudePath(ref, cwd); ok {
			return readClaudeUsage(path)
		}
	case "codex":
		if path, ok := r.codexPath(ref); ok {
			return readCodexUsage(path)
		}
	case "omp":
		if path, ok := r.ompPath(ref); ok {
			return readOMPUsage(path)
		}
	}
	return unknown
}

// usableSessionID accepts an id that may name a file. "unknown" is
// FindHarnessSession's sentinel for no reference, not an id.
func usableSessionID(id string) bool {
	return id != "unknown" && usageSessionIDRE.MatchString(id)
}

func (r UsageReader) claudePath(ref HarnessSession, cwd string) (string, bool) {
	if r.Root == "" || !usableSessionID(ref.ID) || !filepath.IsAbs(cwd) {
		return "", false
	}
	var dir strings.Builder
	for _, c := range cwd {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
			dir.WriteRune(c)
		} else {
			dir.WriteByte('-')
		}
	}
	// Longer names are truncated and hashed by the harness; not reproduced.
	if dir.Len() > 200 {
		return "", false
	}
	return filepath.Join(r.Root, ".claude", "projects", dir.String(), ref.ID+".jsonl"), true
}

// codexPath finds the one rollout file named for the session id under
// sessions/YYYY/MM/DD. Two matches are ambiguous and match nothing.
func (r UsageReader) codexPath(ref HarnessSession) (string, bool) {
	if r.Root == "" || !usableSessionID(ref.ID) {
		return "", false
	}
	suffix := "-" + ref.ID + ".jsonl"
	budget := maxUsageDirEntries
	var found []string
	var walk func(dir string, depth int)
	walk = func(dir string, depth int) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return
		}
		for _, entry := range entries {
			if budget--; budget < 0 {
				return
			}
			switch {
			case depth < 3 && entry.IsDir():
				walk(filepath.Join(dir, entry.Name()), depth+1)
			case depth == 3 && entry.Type().IsRegular() && strings.HasPrefix(entry.Name(), "rollout-") && strings.HasSuffix(entry.Name(), suffix):
				found = append(found, filepath.Join(dir, entry.Name()))
			}
		}
	}
	walk(filepath.Join(r.Root, ".codex", "sessions"), 0)
	if len(found) != 1 || budget < 0 {
		return "", false
	}
	return found[0], true
}

// ompPath accepts the reference only when it is the session file itself,
// inside Root.
func (r UsageReader) ompPath(ref HarnessSession) (string, bool) {
	path := ref.ID
	if r.Root == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || !strings.HasSuffix(path, ".jsonl") {
		return "", false
	}
	rel, err := filepath.Rel(r.Root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	return path, true
}

// count decodes a JSON number as a token count. null, a non-integer, a
// negative or an implausible value is unknown; decoding never fails.
type count struct {
	v  int64
	ok bool
}

func (c *count) UnmarshalJSON(b []byte) error {
	var n int64
	if json.Unmarshal(b, &n) == nil && n >= 0 && n <= maxTokenCount {
		c.v, c.ok = n, true
	}
	return nil
}

// total accumulates one field; it stays unknown until a value is added.
type total struct {
	v    int64
	seen bool
}

func (t *total) add(c count) {
	if c.ok {
		t.v += c.v
		t.seen = true
	}
}
func (t total) ptr() *int64 {
	if !t.seen {
		return nil
	}
	v := t.v
	return &v
}

func (c count) ptr() *int64 {
	if !c.ok {
		return nil
	}
	v := c.v
	return &v
}

func usageModel(s string) string {
	if usageModelRE.MatchString(s) {
		return s
	}
	return ""
}

// scanUsageLines streams the file line by line. It reports false, with no
// partial result, when the file is not a plain regular file, is larger than
// maxUsageFileBytes, or cannot be read. Lines over maxUsageLineBytes are
// skipped whole.
func scanUsageLines(path string, each func(line []byte)) bool {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxUsageFileBytes {
		return false
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	if opened, err := f.Stat(); err != nil || !os.SameFile(info, opened) {
		return false
	}
	reader := bufio.NewReaderSize(io.LimitReader(f, maxUsageFileBytes+1), 64<<10)
	var line []byte
	var read int64
	skipping := false
	for {
		chunk, isPrefix, err := reader.ReadLine()
		read += int64(len(chunk)) + 1
		if read > maxUsageFileBytes {
			return false
		}
		if err != nil {
			if err == io.EOF {
				return true
			}
			return false
		}
		if !skipping {
			if len(line)+len(chunk) > maxUsageLineBytes {
				skipping, line = true, line[:0]
			} else {
				line = append(line, chunk...)
			}
		}
		if isPrefix {
			continue
		}
		if !skipping {
			each(bytes.TrimSpace(line))
		}
		line, skipping = line[:0], false
	}
}

type claudeUsage struct {
	Input, Output, CacheRead, CacheWrite count
}

func readClaudeUsage(path string) TokenUsage {
	out := TokenUsage{Source: usageSourceClaude}
	// A message id seen again replaces its earlier entry, so each API
	// response is counted once, at its latest written values.
	byID := map[string]claudeUsage{}
	model := ""
	ok := scanUsageLines(path, func(line []byte) {
		var entry struct {
			Type    string          `json:"type"`
			Message json.RawMessage `json:"message"`
		}
		if json.Unmarshal(line, &entry) != nil || entry.Type != "assistant" {
			return
		}
		var message struct {
			ID    string `json:"id"`
			Model string `json:"model"`
			Usage *struct {
				Input      count `json:"input_tokens"`
				Output     count `json:"output_tokens"`
				CacheRead  count `json:"cache_read_input_tokens"`
				CacheWrite count `json:"cache_creation_input_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal(entry.Message, &message) != nil || message.ID == "" || message.Usage == nil {
			return
		}
		byID[message.ID] = claudeUsage{message.Usage.Input, message.Usage.Output, message.Usage.CacheRead, message.Usage.CacheWrite}
		if m := usageModel(message.Model); m != "" {
			model = m
		}
	})
	if !ok || len(byID) == 0 {
		return out
	}
	var in, outTok, read, write total
	for _, u := range byID {
		in.add(u.Input)
		outTok.add(u.Output)
		read.add(u.CacheRead)
		write.add(u.CacheWrite)
	}
	out.Input, out.Output, out.CacheRead, out.CacheWrite, out.Model = in.ptr(), outTok.ptr(), read.ptr(), write.ptr(), model
	return out
}

func readCodexUsage(path string) TokenUsage {
	out := TokenUsage{Source: usageSourceCodex}
	var last *struct{ Input, Cached, Output, Reasoning count }
	model := ""
	ok := scanUsageLines(path, func(line []byte) {
		var entry struct {
			Type    string          `json:"type"`
			Payload json.RawMessage `json:"payload"`
		}
		if json.Unmarshal(line, &entry) != nil {
			return
		}
		switch entry.Type {
		case "turn_context":
			var p struct {
				Model string `json:"model"`
			}
			if json.Unmarshal(entry.Payload, &p) == nil {
				if m := usageModel(p.Model); m != "" {
					model = m
				}
			}
		case "event_msg":
			var p struct {
				Type string `json:"type"`
				Info *struct {
					Total *struct {
						Input     count `json:"input_tokens"`
						Cached    count `json:"cached_input_tokens"`
						Output    count `json:"output_tokens"`
						Reasoning count `json:"reasoning_output_tokens"`
					} `json:"total_token_usage"`
				} `json:"info"`
			}
			if json.Unmarshal(entry.Payload, &p) != nil || p.Type != "token_count" || p.Info == nil || p.Info.Total == nil {
				return
			}
			t := p.Info.Total
			last = &struct{ Input, Cached, Output, Reasoning count }{t.Input, t.Cached, t.Output, t.Reasoning}
		}
	})
	if !ok || last == nil {
		return out
	}
	out.Input, out.CacheRead, out.Output, out.Reasoning, out.Model = last.Input.ptr(), last.Cached.ptr(), last.Output.ptr(), last.Reasoning.ptr(), model
	return out
}

func readOMPUsage(path string) TokenUsage {
	out := TokenUsage{Source: usageSourceOMP}
	type ompUsage struct{ Input, Output, CacheRead, CacheWrite count }
	byID := map[string]ompUsage{}
	var anonymous []ompUsage
	model := ""
	ok := scanUsageLines(path, func(line []byte) {
		var entry struct {
			Type    string `json:"type"`
			ID      string `json:"id"`
			Message struct {
				Role  string `json:"role"`
				Model string `json:"model"`
				Usage *struct {
					Input      count `json:"input"`
					Output     count `json:"output"`
					CacheRead  count `json:"cacheRead"`
					CacheWrite count `json:"cacheWrite"`
				} `json:"usage"`
			} `json:"message"`
		}
		if json.Unmarshal(line, &entry) != nil || entry.Type != "message" || entry.Message.Role != "assistant" || entry.Message.Usage == nil {
			return
		}
		u := ompUsage{entry.Message.Usage.Input, entry.Message.Usage.Output, entry.Message.Usage.CacheRead, entry.Message.Usage.CacheWrite}
		if entry.ID != "" {
			byID[entry.ID] = u
		} else {
			anonymous = append(anonymous, u)
		}
		if m := usageModel(entry.Message.Model); m != "" {
			model = m
		}
	})
	if !ok || len(byID)+len(anonymous) == 0 {
		return out
	}
	var in, outTok, read, write total
	add := func(u ompUsage) {
		in.add(u.Input)
		outTok.add(u.Output)
		read.add(u.CacheRead)
		write.add(u.CacheWrite)
	}
	for _, u := range byID {
		add(u)
	}
	for _, u := range anonymous {
		add(u)
	}
	out.Input, out.Output, out.CacheRead, out.CacheWrite, out.Model = in.ptr(), outTok.ptr(), read.ptr(), write.ptr(), model
	return out
}
