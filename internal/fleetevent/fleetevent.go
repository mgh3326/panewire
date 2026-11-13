// Package fleetevent writes fleet-event v1 records: one JSON object per line,
// appended to a local, size-bounded file that Grafana Alloy tails and ships to
// Loki. The same shape is written by panewire, panewire and mergewake; the
// contract lives in panewire's deploy/alloy/README.md ("Fleet events").
//
// Every string field is redacted and bounded before it is written, so a caller
// may pass an error message or a stderr tail without scrubbing it first. Kind,
// outcome and reason become (or feed) Loki labels and are normalised to short
// lowercase codes to keep label cardinality low.
package fleetevent

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/textbound"
)

// Event is the fleet-event v1 record.
type Event struct {
	TS      string `json:"ts"`
	Host    string `json:"host"`
	Tool    string `json:"tool"`
	Kind    string `json:"kind"`
	Job     string `json:"job"`
	Outcome string `json:"outcome"`
	Reason  string `json:"reason"`
	Detail  string `json:"detail"`
	// Placement lines optionally carry the request, chosen host, candidate
	// scores and host/rung selection trace. Other lines keep eight fields.
	RequestID  string          `json:"request_id,omitempty"`
	Chosen     string          `json:"chosen,omitempty"`
	Candidates []Candidate     `json:"candidates,omitempty"`
	Selection  []HostSelection `json:"selection,omitempty"`
	// Shadow is the placement_shadow comparison (docs/placement.md, "Shadow
	// comparison"). Other lines omit it.
	Shadow *PlacementShadow `json:"shadow,omitempty"`
}

// PlacementShadow records how alternative host rankings would have ordered the
// hosts of one job next to the order the live path used. Arms is a map so a
// later arm (hk 3651) adds a key without a schema change. It carries numbers
// and host ids only.
type PlacementShadow struct {
	// Role is the spawn role the work class was derived from.
	Role      string               `json:"role"`
	WorkClass string               `json:"work_class"`
	Live      ShadowOrder          `json:"live"`
	Arms      map[string]ShadowArm `json:"arms"`
	// LoadMeasure is the one load measure the fixed arm used for every host:
	// cpu_busy or load5_per_core.
	LoadMeasure string `json:"load_measure,omitempty"`
	// Truncated says an order or the inputs were cut to MaxCandidates hosts.
	Truncated bool `json:"truncated,omitempty"`
	// Inputs are the readings the arms used, one row per host, once per event.
	Inputs []ShadowInput `json:"inputs,omitempty"`
	// WasteFirst is the account arm (hk 3651). It is not a host order, so it
	// sits beside Arms and never changes the event outcome.
	WasteFirst *WasteFirst `json:"waste_first,omitempty"`
}

// WasteFirst records which account a pool-first order would have spent, next
// to the account the live host-first order spent. Outcome is agree (same
// account), differs (another account), no_choice (the arm found no admitted or
// admissible account) or shadow_error (Error says how). Pass 1 only: the credit
// band that admission tries last is not modelled (docs/placement.md).
type WasteFirst struct {
	Outcome string `json:"outcome"`
	// Live is the account the live path chose, nil when it chose none.
	Live *ShadowChoice `json:"live,omitempty"`
	// Choice is the account the waste-first order would have chosen.
	Choice *ShadowChoice `json:"waste_first,omitempty"`
	// LeftUnspent is, when the outcome is differs, the first waste-risk account
	// the waste-first order could spend that the live choice did not.
	LeftUnspent *ShadowAccount `json:"left_unspent,omitempty"`
	// Unknown lists accounts on a usable host whose quota state the live run
	// never observed. They are never chosen; at most MaxCandidates.
	Unknown []string `json:"unknown,omitempty"`
	Error   string   `json:"shadow_error,omitempty"`
}

// ShadowChoice is one account choice: where it runs and the quota figures it
// was ordered by. Pass is 1 for the ranked band and 2 for the credit band that
// the live path tries only when pass 1 chose nothing.
type ShadowChoice struct {
	Host             string   `json:"host"`
	Profile          string   `json:"profile"`
	Seat             string   `json:"seat,omitempty"`
	AccountKey       string   `json:"account_key"`
	Pool             string   `json:"pool"`
	Tier             string   `json:"tier,omitempty"`
	Pass             int      `json:"pass"`
	WasteRisk        bool     `json:"waste_risk"`
	UnusedAtResetPct *float64 `json:"unused_at_reset_pct,omitempty"`
	ResetHours       *float64 `json:"reset_hours,omitempty"`
}

// ShadowAccount names a waste-risk account that a choice left unspent, with the
// unused share its longest window would have lost at reset.
type ShadowAccount struct {
	Host             string   `json:"host"`
	Pool             string   `json:"pool"`
	AccountKey       string   `json:"account_key"`
	UnusedAtResetPct *float64 `json:"unused_at_reset_pct"`
	ResetHours       *float64 `json:"reset_hours,omitempty"`
}

// ShadowOrder is a host order, best first.
type ShadowOrder struct {
	First string           `json:"first"`
	Order []ShadowHostRank `json:"order"`
}

// ShadowHostRank is one host's place in an order: the live bucket (0 awake
// under the load threshold, 1 awake without a reading, 2 awake over it, 3
// asleep) and the score the order used, when it had one.
type ShadowHostRank struct {
	Host   string   `json:"host"`
	Bucket int      `json:"bucket"`
	Score  *float64 `json:"score,omitempty"`
}

// ShadowArm is one arm's order and how it compares with the live one. Outcome
// is agree (same order), order_differs (same first host, other order),
// disagree (other first host) or shadow_error (the arm failed; Error says
// how, and the live path was not touched).
type ShadowArm struct {
	Outcome string           `json:"outcome"`
	First   string           `json:"first"`
	Order   []ShadowHostRank `json:"order"`
	Error   string           `json:"shadow_error,omitempty"`
}

// ShadowInput is what the arms read for one host. A nil value is a reading the
// hub did not send.
type ShadowInput struct {
	Host       string   `json:"host"`
	Local      bool     `json:"local,omitempty"`
	CPUBusy    *float64 `json:"cpu_busy,omitempty"`
	Load5Core  *float64 `json:"load5_per_core,omitempty"`
	Load1Core  *float64 `json:"load1_per_core,omitempty"`
	MemAvail   *float64 `json:"mem_available,omitempty"`
	MemFreePct *float64 `json:"mem_free_pct,omitempty"`
	Pressure   *float64 `json:"pressure,omitempty"`
	SwapInPerS *float64 `json:"swap_in_per_s,omitempty"`
	Jobs       *int     `json:"jobs,omitempty"`
	Power      *float64 `json:"power,omitempty"`
	Capability *float64 `json:"capability,omitempty"`
	SignalAgeS *float64 `json:"signal_age_s,omitempty"`
	MemorySrc  string   `json:"memory_source,omitempty"`
}

// MaxShadowArms bounds the arms of one line.
const MaxShadowArms = 8

// HostSelection records admission in host rank order, before execution placement.
// It carries no account identities or gate output.
type HostSelection struct {
	Host    string        `json:"host"`
	Rank    int           `json:"rank"`
	Outcome string        `json:"outcome"`
	Tried   []RungAttempt `json:"tried,omitempty"`
}

type RungAttempt struct {
	Profile string `json:"profile"`
	Effort  string `json:"effort"`
	Pool    string `json:"pool"`
	Outcome string `json:"outcome"`
}

// Candidate is one host's part in a placement decision: the five terms of its
// free fraction (each 0..1) and their product free (the value the free floor
// compares), its power and the claims inside the spawn bridge,
// the resulting score, and why it was not chosen when it was not. A term is
// nil when the hub sent none for that host.
type Candidate struct {
	Host    string   `json:"host"`
	Source  string   `json:"source,omitempty"`
	Score   *float64 `json:"score,omitempty"`
	CPU     *float64 `json:"cpu,omitempty"`
	Mem     *float64 `json:"mem,omitempty"`
	IO      *float64 `json:"io,omitempty"`
	Load    *float64 `json:"load,omitempty"`
	Jobs    *float64 `json:"jobs,omitempty"`
	Free    *float64 `json:"free,omitempty"`
	Power   *float64 `json:"power,omitempty"`
	Bridge  *int     `json:"bridge,omitempty"`
	Outcome string   `json:"outcome,omitempty"`
}

// MaxCandidates bounds the candidates of one line.
const MaxCandidates = 16

// MaxRungs bounds the admission attempts recorded for one host.
const MaxRungs = 32

const (
	// MaxDetail bounds the detail field in bytes, after redaction.
	MaxDetail = 1024
	// maxCode bounds kind, outcome and reason.
	maxCode = 64
	// maxJob bounds the job field.
	maxJob = 128
	// maxAccountKey bounds an account key, a fleet id (waste_first arm).
	maxAccountKey = 128
	// DefaultMaxBytes is the size at which the file rotates to <path>.1.
	DefaultMaxBytes = 4 << 20
	// FileName is the events file inside a tool's events directory.
	FileName = "fleet-events.jsonl"
)

// Writer appends events to Path. The zero Path disables it. Emit never returns
// an error to the caller's control flow: telemetry must not change behaviour.
type Writer struct {
	Path     string
	Tool     string
	Host     string
	MaxBytes int64
	Now      func() time.Time
	// Sink, when set, receives each normalised event instead of the file. Tests
	// use it as a fake writer; production leaves it nil.
	Sink func(Event)

	mu sync.Mutex
}

// New returns a writer for <stateDir>/events/fleet-events.jsonl.
func New(stateDir, tool string, now func() time.Time) *Writer {
	path := ""
	if stateDir != "" {
		path = filepath.Join(stateDir, "events", FileName)
	}
	return &Writer{Path: path, Tool: tool, Host: ShortHost(), MaxBytes: DefaultMaxBytes, Now: now}
}

// ShortHost is the machine's hostname up to the first dot, lowercased.
func ShortHost() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	h, _, _ = strings.Cut(h, ".")
	return Code(h)
}

// Emit appends one event. It reports whether the line was written.
func (w *Writer) Emit(kind, job, outcome, reason, detail string) bool {
	return w.EmitEvent(Event{Kind: kind, Job: job, Outcome: outcome, Reason: reason, Detail: detail})
}

// EmitEvent appends ev, stamping the time, host and tool and normalising every
// field. It reports whether the line was written.
func (w *Writer) EmitEvent(ev Event) bool {
	if w == nil || (w.Path == "" && w.Sink == nil) {
		return false
	}
	now := time.Now
	if w.Now != nil {
		now = w.Now
	}
	ev.TS, ev.Host, ev.Tool = now().UTC().Format(time.RFC3339Nano), Code(w.Host), Code(w.Tool)
	ev.Kind, ev.Outcome, ev.Reason = Code(ev.Kind), Code(ev.Outcome), Code(ev.Reason)
	ev.Job, ev.Detail = Bound(Redact(ev.Job), maxJob), Bound(Redact(ev.Detail), MaxDetail)
	ev.RequestID, ev.Chosen = Bound(Redact(ev.RequestID), maxJob), Code(ev.Chosen)
	if len(ev.Candidates) > MaxCandidates {
		ev.Candidates = ev.Candidates[:MaxCandidates]
	}
	ev.Candidates = append([]Candidate(nil), ev.Candidates...)
	for i := range ev.Candidates {
		ev.Candidates[i].Host, ev.Candidates[i].Outcome = Code(ev.Candidates[i].Host), Code(ev.Candidates[i].Outcome)
		ev.Candidates[i].Source = Code(ev.Candidates[i].Source)
	}
	if len(ev.Selection) > MaxCandidates {
		ev.Selection = ev.Selection[:MaxCandidates]
	}
	ev.Selection = append([]HostSelection(nil), ev.Selection...)
	for i := range ev.Selection {
		h := &ev.Selection[i]
		h.Host, h.Outcome = Code(Redact(h.Host)), Code(Redact(h.Outcome))
		if len(h.Tried) > MaxRungs {
			h.Tried = h.Tried[:MaxRungs]
		}
		h.Tried = append([]RungAttempt(nil), h.Tried...)
		for j := range h.Tried {
			r := &h.Tried[j]
			r.Profile, r.Effort = Bound(Redact(r.Profile), maxJob), Code(Redact(r.Effort))
			r.Pool, r.Outcome = Code(Redact(r.Pool)), Code(Redact(r.Outcome))
		}
	}
	ev.Shadow = ev.Shadow.bounded()
	if w.Sink != nil {
		w.Sink(ev)
		return true
	}
	line, err := json.Marshal(ev)
	if err != nil {
		return false
	}
	line = append(line, '\n')
	w.mu.Lock()
	defer w.mu.Unlock()
	// Group-readable (0750 dir, 0640 file): on Linux the alloy system user
	// tails these through a group or ACL grant. Every field is redacted.
	if os.MkdirAll(filepath.Dir(w.Path), 0o750) != nil {
		return false
	}
	w.rotate(int64(len(line)))
	f, err := os.OpenFile(w.Path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o640)
	if err != nil {
		return false
	}
	// One write per line: O_APPEND keeps concurrent writers' lines whole.
	_, err = f.Write(line)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err == nil
}

// rotate renames the file to <path>.1 when the next line would cross
// MaxBytes. It is best effort across processes: two writers racing past the
// limit may drop one rotated generation, never a line of the live file.
func (w *Writer) rotate(next int64) {
	limit := w.MaxBytes
	if limit <= 0 {
		limit = DefaultMaxBytes
	}
	info, err := os.Stat(w.Path)
	if err != nil || info.Size()+next <= limit {
		return
	}
	_ = os.Rename(w.Path, w.Path+".1")
}

var codeRune = regexp.MustCompile(`[^a-z0-9_.:-]+`)

// Code normalises a low-cardinality field: lowercase, [a-z0-9_.:-] only,
// at most 64 bytes.
func Code(s string) string {
	s = codeRune.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), "_")
	return Bound(strings.Trim(s, "_"), maxCode)
}

// Bound returns the longest valid UTF-8 prefix of s within limit bytes,
// after replacing invalid bytes with "?".
func Bound(s string, limit int) string {
	return textbound.PrefixWithin(strings.ToValidUTF8(s, "?"), limit)
}

// Tail returns the last limit bytes of s on a UTF-8 boundary, trimmed.
func Tail(s string, limit int) string {
	return textbound.SuffixWithin(strings.TrimSpace(strings.ToValidUTF8(s, "?")), limit)
}

// secretPatterns matches credential-shaped substrings. It mirrors panewire's
// fleet-event redaction set: URL userinfo, bearer/basic credentials, provider
// token shapes, any NAME=value whose name holds key/token/secret/password,
// whole PEM private-key blocks (or a truncated tail of one), and email
// addresses.
var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\b([a-z][a-z0-9+.-]*://)[^/\s:@]+(:[^/\s@]*)?@`),
	regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?(-----END [A-Z ]*PRIVATE KEY-----|$)`),
	regexp.MustCompile(`(?i)bearer\s+[A-Za-z0-9._~+/=-]{8,}`),
	regexp.MustCompile(`(?i)basic\s+[A-Za-z0-9+/=]{8,}`),
	regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{8,}`),
	regexp.MustCompile(`\b(ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{16,}`),
	regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{20,}`),
	regexp.MustCompile(`\bgl(c|sa)_[A-Za-z0-9_=+/-]{16,}`),
	regexp.MustCompile(`\bxox[baprs]-[A-Za-z0-9-]{8,}`),
	regexp.MustCompile(`\b(AKIA|ASIA)[0-9A-Z]{16}\b`),
	regexp.MustCompile(`eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{5,}`),
	// A NAME=value whose name holds a credential word: a double- or single-
	// quoted value through its closing quote (escapes and spaces included,
	// or to the end when the tail cut the quote off), else the bare word.
	regexp.MustCompile(`(?i)[a-z0-9_.-]*(api[_-]?key|access[_-]?key|private[_-]?key|token|secret|password|passwd|credential|dsn)[a-z0-9_.-]*["']?\s*[:=]\s*("(?:[^"\\]|\\.)*"?|'(?:[^'\\]|\\.)*'?|[^\s"',;&]+)`),
	regexp.MustCompile(`(?i)\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b`),
}

// longBlob matches a long base64-alphabet run: a key or token body that no
// named pattern caught. Pure hex runs (commit SHAs, digests) are kept.
var longBlob = regexp.MustCompile(`[A-Za-z0-9+/_-]{48,}={0,2}`)

var hexOnly = regexp.MustCompile(`^[0-9a-fA-F]+$`)

// Redact replaces credential-shaped substrings and email addresses with
// [redacted]. URL userinfo keeps its scheme so the line still reads as a URL.
func Redact(s string) string {
	for i, p := range secretPatterns {
		if i == 0 {
			s = p.ReplaceAllString(s, "${1}[redacted]@")
			continue
		}
		s = p.ReplaceAllString(s, "[redacted]")
	}
	return longBlob.ReplaceAllStringFunc(s, func(m string) string {
		if hexOnly.MatchString(m) {
			return m
		}
		return "[redacted]"
	})
}

// bounded returns a normalised copy: at most MaxCandidates hosts per order and
// MaxShadowArms arms, codes on every identifier, no free text.
func (p *PlacementShadow) bounded() *PlacementShadow {
	if p == nil {
		return nil
	}
	out := &PlacementShadow{Role: Code(p.Role), WorkClass: Code(p.WorkClass), Live: p.Live.bounded(), Arms: map[string]ShadowArm{}, LoadMeasure: Code(p.LoadMeasure), Truncated: p.Truncated}
	out.WasteFirst = p.WasteFirst.bounded()
	out.Truncated = out.Truncated || len(p.Live.Order) > MaxCandidates || len(p.Inputs) > MaxCandidates || len(p.Arms) > MaxShadowArms
	names := make([]string, 0, len(p.Arms))
	for name := range p.Arms {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if len(out.Arms) == MaxShadowArms {
			break
		}
		out.Arms[Code(name)] = p.Arms[name].bounded()
		out.Truncated = out.Truncated || len(p.Arms[name].Order) > MaxCandidates
	}
	for i, in := range p.Inputs {
		if i == MaxCandidates {
			break
		}
		in.Host, in.MemorySrc = Code(in.Host), Code(in.MemorySrc)
		out.Inputs = append(out.Inputs, in)
	}
	return out
}

// finiteOrNil drops a NaN or infinite quota figure: encoding/json cannot write
// it, and a bad figure must not cost the whole placement_shadow line.
func finiteOrNil(p *float64) *float64 {
	if p == nil || math.IsNaN(*p) || math.IsInf(*p, 0) {
		return nil
	}
	return p
}

// bounded keeps the waste_first arm to its own contract: labels through Code,
// account keys (fleet ids, which Code would rewrite) through Redact and Bound.
func (w *WasteFirst) bounded() *WasteFirst {
	if w == nil {
		return nil
	}
	out := &WasteFirst{Outcome: Code(w.Outcome), Live: w.Live.bounded(), Choice: w.Choice.bounded(), LeftUnspent: w.LeftUnspent.bounded(), Error: Bound(Redact(w.Error), MaxDetail)}
	for i, u := range w.Unknown {
		if i == MaxCandidates {
			break
		}
		out.Unknown = append(out.Unknown, Bound(Redact(u), maxAccountKey))
	}
	return out
}

func (a *ShadowAccount) bounded() *ShadowAccount {
	if a == nil {
		return nil
	}
	out := *a
	out.Host, out.Pool = Code(a.Host), Code(a.Pool)
	out.UnusedAtResetPct, out.ResetHours = finiteOrNil(a.UnusedAtResetPct), finiteOrNil(a.ResetHours)
	out.AccountKey = Bound(Redact(a.AccountKey), maxAccountKey)
	return &out
}

func (c *ShadowChoice) bounded() *ShadowChoice {
	if c == nil {
		return nil
	}
	out := *c
	out.Host, out.Profile, out.Seat, out.Pool, out.Tier = Code(c.Host), Code(c.Profile), Code(c.Seat), Code(c.Pool), Code(c.Tier)
	out.UnusedAtResetPct, out.ResetHours = finiteOrNil(c.UnusedAtResetPct), finiteOrNil(c.ResetHours)
	out.AccountKey = Bound(Redact(c.AccountKey), maxAccountKey)
	return &out
}

func (o ShadowOrder) bounded() ShadowOrder {
	out := ShadowOrder{First: Code(o.First), Order: []ShadowHostRank{}}
	for i, r := range o.Order {
		if i == MaxCandidates {
			break
		}
		r.Host = Code(r.Host)
		out.Order = append(out.Order, r)
	}
	return out
}

func (a ShadowArm) bounded() ShadowArm {
	o := ShadowOrder{First: a.First, Order: a.Order}.bounded()
	return ShadowArm{Outcome: Code(a.Outcome), First: o.First, Order: o.Order, Error: Bound(Redact(a.Error), maxCode)}
}
