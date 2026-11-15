package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Fleet events (fleet-event v1) make node-side failures visible after the
// fact: one JSON object per line in <state dir>/events/fleet-events.jsonl,
// which Grafana Alloy tails and ships to Loki. panewire and mergewake write the
// same shape; the contract is in deploy/alloy/README.md ("Fleet events").
// Writing an event never changes the caller's result.

type fleetEvent struct {
	TS        string `json:"ts"`
	Host      string `json:"host"`
	Tool      string `json:"tool"`
	Kind      string `json:"kind"`
	Job       string `json:"job"`
	RequestID string `json:"request_id,omitempty"`
	Outcome   string `json:"outcome"`
	Reason    string `json:"reason"`
	Detail    string `json:"detail"`
	// Profile and DurationMS ride on a node_spawn line of a successful spawn
	// (the launch profile and how long the node took); omitted elsewhere.
	Profile    string `json:"profile,omitempty"`
	DurationMS *int64 `json:"duration_ms,omitempty"`
}

const (
	fleetEventMaxDetail = 1024
	fleetEventMaxCode   = 64
	fleetEventMaxJob    = 128
	fleetEventMaxBytes  = 4 << 20
	fleetEventFileName  = "fleet-events.jsonl"
)

// fleetEventMu serialises this process's appends and rotation; O_APPEND keeps
// lines whole across processes.
var fleetEventMu sync.Mutex

// panewireStateDir is $XDG_STATE_HOME/panewire, or ~/.local/state/panewire:
// the layout the fleet uses, so one Alloy glob covers both
// tools' fleet events. It holds events/ and crash/.
func panewireStateDir() string {
	state := os.Getenv("XDG_STATE_HOME")
	if !filepath.IsAbs(state) {
		home, err := os.UserHomeDir()
		if err != nil || home == "" {
			return ""
		}
		state = filepath.Join(home, ".local", "state")
	}
	return filepath.Join(state, "panewire")
}

// fleetEventsPath is <state dir>/events/fleet-events.jsonl.
func fleetEventsPath() string {
	dir := panewireStateDir()
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "events", fleetEventFileName)
}

// emitFleetEvent appends one panewire event and reports whether it was written.
func emitFleetEvent(now time.Time, ev fleetEvent) bool {
	ev.TS, ev.Host, ev.Tool = now.UTC().Format(time.RFC3339Nano), fleetEventHost(), "panewire"
	return appendFleetEvent(fleetEventsPath(), fleetEventMaxBytes, ev)
}

func appendFleetEvent(path string, maxBytes int64, ev fleetEvent) bool {
	if path == "" {
		return false
	}
	ev.Host, ev.Tool = fleetEventCode(ev.Host), fleetEventCode(ev.Tool)
	ev.Kind, ev.Outcome, ev.Reason = fleetEventCode(ev.Kind), fleetEventCode(ev.Outcome), fleetEventCode(ev.Reason)
	ev.Profile = fleetEventCode(ev.Profile)
	ev.Job = utf8PrefixWithin(fleetEventRedact(ev.Job), fleetEventMaxJob)
	ev.RequestID = utf8PrefixWithin(fleetEventRedact(ev.RequestID), fleetEventMaxJob)
	ev.Detail = utf8PrefixWithin(fleetEventRedact(ev.Detail), fleetEventMaxDetail)
	line, err := json.Marshal(ev)
	if err != nil {
		return false
	}
	line = append(line, '\n')
	fleetEventMu.Lock()
	defer fleetEventMu.Unlock()
	// Group-readable (0750 dir, 0640 file): on Linux the alloy system user
	// tails these through a group or ACL grant. Every field is redacted.
	dir := filepath.Dir(path)
	if os.MkdirAll(dir, 0o750) != nil {
		return false
	}
	// The modes are set explicitly: a restrictive umask (0077) would
	// otherwise drop the group bits the alloy grant relies on.
	_ = os.Chmod(dir, 0o750)
	// Rotation is best effort across processes: racing writers may drop one
	// rotated generation, never a line of the live file.
	if info, err := os.Stat(path); err == nil && info.Size()+int64(len(line)) > maxBytes {
		_ = os.Rename(path, path+".1")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o640)
	if err != nil {
		return false
	}
	_ = file.Chmod(0o640)
	_, err = file.Write(line)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err == nil
}

func fleetEventHost() string {
	host, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	host, _, _ = strings.Cut(host, ".")
	return host
}

var fleetEventCodeRune = regexp.MustCompile(`[^a-z0-9_.:-]+`)

// fleetEventCode normalises a low-cardinality field (it may become a Loki
// label): lowercase, [a-z0-9_.:-] only, at most 64 bytes.
func fleetEventCode(value string) string {
	value = fleetEventCodeRune.ReplaceAllString(strings.ToLower(strings.TrimSpace(value)), "_")
	return utf8PrefixWithin(strings.Trim(value, "_"), fleetEventMaxCode)
}

// fleetEventExtraSecrets extends the stall redaction set for fleet events and
// crash reports: Basic credentials, Grafana and AWS session tokens, any
// NAME=value whose name holds key/token/secret/password, whole PEM private-key
// blocks (or a truncated tail of one), and email addresses. panewire and
// mergewake carry the same set.
var fleetEventExtraSecrets = []*regexp.Regexp{
	regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?(-----END [A-Z ]*PRIVATE KEY-----|$)`),
	regexp.MustCompile(`(?i)basic\s+[A-Za-z0-9+/=]{8,}`),
	regexp.MustCompile(`\bgl(c|sa)_[A-Za-z0-9_=+/-]{16,}`),
	regexp.MustCompile(`\bASIA[0-9A-Z]{16}\b`),
	// A NAME=value whose name holds a credential word: a double- or single-
	// quoted value through its closing quote (escapes and spaces included,
	// or to the end when the tail cut the quote off), else the bare word.
	regexp.MustCompile(`(?i)[a-z0-9_.-]*(api[_-]?key|access[_-]?key|private[_-]?key|token|secret|password|passwd|credential|dsn)[a-z0-9_.-]*["']?\s*[:=]\s*("(?:[^"\\]|\\.)*"?|'(?:[^'\\]|\\.)*'?|[^\s"',;&]+)`),
	regexp.MustCompile(`(?i)\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b`),
}

var fleetEventUserinfo = regexp.MustCompile(`(?i)\b([a-z][a-z0-9+.-]*://)[^/\s:@]+(:[^/\s@]*)?@`)

// fleetEventHKGuardPattern is hk's secret_like_content refusal token: hk
// names the guard pattern (a rule name such as private_key, never the matched
// text), and the spawn outcome reason must carry it, so redaction keeps
// exactly this form. hk's free-form reason ("code: reason", with a space) and
// any pattern outside the rule-name charset still meet the credential rules.
var fleetEventHKGuardPattern = regexp.MustCompile(`secret_like_content:[a-z0-9_]+`)

// fleetEventLongBlob matches a long base64-alphabet run, a key or token body
// no named pattern caught; pure hex runs (commit SHAs, digests) are kept.
var (
	fleetEventLongBlob = regexp.MustCompile(`[A-Za-z0-9+/_-]{48,}={0,2}`)
	fleetEventHexOnly  = regexp.MustCompile(`^[0-9a-fA-F]+$`)
)

// fleetEventRedact strips credential-shaped substrings and email addresses
// from free text.
func fleetEventRedact(text string) string {
	text = strings.ToValidUTF8(text, "?")
	text = fleetEventUserinfo.ReplaceAllString(text, "${1}[redacted]@")
	var hkGuard []string
	text = fleetEventHKGuardPattern.ReplaceAllStringFunc(text, func(match string) string {
		if len(match) > len("secret_like_content:")+40 {
			return match
		}
		hkGuard = append(hkGuard, match)
		return "\x00hkguard" + strconv.Itoa(len(hkGuard)-1) + "\x00"
	})
	// The extra set goes first: the stall set only removes a PEM header and
	// stops a quoted assignment value at its first space, leaving the rest.
	for _, pattern := range fleetEventExtraSecrets {
		text = pattern.ReplaceAllString(text, "[redacted]")
	}
	text = redactSecrets(text)
	text = fleetEventLongBlob.ReplaceAllStringFunc(text, func(match string) string {
		if fleetEventHexOnly.MatchString(match) {
			return match
		}
		return "[redacted]"
	})
	for i, match := range hkGuard {
		text = strings.ReplaceAll(text, "\x00hkguard"+strconv.Itoa(i)+"\x00", match)
	}
	return text
}
