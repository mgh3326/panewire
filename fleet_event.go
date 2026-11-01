package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Fleet events (fleet-event v1) make node-side failures visible after the
// fact: one JSON object per line in <state dir>/events/fleet-events.jsonl,
// which Grafana Alloy tails and ships to Loki. runtug and mergewake write the
// same shape; the contract is in deploy/alloy/README.md ("Fleet events").
// Writing an event never changes the caller's result.

type fleetEvent struct {
	TS      string `json:"ts"`
	Host    string `json:"host"`
	Tool    string `json:"tool"`
	Kind    string `json:"kind"`
	Job     string `json:"job"`
	Outcome string `json:"outcome"`
	Reason  string `json:"reason"`
	Detail  string `json:"detail"`
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
// the layout runtug and mergewake use too, so one Alloy glob covers all three
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
func emitFleetEvent(now time.Time, kind, job, outcome, reason, detail string) bool {
	return appendFleetEvent(fleetEventsPath(), fleetEventMaxBytes, fleetEvent{
		TS:      now.UTC().Format(time.RFC3339Nano),
		Host:    fleetEventHost(),
		Tool:    "panewire",
		Kind:    kind,
		Job:     job,
		Outcome: outcome,
		Reason:  reason,
		Detail:  detail,
	})
}

func appendFleetEvent(path string, maxBytes int64, ev fleetEvent) bool {
	if path == "" {
		return false
	}
	ev.Host, ev.Tool = fleetEventCode(ev.Host), fleetEventCode(ev.Tool)
	ev.Kind, ev.Outcome, ev.Reason = fleetEventCode(ev.Kind), fleetEventCode(ev.Outcome), fleetEventCode(ev.Reason)
	ev.Job = utf8PrefixWithin(fleetEventRedact(ev.Job), fleetEventMaxJob)
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
	if os.MkdirAll(filepath.Dir(path), 0o750) != nil {
		return false
	}
	// Rotation is best effort across processes: racing writers may drop one
	// rotated generation, never a line of the live file.
	if info, err := os.Stat(path); err == nil && info.Size()+int64(len(line)) > maxBytes {
		_ = os.Rename(path, path+".1")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o640)
	if err != nil {
		return false
	}
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
// blocks (or a truncated tail of one), and email addresses. runtug and
// mergewake carry the same set.
var fleetEventExtraSecrets = []*regexp.Regexp{
	regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?(-----END [A-Z ]*PRIVATE KEY-----|$)`),
	regexp.MustCompile(`(?i)basic\s+[A-Za-z0-9+/=]{8,}`),
	regexp.MustCompile(`\bgl(c|sa)_[A-Za-z0-9_=+/-]{16,}`),
	regexp.MustCompile(`\bASIA[0-9A-Z]{16}\b`),
	regexp.MustCompile(`(?i)[a-z0-9_.-]*(api[_-]?key|access[_-]?key|private[_-]?key|token|secret|password|passwd|credential|dsn)[a-z0-9_.-]*["']?\s*[:=]\s*["']?[^\s"',;&]{6,}`),
	regexp.MustCompile(`(?i)\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b`),
}

var fleetEventUserinfo = regexp.MustCompile(`(?i)\b([a-z][a-z0-9+.-]*://)[^/\s:@]+(:[^/\s@]*)?@`)

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
	// The PEM block goes first: the stall set only removes its header line.
	text = fleetEventExtraSecrets[0].ReplaceAllString(text, "[redacted]")
	text = redactSecrets(text)
	for _, pattern := range fleetEventExtraSecrets[1:] {
		text = pattern.ReplaceAllString(text, "[redacted]")
	}
	return fleetEventLongBlob.ReplaceAllStringFunc(text, func(match string) string {
		if fleetEventHexOnly.MatchString(match) {
			return match
		}
		return "[redacted]"
	})
}
