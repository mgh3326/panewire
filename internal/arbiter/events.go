package arbiter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
)

// Event is one recorded job event, as the events table holds it.
type Event struct {
	Job       string
	Seq       int
	Kind      string
	Payload   map[string]any
	CreatedAt string
}

var unsafeKind = regexp.MustCompile(`[^A-Za-z0-9._-]`)

// safeKind is the file-name form of an event kind.
func safeKind(kind string) string { return unsafeKind.ReplaceAllString(kind, "_") }

// claimKinds are minted only by Claim. The job readers match these exact
// names, and the file readers match the sanitised claim/reclaim suffix.
var claimKinds = map[string]bool{"job.claim": true, "job.claimed": true, "job.reclaim": true}

func validJobID(job string) error {
	if job == "" || strings.ContainsAny(job, "/\x00") || strings.HasPrefix(job, ".") {
		return errf(ExitUsage, "invalid job id %q", job)
	}
	return nil
}

func (s *Store) eventsDir(job string) string { return filepath.Join(s.Inbox, job, "events") }

// AppendEvent records an event for a job without requiring a local claim: a
// job claimed on another host (a remote placement) has events here but no
// jobs row. Claim events are refused; only Claim mints them. seq, when
// non-nil, is an explicit number and a taken one is ExitDuplicate. The state
// database is created on first use.
func (s *Store) AppendEvent(ctx context.Context, job, kind string, payload map[string]any, seq *int) (int, error) {
	if e := validJobID(job); e != nil {
		return 0, e
	}
	if claimKinds[kind] || strings.HasSuffix(safeKind(kind), "claim") {
		return 0, errf(ExitUsage, "event kind %q is refused: claim events are recorded by Claim only", kind)
	}
	if payload == nil {
		payload = map[string]any{}
	}
	c, e := s.rw(ctx, true)
	if e != nil {
		return 0, e
	}
	defer c.Close()
	var n int
	e = c.write(func() error {
		var err error
		n, err = s.recordEvent(c, job, kind, payload, seq)
		return err
	})
	return n, e
}

// Events lists a job's recorded events in sequence order.
func (s *Store) Events(ctx context.Context, job string) ([]Event, error) {
	c, e := s.ro(ctx)
	if e != nil {
		return nil, e
	}
	defer c.Close()
	rows, e := c.rows("SELECT seq, kind, payload, created_at FROM events WHERE job_id = ? ORDER BY seq", job)
	if e != nil {
		return nil, sqlErr(e)
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		ev := Event{Job: job}
		var body string
		if e = rows.Scan(&ev.Seq, &ev.Kind, &body, &ev.CreatedAt); e != nil {
			return nil, sqlErr(e)
		}
		var doc struct {
			Payload map[string]any `json:"payload"`
		}
		if e = json.Unmarshal([]byte(body), &doc); e != nil {
			return nil, errf(ExitStorage, "event %s/%d is unreadable: %v", job, ev.Seq, e)
		}
		ev.Payload = doc.Payload
		out = append(out, ev)
	}
	return out, sqlErr(rows.Err())
}

// highestPrefix is the highest numeric file prefix in an events directory
// (0 when it is absent or unreadable). Other writers number files in the same
// directory, so the database sequence alone would let two writers pick the
// same number.
func highestPrefix(dir string) int {
	entries, e := os.ReadDir(dir)
	if e != nil {
		return 0
	}
	highest := 0
	for _, ent := range entries {
		head, _, _ := strings.Cut(ent.Name(), "-")
		if n, e := strconv.Atoi(head); e == nil && n > highest {
			highest = n
		}
	}
	return highest
}

// recordEvent writes the events row and the inbox artifact inside the
// caller's transaction, under the events lock. Numbering and submission are
// serialised by <job dir>/.wrk-events.lock; the UNIQUE(job_id, seq) index and
// the exclusive artifact link stop a double consumption.
func (s *Store) recordEvent(c *conn, job, kind string, payload map[string]any, seq *int) (int, error) {
	dir := s.eventsDir(job)
	if e := os.MkdirAll(dir, 0o755); e != nil {
		return 0, errf(ExitStorage, "cannot create the events directory: %v", e)
	}
	lock, e := os.OpenFile(filepath.Join(filepath.Dir(dir), ".wrk-events.lock"), os.O_WRONLY|os.O_CREATE, 0o644)
	if e != nil {
		return 0, errf(ExitStorage, "cannot open the events lock: %v", e)
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); e != nil {
		return 0, errf(ExitStorage, "cannot take the events lock: %v", e)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	var n int
	if seq != nil {
		n = *seq
	} else {
		var max int
		if e = c.row("SELECT COALESCE(MAX(seq), 0) FROM events WHERE job_id = ?", job).Scan(&max); e != nil {
			return 0, sqlErr(e)
		}
		n = max
		if h := highestPrefix(dir); h > n {
			n = h
		}
		n++
	}
	created := iso(s.now())
	body, e := encodeBody(map[string]any{"job_id": job, "seq": n, "kind": kind, "payload": payload, "created_at": created})
	if e != nil {
		return 0, errf(ExitUsage, "event payload is not encodable: %v", e)
	}
	if _, e = c.exec("INSERT INTO events (job_id, seq, kind, payload, created_at) VALUES (?, ?, ?, ?, ?)", job, n, kind, body, created); e != nil {
		if isUnique(e) {
			err := errf(ExitDuplicate, "event duplicate: job_id=%s seq=%d already exists", job, n)
			err.Detail = map[string]any{"job_id": job, "seq": n}
			return 0, err
		}
		return 0, sqlErr(e)
	}
	if e = writeArtifact(dir, n, kind, body); e != nil {
		return 0, e
	}
	return n, nil
}

// encodeBody is the artifact body: one JSON object with sorted keys and no
// HTML escaping.
func encodeBody(v any) (string, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if e := enc.Encode(v); e != nil {
		return "", e
	}
	return strings.TrimSuffix(b.String(), "\n"), nil
}

// writeArtifact submits <seq>-<kind>.json: written to a temporary file, then
// hard-linked into place so no reader sees a partial file and no existing
// file is overwritten.
func writeArtifact(dir string, seq int, kind, body string) error {
	final := filepath.Join(dir, pad5(seq)+"-"+safeKind(kind)+".json")
	tmp, e := os.CreateTemp(dir, ".tmp-*.json")
	if e != nil {
		return errf(ExitStorage, "cannot submit the event artifact: %v", e)
	}
	defer os.Remove(tmp.Name())
	_, e = tmp.WriteString(body + "\n")
	if e == nil {
		e = tmp.Sync()
	}
	if ce := tmp.Close(); e == nil {
		e = ce
	}
	if e != nil {
		return errf(ExitStorage, "cannot submit the event artifact: %v", e)
	}
	if e = os.Link(tmp.Name(), final); e != nil {
		if errors.Is(e, fs.ErrExist) {
			return errf(ExitDuplicate, "event artifact name already exists: %s", final)
		}
		// Hard links unavailable: create the final name exclusively. Never
		// rename over it — another writer's record must not be overwritten.
		f, oe := os.OpenFile(final, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if oe != nil {
			if errors.Is(oe, fs.ErrExist) {
				return errf(ExitDuplicate, "event artifact name already exists: %s", final)
			}
			return errf(ExitStorage, "cannot submit the event artifact: %v", oe)
		}
		_, oe = f.WriteString(body + "\n")
		if oe == nil {
			oe = f.Sync()
		}
		if ce := f.Close(); oe == nil {
			oe = ce
		}
		if oe != nil {
			return errf(ExitStorage, "cannot submit the event artifact: %v", oe)
		}
	}
	if d, e := os.Open(dir); e == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

func pad5(n int) string {
	s := strconv.Itoa(n)
	if len(s) < 5 {
		s = strings.Repeat("0", 5-len(s)) + s
	}
	return s
}

func isUnique(e error) bool { return strings.Contains(e.Error(), "UNIQUE constraint failed") }
