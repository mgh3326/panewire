package quotastop

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// The devin CLI leaves a structured record of a session's turn outcomes in the
// agent process's own tracing log. The log file is devin_<start>_<pid>.log
// (rotated copies carry .gz) under <data>/devin/cli/logs; the process pid is
// the one devin writes into <data>/devin/cli/session_locks/<session>.lock, and
// every turn it runs ends either with an "Sending error response ...
// method=session/prompt" line or, when it succeeds, with none. A free-model
// limit exhausts the inference retries, ends the turn with Devin's own limit
// message in that error response, and logs no further prompt until someone
// sends one (hk 4542).
//
// Only that error response is evidence: the same limit text reaches the log
// from other paths (a subagent's failed tool call, an assistant quoting it),
// and none of those stop the session's own turn.

// DevinSessionLogLimit bounds how much of one process log is read. A longer
// log is read from its end, so the latest turn is still seen; evidence older
// than the window reads as unknown and the screen classifier decides alone.
const DevinSessionLogLimit = 16 << 20

// DevinSessionState is what the devin CLI's record says about a session.
type DevinSessionState int

const (
	// DevinSessionUnknown is no evidence: no log was found, the session was
	// not named in it, the turn outcome was outside the window read, or the
	// session's last turn failed for another reason.
	DevinSessionUnknown DevinSessionState = iota
	// DevinSessionRunning is a session whose last turn did not end in the
	// free-model limit, and which has been prompted since any earlier failed
	// turn: a free-limit reading of its screen is a quoted status.
	DevinSessionRunning
	// DevinSessionFreeLimit is a session whose last turn ended in the
	// free-model limit and has not been prompted since.
	DevinSessionFreeLimit
)

// DevinSessionEvidence is the devin CLI's record of a session. The zero value
// is DevinSessionUnknown.
type DevinSessionEvidence struct {
	State DevinSessionState
	At    time.Time // when the free-limit turn ended (FreeLimit only)
	Reset time.Time // the free limit's reset; zero when unreadable
}

var (
	devinLogStamp  = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$`)
	devinLogError  = regexp.MustCompile(` message: "([^"]*)"`)
	devinPromptRow = `acp_bridge_dispatch{method="session/prompt"`
	devinErrorRow  = "Sending error response"
)

// DevinCLIDir is the devin CLI's state directory on this host: the devin
// subtree of the XDG data home ($XDG_DATA_HOME, else home/.local/share).
// home may be empty when os.UserHomeDir already answered.
func DevinCLIDir(home string) string {
	data := strings.TrimSpace(os.Getenv("XDG_DATA_HOME"))
	if data == "" {
		if home == "" {
			home, _ = os.UserHomeDir()
		}
		data = filepath.Join(home, ".local", "share")
	}
	return filepath.Join(data, "devin", "cli")
}

// DevinSessionEvidence reads the devin CLI's structured record for session
// under cliDir (see DevinCLIDir). Any failure, an unknown session, or a log
// that does not name the session is the zero value, so no caller can mistake
// another session's log for this one.
func ReadDevinSessionEvidence(cliDir, session string, local *time.Location) DevinSessionEvidence {
	if local == nil {
		local = time.Local
	}
	if !validDevinSessionID(session) {
		return DevinSessionEvidence{}
	}
	path, ok := devinSessionLog(cliDir, session)
	if !ok {
		return DevinSessionEvidence{}
	}
	data, truncated, err := readLogTail(path, DevinSessionLogLimit)
	if err != nil {
		return DevinSessionEvidence{}
	}
	return parseDevinSessionLog(data, session, truncated, local)
}

// validDevinSessionID accepts devin's own session ids (short names such as
// "foul-poison") and nothing that could name another file.
func validDevinSessionID(session string) bool {
	if session == "" || len(session) > 128 || session == "." || session == ".." {
		return false
	}
	for _, r := range session {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
		default:
			return false
		}
	}
	return true
}

// devinSessionLog finds the process log of session: the pid from its lock
// file, then the log named for that pid and the process's start time.
func devinSessionLog(cliDir, session string) (string, bool) {
	lock := filepath.Join(cliDir, "session_locks", session+".lock")
	info, err := os.Stat(lock)
	if err != nil || info.IsDir() {
		return "", false
	}
	body, err := os.ReadFile(lock)
	if err != nil {
		return "", false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(body)))
	if err != nil || pid <= 0 {
		return "", false
	}
	logs := filepath.Join(cliDir, "logs")
	// devin names the log with the second the process started, which is the
	// lock file's creation time; a second either side covers the write order.
	start := info.ModTime().Local()
	for offset := -2; offset <= 2; offset++ {
		base := "devin_" + start.Add(time.Duration(offset)*time.Second).Format("20060102-150405") + "_" + strconv.Itoa(pid)
		for _, ext := range []string{".log", ".log.gz"} {
			path := filepath.Join(logs, base+ext)
			if st, err := os.Stat(path); err == nil && !st.IsDir() {
				return path, true
			}
		}
	}
	return devinLogByPID(logs, pid)
}

// devinLogByPID is the fallback log lookup: the newest log file whose name
// ends with the pid. It is only reached when the lock's time did not name an
// existing file, so the directory scan is off the common path.
func devinLogByPID(logs string, pid int) (string, bool) {
	entries, err := os.ReadDir(logs)
	if err != nil {
		return "", false
	}
	suffix := "_" + strconv.Itoa(pid) + ".log"
	best := ""
	bestName := ""
	for _, entry := range entries {
		name := strings.TrimSuffix(entry.Name(), ".gz")
		if entry.IsDir() || !strings.HasPrefix(name, "devin_") || !strings.HasSuffix(name, suffix) {
			continue
		}
		if name > bestName {
			best, bestName = entry.Name(), name
		}
	}
	return filepath.Join(logs, best), best != ""
}

// readLogTail reads at most limit bytes of path: its end when it is larger,
// and its whole body otherwise. truncated reports that the front was cut.
func readLogTail(path string, limit int64) (data []byte, truncated bool, err error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	var reader io.Reader = file
	if strings.HasSuffix(path, ".gz") {
		zipped, err := gzip.NewReader(file)
		if err != nil {
			return nil, false, err
		}
		defer zipped.Close()
		reader = zipped
	} else if info, err := file.Stat(); err == nil && info.Size() > limit {
		if _, err := file.Seek(info.Size()-limit, io.SeekStart); err != nil {
			return nil, false, err
		}
		truncated = true
	}
	var buf bytes.Buffer
	chunk := make([]byte, 64<<10)
	for {
		n, readErr := reader.Read(chunk)
		if n > 0 {
			buf.Write(chunk[:n])
			if int64(buf.Len()) > limit {
				keep := buf.Bytes()[int64(buf.Len())-limit:]
				buf.Reset()
				buf.Write(keep)
				truncated = true
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, truncated, readErr
		}
	}
	data = buf.Bytes()
	if truncated {
		// The window starts mid-row; that row cannot be read as a whole.
		if i := bytes.IndexByte(data, '\n'); i >= 0 {
			data = data[i+1:]
		}
	}
	return data, truncated, nil
}

// parseDevinSessionLog reports the session's last turn outcome from the lines
// of its process log. The pane is stopped only when the last turn ended in the
// free-model limit and no later prompt started: a prompt after it means the
// session is running again, and any other turn outcome is not this stop.
func parseDevinSessionLog(data []byte, session string, truncated bool, local *time.Location) DevinSessionEvidence {
	if !logNamesSession(data, session) {
		return DevinSessionEvidence{}
	}
	var (
		responseAt time.Time
		response   string
		promptAt   time.Time
	)
	for _, line := range strings.Split(string(data), "\n") {
		t, ok := logLineTime(line)
		if !ok {
			continue
		}
		switch {
		case strings.Contains(line, devinErrorRow) && strings.Contains(line, `method=session/prompt`):
			m := devinLogError.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			responseAt, response = t, m[1]
		case strings.Contains(line, devinPromptRow):
			promptAt = t
		}
	}
	if responseAt.IsZero() {
		// No turn outcome in the window. A truncated window may hide it, so
		// that is not evidence either way; a whole log with none is a session
		// that has not failed a turn.
		if truncated {
			return DevinSessionEvidence{}
		}
		return DevinSessionEvidence{State: DevinSessionRunning}
	}
	if !promptAt.IsZero() && promptAt.After(responseAt) {
		return DevinSessionEvidence{State: DevinSessionRunning}
	}
	stop, ok := classifyStatus(response, responseAt, local)
	if !ok || stop.Pool != "devin-free" {
		// A failed turn for another reason says nothing about the free limit;
		// the screen classifier still reads that pane's own banner.
		return DevinSessionEvidence{}
	}
	return DevinSessionEvidence{State: DevinSessionFreeLimit, At: responseAt, Reset: stop.Reset}
}

// logNamesSession reports whether the log's own session lines name session.
// devin logs "Saved N message nodes for session <id>", so a foreign log (a
// recycled pid) never passes for this session's.
func logNamesSession(data []byte, session string) bool {
	needle := "for session " + session
	for {
		i := bytes.Index(data, []byte(needle))
		if i < 0 {
			return false
		}
		after := data[i+len(needle):]
		if len(after) == 0 || after[0] == ' ' || after[0] == '\n' || after[0] == '\r' || after[0] == '\t' {
			return true
		}
		data = after
	}
}

// logLineTime reads a tracing line's leading RFC 3339 timestamp.
func logLineTime(line string) (time.Time, bool) {
	i := strings.IndexByte(line, ' ')
	if i <= 0 {
		return time.Time{}, false
	}
	stamp := line[:i]
	if !devinLogStamp.MatchString(stamp) {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
