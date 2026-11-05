package panewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
)

const nodeCommandJobResult = "job.result"
const nodeJobFindingsLimit = 24 << 10
const nodeJobLastLineLimit = 256

type nodeJobResultArgs struct {
	Job string `json:"job"`
}
type nodeJobReport struct {
	Present  bool   `json:"present"`
	Bytes    int64  `json:"bytes"`
	SHA256   string `json:"sha256"`
	LastLine string `json:"last_line"`
}
type nodeJobResult struct {
	Found         bool            `json:"found"`
	Findings      json.RawMessage `json:"findings"`
	FindingsError string          `json:"findings_error"`
	Report        nodeJobReport   `json:"report"`
	Completed     bool            `json:"completed"`
}

// openNodeResultFile rejects links and special files, and never blocks on a FIFO
// substituted between the check and open. Root confines even a raced symlink.
func openNodeResultFile(root *os.Root, name string) (*os.File, error) {
	info, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, os.ErrNotExist
	}
	f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	info, err = f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, os.ErrNotExist
	}
	return f, nil
}

func validNodeFindings(data []byte) string {
	if !utf8.Valid(data) || !json.Valid(data) {
		return "not_json"
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(data, &object) != nil || object == nil {
		return "schema"
	}
	var findings []json.RawMessage
	raw, ok := object["findings"]
	if !ok || json.Unmarshal(raw, &findings) != nil || findings == nil {
		return "schema"
	}
	return ""
}

func (client *HubClient) nodeJobResult(args nodeJobResultArgs) (any, string) {
	root, _, found, err := client.openNodeJobDir(args.Job)
	if errors.Is(err, errNodeJobsRootUnset) {
		return nil, "jobs_root_unset"
	}
	if err != nil {
		return nil, "result_failed"
	}
	result := nodeJobResult{Found: found}
	if !found {
		return result, ""
	}
	defer root.Close()
	f, err := openNodeResultFile(root, "findings.json")
	if errors.Is(err, os.ErrNotExist) {
		result.FindingsError = "missing"
	} else if err != nil {
		return nil, "result_failed"
	} else {
		data, readErr := io.ReadAll(io.LimitReader(f, nodeJobFindingsLimit+1))
		f.Close()
		if readErr != nil {
			return nil, "result_failed"
		}
		if len(data) > nodeJobFindingsLimit {
			result.FindingsError = "too_large"
		} else if reason := validNodeFindings(data); reason != "" {
			result.FindingsError = reason
		} else {
			result.Findings = data
		}
	}
	f, err = openNodeResultFile(root, "report.md")
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, "result_failed"
	}
	if err == nil {
		hash := sha256.New()
		var line, previous []byte
		var size int64
		buf := make([]byte, 32<<10)
		for {
			n, readErr := f.Read(buf)
			if n > 0 {
				chunk := buf[:n]
				size += int64(n)
				hash.Write(chunk)
				for _, b := range chunk {
					if b == '\n' {
						previous = append(previous[:0], line...)
						line = line[:0]
					} else if len(line) < nodeJobLastLineLimit+4 {
						line = append(line, b)
					}
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				f.Close()
				return nil, "result_failed"
			}
		}
		f.Close()
		if len(line) == 0 {
			line = previous
		}
		result.Report = nodeJobReport{true, size, hex.EncodeToString(hash.Sum(nil)), utf8PrefixWithin(strings.ToValidUTF8(string(line), "�"), nodeJobLastLineLimit)}
	}
	events, err := root.OpenRoot("events")
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, "result_failed"
	}
	if err == nil {
		defer events.Close()
		dir, err := events.Open(".")
		if err != nil {
			return nil, "result_failed"
		}
		entries, err := dir.ReadDir(-1)
		dir.Close()
		if err != nil {
			return nil, "result_failed"
		}
		for _, entry := range entries {
			if !strings.HasSuffix(entry.Name(), "-job.completed.json") {
				continue
			}
			f, err := openNodeResultFile(events, entry.Name())
			if err != nil {
				continue
			}
			data, err := io.ReadAll(io.LimitReader(f, 1<<20))
			f.Close()
			if err != nil {
				return nil, "result_failed"
			}
			var event struct {
				Kind string `json:"kind"`
				Type string `json:"type"`
			}
			if json.Unmarshal(data, &event) == nil && (event.Kind == "job.completed" || event.Type == "job.completed") {
				result.Completed = true
				break
			}
		}
	}
	return result, ""
}

func validNodeJobResult(r nodeJobResult) bool {
	if isJSONNull(r.Findings) {
		r.Findings = nil
	}
	if !r.Found {
		return r.Findings == nil && r.FindingsError == "" && r.Report == (nodeJobReport{}) && !r.Completed
	}
	if r.Findings != nil {
		if len(r.Findings) > nodeJobFindingsLimit || validNodeFindings(r.Findings) != "" || r.FindingsError != "" {
			return false
		}
	} else if !strings.Contains("|missing|too_large|not_json|schema|", "|"+r.FindingsError+"|") {
		return false
	}
	return r.Report.Bytes >= 0 && len(r.Report.LastLine) <= nodeJobLastLineLimit && (r.Report.Present || r.Report == (nodeJobReport{}))
}

func marshalNodeCommandJSON(value any) ([]byte, error) {
	var b bytes.Buffer
	encoder := json.NewEncoder(&b)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(value)
	data := bytes.TrimSuffix(b.Bytes(), []byte("\n"))
	// U+2028 and U+2029 are valid JSON characters; Go escapes them even when
	// HTML escaping is disabled. Preserve their compact form in this transport.
	var out []byte
	for i := 0; i < len(data); {
		if data[i] == '\\' {
			start := i
			for i < len(data) && data[i] == '\\' {
				i++
			}
			n := i - start
			out = append(out, data[start:i]...)
			if n%2 == 1 && i+5 <= len(data) && (string(data[i:i+5]) == "u2028" || string(data[i:i+5]) == "u2029") {
				out = out[:len(out)-1]
				if data[i+4] == '8' {
					out = append(out, 0xe2, 0x80, 0xa8)
				} else {
					out = append(out, 0xe2, 0x80, 0xa9)
				}
				i += 5
			}
			continue
		}
		out = append(out, data[i])
		i++
	}
	return out, err
}

func (peer *hubClientConnection) writeNodeResult(ctx context.Context, value any) error {
	data, err := marshalNodeCommandJSON(value)
	if err != nil {
		return err
	}
	peer.writeMu.Lock()
	defer peer.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return peer.connection.Write(ctx, websocket.MessageText, data)
}
