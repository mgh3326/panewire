package fleet

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
	"github.com/mgh3326/panewire/internal/quotastop"
)

// A rejection has no known expiry. See docs/placement.md#exhausted-api-credit.
// The sentinel is not a billing expiry.
var creditRejectionExpiry = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

var (
	errCreditAccountMismatch       = errors.New("credit rejection account does not match pool")
	errCreditAccountUnidentifiable = errors.New("shared account fingerprint required")
)

func (a *App) publishCreditRejection(ctx context.Context, j Job, stop quotastop.Stop) error {
	key := str(j.Receipt, "account_key")
	if key != "" && (!validAccountKey(key) || accountPool(key) != stop.Pool) {
		return errCreditAccountMismatch
	}
	if key == "" {
		var err error
		key, err = manualAccountKey(a, stop.Pool)
		if err != nil {
			return fmt.Errorf("%w: %s", errCreditAccountUnidentifiable, err)
		}
	}
	observation := shared.Observation{
		Kind: "manual", Window: "credit", MeasuredAt: stop.CreditExhaustedAt,
		Plan:      "credit-exhausted:" + stop.CreditExhaustedAt.UTC().Format(time.RFC3339Nano),
		ExpiresAt: &creditRejectionExpiry,
		Buckets:   []shared.Bucket{{Window: "credit", UsedPct: 100, Scope: "account"}},
	}
	return a.withManualLease(ctx, key, func(holder string) error {
		return a.publishManual(ctx, key, holder, observation)
	})
}

// ompCreditStop reads only complete session records. Pane text is never evidence
// of a provider rejection; an answer may quote the same error verbatim.
func ompCreditStop(home string, j Job) (quotastop.Stop, bool) {
	ref, ok := receiptHarnessSession(j.Receipt)
	if !ok || ref.Kind != "omp" {
		return quotastop.Stop{}, false
	}
	rootPath := filepath.Join(home, ".omp", "agent", "sessions")
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return quotastop.Stop{}, false
	}
	defer root.Close()
	path := ref.ID
	var rel string
	if filepath.IsAbs(path) {
		if filepath.Clean(path) != path || !strings.HasSuffix(path, ".jsonl") {
			return quotastop.Stop{}, false
		}
		rel, err = filepath.Rel(rootPath, path)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return quotastop.Stop{}, false
		}
	} else {
		rel, err = findOMPSession(root, path, str(j.Receipt, "cwd"))
		if err != nil {
			return quotastop.Stop{}, false
		}
	}
	f, err := root.Open(rel)
	if err != nil {
		return quotastop.Stop{}, false
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return quotastop.Stop{}, false
	}
	h, err := readOMPSessionHeader(f)
	if err != nil || h.CWD == "" || h.CWD != str(j.Receipt, "cwd") {
		return quotastop.Stop{}, false
	}
	const maxTail = 4 << 20
	offset := max(int64(0), info.Size()-maxTail)
	tail := make([]byte, info.Size()-offset)
	if _, err := f.ReadAt(tail, offset); err != nil && err != io.EOF {
		return quotastop.Stop{}, false
	}
	if offset > 0 {
		if i := bytes.IndexByte(tail, '\n'); i >= 0 {
			tail = tail[i+1:]
		} else {
			return quotastop.Stop{}, false
		}
	}
	if len(tail) == 0 || tail[len(tail)-1] != '\n' {
		return quotastop.Stop{}, false
	}
	type ompMessageEntry struct {
		Type      string `json:"type"`
		Timestamp string `json:"timestamp"`
		Message   struct {
			Role         string `json:"role"`
			StopReason   string `json:"stopReason"`
			ErrorStatus  int    `json:"errorStatus"`
			Provider     string `json:"provider"`
			ErrorMessage string `json:"errorMessage"`
			CompletedAt  int64  `json:"completedAt"`
		} `json:"message"`
	}
	var last ompMessageEntry
	found := false
	for _, line := range bytes.Split(tail, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var entry struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(line, &entry) != nil {
			return quotastop.Stop{}, false
		}
		if entry.Type == "message" {
			last = ompMessageEntry{}
			if json.Unmarshal(line, &last) != nil {
				return quotastop.Stop{}, false
			}
			found = true
		}
	}
	m := last.Message
	pool := str(j.Receipt, "pool")
	if !found || m.Role != "assistant" || m.StopReason != "error" {
		return quotastop.Stop{}, false
	}
	body := m.ErrorMessage
	if i := strings.IndexByte(body, '{'); i >= 0 {
		body = body[i:]
	} else {
		return quotastop.Stop{}, false
	}
	var response struct {
		Error struct {
			Type    string `json:"type"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(body), &response) != nil {
		return quotastop.Stop{}, false
	}
	switch pool {
	case "anthropic-api":
		if m.Provider != "anthropic" || m.ErrorStatus != 400 || !strings.HasSuffix(response.Error.Type, "_error") || !strings.HasPrefix(response.Error.Message, "Your credit balance is too low") {
			return quotastop.Stop{}, false
		}
	case "openai-api":
		if m.Provider != "openai" || m.ErrorStatus != 429 || response.Error.Code != "insufficient_quota" {
			return quotastop.Stop{}, false
		}
	default:
		return quotastop.Stop{}, false
	}
	var at time.Time
	if m.CompletedAt > 0 {
		at = time.UnixMilli(m.CompletedAt).UTC()
	} else {
		at, err = time.Parse(time.RFC3339Nano, last.Timestamp)
		if err != nil {
			return quotastop.Stop{}, false
		}
	}
	return quotastop.Stop{Pool: pool, CreditExhaustedAt: at}, true
}

type ompSessionHeader struct {
	Type string `json:"type"`
	ID   string `json:"id"`
	CWD  string `json:"cwd"`
}

// Omp writes a title prologue before the session header. Bound the search
// and reject other records before the header rather than guessing an identity.
func readOMPSessionHeader(r io.Reader) (ompSessionHeader, error) {
	reader := bufio.NewReader(io.LimitReader(r, 64<<10))
	for i := 0; i < 8; i++ {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			break
		}
		var header ompSessionHeader
		if json.Unmarshal(line, &header) != nil {
			break
		}
		if header.Type == "session" {
			return header, nil
		}
		if header.Type != "title" {
			break
		}
	}
	return ompSessionHeader{}, fmt.Errorf("session header unavailable")
}

// findOMPSession accepts an ID only when one bounded, confined search finds a
// unique matching session header for the job's working directory.
func findOMPSession(root *os.Root, id, cwd string) (string, error) {
	if !usageSessionIDRE.MatchString(id) || id == "unknown" || cwd == "" {
		return "", fmt.Errorf("invalid session identity")
	}
	var found string
	count := 0
	err := fs.WalkDir(root.FS(), ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		count++
		if count > 10000 {
			return fmt.Errorf("session search limit exceeded")
		}
		if !entry.Type().IsRegular() || !strings.HasSuffix(path, ".jsonl") {
			return nil
		}
		f, err := root.Open(path)
		if err != nil {
			return err
		}
		header, err := readOMPSessionHeader(f)
		f.Close()
		if err == nil && header.ID == id && header.CWD == cwd {
			if found != "" {
				return fmt.Errorf("ambiguous session identity")
			}
			found = path
		}
		return nil
	})
	if err != nil || found == "" {
		return "", fmt.Errorf("session identity unavailable")
	}
	return found, nil
}
