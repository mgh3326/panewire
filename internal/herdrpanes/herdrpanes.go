// Package herdrpanes reads herdr's pane inventory so that a cut answer is
// never taken for a complete one. herdr's `pane list` answer can stop at a
// 4096 or 8192 byte boundary (hk 5335); it then fails to decode. Read retries
// such an answer once. A cut or unreadable answer is returned as an error,
// which every caller already treats as not proven, never as an empty list or
// as proof that a pane is absent. A reader that knows the one pane it needs
// reads it with PaneGet instead.
package herdrpanes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
)

// Runner runs herdr with args and returns its stdout, as exec.Cmd.Output does.
type Runner = func(ctx context.Context, args ...string) ([]byte, error)

// cutBoundaries are the byte counts a cut `herdr pane list` answer stops at.
var cutBoundaries = [...]int{4096, 8192}

// ErrCut reports a pane list answer that was still cut after the one retry.
var ErrCut = errors.New("herdr pane list answer cut at a byte boundary")

// ErrUnreadable reports a `pane get` answer that does not name the pane asked for.
var ErrUnreadable = errors.New("herdr pane get answer does not name the pane")

// cutDiagnostic is the message herdr's CLI prints when its own JSON parse of
// the reply stops at a boundary: "EOF while parsing a string, line 1, column
// 8192". herdr 0.9.5 parses the socket reply before it prints anything, so a
// cut reply is a nonzero exit with this text on stderr and no stdout.
var cutDiagnostic = regexp.MustCompile(`EOF while parsing [^\n]*?line \d+, column (\d+)`)

// cutExit reports whether a failed herdr call failed on the CLI's boundary
// diagnostic (see cutDiagnostic), read from the error text or from the stderr
// that exec.Cmd.Output captures.
func cutExit(err error) bool {
	texts := []string{err.Error()}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		texts = append(texts, string(exitErr.Stderr))
	}
	for _, text := range texts {
		for _, m := range cutDiagnostic.FindAllStringSubmatch(text, -1) {
			if column, convErr := strconv.Atoi(m[1]); convErr == nil && atBoundary(column) {
				return true
			}
		}
	}
	return false
}

// IsCut reports whether an answer of size bytes that failed to decode stopped
// at a byte boundary, which is how herdr's cut answer presents.
func IsCut(size int, decodeErr error) bool {
	return decodeErr != nil && atBoundary(size)
}

// atBoundary reports whether n is one of the byte counts a cut answer stops at.
func atBoundary(n int) bool {
	for _, boundary := range cutBoundaries {
		if n == boundary {
			return true
		}
	}
	return false
}

// Read fetches an answer and decodes it with decode, which is the caller's
// strict parse: a nil error means the answer is complete. An answer that
// fails to decode at a boundary (see IsCut) is fetched once more. A second cut
// answer returns ErrCut. Any other fetch or decode error is returned as it is,
// with no retry, so an unreadable answer is never read as an empty one. The
// answer bytes are returned on success.
func Read(ctx context.Context, fetch func(context.Context) ([]byte, error), decode func([]byte) error) ([]byte, error) {
	for attempt := 1; ; attempt++ {
		out, err := fetch(ctx)
		if err != nil {
			// herdr's CLI exits nonzero on a cut reply: the cut is the failure,
			// with no stdout to decode.
			if !cutExit(err) {
				return nil, err
			}
			if attempt >= 2 {
				return nil, fmt.Errorf("%w: %v", ErrCut, err)
			}
			continue
		}
		decodeErr := decode(out)
		if decodeErr == nil {
			return out, nil
		}
		if !IsCut(len(out), decodeErr) {
			return nil, decodeErr
		}
		if attempt >= 2 {
			return nil, fmt.Errorf("%w (%d bytes)", ErrCut, len(out))
		}
	}
}

// List reads `herdr pane list` through run with Read's retry and decode rules.
func List(ctx context.Context, run Runner, decode func([]byte) error) ([]byte, error) {
	return Read(ctx, func(ctx context.Context) ([]byte, error) {
		return run(ctx, "pane", "list")
	}, decode)
}

// PaneGet reports whether herdr has pane, by `herdr pane get`. It returns
// present when herdr answers with that pane, and absent only on herdr's own
// pane_not_found answer. Any other failure, and an answer that does not name
// pane, is an error: an unknown pane is never an absent one.
func PaneGet(ctx context.Context, run Runner, pane string) (bool, error) {
	out, err := run(ctx, "pane", "get", pane)
	if err != nil {
		if paneNotFound(out, err) {
			return false, nil
		}
		return false, err
	}
	var answer struct {
		Result struct {
			Pane struct {
				PaneID string `json:"pane_id"`
			} `json:"pane"`
		} `json:"result"`
	}
	if err := json.Unmarshal(out, &answer); err != nil {
		return false, err
	}
	if answer.Result.Pane.PaneID != pane {
		return false, ErrUnreadable
	}
	return true, nil
}

// paneNotFound reports whether a failed herdr call's error object (on stdout or
// in the captured stderr of an exec error) carries the pane_not_found code.
func paneNotFound(out []byte, err error) bool {
	if errorCode(out) == "pane_not_found" {
		return true
	}
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr) && errorCode(exitErr.Stderr) == "pane_not_found"
}

// errorCode returns the code of the error object in a herdr answer, or "".
func errorCode(answer []byte) string {
	if !bytes.Contains(answer, []byte(`"error"`)) {
		return ""
	}
	var env struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(answer, &env) != nil {
		return ""
	}
	return env.Error.Code
}
