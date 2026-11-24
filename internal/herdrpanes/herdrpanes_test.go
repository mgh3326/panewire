package herdrpanes

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"strings"
	"testing"
)

// cutAnswer is a pane list answer herdr cut at 8192 bytes: the bytes up to the
// boundary, which do not decode.
func cutAnswer() []byte {
	full := `{"result":{"panes":[{"pane_id":"w1:p1","label":"a","cwd":"` + strings.Repeat("/w", 6000) + `"}]}}`
	return []byte(full[:8192])
}

const wholeAnswer = `{"result":{"panes":[{"pane_id":"w1:p1","label":"a"}]}}`

func strictDecode(out []byte) error {
	var v map[string]json.RawMessage
	return json.Unmarshal(out, &v)
}

// A cut answer at the 8192 boundary is read again once, and the second,
// complete answer is the one returned.
func TestReadRetriesCutAnswerOnce(t *testing.T) {
	answers := [][]byte{cutAnswer(), []byte(wholeAnswer)}
	fetches := 0
	out, err := Read(t.Context(), func(context.Context) ([]byte, error) {
		fetches++
		return answers[fetches-1], nil
	}, strictDecode)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if fetches != 2 || string(out) != wholeAnswer {
		t.Fatalf("fetches=%d out=%q, want 2 and the whole answer", fetches, out)
	}
}

// A second cut answer is ErrCut, not an empty answer and not an absent pane.
func TestReadCutTwiceIsErrCut(t *testing.T) {
	fetches := 0
	out, err := Read(t.Context(), func(context.Context) ([]byte, error) {
		fetches++
		return cutAnswer(), nil
	}, strictDecode)
	if !errors.Is(err, ErrCut) || out != nil {
		t.Fatalf("out=%q err=%v, want ErrCut and no answer", out, err)
	}
	if fetches != 2 {
		t.Fatalf("fetches=%d, want exactly one retry", fetches)
	}
}

// A decode failure away from a boundary is an ordinary error: no retry.
func TestReadNonBoundaryDecodeErrorIsNotRetried(t *testing.T) {
	fetches := 0
	_, err := Read(t.Context(), func(context.Context) ([]byte, error) {
		fetches++
		return []byte(`not json`), nil
	}, strictDecode)
	if err == nil || errors.Is(err, ErrCut) || fetches != 1 {
		t.Fatalf("err=%v fetches=%d, want the decode error after one fetch", err, fetches)
	}
}

// A fetch failure is returned as it is, with no retry.
func TestReadFetchErrorIsNotRetried(t *testing.T) {
	fetches := 0
	boom := errors.New("boom")
	_, err := Read(t.Context(), func(context.Context) ([]byte, error) {
		fetches++
		return nil, boom
	}, strictDecode)
	if !errors.Is(err, boom) || fetches != 1 {
		t.Fatalf("err=%v fetches=%d", err, fetches)
	}
}

// List asks herdr for `pane list` and applies the same retry rules.
func TestListReadsPaneListWithRetry(t *testing.T) {
	var calls [][]string
	answers := [][]byte{cutAnswer(), []byte(wholeAnswer)}
	run := func(_ context.Context, args ...string) ([]byte, error) {
		calls = append(calls, args)
		out := answers[0]
		answers = answers[1:]
		return out, nil
	}
	if _, err := List(t.Context(), run, strictDecode); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(calls) != 2 || strings.Join(calls[0], " ") != "pane list" || strings.Join(calls[1], " ") != "pane list" {
		t.Fatalf("calls %v, want two `pane list`", calls)
	}
}

// exitWithStderr is the error exec.Cmd.Output returns for a failed herdr
// call: exit 1 with its error object captured on stderr.
func exitWithStderr(t *testing.T, stderr string) error {
	t.Helper()
	_, err := exec.CommandContext(t.Context(), "sh", "-c", `printf '%s' "$0" >&2; exit 1`, stderr).Output()
	if err == nil {
		t.Fatal("want a failed command")
	}
	return err
}

const notFoundBody = `{"error":{"code":"pane_not_found","message":"pane w1:p9 not found"},"id":"cli:pane:get"}`

// herdr's pane_not_found is the one answer that proves a pane absent.
func TestPaneGetNotFoundIsAbsent(t *testing.T) {
	run := func(context.Context, ...string) ([]byte, error) {
		return nil, exitWithStderr(t, notFoundBody)
	}
	present, err := PaneGet(t.Context(), run, "w1:p9")
	if present || err != nil {
		t.Fatalf("present=%v err=%v, want absent with no error", present, err)
	}
}

// Any other failure is unknown: the error is returned and the pane is not
// reported absent.
func TestPaneGetOtherFailureIsUnknownNotAbsent(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  Runner
	}{
		{"exit error without the code", func(context.Context, ...string) ([]byte, error) {
			return nil, exitWithStderr(t, `{"error":{"code":"herdr_busy"}}`)
		}},
		{"plain error", func(context.Context, ...string) ([]byte, error) {
			return nil, errors.New("socket closed")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			present, err := PaneGet(t.Context(), tc.run, "w1:p9")
			if present || err == nil {
				t.Fatalf("present=%v err=%v, want unknown (an error)", present, err)
			}
		})
	}
}

// The pane answer must name the pane asked for; anything else is unreadable.
func TestPaneGetAnswerNamingAnotherPaneIsUnreadable(t *testing.T) {
	run := func(context.Context, ...string) ([]byte, error) {
		return []byte(`{"result":{"pane":{"pane_id":"w1:p2"}}}`), nil
	}
	present, err := PaneGet(t.Context(), run, "w1:p1")
	if present || !errors.Is(err, ErrUnreadable) {
		t.Fatalf("present=%v err=%v, want ErrUnreadable", present, err)
	}
}

func TestPaneGetPresentPane(t *testing.T) {
	var got []string
	run := func(_ context.Context, args ...string) ([]byte, error) {
		got = args
		return []byte(`{"result":{"pane":{"pane_id":"w1:p1"}}}`), nil
	}
	present, err := PaneGet(t.Context(), run, "w1:p1")
	if !present || err != nil {
		t.Fatalf("present=%v err=%v", present, err)
	}
	if strings.Join(got, " ") != "pane get w1:p1" {
		t.Fatalf("ran %v, want `pane get w1:p1`", got)
	}
}

func TestIsCutOnlyAtBoundaries(t *testing.T) {
	cases := []struct {
		size int
		err  error
		want bool
	}{
		{8192, errors.New("eof"), true},
		{4096, errors.New("eof"), true},
		{8191, errors.New("eof"), false},
		{8192, nil, false},
	}
	for _, tc := range cases {
		if got := IsCut(tc.size, tc.err); got != tc.want {
			t.Errorf("IsCut(%d, %v) = %v, want %v", tc.size, tc.err, got, tc.want)
		}
	}
}

const cutExitDiagnostic = "Error: EOF while parsing a string, line 1, column 8192"

// herdr's CLI exits nonzero on a cut reply, with the diagnostic on stderr and
// no stdout. The retry runs on that exit, and a second cut exit is ErrCut.
func TestReadRetriesCutExitOnce(t *testing.T) {
	fetches := 0
	out, err := Read(t.Context(), func(context.Context) ([]byte, error) {
		fetches++
		if fetches == 1 {
			return nil, exitWithStderr(t, cutExitDiagnostic)
		}
		return []byte(wholeAnswer), nil
	}, strictDecode)
	if err != nil || string(out) != wholeAnswer || fetches != 2 {
		t.Fatalf("out=%q err=%v fetches=%d, want the whole answer after one retry", out, err, fetches)
	}
}

func TestReadCutExitTwiceIsErrCut(t *testing.T) {
	fetches := 0
	_, err := Read(t.Context(), func(context.Context) ([]byte, error) {
		fetches++
		return nil, exitWithStderr(t, cutExitDiagnostic)
	}, strictDecode)
	if !errors.Is(err, ErrCut) || fetches != 2 {
		t.Fatalf("err=%v fetches=%d, want ErrCut after exactly one retry", err, fetches)
	}
}

// A nonzero exit whose diagnostic is not a boundary cut is an ordinary error.
func TestReadNonBoundaryExitIsNotRetried(t *testing.T) {
	fetches := 0
	_, err := Read(t.Context(), func(context.Context) ([]byte, error) {
		fetches++
		return nil, exitWithStderr(t, "Error: EOF while parsing a string, line 1, column 100")
	}, strictDecode)
	if err == nil || errors.Is(err, ErrCut) || fetches != 1 {
		t.Fatalf("err=%v fetches=%d, want the exit error after one fetch", err, fetches)
	}
}

func TestListRetriesCutExitOnce(t *testing.T) {
	calls := 0
	run := func(_ context.Context, args ...string) ([]byte, error) {
		calls++
		if calls == 1 {
			return nil, exitWithStderr(t, cutExitDiagnostic)
		}
		return []byte(wholeAnswer), nil
	}
	if _, err := List(t.Context(), run, strictDecode); err != nil || calls != 2 {
		t.Fatalf("err=%v calls=%d, want success after one retry", err, calls)
	}
}

func TestSessionNotFoundProvesSessionAbsent(t *testing.T) {
	cases := []struct {
		name   string
		out    []byte
		err    error
		wantOK bool
	}{
		{"code server_not_running in stderr", nil, exitWithStderr(t, `{"error":{"code":"server_not_running","message":"no herdr server is running at /sessions/x/herdr.sock"}}`), true},
		{"code session_not_found in stderr", nil, exitWithStderr(t, `{"error":{"code":"session_not_found","message":"no such session"}}`), true},
		{"no herdr server is running in stderr text", nil, exitWithStderr(t, `no herdr server is running at /sessions/x/herdr.sock`), true},
		{"code in stdout", []byte(`{"error":{"code":"server_not_running"}}`), errors.New("exit status 1"), true},
		{"code internal_error in stderr", nil, exitWithStderr(t, `{"error":{"code":"internal_error"}}`), false},
		{"generic exit without output", nil, exitWithStderr(t, ``), false},
		{"plain error", nil, errors.New("connection reset"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SessionNotFound(tc.out, tc.err); got != tc.wantOK {
				t.Fatalf("SessionNotFound() = %v, want %v", got, tc.wantOK)
			}
		})
	}
}
