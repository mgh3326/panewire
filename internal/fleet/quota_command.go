package fleet

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"
)

func runQuotaCommand(ctx context.Context, runner Runner, cmd Command, what string, timeout time.Duration) Result {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	r := runner.Run(ctx, cmd)
	for _, line := range strings.Split(r.Err, "\n") {
		if line != "" {
			slog.Debug("pacelever diagnostic", "line", line)
		}
	}
	if ctx.Err() == context.DeadlineExceeded || r.Code == exitTimeout {
		// The deadline's cause is the timeout; the last non-warning stderr
		// line, if the command wrote one, says what it was doing (hk 2944).
		out := what + ": timed out after " + timeout.String()
		if lines := diagnosticLines(r.Err); len(lines) > 0 {
			out += ": " + lines[len(lines)-1]
		}
		return Result{Code: exitTimeout, Out: out}
	}
	return r
}

// diagnosticLines returns the stderr lines that can name a failure's cause.
// The quota command prints `warning:` lines (unknown pool, retired config)
// before its answer; they never explain a refusal or a deadline (hk 2944).
func diagnosticLines(stderr string) []string {
	var lines []string
	for _, line := range strings.Split(stderr, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "warning:") {
			lines = append(lines, line)
		}
	}
	return lines
}

func checkQuota(r Result, what string) error {
	if r.Code == 0 {
		return nil
	}
	if r.Code == exitTimeout {
		return fail(r.Code, "%s", r.Out)
	}
	var body struct {
		Reason string `json:"reason"`
		Error  string `json:"error"`
	}
	_ = json.Unmarshal([]byte(r.Out), &body)
	reason := body.Reason
	if reason == "" {
		reason = body.Error
	}
	if lines := diagnosticLines(r.Err); reason == "" && len(lines) > 0 {
		reason = lines[0]
	}
	if reason == "" {
		reason = "dependency failed"
	}
	return fail(r.Code, "%s: %s", what, reason)
}

func checkQuotaPlain(r Result, what string) error {
	if r.Code == 0 {
		return nil
	}
	if r.Code == exitTimeout {
		return fail(r.Code, "%s", r.Out)
	}
	summary := ""
	if lines := diagnosticLines(r.Err); len(lines) > 0 {
		summary = lines[0]
	}
	return fail(r.Code, "%s: exit %d: %s", what, r.Code, summary)
}
