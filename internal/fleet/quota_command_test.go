package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestQuotaCommandRefusalJSONAndTimeout(t *testing.T) {
	runner := &fakeRunner{Do: func(c Command) Result {
		return Result{Code: 3, Out: `{"reason":"quota exhausted"}`, Err: "warning: private warning\nother diagnostic"}
	}}
	result := runQuotaCommand(context.Background(), runner, Command{Name: "pacelever"}, "quota gate", time.Second)
	err := checkQuota(result, "quota gate")
	if err == nil || !strings.Contains(err.Error(), "quota exhausted") || strings.Contains(err.Error(), "warning") || strings.Contains(err.Error(), "diagnostic") {
		t.Fatalf("refusal = %v", err)
	}
	timeout := checkQuota(Result{Code: exitTimeout, Out: "quota gate: timed out after 30s"}, "quota gate")
	if timeout == nil || timeout.Error() != "quota gate: timed out after 30s" {
		t.Fatal(timeout)
	}
}

func TestQuotaExecRunnerSuccessfulProcessWithInheritedPipe(t *testing.T) {
	pidfile := filepath.Join(t.TempDir(), "child.pid")
	t.Cleanup(func() {
		data, err := os.ReadFile(pidfile)
		if err != nil {
			t.Error(err)
			return
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			t.Error(err)
			return
		}
		if p, err := os.FindProcess(pid); err == nil {
			_ = p.Kill()
		}
	})
	r := ExecRunner{}.Run(context.Background(), Command{Name: "sh", Args: []string{"-c", `sleep 30 & echo $! > "$1"; echo '{"ok":true}'`, "sh", pidfile}})
	if r.Code != 0 || r.Out != "{\"ok\":true}\n" {
		t.Fatalf("result: %+v", r)
	}
}
