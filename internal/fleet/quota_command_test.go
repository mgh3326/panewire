package fleet

import (
	"context"
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
