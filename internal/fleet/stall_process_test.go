package fleet

import (
	"context"
	"testing"
)

func TestStallForegroundProcess(t *testing.T) {
	for _, tc := range []struct {
		name, info string
		busy       bool
	}{
		{"harness", `{"process_info":{"shell_pid":1,"foreground_processes":[{"pid":2,"argv0":"codex"}]}}`, false},
		{"shell", `{"process_info":{"shell_pid":1,"foreground_processes":[{"pid":1,"argv0":"zsh"}]}}`, false},
		{"build", `{"process_info":{"shell_pid":1,"foreground_processes":[{"pid":3,"argv0":"xcodebuild"}]}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := testApp(t)
			a.Runner = &fakeRunner{Do: func(Command) Result { return Result{Out: tc.info} }}
			busy, err := a.StallForegroundBusy(context.Background(), "w:p1", "codex")
			if err != nil || busy != tc.busy {
				t.Fatalf("busy=%v err=%v; want %v", busy, err, tc.busy)
			}
		})
	}
}

func TestStallForegroundProcessUnavailable(t *testing.T) {
	for _, info := range []string{
		`{"process_info":{"shell_pid":1,"foreground_processes":[]}}`,
		`{"process_info":{"shell_pid":1,"foreground_processes":[{"pid":2}]}}`,
	} {
		a := testApp(t)
		a.Runner = &fakeRunner{Do: func(Command) Result { return Result{Out: info} }}
		if _, err := a.StallForegroundBusy(context.Background(), "w:p1", "codex"); err == nil {
			t.Fatalf("accepted incomplete process information: %s", info)
		}
	}
}
