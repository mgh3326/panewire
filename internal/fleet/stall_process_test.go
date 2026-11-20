package fleet

import (
	"context"
	"testing"

	"github.com/mgh3326/panewire/internal/quotastop"
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

func TestStallReviewerQuotaUsesWatcherIntent(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	old := announceStartWatch
	announceStartWatch = func(a *App, _ string) error {
		if a.HerdrSession != "session-a" {
			t.Fatalf("watcher session=%q", a.HerdrSession)
		}
		return nil
	}
	t.Cleanup(func() { announceStartWatch = old })
	for i := 0; i < 2; i++ {
		if err := RecordReviewerQuotaIntent(context.Background(), a.Jobs, "j", "w:p1", "session-a", quotastop.Stop{Pool: "claude"}); err != nil {
			t.Fatal(err)
		}
	}
	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range j.Events {
		if event.Kind == quotaIntentKind {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("quota intents=%d, want 1", count)
	}
	if _, ok := pendingQuotaIntent(j); !ok {
		t.Fatal("watcher has no pending quota intent")
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
