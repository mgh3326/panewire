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

func TestStallReviewerQuotaIsAnnouncedForTheHub(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	old := announceStartWatch
	started := 0
	announceStartWatch = func(*App, string) error { started++; return nil }
	t.Cleanup(func() { announceStartWatch = old })
	for i := 0; i < 2; i++ {
		attempt, err := CurrentAttempt(a.Jobs, "j")
		if err != nil {
			t.Fatal(err)
		}
		if err := AnnounceQuotaStop(context.Background(), a.Jobs, "j", "w:p1", attempt, quotastop.Stop{Pool: "claude"}); err != nil {
			t.Fatal(err)
		}
	}
	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := announcedQuotaStop(j); !ok {
		t.Fatal("the reviewer's stop is not announced for the hub")
	}
	if _, ok := pendingQuotaIntent(j); ok {
		t.Fatal("a reviewer's stop recorded a close intent: nothing could replace it")
	}
	if started == 0 {
		t.Fatal("no watcher was started for the announced stop")
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
