package panewire

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestSpawnRefusalReplayableTable(t *testing.T) {
	for _, test := range []struct {
		reason string
		want   bool
	}{
		{"checkout_refused:branch-elsewhere", false},
		{"checkout_refused:branch-elsewhere: cannot prepare worktree x on machine-a", false},
		{"checkout_refused:origin-mismatch", false},
		{"checkout_refused:busy", true},
		{"checkout_refused:dirty", true},
		{"checkout_refused:sha-missing", true},
		{"checkout_refused:work_root_unset", true},
		{"checkout_refused:git_too_old", true},
		{"checkout_refused:git-failed", true},
		{"timeout", true},
		{"disconnected", true},
		{"node_upgrade_required", true},
		{"admission_refused: cannot place", true},
	} {
		if got := spawnRefusalReplayable(test.reason); got != test.want {
			t.Errorf("spawnRefusalReplayable(%q)=%v, want %v", test.reason, got, test.want)
		}
	}
}

// The classification is the fix text's own promise: a fix that tells the
// operator to replay the request is replayable, one that tells them to inspect
// and correct is not. git_too_old is the one fix that names neither; upgrading
// git is the operator's repair and the same request then succeeds.
func TestSpawnRefusalReplayableFollowsNodeFix(t *testing.T) {
	for _, code := range []string{"busy", "dirty", "sha-missing", "work_root_unset", "origin-mismatch", "branch-elsewhere", "git_too_old", "git-failed"} {
		fix := nodeSpawnRefusalFix("checkout_refused:"+code, "")
		want := strings.Contains(fix, "replay")
		if code == "git_too_old" {
			want = true
		}
		if got := spawnRefusalReplayable("checkout_refused:" + code); got != want {
			t.Errorf("%s: replayable=%v but fix %q", code, got, fix)
		}
	}
}

func TestSpawnCheckoutRefusalCompletesFailedOrRetryable(t *testing.T) {
	for _, test := range []struct {
		code, status string
		reserved     bool
	}{
		{"branch-elsewhere", "failed", true},
		{"origin-mismatch", "failed", true},
		{"busy", "retryable", false},
		{"dirty", "retryable", false},
	} {
		t.Run(test.code, func(t *testing.T) {
			h, _, _ := spawnNotifyHub(t, "")
			task, _, err := h.inbox.store.claimSpawn(t.Context(), durableSpawnRequest(), "test-job", "parent", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			// The node marks every checkout refusal retryable; the hub decides.
			outcome := hubSpawnOutcome{RC: 81, Stage: "checkout", Reason: "checkout_refused:" + test.code + ": refused", Host: "machine-a", Retryable: true, NodeResult: true}
			if _, err = h.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil {
				t.Fatal(err)
			}
			var status string
			var reservations int
			if err = h.inbox.store.db.QueryRow(`SELECT status FROM spawn_operations`).Scan(&status); err != nil || status != test.status {
				t.Fatalf("status=%q %v, want %q", status, err, test.status)
			}
			if err = h.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_reservations`).Scan(&reservations); err != nil || (reservations == 1) != test.reserved {
				t.Fatalf("reservations=%d %v, want held=%v", reservations, err, test.reserved)
			}
			h.flushSpawnNotifications(t.Context())
		})
	}
}

func submittedBy(flag, lane string) func(*hubSpawnTask) {
	return func(task *hubSpawnTask) {
		task.Request.Args = append(slices.Clone(task.Request.Args), flag, lane)
	}
}

func failedSpawnNotifications(t *testing.T, owner string, mutate func(*hubSpawnTask)) []hubRelayInjectEvent {
	t.Helper()
	h, _, destination := spawnNotifyHubLanes(t, "", "lane-a", "lane-b")
	task, _, err := h.inbox.store.claimSpawn(t.Context(), durableSpawnRequest(), "test-job", owner, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if mutate != nil {
		mutate(&task)
	}
	outcome := hubSpawnOutcome{RC: 81, Stage: "checkout", Reason: "checkout_refused:branch-elsewhere: refused", Fix: "Inspect the checkout.", Host: "machine-a", Retryable: true, NodeResult: true}
	for range 2 { // The second completion is a replay of the same transition.
		if _, err = h.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		h.flushSpawnNotifications(t.Context())
	}
	var rows, undelivered int
	if err = h.inbox.store.db.QueryRow(`SELECT count(*), coalesce(sum(1-delivered),0) FROM spawn_notifications`).Scan(&rows, &undelivered); err != nil || undelivered != 0 {
		t.Fatalf("rows=%d undelivered=%d %v", rows, undelivered, err)
	}
	injected := spawnFailedInjections(destination)
	if len(injected) != rows {
		t.Fatalf("%d rows but %d injections: %+v", rows, len(injected), injected)
	}
	return injected
}

func TestSpawnFailedNotifiesOwnerAndSubmittingLane(t *testing.T) {
	for _, test := range []struct {
		name string
		by   func(*hubSpawnTask)
	}{
		{"requested-by", submittedBy("--requested-by", "lane-b")},
		{"parent", submittedBy("--parent", "lane-b")},
	} {
		t.Run(test.name, func(t *testing.T) {
			injected := failedSpawnNotifications(t, "lane-a", test.by)
			panes := map[string]string{}
			for _, relay := range injected {
				panes[relay.Pane] = relay.Text
				for _, want := range []string{"SPAWN_FAILED", "replay=no", "branch-elsewhere", "Inspect the checkout."} {
					if !strings.Contains(relay.Text, want) {
						t.Fatalf("text omits %q: %q", want, relay.Text)
					}
				}
			}
			if len(injected) != 2 || panes["w1:p1"] == "" || panes["w1:p2"] == "" {
				t.Fatalf("want one event for each of lane-a (w1:p1) and lane-b (w1:p2): %+v", injected)
			}
		})
	}
}

func TestSpawnFailedNotifiesOnceWhenOwnerSubmitted(t *testing.T) {
	for name, by := range map[string]func(*hubSpawnTask){
		"no submitter":                 nil,
		"requested-by the owner":       submittedBy("--requested-by", "lane-a"),
		"parent the owner":             submittedBy("--parent", "lane-a"),
		"requested-by not a lane name": submittedBy("--requested-by", "bad lane!"),
	} {
		t.Run(name, func(t *testing.T) {
			if injected := failedSpawnNotifications(t, "lane-a", by); len(injected) != 1 || injected[0].Pane != "w1:p1" {
				t.Fatalf("want exactly one event for lane-a: %+v", injected)
			}
		})
	}
}

// A spawn that started is reported to its owner as before; the submitter only
// needs to hear about the one that did not.
func TestSpawnSucceededDoesNotNotifySubmittingLane(t *testing.T) {
	h, _, destination := spawnNotifyHubLanes(t, "", "lane-a", "lane-b")
	task, _, err := h.inbox.store.claimSpawn(t.Context(), durableSpawnRequest(), "test-job", "lane-a", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	submittedBy("--requested-by", "lane-b")(&task)
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{Host: "machine-a", Pane: "w1:p9"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.flushSpawnNotifications(t.Context())
	if injected := spawnFailedInjections(destination); len(injected) != 1 || injected[0].Pane != "w1:p1" {
		t.Fatalf("want one event for the owner only: %+v", injected)
	}
}
