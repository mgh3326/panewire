package panewire

import (
	"errors"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A login sighting older than the pending clean observation cannot restart the
// clear countdown (GoneSince). The order here is the sighting at t2, a clean
// report at t3 that starts the countdown, then a delayed refusal stamped t1 < t2
// that lands last. The countdown must stand, on a told row and on an untold one
// (hk 5800, from hk 5781).
func TestOneTimeNoticeGoneSinceKeptByDelayedLoginSighting(t *testing.T) {
	cases := []struct {
		name   string
		notify func(*sentry.Event) (bool, error)
		told   bool
	}{
		{"told", func(*sentry.Event) (bool, error) { return true, nil }, true},
		{"untold", func(*sentry.Event) (bool, error) { return false, errors.New("sentry unavailable") }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hub, server := fsHub(t, "")
			fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
			hub.oneTimeNotify = tc.notify
			t1 := hub.now().UTC()
			t2 := t1.Add(time.Minute)
			t3 := t2.Add(time.Minute)
			key := oneTimeActionKey{machine: "node-a", kind: "agy", need: oneTimeLoginMissing}

			hub.noteReadinessActions("node-a", readinessWith(t2, "agy", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginMissing}), t2)
			settleOneTimeNotices(t, hub)
			hub.noteReadinessActions("node-a", readinessWith(t3, "agy", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent}), t3)
			settleOneTimeNotices(t, hub)
			hub.noteReadinessActions("node-a", readinessWith(t1, "agy", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginMissing}), t1)
			settleOneTimeNotices(t, hub)

			hub.oneTimeMu.Lock()
			state := hub.oneTimeActions[key]
			hub.oneTimeMu.Unlock()
			if !state.GoneSince.Equal(t3) {
				t.Fatalf("GoneSince = %v after a delayed t1 sighting, want the clean observation %v kept (told=%v)", state.GoneSince, t3, tc.told)
			}
			if got := !state.ToldAt.IsZero(); got != tc.told {
				t.Fatalf("told = %v, want %v", got, tc.told)
			}
		})
	}
}

// A login sighting newer than the pending clean observation still restarts the
// countdown, on both the told and untold branches.
func TestOneTimeNoticeGoneSinceResetByNewerLoginSighting(t *testing.T) {
	cases := []struct {
		name   string
		notify func(*sentry.Event) (bool, error)
	}{
		{"told", func(*sentry.Event) (bool, error) { return true, nil }},
		{"untold", func(*sentry.Event) (bool, error) { return false, errors.New("sentry unavailable") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hub, server := fsHub(t, "")
			fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
			hub.oneTimeNotify = tc.notify
			t1 := hub.now().UTC()
			t2 := t1.Add(time.Minute)
			t3 := t2.Add(time.Minute)
			t4 := t3.Add(time.Minute)
			key := oneTimeActionKey{machine: "node-a", kind: "agy", need: oneTimeLoginMissing}

			hub.noteReadinessActions("node-a", readinessWith(t2, "agy", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginMissing}), t2)
			settleOneTimeNotices(t, hub)
			hub.noteReadinessActions("node-a", readinessWith(t3, "agy", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent}), t3)
			settleOneTimeNotices(t, hub)
			hub.oneTimeMu.Lock()
			if !hub.oneTimeActions[key].GoneSince.Equal(t3) {
				hub.oneTimeMu.Unlock()
				t.Fatalf("the clean report at t3 did not start the countdown")
			}
			hub.oneTimeMu.Unlock()

			hub.noteReadinessActions("node-a", readinessWith(t4, "agy", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginMissing}), t4)
			settleOneTimeNotices(t, hub)
			hub.oneTimeMu.Lock()
			state := hub.oneTimeActions[key]
			hub.oneTimeMu.Unlock()
			if !state.GoneSince.IsZero() {
				t.Fatalf("GoneSince = %v after a newer t4 sighting, want it reset", state.GoneSince)
			}
		})
	}
}
