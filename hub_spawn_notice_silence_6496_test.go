package panewire

import (
	"fmt"
	"testing"
	"time"
)

// hk 6496 (PR #884 finding): workspace_lock_root_unsafe never clears on a replay,
// so a design owner must hear it at once like the other must-fix causes. The
// list is read from two copies: the embedded delivery-policy.yaml, which a
// default hub runs, and defaultSpawnNoticeMustFix, which applies when a policy
// file omits the key. Both must name the cause.
func TestWorkspaceLockRootUnsafeIsMustFix_6496(t *testing.T) {
	const reason = "spawn_failed:workspace_lock_root_unsafe: workspace lock root /tmp/panewire-501 is not a private directory of uid 501 (owner 502, mode 700, link false); remove it as its owner or root, then retry"
	const cause = "workspace_lock_root_unsafe"

	embedded, err := parseDeliveryPolicy(defaultDeliveryPolicy)
	if err != nil {
		t.Fatal(err)
	}
	if !embedded.spawnNoticeMustFix(spawnNoticeCause(reason)) {
		t.Fatalf("embedded delivery-policy.yaml must-fix list=%v, want %s", embedded.Relay.SpawnNoticeMustFix, cause)
	}
	builtin, err := parseDeliveryPolicy([]byte("default: interrupt\nrelay:\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !builtin.spawnNoticeMustFix(cause) {
		t.Fatalf("built-in must-fix list=%v, want %s", builtin.Relay.SpawnNoticeMustFix, cause)
	}

	// End to end: a design owner whose spawn has a distinct requester hears the
	// first refusal at once, and it is not counted as silenced.
	now := time.Date(2026, 11, 25, 3, 0, 0, 0, time.UTC)
	h, _ := s5360Hub(t, &now)
	s5360Spawn(t, h, fmt.Sprintf("pw-6496-lock-root-%d", len(reason)), nil, hubSpawnOutcome{RC: 70, Stage: "placement", Reason: reason, Fix: "Remove the named directory, then retry.", Host: "rpi", Retryable: true})
	s5360Flush(t, h)
	items := t4270Items(t, h, s5360Owner)
	if len(items) != 1 {
		t.Fatalf("owner items=%d, want the first %s failure told at once", len(items), cause)
	}
	if n := s5360OwnerSilenced(t, h, now); n != 0 {
		t.Fatalf("a must-fix cause was counted as silenced (%d)", n)
	}
}
