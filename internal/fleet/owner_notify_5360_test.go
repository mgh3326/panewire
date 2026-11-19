package fleet

import (
	"context"
	"slices"
	"testing"
)

// hk 5360: --owner-notify takes final or never; anything else is a usage error
// and never reaches the hub.

func TestOwnerNotifyValuesAccepted_5360(t *testing.T) {
	for value, want := range map[string]bool{"": true, "final": true, "never": true, "Never": false, "sometimes": false, "always": false} {
		if got := ValidOwnerNotify(value); got != want {
			t.Fatalf("ValidOwnerNotify(%q)=%v, want %v", value, got, want)
		}
	}
}

func TestOwnerNotifyAdvertisedAsFeature_5360(t *testing.T) {
	if !HasFeatureCapability("owner_notify_v1") || !slices.Contains(FeatureCapabilities(), OwnerNotifyCapability) {
		t.Fatalf("feature capabilities %v do not advertise owner_notify_v1", FeatureCapabilities())
	}
}

func TestParseSpawnRejectsUnknownOwnerNotify_5360(t *testing.T) {
	a, s, _ := urgencyFixture(t, "3", nil, nil, nil)
	s.OwnerNotify = "sometimes"
	s.Profile = "auto"
	if e := a.Spawn(context.Background(), s); e == nil || exitCode(e) != 2 {
		t.Fatalf("unknown --owner-notify must be a usage error: %v", e)
	}
}

// The requester's value travels in the submitted args, so the hub can apply
// the mode from the saved request.
func TestOwnerNotifyReachesSubmittedArgs_5360(t *testing.T) {
	args := submissionArgs(Spawn{OwnerNotify: OwnerNotifyNever})
	for i := range args {
		if args[i] == OwnerNotifyArg {
			if i+1 < len(args) && args[i+1] == OwnerNotifyNever {
				return
			}
			t.Fatalf("args %v carry --owner-notify without its value", args)
		}
	}
	t.Fatalf("args %v do not carry --owner-notify", args)
}
