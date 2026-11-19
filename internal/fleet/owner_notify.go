package fleet

// Owner notification mode (hk 5360, operator decision 433, split agreed with
// mergewake-design). A requester passes --owner-notify final|never on a spawn
// to say what the owner lane hears about that spawn. The owner lane is the
// --owner lane; the requester's own copy of a failure is never affected,
// because that copy is what makes the requester replay.
//
//   - never: the owner lane gets none of the spawn's notices (SPAWN_FAILED,
//     SPAWNED, LANE_UNREGISTERED, job.completed, delivery-unconfirmed). Each is
//     recorded and counted per cause in the loops daily row.
//   - final: the owner lane gets only a terminal notice: a failure that cannot
//     be replayed (replay=no) or a given-up request. Every other notice is
//     recorded and counted, whatever its cause.
//   - absent: the cause-based rules of hub_spawn_notice_route.go decide.
//
// A node that predates OwnerNotifyCapability refuses the unknown flag and
// drops the whole spawn, so the hub strips the flag before it sends the spawn
// to such a node (withoutSpawnFlag). The hub still applies the mode from the
// request it saved, so the owner's notices follow the requester's choice on
// every node.

import "slices"

// OwnerNotifyArg is the spawn argument that carries the owner notification mode.
const OwnerNotifyArg = "--owner-notify"

// OwnerNotifyFinal and OwnerNotifyNever are the two values --owner-notify takes.
const (
	OwnerNotifyFinal = "final"
	OwnerNotifyNever = "never"
)

// OwnerNotifyCapability is the feature name a requester gates on. A node
// advertises it in its hello, the hub advertises it in its own capability
// list, and `panewire version --json` lists it.
const OwnerNotifyCapability = "owner_notify_v1"

// ValidOwnerNotify reports whether value is an accepted --owner-notify value.
// The empty value means the flag is absent.
func ValidOwnerNotify(value string) bool {
	return value == "" || value == OwnerNotifyFinal || value == OwnerNotifyNever
}

// FeatureCapabilities is the closed list of feature names panewire advertises
// on this build: `panewire version --json` prints it so a requester can gate on
// a feature before it passes the flag.
func FeatureCapabilities() []string {
	return []string{OwnerNotifyCapability}
}

// HasFeatureCapability reports whether this build advertises feature.
func HasFeatureCapability(feature string) bool {
	return slices.Contains(FeatureCapabilities(), feature)
}
