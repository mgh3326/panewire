package panewire

import "github.com/mgh3326/panewire/internal/fleet"

// consultDesignLanes is the design-lane classifier the consult SS floor uses
// for a loaded hub relay policy (hk 4241). An absent relay.design_lanes or the
// default one (*-design, as the embedded policy writes it) keeps the built-in
// list (*-design, routing-policy, fleet-policy), but a lane the loaded policy
// names in relay.desk_lanes is never a design lane, as the hub's own rule.
// Only an explicitly loaded list, including an explicit empty one, uses the
// policy's own classifier, which already excludes desk lanes. nil means no
// policy is loaded, so fleet keeps the built-in list too. Hub spawns and CLI
// spawn/resolve both go through here, so they agree.
func consultDesignLanes(p *deliveryPolicy) func(string) bool {
	if p == nil {
		return nil
	}
	if d := p.Relay.DesignLanes; len(d) == 1 && d[0] == defaultDesignLanePattern {
		return func(lane string) bool { return !p.deskLane(lane) && fleet.BuiltInDesignLane(lane) }
	}
	return p.designLane
}
