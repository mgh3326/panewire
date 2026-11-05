package panewire

import (
	"strings"
	"testing"
)

func TestSpawnedNoticeNamesUnregisteredLane(t *testing.T) {
	ok := hubSpawnOutcome{Host: "node-b", Profile: "sonnet", Pane: "w:p9", Model: "m"}
	if text := spawnNotificationText("job", "req", ok); strings.Contains(text, "LANE_UNREGISTERED") || spawnNotificationType(ok) != "report" {
		t.Fatalf("registered spawn notice changed: %q %s", text, spawnNotificationType(ok))
	}
	ok.LaneUnregistered = true
	text := spawnNotificationText("job", "req", ok)
	if !strings.HasPrefix(text, "[panewire] SPAWNED job ") || !strings.Contains(text, "LANE_UNREGISTERED") {
		t.Fatalf("SPAWNED notice does not name the problem: %q", text)
	}
	if spawnNotificationType(ok) != "automation-failed" {
		t.Fatalf("unregistered lane notice type=%s, want the loud type", spawnNotificationType(ok))
	}
}
