package panewire

import (
	"os"
	"strings"
	"testing"
)

// The detached `_watch` children record a running job's result (hk 4345), so
// the service definitions shipped in deploy/ must not let the service manager
// kill them when the daemon is stopped for a rollout.
func TestDeployUnitsLetWatchersOutliveDaemon(t *testing.T) {
	unit, err := os.ReadFile("deploy/panewired.service")
	if err != nil {
		t.Fatal(err)
	}
	if !hasUnitSetting(string(unit), "KillMode=process") {
		t.Error("deploy/panewired.service must set KillMode=process: control-group kills the watchers, and mixed SIGKILLs them once the daemon exits")
	}
	if !hasUnitSetting(string(unit), "TimeoutStopSec=60") {
		t.Error("deploy/panewired.service must allow the daemon's 30 s stop drain: TimeoutStopSec=60")
	}
	plist, err := os.ReadFile("deploy/dev.panewire.panewired.plist")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(plist), "<key>AbandonProcessGroup</key><true/>") {
		t.Error("the plist must set AbandonProcessGroup so launchd leaves the watchers running")
	}
	if !strings.Contains(string(plist), "<key>ExitTimeOut</key><integer>60</integer>") {
		t.Error("the plist must allow the daemon's 30 s stop drain: ExitTimeOut 60")
	}
}

func hasUnitSetting(unit, setting string) bool {
	for _, line := range strings.Split(unit, "\n") {
		if strings.TrimSpace(line) == setting {
			return true
		}
	}
	return false
}
