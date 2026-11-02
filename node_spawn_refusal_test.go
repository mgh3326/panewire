package panewire

import (
	"strings"
	"testing"
)

func TestNodeSpawnRefusalsNameRepair(t *testing.T) {
	for _, test := range []struct{ code, rule, want string }{{"spawn_disabled", "workspace_required", "herdr workspace list"}, {"spawn_disabled", "config_json_invalid", "remove retired runtug_path"}, {"checkout_refused:work_root_unset", "", "spawn.json.work_root"}, {"checkout_refused:busy", "", "same saved request"}, {"timeout", "", "same node journal"}, {"cwd_unmapped", "", "spawn.json.cwd_map"}} {
		if fix := nodeSpawnRefusalFix(test.code, test.rule); !strings.Contains(fix, test.want) {
			t.Fatalf("%s/%s fix=%q", test.code, test.rule, fix)
		}
	}
}
