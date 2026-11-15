package fleet

import (
	"net/http"
	"testing"
)

// screenDocPut is the fake hk's PUT of job's screen document, or nil.
func screenDocPut(t *testing.T, hk *spawnHK, job string) map[string]any {
	t.Helper()
	for _, r := range hk.Requests() {
		if r.Route() == docRoute(http.MethodPut, "screens/"+job+"/pane-screen.txt") {
			return hkJSON(t, r)
		}
	}
	return nil
}
