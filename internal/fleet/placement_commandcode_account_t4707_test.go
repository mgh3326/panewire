package fleet

import (
	"context"
	"strings"
	"testing"
	"time"
)

// hk 4707 review: between the plan and the remote admission a new presence
// announcement can move the selected Command Code account out of the host's
// window. The host then holds a different account, and admission (judged on the
// key remoteAccount returned) and the delegated spawn (which carries the selected
// key) would disagree. The host is refused instead, in the plan and in the spawn.
func TestPlacementRefusesAHostWhoseCommandcodeAccountChanged(t *testing.T) {
	for _, mode := range []string{"plan", "auto"} {
		for _, moved := range []bool{false, true} {
			name := mode + "/still held"
			if moved {
				name = mode + "/moved out of the window"
			}
			t.Run(name, func(t *testing.T) {
				src := &fakePlacementSource{hosts: []HostView{{Name: "host-b", Connected: true}}}
				spawns := 0
				a, s, _ := remoteFixture(t, src, &fakeWaker{}, "2026-01-01T00:00:00Z", func(args string) Result {
					if strings.Contains(args, "_spawn-stdin") {
						spawns++
						return Result{Out: "OK pane=w:p1 job=job-a\n"}
					}
					return Result{}
				})
				// The first presence read (candidate enumeration) sees the selected
				// account held; with moved set, every later read (the remote
				// admission) sees it announced 40 minutes ago, outside the window.
				reads := 0
				src.presenceDo = func() []PresenceRecord {
					reads++
					selectedSeen := a.Now().Add(-time.Minute)
					if moved && reads > 1 {
						selectedSeen = a.Now().Add(-40 * time.Minute)
					}
					return []PresenceRecord{
						{HostID: "presence-b", AccountKey: ccKeyA, LastSeen: selectedSeen},
						{HostID: "presence-b", AccountKey: ccKeyB, LastSeen: a.Now()},
					}
				}
				src.accounts = map[string]SharedAccount{
					ccKeyA: {Key: ccKeyA, Observed: true, Fresh: true, MeasuredAt: a.Now()},
					ccKeyB: {Key: ccKeyB, Observed: true, Fresh: true, MeasuredAt: a.Now()},
				}
				a.Config.Hosts = map[string]map[string]any{"host-b": {"presence_id": "presence-b", "cwd_map": map[string]any{s.CWD: "/srv/repo"}}}
				a.Config.HostOrder = []string{"host-b"}
				row := ompRow("omp-opus", "anthropic/claude-opus-5-5", "commandcode")
				q := &fakeQuota{CatalogRows: []CatalogRow{row}, PoolViews: map[string]PoolView{"commandcode": {Status: "ok", Class: "spend", Blocking: ptrF(1)}}}
				q.LaunchValue = Launch{Profile: row.Profile, Effort: row.Effort, ModelID: row.ModelID, Grade: row.Grade, Pool: row.Pool, Gate: row.Gate}
				var gateKeys []string
				admit := gatePools(map[string]string{"omp-opus": "commandcode"})
				q.GateDo = func(r QuotaRequest) (string, error) {
					gateKeys = append(gateKeys, r.AccountKey)
					return admit(r)
				}
				a.Quota = q
				serveOMPRouting(t, a)
				s.Profile, s.Effort, s.T, s.AccountKey = "omp-opus", "high", "T1", ccKeyA

				var err error
				host := ""
				if mode == "plan" {
					_, host, err = a.planHosts(context.Background(), s, "A")
				} else {
					_, _, err = a.place(context.Background(), s, "fixture brief")
				}
				if !moved {
					if err != nil || mode == "plan" && host != "host-b" || mode == "auto" && spawns != 1 {
						t.Fatalf("selected account still held: host=%q err=%v spawns=%d", host, err, spawns)
					}
				} else if err == nil || host != "" || spawns != 0 {
					t.Fatalf("a host whose account changed must be refused: host=%q err=%v spawns=%d", host, err, spawns)
				}
				for _, key := range gateKeys {
					if key == ccKeyB {
						t.Fatalf("admission judged account B for a spawn that carries %s", ccKeyA)
					}
				}
			})
		}
	}
}
