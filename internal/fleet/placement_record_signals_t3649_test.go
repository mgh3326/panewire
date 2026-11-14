package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// (d) The hk placement record gets the fields it already names from the hub's
// load signals: cpu_busy_short, signal_age_s and power_source. The hub also sends
// swap_out_per_s and user_idle_s, which the record does not carry.
func TestPlacementRecordTakesNodeLiveSignalsFromHubState(t *testing.T) {
	a := testApp(t)
	var snap FleetSnapshot
	data := fmt.Sprintf(`{"v":1,"at":%q,"states":[{"host":"host-b","kind":"publisher","publisher":{"connected":true,"last_seen":%q}},{"host":"host-b","kind":"load","measured_at":%q,"load":{"connected":true,"score":123,"signals":{"cpu_busy_short":0.17,"signal_age_s":7,"power_source":"battery","swap_in_per_s":2,"swap_out_per_s":500,"pressure":0.5,"user_idle_s":0}}}]}`, a.Now().Format(time.RFC3339), a.Now().Format(time.RFC3339), a.Now().Format(time.RFC3339))
	if err := json.Unmarshal([]byte(data), &snap); err != nil {
		t.Fatal(err)
	}
	a.Config.Hub = map[string]any{"hub_url": "https://hub.invalid"}
	a.fleetLog = &fleetStateLog{snapshot: &snap, fetchedAt: a.Now()}
	hosts, why := a.hubHosts(context.Background())
	if why != "" || len(hosts) != 1 {
		t.Fatalf("hub state hosts=%+v why=%q", hosts, why)
	}
	signals := hosts[0].Signals
	if signals.CPUBusyShort == nil || *signals.CPUBusyShort != 0.17 || signals.SignalAgeS == nil || *signals.SignalAgeS != 7 || signals.PowerSource != "battery" {
		t.Fatalf("record signals=%+v; want cpu_busy_short 0.17, signal_age_s 7, power_source battery", signals)
	}
	encoded, err := json.Marshal(PlacementCandidateRecord{Host: "host-b", Outcome: "skipped", Reason: "not_chosen", PlacementSignals: signals})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"cpu_busy_short":0.17`, `"signal_age_s":7`, `"power_source":"battery"`} {
		if !strings.Contains(string(encoded), want) {
			t.Fatalf("hk record omitted %s: %s", want, encoded)
		}
	}
	for _, unwanted := range []string{"swap_out_per_s", "user_idle_s", "swap_in_per_s", "pressure"} {
		if strings.Contains(string(encoded), unwanted) {
			t.Fatalf("hk record gained %s, which the record contract does not name: %s", unwanted, encoded)
		}
	}
}
