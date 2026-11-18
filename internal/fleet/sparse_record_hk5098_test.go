package fleet

import (
	"testing"
)

// hk 5098: the job record says which checkout the node prepared, so a reader
// (mergewake's reviewer paragraph) knows the absent paths are absent on
// purpose. A spawn that names no spec carries no key at all.

func TestSpawnRecordCarriesCheckoutMode(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode string
		want any
	}{
		{"sparse", "sparse", "sparse"},
		{"full", "full", "full"},
		{"no checkout spec", "", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, _, _ := cmdFixture(t)
			s.checkoutMode = tc.mode
			if err := a.Spawn(t.Context(), s); err != nil {
				t.Fatal(err)
			}
			events, err := a.events(s.Job)
			if err != nil {
				t.Fatal(err)
			}
			var spawned *Event
			for i := range events {
				if events[i].Kind == "job.spawned" {
					spawned = &events[i]
				}
			}
			if spawned == nil {
				t.Fatal("no job.spawned record")
			}
			got, present := spawned.Payload["checkout_mode"]
			if tc.want == nil {
				if present {
					t.Fatalf("a spawn with no checkout spec carries checkout_mode=%v", got)
				}
				return
			}
			if !present || got != tc.want {
				t.Fatalf("checkout_mode=%v (present=%v), want %v", got, present, tc.want)
			}
		})
	}
}
