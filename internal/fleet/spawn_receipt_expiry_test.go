package fleet

import (
	"testing"
	"time"
)

// The receipt's expires_at is the hub's request expiry when the frame carried
// it, else --expires, else the role's default from now. The pane sweep reads it
// to end a reviewer or consult by its real limit (hk 4747).
func TestSpawnExpiresAtRecordsTheRequestLimit(t *testing.T) {
	a := testApp(t)
	base := a.Now()
	hub := base.Add(4 * time.Hour)
	for name, c := range map[string]struct {
		spawn Spawn
		want  time.Time
	}{
		"hub expiry":                      {Spawn{Role: "reviewer", hubExpiresAt: hub}, hub},
		"hub expiry wins over --expires":  {Spawn{Role: "reviewer", Expires: "90m", hubExpiresAt: hub}, hub},
		"--expires":                       {Spawn{Role: "consult", Expires: "90m"}, base.Add(90 * time.Minute)},
		"reviewer default":                {Spawn{Role: "reviewer"}, base.Add(time.Hour)},
		"implementer default":             {Spawn{Role: "implementer"}, base.Add(6 * time.Hour)},
		"an unknown role keeps 6h":        {Spawn{Role: "builder"}, base.Add(6 * time.Hour)},
		"no role keeps 6h":                {Spawn{}, base.Add(6 * time.Hour)},
		"an invalid --expires is ignored": {Spawn{Role: "reviewer", Expires: "soon"}, base.Add(time.Hour)},
		"counsel is a consult":            {Spawn{Role: "counsel"}, base.Add(time.Hour)},
	} {
		t.Run(name, func(t *testing.T) {
			if got := a.spawnExpiresAt(c.spawn); !got.Equal(c.want) {
				t.Fatalf("got %s want %s", got, c.want)
			}
		})
	}
}
