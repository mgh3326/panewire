package panewire

import "github.com/mgh3326/panewire/internal/fleet"

// hubSpawnIdentity is what a hub spawn's argv names, read as the launcher reads
// it (fleet.ParseSpawnIdentity). An argv the launcher cannot read names
// nothing: it is never launched.
func hubSpawnIdentity(args []string) fleet.SpawnIdentity {
	identity, _ := fleet.ParseSpawnIdentity(args)
	return identity
}
