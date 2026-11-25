package panewire

import "github.com/mgh3326/panewire/internal/fleet"

// hubSpawnIdentity is what a hub spawn's argv names, read as the launcher reads
// it (fleet.ParseSpawnIdentity). An argv the launcher cannot read names
// nothing: it is never launched.
func hubSpawnIdentity(args []string) fleet.SpawnIdentity {
	identity, _ := fleet.ParseSpawnIdentity(args)
	return identity
}

// hubStoredSpawnIdentity reads durable spawn argv, which can outlive flags
// retired after the record was written.
func hubStoredSpawnIdentity(args []string) fleet.SpawnIdentity {
	identity, _ := fleet.ParseSpawnIdentityIgnoring(args, []string{"builder-family", "routing-role"})
	return identity
}
