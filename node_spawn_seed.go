package panewire

import (
	"context"
	"log/slog"
	"time"
)

const (
	spawnSeedHoldMargin    = 2 * time.Minute
	spawnSeedLegacyHold    = 10 * time.Minute
	spawnSeedReadErrorHold = 2 * time.Minute
)

type seededSpawnHold struct {
	key, root string
	releaseAt time.Time
}

// startSpawnRecovery closes admission before starting the seed read. The seed
// and the first settle pass share the startup goroutine, but admission can
// resume as soon as the snapshot is installed.
func (client *HubClient) startSpawnRecovery(ctx context.Context, processStart time.Time) {
	slots := client.nodeSpawnSlots()
	client.spawnSlotsAtStart = slots
	if slots > 1 {
		a := client.nodeAdmission()
		a.mu.Lock()
		a.seeding = true
		a.mu.Unlock()
	}
	go func() {
		if slots > 1 {
			client.seedSpawnAdmission(ctx, processStart, client.readSpawnSeed)
		}
		client.runSpawnSettle(ctx)
	}()
}

func (client *HubClient) readSpawnSeed(ctx context.Context) ([]executingSpawn, error) {
	journal, err := client.acquireSpawnJournal()
	if err != nil {
		return nil, err
	}
	defer client.releaseSpawnJournal()
	return journal.executing(ctx)
}

func (client *HubClient) seedSpawnAdmission(ctx context.Context, processStart time.Time, read func(context.Context) ([]executingSpawn, error)) {
	const errorSite = "node_spawn_seed.go:executing"
	a := client.nodeAdmission()
	for delay := time.Second; ; delay *= 2 {
		// A replay can settle a row during startup. Serialize its durable
		// completion with taking and publishing the seed snapshot.
		client.spawnActiveMu.Lock()
		readCtx, cancel := context.WithTimeout(ctx, spawnSettleTimeout)
		rows, err := read(readCtx)
		cancel()
		if err == nil {
			a.installSpawnSeed(rows, processStart)
			client.spawnActiveMu.Unlock()
			client.nodeLoopClearSilent(errorSite)
			return
		}
		client.spawnActiveMu.Unlock()
		if ctx.Err() != nil {
			return
		}
		if delay > 4*time.Second {
			slog.Warn("spawn admission seed failed", "error", err)
			client.nodeLoopNoteSilentError(nodeSpawnSettle, errorSite, err)
			a.mu.Lock()
			a.seedFailedUntil = processStart.Add(spawnSeedReadErrorHold)
			a.seeding = false
			a.wakeLocked()
			a.mu.Unlock()
			return
		}
		if waitHubRetry(ctx, delay) != nil {
			return
		}
	}
}

func (a *nodeSpawnAdmission) installSpawnSeed(rows []executingSpawn, processStart time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	holds := make(map[string]seededSpawnHold)
	now := a.now()
	for _, row := range rows {
		legacy := row.CreatedMS == 0
		if row.Key == "" && !row.HasCheckout && !legacy {
			continue
		}
		releaseAt := time.UnixMilli(max(row.DeadlineMS, row.StartByMS)).Add(spawnSeedHoldMargin)
		if legacy {
			releaseAt = processStart.Add(spawnSeedLegacyHold)
		}
		if now.Before(releaseAt) {
			holds[row.RequestID] = seededSpawnHold{key: row.Key, root: row.RootReal, releaseAt: releaseAt}
		}
	}
	a.seeded = holds
	a.seeding = false
	a.wakeLocked()
}

func (a *nodeSpawnAdmission) releaseSeededSpawn(requestID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.seeded[requestID]; ok {
		delete(a.seeded, requestID)
		a.clearIdleRoot()
		a.wakeLocked()
	}
}

// seededAdmission runs with the admission lock held. Waiters arm their
// own timer for the next expiry even when settlement makes no progress.
func (a *nodeSpawnAdmission) seededAdmission(key string) (blocked bool, recheck time.Duration) {
	if len(a.seeded) == 0 && a.seedFailedUntil.IsZero() {
		return false, 0
	}
	now := a.now()
	if recheck = a.seedFailedUntil.Sub(now); recheck > 0 {
		return true, recheck
	}
	a.seedFailedUntil = time.Time{}
	recheck = 0
	for id, hold := range a.seeded {
		wait := hold.releaseAt.Sub(now)
		if wait <= 0 {
			delete(a.seeded, id)
			continue
		}
		if recheck == 0 || wait < recheck {
			recheck = wait
		}
		blocked = blocked || key == "" || hold.key == "" || spawnOverlapConflicts(hold.key, key)
	}
	a.clearIdleRoot()
	return blocked, recheck
}

func (a *nodeSpawnAdmission) hasSeededRoot() bool {
	for _, hold := range a.seeded {
		if hold.root != "" {
			return true
		}
	}
	return false
}

func (a *nodeSpawnAdmission) matchesSeededRoot(real string) bool {
	for _, hold := range a.seeded {
		if hold.root != "" && hold.root != real {
			return false
		}
	}
	return true
}
