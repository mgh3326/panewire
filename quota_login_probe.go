package panewire

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Each host proves its own harness login (docs/fleet-state.md, "Login
// probe"). A shared quota reading is per account, so a host whose shared
// reading is fresh never uses its own credential and cannot learn that the
// credential stopped working. Two node-side signals close that gap:
//
//   - the login probe: a measure-only call with this host's credential, at most
//     once per `[quota].login_probe_interval`, published nowhere;
//   - the login screen: a spawn that ended rc=76 whose saved screen capture is
//     the harness's login screen.
//
// Both only add to the reports of the node's quota loop (readiness_node.go
// reads them beside the loop's own note).

// loginScreenCaptureMax bounds the capture read: the node saves 80 visible
// lines, so a larger file is not a capture it wrote.
const loginScreenCaptureMax = 64 << 10

// nodeLoginScreenTTL is how long a login-screen mark stays reported when
// nothing else clears it. A host with the mark is not launched on, so no
// launch could clear it once the host owner has logged in; after the TTL the
// pair gets one more launch, as with a first-run dialog.
const nodeLoginScreenTTL = nodeDialogTTL

// loginProbeOutcome is what one probe of a pool found.
type loginProbeOutcome int

const (
	// loginProbeSkipped: the pool is not probed from this host (no measurable
	// account), so nothing is known.
	loginProbeSkipped loginProbeOutcome = iota
	// loginProbeInconclusive: the measurement failed for a reason that says
	// nothing about the login (network, rate limit, timeout).
	loginProbeInconclusive
	loginProbeLoginPresent
	loginProbeLoginMissing
)

// measurableHere says whether this host lists the account it holds for the
// pool as measurable (`[quota].measurable_accounts`). A host with none has a
// configuration gap, not a login problem (hk 3783), so it is never probed.
func (s *nativeQuotaSource) measurableHere(ctx context.Context) bool {
	if s.Pool == "agy" {
		fp, measurable := s.agyAccount()
		return fp != "" && measurable
	}
	_, accountFP, _ := s.poolMeasurement()
	fp, err := accountFP(ctx)
	if err != nil || fp == "" {
		return false
	}
	listed := fleet.HostMeasurableAccount
	if s.MeasurableAccount != nil {
		listed = s.MeasurableAccount
	}
	return listed(s.Pool + ":" + fp)
}

// probeLogin measures the pool once with this host's credential and reports
// what that says about the login. It touches no shared store: no lease, no
// publication, no presence.
func (s *nativeQuotaSource) probeLogin(ctx context.Context) loginProbeOutcome {
	if !s.measurableHere(ctx) {
		return loginProbeSkipped
	}
	_, err := s.measureOnly(ctx)
	switch {
	case err == nil:
		return loginProbeLoginPresent
	case ctx.Err() != nil:
		// A timed-out measurement says nothing about the login.
		return loginProbeInconclusive
	case fleetQuotaLoginMissingKinds[quotaErrorKind(err)]:
		return loginProbeLoginMissing
	}
	return loginProbeInconclusive
}

// loginProbeInterval is the configured probe interval; zero is disabled.
func (client *HubClient) loginProbeInterval() time.Duration {
	if client.loginProbeIntervalOf != nil {
		return client.loginProbeIntervalOf()
	}
	return fleet.HostLoginProbeInterval()
}

// loginProbeDue says whether pool's probe is due, and marks it started: at most
// one probe per interval, whatever it finds.
func (client *HubClient) loginProbeDue(pool string, interval time.Duration) bool {
	now := client.nowUTC()
	client.readiness.mu.Lock()
	defer client.readiness.mu.Unlock()
	if last, probed := client.readiness.probedAt[pool]; probed && !last.After(now) && now.Sub(last) < interval {
		return false
	}
	if client.readiness.probedAt == nil {
		client.readiness.probedAt = map[string]time.Time{}
	}
	client.readiness.probedAt[pool] = now
	return true
}

// probeFleetLogin is the login probe of one pool. The node quota loop calls it
// after the pool's own measurement in the same goroutine, so the two never run
// together. A login failure marks the pool's login missing as a failed
// measurement does; a pass clears it; anything else changes nothing.
func (client *HubClient) probeFleetLogin(ctx context.Context, pool string) {
	if !isNativeQuotaPool(pool) {
		return
	}
	interval := client.loginProbeInterval()
	if interval <= 0 || !client.loginProbeDue(pool, interval) {
		return
	}
	run, cancel := context.WithTimeout(ctx, fleetQuotaTimeout)
	defer cancel()
	switch client.nativeQuotaSource(pool).probeLogin(run) {
	case loginProbeLoginMissing:
		client.noteLoginProbe(pool, true)
	case loginProbeLoginPresent:
		client.noteLoginProbe(pool, false)
	}
}

// noteLoginProbe records what a probe found. A pass also forgets a login-screen
// mark: this host's credential works now. A change asks for a readiness
// refresh and logs one line, never the measurement or the credential.
func (client *HubClient) noteLoginProbe(pool string, missing bool) {
	client.readiness.mu.Lock()
	changed := client.readiness.probeMissing[pool] != missing
	if missing {
		if client.readiness.probeMissing == nil {
			client.readiness.probeMissing = map[string]bool{}
		}
		client.readiness.probeMissing[pool] = true
	} else {
		delete(client.readiness.probeMissing, pool)
		if _, marked := client.readiness.loginScreens[pool]; marked {
			delete(client.readiness.loginScreens, pool)
			changed = true
		}
	}
	if changed {
		client.readiness.dirty = true
	}
	client.readiness.mu.Unlock()
	if !changed {
		return
	}
	if missing {
		client.warnMessage(fmt.Sprintf("fleet state: login probe for pool %s failed with its own credential; harness login missing", pool))
		return
	}
	client.warnMessage(fmt.Sprintf("fleet state: login probe for pool %s passed; harness login present", pool))
}

// launchShowedLoginScreen reads the newest spawn-failure capture of job, the
// screen the executing node saved before it closed the pane
// (internal/fleet/spawn.go captureFailure), and says whether it is kind's login
// screen. No jobs root, no capture or an unreadable one is not a login screen.
func (client *HubClient) launchShowedLoginScreen(kind, job string) bool {
	if job == "" {
		return false
	}
	dir, _, found, err := client.openNodeJobDir(job)
	if err != nil || !found {
		return false
	}
	defer dir.Close()
	entries, err := fs.ReadDir(dir.FS(), ".")
	if err != nil {
		return false
	}
	var newest string
	var newestAt time.Time
	for _, entry := range entries {
		match, _ := filepath.Match("spawn-failure-*.txt", entry.Name())
		if !match || !entry.Type().IsRegular() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if newest == "" || info.ModTime().After(newestAt) {
			newest, newestAt = entry.Name(), info.ModTime()
		}
	}
	if newest == "" {
		return false
	}
	file, err := dir.Open(newest)
	if err != nil {
		return false
	}
	defer file.Close()
	screen, err := io.ReadAll(io.LimitReader(file, loginScreenCaptureMax))
	if err != nil {
		return false
	}
	return harnessLoginScreen(kind, string(screen))
}

// loginScreenFresh says whether kind has a login-screen mark still inside the
// TTL.
func loginScreenFresh(marks map[string]time.Time, kind string, now time.Time) bool {
	at, marked := marks[kind]
	return marked && !at.After(now) && now.Sub(at) < nodeLoginScreenTTL
}
