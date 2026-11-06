package devin

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// unknownResetHold is how long a limit record with no readable reset stays
// active, as pacelever's UnknownResetHold.
const unknownResetHold = 60 * time.Minute

// limitRecord is one entry of pacelever's devin-free-limit.json: an exceed of
// devin's SWE-2 free tier recorded by `panewire quota observe devin-free-limit`
// (pacelever `observe`). Only the times matter here.
type limitRecord struct {
	ResetAt    string `json:"reset_at"`
	ObservedAt string `json:"observed_at"`
}

// effectiveReset is when the record stops holding. An unreadable reset falls
// back to unknownResetHold after the observation, so a damaged record holds
// instead of admitting; it errors only when neither time is usable.
func (r limitRecord) effectiveReset() (time.Time, bool) {
	if reset, err := time.Parse(time.RFC3339Nano, r.ResetAt); err == nil {
		return reset, true
	}
	observed, err := time.Parse(time.RFC3339Nano, r.ObservedAt)
	if err != nil {
		return time.Time{}, false
	}
	return observed.Add(unknownResetHold), true
}

func longFingerprint(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:16])
}

// limitIdentities are the keys pacelever may file this host's record under: the
// account when a snapshot named one, else this host and user. A host that
// measures has a login, so both are looked at.
func (c Config) limitIdentities() []string {
	host := ""
	if c.Hostname != nil {
		host, _ = c.Hostname()
	} else {
		host, _ = os.Hostname()
	}
	uid := os.Getuid()
	if c.UID != nil {
		uid = c.UID()
	}
	ids := []string{"host-user-" + longFingerprint(host+"\x00"+strconv.Itoa(uid))}
	if fp := AccountFP(c); fp != "" {
		ids = append(ids, "account-"+longFingerprint("account\x00"+fp))
	}
	return ids
}

// cacheDir is pacelever's cache directory: PACELEVER_CACHE's directory, else
// <XDG cache>/pacelever.
func (c Config) cacheDir() string {
	if p := c.env("PACELEVER_CACHE"); p != "" {
		return filepath.Dir(p)
	}
	return filepath.Join(c.cacheBase(), "pacelever")
}

// LocalFree is this host's own free-tier state at now, read from the record
// file pacelever and `observe devin-free-limit` write: exhausted with the
// record's reset while one holds, unknown when a record's times cannot be read
// (a damaged record holds, it does not admit), else available. A record file
// that is present but cannot be read, parsed or has no `records` object is unknown too, never available:
// only an absent file means no exceed was ever recorded. When several records
// hold, the one that ends last wins.
func LocalFree(cfg Config) func(now time.Time) *shared.FreeBucket {
	return func(now time.Time) *shared.FreeBucket {
		// records must be present and an object: {} or "records": null is a
		// truncated store that may have held an active exceed.
		var store struct {
			Records *map[string]limitRecord `json:"records"`
		}
		data, err := cfg.readFileErr(filepath.Join(cfg.cacheDir(), "devin-free-limit.json"))
		switch {
		case errors.Is(err, fs.ErrNotExist):
			return &shared.FreeBucket{State: shared.FreeAvailable}
		case err != nil || !decodeJSONObject(data, &store) || store.Records == nil:
			return &shared.FreeBucket{State: shared.FreeUnknown}
		}
		records := *store.Records
		var holds *time.Time
		unknown := false
		for _, id := range cfg.limitIdentities() {
			record, ok := records[id]
			if !ok {
				continue
			}
			reset, ok := record.effectiveReset()
			switch {
			case !ok:
				unknown = true
			case reset.After(now) && (holds == nil || reset.After(*holds)):
				holds = &reset
			}
		}
		switch {
		case holds != nil:
			return &shared.FreeBucket{State: shared.FreeExhausted, ResetsAt: holds}
		case unknown:
			return &shared.FreeBucket{State: shared.FreeUnknown}
		}
		return &shared.FreeBucket{State: shared.FreeAvailable}
	}
}
