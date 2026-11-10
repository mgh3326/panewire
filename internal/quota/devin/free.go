package devin

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mgh3326/panewire/internal/durable"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// unknownResetHold is how long a limit record with no readable reset stays
// active, as pacelever's UnknownResetHold.
const unknownResetHold = 60 * time.Minute

// limitRecord is one entry of the compatible devin-free-limit.json file.
type limitRecord struct {
	ResetAt    string `json:"reset_at"`
	ObservedAt string `json:"observed_at"`
	Rung       string `json:"rung,omitempty"`
	Source     string `json:"source,omitempty"`
	Unparsed   bool   `json:"unparsed,omitempty"`
}

const limitRecordSchema = "pacelever.devin-free-limit.v1"

var (
	ErrInvalidFreeLimitObservation = errors.New("invalid devin free-limit observation")
	ErrFreeLimitStore              = errors.New("devin free-limit store failure")
)

// ObserveFreeLimit records or clears a local devin free-tier limit observation
// in the compatible cache format. An unparsed reset is held for one hour.
func ObserveFreeLimit(cfg Config, resetAt, observedAt, rung, source, accountKey string, unparsed, clear bool) error {
	path := filepath.Join(cfg.cacheDir(), "devin-free-limit.json")
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("%w: devin 쿼터 캐시 디렉터리를 만들 수 없습니다", ErrFreeLimitStore)
	}
	identities := cfg.limitIdentities()
	if accountKey != "" {
		pool, fp, ok := strings.Cut(accountKey, ":")
		if !ok || pool != "devin" || len(fp) != 16 || strings.Trim(fp, "0123456789abcdef") != "" {
			return fmt.Errorf("%w: devin account key가 올바르지 않습니다", ErrInvalidFreeLimitObservation)
		}
		identities = []string{"account-" + longFingerprint("account\x00"+fp)}
	}
	observed := cfg.now().UTC()
	if observedAt != "" {
		parsed, err := time.Parse(time.RFC3339Nano, observedAt)
		if err != nil {
			return fmt.Errorf("%w: observation 시간이 올바르지 않습니다", ErrInvalidFreeLimitObservation)
		}
		observed = parsed.UTC()
	}
	reset := resetAt
	if reset != "" {
		parsed, err := time.Parse(time.RFC3339Nano, reset)
		if err != nil {
			return fmt.Errorf("%w: reset 시간이 올바르지 않습니다", ErrInvalidFreeLimitObservation)
		}
		reset = parsed.UTC().Format(time.RFC3339Nano)
	} else if !clear {
		if !unparsed {
			return fmt.Errorf("%w: --unparsed 또는 --reset이 필요합니다", ErrInvalidFreeLimitObservation)
		}
		reset = observed.Add(unknownResetHold).Format(time.RFC3339Nano)
	}
	if len(rung) > 256 || len(source) > 256 {
		return fmt.Errorf("%w: observation metadata가 너무 깁니다", ErrInvalidFreeLimitObservation)
	}
	if len(accountKey) > 128 {
		return fmt.Errorf("%w: account key가 너무 깁니다", ErrInvalidFreeLimitObservation)
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("%w: devin 쿼터 캐시 lock을 열 수 없습니다", ErrFreeLimitStore)
	}
	defer lock.Close()
	if err := lock.Chmod(0o600); err != nil {
		return fmt.Errorf("%w: devin 쿼터 캐시 lock을 보호할 수 없습니다", ErrFreeLimitStore)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("%w: devin 쿼터 캐시 lock을 얻을 수 없습니다", ErrFreeLimitStore)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)

	document := map[string]json.RawMessage{}
	records := map[string]json.RawMessage{}
	info, statErr := os.Lstat(path)
	if statErr != nil && !errors.Is(statErr, fs.ErrNotExist) {
		if clear && removeLimitFile(path) == nil {
			return nil
		}
		return fmt.Errorf("%w: devin 쿼터 캐시를 확인할 수 없습니다", ErrFreeLimitStore)
	}
	if errors.Is(statErr, fs.ErrNotExist) {
		if clear {
			return nil
		}
	} else {
		if !info.Mode().IsRegular() {
			if clear && removeLimitFile(path) == nil {
				return nil
			}
			return fmt.Errorf("%w: devin 쿼터 캐시는 일반 파일이어야 합니다", ErrFreeLimitStore)
		}
		if info.Mode().Perm()&0o077 != 0 && !clear {
			return fmt.Errorf("%w: devin 쿼터 캐시는 mode 0600이어야 합니다; 복구하려면 quota observe devin-free-limit --clear를 실행하세요", ErrFreeLimitStore)
		}
		data, err := os.ReadFile(path)
		if err != nil || !decodeJSONObject(data, &document) {
			if clear && removeLimitFile(path) == nil {
				return nil
			}
			return fmt.Errorf("%w: devin 쿼터 캐시를 읽을 수 없습니다; 복구하려면 quota observe devin-free-limit --clear를 실행하세요", ErrFreeLimitStore)
		}
		if raw := document["records"]; len(raw) == 0 || json.Unmarshal(raw, &records) != nil || records == nil {
			if clear && removeLimitFile(path) == nil {
				return nil
			}
			return fmt.Errorf("%w: devin 쿼터 캐시 records가 올바르지 않습니다; 복구하려면 quota observe devin-free-limit --clear를 실행하세요", ErrFreeLimitStore)
		}
	}
	if records == nil {
		records = map[string]json.RawMessage{}
	}
	if clear {
		for _, id := range identities {
			delete(records, id)
		}
	} else {
		record := limitRecord{ResetAt: reset, ObservedAt: observed.Format(time.RFC3339Nano), Rung: rung, Source: source, Unparsed: unparsed}
		encoded, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("%w: devin 쿼터 observation을 인코딩할 수 없습니다", ErrFreeLimitStore)
		}
		for _, id := range identities {
			if unparsed {
				if raw, exists := records[id]; exists {
					var previous limitRecord
					if err := json.Unmarshal(raw, &previous); err != nil {
						continue
					}
					previousReset, ok := previous.effectiveReset()
					newReset, _ := record.effectiveReset()
					if !ok || previousReset.After(observed) && previousReset.After(newReset) {
						continue
					}
				}
			}
			records[id] = encoded
		}
	}
	if clear && len(records) == 0 {
		return removeLimitFile(path)
	}
	encodedRecords, err := json.Marshal(records)
	if err != nil {
		return fmt.Errorf("%w: devin 쿼터 캐시를 인코딩할 수 없습니다", ErrFreeLimitStore)
	}
	encodedSchema, _ := json.Marshal(limitRecordSchema)
	document["schema"], document["records"] = encodedSchema, encodedRecords
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("%w: devin 쿼터 캐시를 인코딩할 수 없습니다", ErrFreeLimitStore)
	}
	if err := durable.ReplaceFileAtomically(path, ".devin-free-limit-*", append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("%w: devin 쿼터 observation을 저장할 수 없습니다", ErrFreeLimitStore)
	}
	return nil
}

func removeLimitFile(path string) error {
	err := os.Remove(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: devin 쿼터 캐시를 제거할 수 없습니다", ErrFreeLimitStore)
	}
	durable.SyncDirectoryBestEffort(filepath.Dir(path))
	return nil
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
