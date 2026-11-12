package devin

import (
	"bytes"
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

	"github.com/mgh3326/panewire/internal/configpath"
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
	Damaged    bool   `json:"damaged,omitempty"`
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
	records, found, readErr := readLimitRecords(cfg)
	if readErr != nil {
		if clear && accountKey == "" {
			for _, candidate := range cfg.cachePaths() {
				if err := removeLimitFile(candidate); err != nil {
					return err
				}
			}
			return nil
		}
		if clear && accountKey != "" {
			return clearCorruptLimitFile(path, accountKey)
		}
		return fmt.Errorf("%w: devin 쿼터 캐시를 읽을 수 없습니다; 복구하려면 quota observe devin-free-limit --clear를 실행하세요", ErrFreeLimitStore)
	}
	if clear && !found {
		return nil
	}
	if data, err := cfg.readFileErr(path); err == nil {
		_ = decodeJSONObject(data, &document)
	}
	if len(document) == 0 {
		document = map[string]json.RawMessage{}
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
					if err := json.Unmarshal(raw, &previous); err == nil {
						previousReset, ok := previous.effectiveReset()
						newReset, _ := record.effectiveReset()
						if ok && previousReset.After(observed) && previousReset.After(newReset) {
							continue
						}
					}
				}
			}
			records[id] = encoded
		}
	}
	cachePaths := cfg.cachePaths()
	legacyPaths := cachePaths[1:]
	if clear && len(records) == 0 {
		if err := removeLimitFile(path); err != nil {
			return err
		}
		for _, candidate := range legacyPaths {
			if err := removeLimitFile(candidate); err != nil {
				return err
			}
		}
		return nil
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
	replace := cfg.ReplaceFile
	if replace == nil {
		replace = durable.ReplaceFileAtomically
	}
	if err := replace(path, ".devin-free-limit-*", append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("%w: devin 쿼터 observation을 저장할 수 없습니다", ErrFreeLimitStore)
	}
	if err := verifyLimitFile(cfg, path, records); err != nil {
		return fmt.Errorf("%w: devin 쿼터 observation을 저장 후 확인할 수 없습니다", ErrFreeLimitStore)
	}
	for _, candidate := range legacyPaths {
		if err := removeLegacyLimitFile(path, candidate); err != nil {
			return err
		}
	}
	return nil
}

func verifyLimitFile(cfg Config, path string, expected map[string]json.RawMessage) error {
	data, err := cfg.readFileErr(path)
	if err != nil {
		return err
	}
	if cfg.ReadFile == nil {
		info, statErr := os.Lstat(path)
		if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
			return ErrFreeLimitStore
		}
	}
	var document struct {
		Schema  string                      `json:"schema"`
		Records *map[string]json.RawMessage `json:"records"`
	}
	if !decodeJSONObject(data, &document) || document.Schema != limitRecordSchema || document.Records == nil || !sameLimitRecords(expected, *document.Records) {
		return ErrFreeLimitStore
	}
	return nil
}

func sameLimitRecords(left, right map[string]json.RawMessage) bool {
	if len(left) != len(right) {
		return false
	}
	for id, leftRaw := range left {
		rightRaw, ok := right[id]
		if !ok {
			return false
		}
		var leftJSON, rightJSON bytes.Buffer
		if json.Compact(&leftJSON, leftRaw) != nil || json.Compact(&rightJSON, rightRaw) != nil || !bytes.Equal(leftJSON.Bytes(), rightJSON.Bytes()) {
			return false
		}
	}
	return true
}

func removeLegacyLimitFile(primary, candidate string) error {
	primaryInfo, err := os.Stat(primary)
	if err != nil {
		return fmt.Errorf("%w: devin 쿼터 캐시 파일을 비교할 수 없습니다", ErrFreeLimitStore)
	}
	candidateInfo, err := os.Stat(candidate)
	if errors.Is(err, fs.ErrNotExist) {
		return removeLimitFile(candidate)
	}
	if err != nil {
		return fmt.Errorf("%w: devin 쿼터 캐시 파일을 비교할 수 없습니다", ErrFreeLimitStore)
	}
	if os.SameFile(primaryInfo, candidateInfo) {
		return nil
	}
	return removeLimitFile(candidate)
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

func clearCorruptLimitFile(path, accountKey string) error {
	if accountKey != "" {
		return fmt.Errorf("%w: account-scoped clear cannot recover a damaged cache; run quota observe devin-free-limit --clear without --account-key", ErrFreeLimitStore)
	}
	return removeLimitFile(path)
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

// cachePath is panewire's record path, followed by the legacy pacelever path.
// An explicit cache variable names the record file itself.
func (c Config) cachePaths() []string {
	return configpath.CacheCandidates(c.env, c.home(), "devin-free-limit.json")
}

// cacheDir is panewire's cache directory, used for new records and locks.
func (c Config) cacheDir() string {
	if p := c.env("PANEWIRE_CACHE"); p != "" {
		return filepath.Dir(filepath.Join(filepath.Dir(p), "devin-free-limit.json"))
	}
	return filepath.Join(c.cacheBase(), "panewire")
}

func readLimitRecords(c Config) (map[string]json.RawMessage, bool, error) {
	merged := map[string]json.RawMessage{}
	found := false
	for _, path := range c.cachePaths() {
		data, err := c.readFileErr(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, true, err
		}
		if c.ReadFile == nil {
			info, statErr := os.Lstat(path)
			if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
				return nil, true, ErrFreeLimitStore
			}
		}
		var document struct {
			Records *map[string]json.RawMessage `json:"records"`
		}
		if !decodeJSONObject(data, &document) || document.Records == nil {
			return nil, true, ErrFreeLimitStore
		}
		found = true
		for id, raw := range *document.Records {
			previous, exists := merged[id]
			if !exists {
				merged[id] = raw
				continue
			}
			merged[id] = conservativeLimitRecord(previous, raw)
		}
	}
	return merged, found, nil
}

func conservativeLimitRecord(a, b json.RawMessage) json.RawMessage {
	var left, right limitRecord
	leftOKJSON := json.Unmarshal(a, &left) == nil
	if !leftOKJSON {
		left = limitRecord{}
	}
	rightOKJSON := json.Unmarshal(b, &right) == nil
	if !rightOKJSON {
		right = limitRecord{}
	}
	leftReset, leftOK := left.effectiveReset()
	rightReset, rightOK := right.effectiveReset()
	merged := left
	switch {
	case !leftOK && rightOK:
		merged = right
	case leftOK && rightOK && rightReset.After(leftReset):
		merged = right
	}
	merged.Damaged = left.Damaged || right.Damaged || !leftOKJSON || !rightOKJSON || !leftOK || !rightOK
	encoded, err := json.Marshal(merged)
	if err != nil {
		return a
	}
	return encoded
}

func readLimitFile(c Config) ([]byte, error) {
	records, found, err := readLimitRecords(c)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fs.ErrNotExist
	}
	data, err := json.Marshal(struct {
		Records map[string]json.RawMessage `json:"records"`
	}{Records: records})
	return data, err
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
		data, err := readLimitFile(cfg)
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
			if record.Damaged {
				unknown = true
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
