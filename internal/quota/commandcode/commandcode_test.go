package commandcode

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

// fetchedA is the fetchedAt of account A in testdata/omp_usage_two_accounts.json
// (account B's is 30 s older). The fixtures are `omp usage --json --provider
// commandcode` output recorded on 2026-11-15 (omp 18.14.2) with the account ids
// and e-mail replaced: no live call is made by a test.
var fetchedA = time.UnixMilli(1794696766070).UTC()

const (
	fpA = "7d279b7dcc317cda" // hex(sha256("commandcode-account|user_fixture_a"))[:16]
	fpB = "a1fd7fc5ae227218"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// omp is a fake omp: it answers `usage --json` from replies in turn (the last one
// repeats) and counts the calls, `usage invalidate` separately.
type omp struct {
	replies     [][]byte
	err         error
	reads       int
	invalidates int
	args        [][]string
}

func (o *omp) run(_ context.Context, args ...string) ([]byte, error) {
	o.args = append(o.args, args)
	if o.err != nil {
		return nil, o.err
	}
	if len(args) >= 2 && args[1] == "invalidate" {
		o.invalidates++
		return []byte("Invalidated\n"), nil
	}
	reply := o.replies[min(o.reads, len(o.replies)-1)]
	o.reads++
	return reply, nil
}

func config(o *omp, now time.Time) Config {
	return Config{Run: o.run, Now: func() time.Time { return now }}
}

func bucket(t *testing.T, a Account, window string) quota.Bucket {
	t.Helper()
	for _, b := range a.Observation.Buckets {
		if b.Window == window {
			return b
		}
	}
	t.Fatalf("account %s has no %s window: %+v", a.FP, window, a.Observation.Buckets)
	return quota.Bucket{}
}

func TestMeasureReadsEveryAccountOmpHolds(t *testing.T) {
	o := &omp{replies: [][]byte{fixture(t, "omp_usage_two_accounts.json")}}
	accounts, err := Measure(t.Context(), config(o, fetchedA.Add(90*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	if len(accounts) != 2 || accounts[0].FP != fpA || accounts[1].FP != fpB {
		t.Fatalf("accounts = %+v, want %s then %s", accounts, fpA, fpB)
	}
	if fmt.Sprint(o.args) != "[[usage --json --provider commandcode]]" || o.invalidates != 0 {
		t.Fatalf("omp calls = %v; one read-only usage call is expected", o.args)
	}
	a := accounts[0]
	if a.Observation.Source != quota.SourceOmpUsage || !a.Observation.MeasuredAt.Equal(fetchedA) {
		t.Fatalf("source %q measured_at %v, want omp-usage stamped with omp's fetchedAt %v", a.Observation.Source, a.Observation.MeasuredAt, fetchedA)
	}
	five, week := bucket(t, a, "5h"), bucket(t, a, "7d")
	if got := *five.UsedPct; got < 1.94 || got > 1.95 {
		t.Fatalf("5h used = %v, want 0.271789812 of 14 credits (1.94%%)", got)
	}
	if got := *week.UsedPct; got < 73.05 || got > 73.06 {
		t.Fatalf("7d used = %v, want 25.569563761 of 35 credits (73.06%%)", got)
	}
	// omp's weekly reset is a fixed boundary (unchanged across three days of
	// recorded snapshots), so the weekly window is paceable.
	if week.ResetsAt != "2026-11-17T14:54:38.537Z" || five.ResetsAt == "" {
		t.Fatalf("resets 5h=%q 7d=%q", five.ResetsAt, week.ResetsAt)
	}
	if five.Scope.Kind != quota.ScopeAccount || week.Scope.Kind != quota.ScopeAccount {
		t.Fatalf("scopes %+v %+v, want account", five.Scope, week.Scope)
	}
}

// Decision 289: the month and the credit balance are neither read nor
// required. The fixture carries a credit balance and no 30-day window; neither
// is a bucket, and the reading is complete without a 30d one.
func TestMeasureHasNoMonthlyWindowAndReadsNoCreditBalance(t *testing.T) {
	o := &omp{replies: [][]byte{fixture(t, "omp_usage_two_accounts.json")}}
	accounts, err := Measure(t.Context(), config(o, fetchedA))
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range accounts {
		var windows []string
		for _, b := range a.Observation.Buckets {
			windows = append(windows, b.Window)
		}
		if strings.Join(windows, ",") != "5h,7d" {
			t.Fatalf("account %s windows %v, want exactly 5h,7d", a.FP, windows)
		}
	}
}

func TestExhaustedAccountReadsAsFullWindow(t *testing.T) {
	o := &omp{replies: [][]byte{fixture(t, "omp_usage_two_accounts.json")}}
	accounts, err := Measure(t.Context(), config(o, fetchedA))
	if err != nil {
		t.Fatal(err)
	}
	if got := *bucket(t, accounts[1], "7d").UsedPct; got != 100 {
		t.Fatalf("exhausted weekly window reads %v, want 100", got)
	}
	if got := *bucket(t, accounts[1], "5h").UsedPct; got != 25 {
		t.Fatalf("account B 5h reads %v, want 25 (3.5 of 14)", got)
	}
}

func TestFingerprintContinuesTheAPIReading(t *testing.T) {
	// The earlier API reading keyed the account by hex(sha256("commandcode-account|"+id))[:16]
	// over the Command Code user id; omp reports the same id, so its key is the same.
	if got := Fingerprint("user_fixture_1"); got != "71430d9ff98e30c3" {
		t.Fatalf("Fingerprint = %s, want the key the API reading gave this id", got)
	}
	if Fingerprint("") != "" {
		t.Fatal("an empty id has no fingerprint")
	}
}

func TestNoOmpOrNoAccountIsNoLogin(t *testing.T) {
	missing := &omp{err: fmt.Errorf("start: %w", exec.ErrNotFound)}
	if _, err := Measure(t.Context(), config(missing, fetchedA)); quota.KindOf(err) != quota.ErrKindCredentials {
		t.Fatalf("no omp: %v, want credentials", err)
	}
	none := &omp{replies: [][]byte{[]byte(`{"generatedAt":1,"reports":[],"accountsWithoutUsage":[]}`)}}
	if _, err := Measure(t.Context(), config(none, fetchedA)); quota.KindOf(err) != quota.ErrKindCredentials {
		t.Fatalf("no commandcode account: %v, want credentials", err)
	}
}

// An account with only a credit balance (pay-as-you-go) has no window to judge;
// it is left out, and with no other account the pool is unmeasurable here.
func TestAccountWithoutWindowsIsNotMeasurable(t *testing.T) {
	o := &omp{replies: [][]byte{fixture(t, "omp_usage_payg_only.json")}}
	if _, err := Measure(t.Context(), config(o, fetchedA)); quota.KindOf(err) != quota.ErrKindParse {
		t.Fatalf("balance-only account: %v, want parse_error", err)
	}
}

func TestUnknownShapeFailsClosed(t *testing.T) {
	good := string(fixture(t, "omp_usage_two_accounts.json"))
	cases := map[string]string{
		"not json":              "omp 99: usage is now a table",
		"no reports":            `{"generatedAt":1}`,
		"reports not a list":    `{"reports":{"commandcode":[]}}`,
		"no fetchedAt":          strings.ReplaceAll(good, `"fetchedAt"`, `"fetched"`),
		"fetchedAt as string":   strings.Replace(good, `"fetchedAt": 1794696766070`, `"fetchedAt": "1794696766070"`, 1),
		"limits missing":        `{"reports":[{"provider":"commandcode","fetchedAt":1794696766070}]}`,
		"account id missing":    strings.ReplaceAll(good, `"accountId": "user_fixture_a"`, `"accountId": ""`),
		"5h length changed":     strings.Replace(good, `"durationMs": 18000000`, `"durationMs": 3600000`, 1),
		"weekly length changed": strings.Replace(good, `"durationMs": 604800000`, `"durationMs": 2592000000`, 1),
		"limit without window":  strings.Replace(good, `"window": {`, `"windows": {`, 1),
		"duplicate account":     strings.ReplaceAll(good, "user_fixture_b", "user_fixture_a"),
	}
	for name, output := range cases {
		o := &omp{replies: [][]byte{[]byte(output)}}
		_, err := Measure(t.Context(), config(o, fetchedA))
		if quota.KindOf(err) != quota.ErrKindParse {
			t.Errorf("%s: err = %v, want parse_error (unknown shape fails closed)", name, err)
		}
	}
}

// A recognized window whose amount cannot be read is an unknown shape, not a
// partial reading: an exhausted week with a changed amount beside a valid 5h
// must not publish a 5h-only account.
func TestUnreadableRecognizedWindowFailsClosed(t *testing.T) {
	window := func(id, duration, amount string) string {
		return fmt.Sprintf(`{"id":"commandcode:%s","scope":{"accountId":"user_fixture_a"},"window":{"durationMs":%s,"resetsAt":1794927278537},"amount":%s,"status":"ok"}`, id, duration, amount)
	}
	five := window("5h", "18000000", `{"used":0.27,"limit":14,"usedFraction":0.019}`)
	for name, week := range map[string]string{
		"amount fields renamed": window("7d", "604800000", `{"spent":35,"cap":35}`),
		"amount as strings":     window("7d", "604800000", `{"used":"35","limit":"35"}`),
		"zero limit":            window("7d", "604800000", `{"used":1,"limit":0}`),
		"negative fraction":     window("7d", "604800000", `{"usedFraction":-1}`),
	} {
		doc := fmt.Sprintf(`{"reports":[{"provider":"commandcode","fetchedAt":%d,"limits":[%s,%s]}]}`, fetchedA.UnixMilli(), five, week)
		o := &omp{replies: [][]byte{[]byte(doc)}}
		if accounts, err := Measure(t.Context(), config(o, fetchedA)); quota.KindOf(err) != quota.ErrKindParse {
			t.Errorf("%s: accounts %v err %v, want parse_error and no partial reading", name, accounts, err)
		}
	}
}

func TestErrorsCarryNothingOfOmpsOutput(t *testing.T) {
	secret := "sk-must-not-leak"
	for name, o := range map[string]*omp{
		"junk output": {replies: [][]byte{[]byte(secret)}},
		"omp fails":   {err: errors.New("exit status 1: " + secret)},
	} {
		_, err := Measure(t.Context(), config(o, fetchedA))
		if err == nil || strings.Contains(err.Error(), secret) {
			t.Errorf("%s: err = %v; the failure must be a fixed sentence", name, err)
		}
	}
	timeout := &omp{err: context.DeadlineExceeded}
	if _, err := Measure(t.Context(), config(timeout, fetchedA)); quota.KindOf(err) != quota.ErrKindTimeout {
		t.Fatalf("deadline: %v, want timeout", err)
	}
	failing := &omp{err: errors.New("exit status 2")}
	if _, err := Measure(t.Context(), config(failing, fetchedA)); quota.KindOf(err) != quota.ErrKindProbe {
		t.Fatalf("omp failure: %v, want probe_error", err)
	}
}

// omp serves a cached report; an account's report older than MaxReportAge is
// unknown. The cache is invalidated once to ask for a refetch, and the refetched
// report is what is judged.
func TestStaleReportIsInvalidatedOnceThenJudged(t *testing.T) {
	now := fetchedA.Add(MaxReportAge + time.Minute)
	fresh := strings.ReplaceAll(string(fixture(t, "omp_usage_two_accounts.json")), "1794696766070", fmt.Sprint(now.Add(-time.Second).UnixMilli()))
	fresh = strings.ReplaceAll(fresh, "1794696736070", fmt.Sprint(now.Add(-2*time.Second).UnixMilli()))
	o := &omp{replies: [][]byte{fixture(t, "omp_usage_two_accounts.json"), []byte(fresh)}}
	accounts, err := Measure(t.Context(), config(o, now))
	if err != nil || len(accounts) != 2 {
		t.Fatalf("accounts %v err %v, want both after the refetch", accounts, err)
	}
	if o.invalidates != 1 || o.reads != 2 {
		t.Fatalf("invalidates %d reads %d, want one invalidate between two reads", o.invalidates, o.reads)
	}
	if got := fmt.Sprint(o.args[1]); got != "[usage invalidate --provider commandcode]" {
		t.Fatalf("invalidate argv = %s", got)
	}
}

func TestStaleAfterInvalidateIsUnknown(t *testing.T) {
	o := &omp{replies: [][]byte{fixture(t, "omp_usage_two_accounts.json")}}
	_, err := Measure(t.Context(), config(o, fetchedA.Add(MaxReportAge+time.Second)))
	if quota.KindOf(err) != quota.ErrKindProbe || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("err = %v, want probe_error stale: a report omp would not refresh is unknown", err)
	}
	if o.invalidates != 1 {
		t.Fatalf("invalidates = %d, want exactly one attempt", o.invalidates)
	}
}

// One stale account is left out and the other is kept: "usable while one has room".
func TestOneStaleAccountIsLeftOut(t *testing.T) {
	now := fetchedA.Add(MaxReportAge) // A is exactly at the limit, B is 30 s older than it
	o := &omp{replies: [][]byte{fixture(t, "omp_usage_two_accounts.json")}}
	accounts, err := Measure(t.Context(), config(o, now))
	if err != nil || len(accounts) != 1 || accounts[0].FP != fpA {
		t.Fatalf("accounts %+v err %v, want only account A", accounts, err)
	}
}

func TestReportFromTheFutureIsUnknown(t *testing.T) {
	o := &omp{replies: [][]byte{fixture(t, "omp_usage_two_accounts.json")}}
	if _, err := Measure(t.Context(), config(o, fetchedA.Add(-time.Hour))); quota.KindOf(err) != quota.ErrKindProbe {
		t.Fatalf("future report: %v, want probe_error", err)
	}
}

func TestMaxReportAgeMatchesTheSharedRefreshDue(t *testing.T) {
	if MaxReportAge != 3*time.Minute {
		t.Fatalf("MaxReportAge = %v, want 3m (shared.OtherRefreshTTL)", MaxReportAge)
	}
}
