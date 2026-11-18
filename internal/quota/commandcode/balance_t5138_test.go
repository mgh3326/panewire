package commandcode

import (
	"strings"
	"testing"
)

// hk 5138 (decision 403): the omp report's `commandcode:balance` row carries
// the account's remaining credits. It is read into the reading, and a
// malformed value is unknown, not zero.
func TestParseReadsCreditBalance_t5138(t *testing.T) {
	accounts, err := parse(fixture(t, "omp_usage_two_accounts.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(accounts) != 2 {
		t.Fatalf("accounts = %+v, want two", accounts)
	}
	for _, account := range accounts {
		if account.Observation.Balance == nil || *account.Observation.Balance != 17.5330931265 {
			t.Fatalf("account %s balance = %v, want 17.5330931265 credits", account.FP, account.Observation.Balance)
		}
	}
}

func TestParseMalformedCreditBalanceIsUnknown_t5138(t *testing.T) {
	report := string(fixture(t, "omp_usage_two_accounts.json"))
	for name, mutation := range map[string]struct {
		from, to string
	}{
		"not a number": {`"remaining": 17.5330931265`, `"remaining": "many"`},
		"null":         {`"remaining": 17.5330931265`, `"remaining": null`},
		"negative":     {`"remaining": 17.5330931265`, `"remaining": -3`},
		"absent":       {`"remaining": 17.5330931265`, `"otherField": 7`},
	} {
		t.Run(name, func(t *testing.T) {
			mutated := strings.Replace(report, mutation.from, mutation.to, 1)
			if mutated == report {
				t.Fatalf("fixture does not carry %s", mutation.from)
			}
			accounts, err := parse([]byte(mutated))
			if err != nil {
				t.Fatalf("a malformed balance failed the reading: %v", err)
			}
			if len(accounts) != 2 {
				t.Fatalf("accounts = %+v, want two", accounts)
			}
			if balance := accounts[0].Observation.Balance; balance != nil {
				t.Fatalf("malformed balance read as %v, want unknown", *balance)
			}
			// The other account's well-formed balance still reads.
			if balance := accounts[1].Observation.Balance; balance == nil || *balance != 17.5330931265 {
				t.Fatalf("sibling balance = %v, want 17.5330931265", balance)
			}
		})
	}
}

// A duplicate balance row cannot be told apart: unknown, not the first value.
func TestParseDuplicateCreditBalanceIsUnknown_t5138(t *testing.T) {
	report := `{"reports":[{"provider":"commandcode","fetchedAt":1794696766070,"limits":[
{"id":"commandcode:5h","scope":{"accountId":"user_x"},"window":{"durationMs":18000000},"amount":{"usedFraction":0.1}},
{"id":"commandcode:7d","scope":{"accountId":"user_x"},"window":{"durationMs":604800000},"amount":{"usedFraction":0.2}},
{"id":"commandcode:balance","scope":{"accountId":"user_x"},"amount":{"remaining":5}},
{"id":"commandcode:balance","scope":{"accountId":"user_x"},"amount":{"remaining":6}}]}]}`
	accounts, err := parse([]byte(report))
	if err != nil {
		t.Fatalf("a duplicate balance failed the reading: %v", err)
	}
	if len(accounts) != 1 {
		t.Fatalf("accounts = %+v, want one", accounts)
	}
	if balance := accounts[0].Observation.Balance; balance != nil {
		t.Fatalf("duplicate balance read as %v, want unknown", *balance)
	}
}
