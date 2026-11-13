package panewire

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func checkoutPullAnswer(t *testing.T, record string, hosts ...string) hubCheckoutPullResponse {
	t.Helper()
	answer := hubCheckoutPullResponse{Checkout: "rstack", Record: hubCheckoutPullRecord{Key: "deploy/rstack/2026-11-13-x", Outcome: record}}
	for index, raw := range hosts {
		row := hubCheckoutPullHost{Machine: "machine-" + string(rune('a'+index)), State: "connected"}
		if strings.HasPrefix(raw, "error:") {
			row.Error = strings.TrimPrefix(raw, "error:")
		} else {
			row.Result = json.RawMessage(raw)
		}
		answer.Hosts = append(answer.Hosts, row)
	}
	return answer
}

func TestCheckoutPullAllOKDecidesExit(t *testing.T) {
	const pulled = `{"outcome":"pulled","checkout":"rstack","from":"` + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" + `","to":"` + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" + `","install_ran":false,"install_exit":0,"target_reached":true}`
	const current = `{"outcome":"current","checkout":"rstack","to":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","install_ran":false,"install_exit":0,"target_reached":false}`
	const installFailed = `{"outcome":"pulled","checkout":"rstack","from":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","to":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","install_ran":true,"install_exit":3,"target_reached":true}`
	const dirty = `{"outcome":"refused","checkout":"rstack","reason":"dirty","install_ran":false,"install_exit":0}`
	cases := []struct {
		name   string
		answer hubCheckoutPullResponse
		rev    string
		want   bool
	}{
		{"all pulled", checkoutPullAnswer(t, hubCheckoutRecordWrote, pulled, current), "", true},
		{"a refusal fails", checkoutPullAnswer(t, hubCheckoutRecordWrote, pulled, dirty), "", false},
		{"an unreachable host fails", checkoutPullAnswer(t, hubCheckoutRecordWrote, pulled, "error:node_unavailable"), "", false},
		{"a failed install fails", checkoutPullAnswer(t, hubCheckoutRecordWrote, installFailed), "", false},
		{"a missing record fails", checkoutPullAnswer(t, hubCheckoutRecordFailed, pulled), "", false},
		{"a target not reached fails", checkoutPullAnswer(t, hubCheckoutRecordWrote, current), "bbbbbbb", false},
		{"a target reached passes", checkoutPullAnswer(t, hubCheckoutRecordWrote, pulled), "bbbbbbb", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := checkoutPullAllOK(tc.answer, tc.rev); got != tc.want {
				t.Fatalf("checkoutPullAllOK=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestRenderCheckoutPullTable(t *testing.T) {
	answer := checkoutPullAnswer(t, hubCheckoutRecordWrote,
		`{"outcome":"pulled","checkout":"rstack","from":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","to":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","install_ran":true,"install_exit":3,"target_reached":true}`,
		"error:node_unavailable")
	var out bytes.Buffer
	renderCheckoutPull(&out, answer)
	got := out.String()
	for _, want := range []string{
		"MACHINE\tSTATE\tOUTCOME\tFROM\tTO\tINSTALL\tTARGET\tDETAIL",
		"machine-a\tconnected\tpulled\taaaaaaaaaaaa\tbbbbbbbbbbbb\texit 3\treached\t",
		"machine-b\tconnected\t-\t-\t-\t-\t-\tnode_unavailable",
		"record: deploy/rstack/2026-11-13-x (written)",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
}

func TestFleetCheckoutPullUsageIsRefused(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"deploy"},
		{"checkout-pull"},
		{"checkout-pull", "--hub-url", "https://hub.example", "--hub-token-env", "/nope", "--checkout", "../etc"},
		{"checkout-pull", "--hub-url", "https://hub.example", "--hub-token-env", "/nope", "--rev", "zz"},
	} {
		var stdout, stderr bytes.Buffer
		if code := runFleetCLI(args, &stdout, &stderr, hubCLIDeps{}); code != ExitUsage {
			t.Fatalf("args=%v exit=%d, want ExitUsage (stderr %q)", args, code, stderr.String())
		}
	}
}
