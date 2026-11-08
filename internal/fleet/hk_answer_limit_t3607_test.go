package fleet

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
)

// paddedAnswer is body followed by whitespace up to exactly hkAnswerLimit
// bytes, then tail.
func paddedAnswer(t *testing.T, body, tail string) string {
	t.Helper()
	if len(body) > hkAnswerLimit {
		t.Fatalf("body of %d bytes exceeds the limit", len(body))
	}
	return body + strings.Repeat(" ", hkAnswerLimit-len(body)) + tail
}

// A 2xx answer longer than hkAnswerLimit is cut by the capture, and the kept
// prefix may be complete JSON; it must read as unreadable, never as success.
func TestHKAnswerOverLimitIsUnreadable(t *testing.T) {
	ctx := context.Background()
	merged := hkTaskJSON(42, "merged", "change (grade A, T1)", "", "Example", `{}`)

	t.Run("task read", func(t *testing.T) {
		newSweepFixture(t).hk.Handle(hkAnswers(map[string]hkAnswerFixture{
			"GET /v1/tasks/42": hkOK(paddedAnswer(t, merged, "garbage")),
		}))
		b, e := hkTaskBody(ctx, "42")
		if e == nil && json.Valid(b) {
			t.Fatalf("over-limit answer read as a valid %d-byte body", len(b))
		}
	})

	t.Run("exactly the limit stays readable", func(t *testing.T) {
		newSweepFixture(t).hk.Handle(hkAnswers(map[string]hkAnswerFixture{
			"GET /v1/tasks/42": hkOK(paddedAnswer(t, merged, "")),
		}))
		b, e := hkTaskBody(ctx, "42")
		if e != nil || !json.Valid(b) {
			t.Fatalf("body of exactly the limit: valid=%v err=%v", json.Valid(b), e)
		}
	})

	t.Run("sweep task read", func(t *testing.T) {
		fx := newSweepFixture(t)
		key := sweepPrefix + "/dataset"
		fx.hk.Handle(hkAnswers(map[string]hkAnswerFixture{
			"GET /v1/tasks/42":            hkOK(paddedAnswer(t, merged, "garbage")),
			"GET /v1/documents":           hkOK(docList(key)),
			docRoute(http.MethodGet, key): hkOK(docAnswer(key, "artifact", fx.record)),
		}))
		if e := fx.a.artifactSweep(context.Background(), []string{"--job", "j", "--age", "0s", "--apply"}); e != nil {
			t.Fatal(e)
		}
		if _, e := os.Stat(fx.path); e != nil {
			t.Fatalf("artifact deleted on an over-limit task answer: %v", e)
		}
		for _, r := range fx.hk.Requests() {
			if strings.HasPrefix(r.Path, "/v1/documents") {
				t.Fatalf("sweep went on to %s after an unreadable task answer", r.Route())
			}
		}
	})

	t.Run("sweep deletion recheck", func(t *testing.T) {
		fx := newSweepFixture(t)
		key := sweepPrefix + "/dataset"
		reads := 0
		answers := hkAnswers(map[string]hkAnswerFixture{
			"GET /v1/documents":           hkOK(docList(key)),
			docRoute(http.MethodGet, key): hkOK(docAnswer(key, "artifact", fx.record)),
		})
		fx.hk.Handle(func(r hkRequest) (int, string, bool) {
			if r.Route() == "GET /v1/tasks/42" {
				reads++
				if reads > 1 {
					return http.StatusOK, paddedAnswer(t, merged, "garbage"), true
				}
				return http.StatusOK, merged, true
			}
			return answers(r)
		})
		_ = fx.a.artifactSweep(context.Background(), []string{"--job", "j", "--age", "0s", "--apply"})
		if reads < 2 {
			t.Fatalf("task read %d times, want the deletion recheck reached", reads)
		}
		if _, e := os.Stat(fx.path); e != nil {
			t.Fatalf("artifact deleted on an over-limit recheck answer: %v", e)
		}
	})
}
