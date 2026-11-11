package shared

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

const presenceRecord = `{"host_id":"node-a","account_key":"anthropic-api:5984150754efdf68","last_seen":"2026-11-11T15:00:00Z"}`

// presenceClient answers every presence GET with body.
func presenceClient(t *testing.T, body string) Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/quota/presence" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return Client{BaseURL: server.URL}
}

// The store's answer is read as the records themselves or inside the envelope
// handoffkeep may name them with.
func TestPresenceReadsEveryAcceptedShape(t *testing.T) {
	for name, body := range map[string]string{
		"array":    `[` + presenceRecord + `]`,
		"records":  `{"records":[` + presenceRecord + `]}`,
		"presence": `{"presence":[` + presenceRecord + `]}`,
		"rows":     `{"rows":[` + presenceRecord + `]}`,
		"empty":    `[]`,
	} {
		t.Run(name, func(t *testing.T) {
			rows, err := presenceClient(t, body).Presence(context.Background())
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if body == "[]" {
				if len(rows) != 0 {
					t.Fatalf("rows = %+v, want none", rows)
				}
				return
			}
			if len(rows) != 1 || rows[0].HostID != "node-a" || rows[0].AccountKey != "anthropic-api:5984150754efdf68" {
				t.Fatalf("rows = %+v", rows)
			}
		})
	}
}

// An answer in none of the accepted shapes is an error, never an empty list: a
// caller must not read a reply it did not understand as "no host holds the
// account" (hk 3998 review).
func TestPresenceRejectsAnUnrecognisedShape(t *testing.T) {
	for name, body := range map[string]string{
		"other key":  `{"items":[]}`,
		"not a list": `{"records":{"host_id":"node-a"}}`,
		"not json":   `not json`,
		"null":       `null`,
	} {
		t.Run(name, func(t *testing.T) {
			if rows, err := presenceClient(t, body).Presence(context.Background()); err == nil {
				t.Fatalf("rows = %+v, want an error for an answer in no accepted shape", rows)
			}
		})
	}
}
