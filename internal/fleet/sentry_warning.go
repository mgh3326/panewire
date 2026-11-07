package fleet

import (
	"os"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
)

// Warning-level Sentry reports from the node-side lifecycle paths. Only
// findings that would otherwise leave no trace take this path; the crash
// reporter's fatal-only budget and the hub's spawn client are untouched.
// The DSN is the same variable crash reporting uses, so a node without crash
// reporting sends nothing.

const sentryWarningDSNEnv = "SENTRY_DSN_PANEWIRE"

const sentryWarningFlush = 2 * time.Second

func (a *App) captureSentryWarning(fingerprint []string, message string, tags map[string]string) {
	if a.SentryWarning != nil {
		a.SentryWarning(fingerprint, message, tags)
		return
	}
	dsn := strings.TrimSpace(os.Getenv(sentryWarningDSNEnv))
	if dsn == "" {
		return
	}
	client, err := sentry.NewClient(sentry.ClientOptions{
		Dsn:            dsn,
		Release:        "panewire@" + BuildVersion,
		Environment:    "fleet",
		ServerName:     "panewire-node",
		SendDefaultPII: false,
		BeforeSend:     scrubNodeSentryEvent,
	})
	if err != nil {
		return
	}
	hub := sentry.NewHub(client, sentry.NewScope())
	hub.CaptureEvent(&sentry.Event{Level: sentry.LevelWarning, Message: message, Fingerprint: fingerprint, Tags: tags})
	client.Flush(sentryWarningFlush)
}

// scrubNodeSentryEvent drops the fields that can identify a user or request.
func scrubNodeSentryEvent(event *sentry.Event, _ *sentry.EventHint) *sentry.Event {
	event.User = sentry.User{}
	event.Request = nil
	event.Breadcrumbs = nil
	return event
}
