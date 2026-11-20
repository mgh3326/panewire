package panewire

import (
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The agy sign-in screen as the 2026-11-20 mac-dev capture shows it, with the
// URL and authorization-code lines removed.
const agyNewLoginScreen = ` Your browser should open automatically. If not:
 Copy and paste the URL or click on the link below:
 → Click here to authenticate
 If you aren't automatically redirected, paste the authorization code below:
 authorization code...
`

func TestLoginScreenAgyNewScreenT5810(t *testing.T) {
	if !harnessLoginScreen("agy", agyNewLoginScreen) {
		t.Fatal("the 2026-11-20 agy sign-in screen is not a login screen")
	}
	result, client, _ := t3891RunHubSpawn(t, "agy-flash38-medium", true, agyNewLoginScreen)
	if result.RC != t3891AuthUnavailableRC || spawnResultReason(result) != "spawn_failed:auth_unavailable" {
		t.Fatalf("sent result=%+v reason=%q", result, spawnResultReason(result))
	}
	if !strings.Contains(result.StdoutTail, loginScreenAuthMessage) {
		t.Fatalf("translated tail lacks the login-missing line: %q", result.StdoutTail)
	}
	if got := poolLogin(t, client, "agy").Login; got != fleet.ReadinessLoginMissing {
		t.Fatalf("login after the new login screen = %q, want missing", got)
	}
}

func TestLoginScreenAgyOldScreenStillMatchesT5810(t *testing.T) {
	if !harnessLoginScreen("agy", t3891AgyLoginScreen) {
		t.Fatal("the 2026-11-10 agy login screen no longer matches")
	}
	result, _, _ := t3891RunHubSpawn(t, "agy-flash38-medium", true, t3891AgyLoginScreen)
	if result.RC != t3891AuthUnavailableRC {
		t.Fatalf("old screen sent rc=%d, want %d", result.RC, t3891AuthUnavailableRC)
	}
}

// A screen with only one line of the new row, or of any other harness's rows,
// is not a login screen: the spawn keeps its rc=76 refusal.
func TestLoginScreenAgyPartialNewScreenT5810(t *testing.T) {
	partial := []struct {
		name, screen string
	}{
		{"only the URL line", " Copy and paste the URL or click on the link below:\n → Click here to authenticate\n"},
		{"only the code line", " If you aren't automatically redirected, paste the authorization code below:\n authorization code...\n"},
	}
	for _, tc := range partial {
		t.Run(tc.name, func(t *testing.T) {
			if harnessLoginScreen("agy", tc.screen) {
				t.Fatalf("a partial screen matched as the login screen: %q", tc.screen)
			}
			result, client, _ := t3891RunHubSpawn(t, "agy-flash38-medium", true, tc.screen)
			if result.RC != fleet.ExitSubmitUnconfirmed {
				t.Fatalf("partial screen sent rc=%d, want %d", result.RC, fleet.ExitSubmitUnconfirmed)
			}
			if got := poolLogin(t, client, "agy").Login; got == fleet.ReadinessLoginMissing {
				t.Fatalf("partial screen marked agy login missing")
			}
		})
	}
	if harnessLoginScreen("opus", agyNewLoginScreen) {
		t.Fatal("the agy sign-in screen matched another harness")
	}
}
