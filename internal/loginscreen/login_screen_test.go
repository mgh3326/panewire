package loginscreen

import "testing"

const agyLoginScreenFirstRow = "  Open the URL below in your browser:\n  https://example.invalid/auth\n  After authenticating, copy the code displayed in the browser and paste it below:\n  > "

const agyLoginScreenSecondRow = "Antigravity CLI\n  Copy and paste the URL or click on the link below:\n  https://example.invalid/auth\n  paste the authorization code below:\n  > "

func TestLoginScreenMatchesEachAgyRow(t *testing.T) {
	for name, screen := range map[string]string{"first row": agyLoginScreenFirstRow, "second row": agyLoginScreenSecondRow} {
		if !Matches("agy", screen) {
			t.Fatalf("%s: agy login screen not matched", name)
		}
	}
}

func TestLoginScreenNeedsEveryLineOfARow(t *testing.T) {
	partial := "Open the URL below in your browser:\n> "
	if Matches("agy", partial) {
		t.Fatal("a row with one of its lines missing matched")
	}
}

func TestLoginScreenNoRowNoMatch(t *testing.T) {
	for _, kind := range []string{"claude", "codex", "opus", ""} {
		if Matches(kind, agyLoginScreenFirstRow) {
			t.Fatalf("kind %q matched a screen it has no row for", kind)
		}
	}
	if Matches("agy", "idle prompt\n> ") {
		t.Fatal("an ordinary agy screen matched")
	}
}
