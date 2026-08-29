package panewire_test

import (
	"slices"
	"testing"
)

// r1FixtureNames is intentionally the single source used by the local
// manifest guard.  The acceptance command independently enumerates the Go
// test binary, so this list cannot mask a missing test function.
var r1FixtureNames = []string{
	"TestR1DestinationMismatch",
	"TestR1LogicalPathTraversalRejected",
	"TestR1DuplicateRedelivery",
	"TestR1SenderCrashBeforeVsAfterPublish",
	"TestR1CrashBeforeInboxWrite",
	"TestR1CrashAfterInboxWriteBeforeAck",
	"TestR1OfflineReceiverReconnect",
	"TestR1TransportOutageBackoff",
	"TestR1ExpiredAndOversizedPayload",
	"TestR1TamperedHash",
	"TestR1CompanyDataFailClosed",
	"TestR1SecretRepoLeakGuard",
	"TestR1SQLiteBodyAbsence",
	"TestR1WrkGateDenialNoBypass",
	"TestR1ReverseCompletionIdempotent",
	"TestR1AdapterContract",
	"TestR1CoreNoSDKLeakage",
}

func TestStage2FixtureManifestGuard(t *testing.T) {
	want := append([]string(nil), r1FixtureNames...)
	got := []string{
		"TestR1DestinationMismatch",
		"TestR1LogicalPathTraversalRejected",
		"TestR1DuplicateRedelivery",
		"TestR1SenderCrashBeforeVsAfterPublish",
		"TestR1CrashBeforeInboxWrite",
		"TestR1CrashAfterInboxWriteBeforeAck",
		"TestR1OfflineReceiverReconnect",
		"TestR1TransportOutageBackoff",
		"TestR1ExpiredAndOversizedPayload",
		"TestR1TamperedHash",
		"TestR1CompanyDataFailClosed",
		"TestR1SecretRepoLeakGuard",
		"TestR1SQLiteBodyAbsence",
		"TestR1WrkGateDenialNoBypass",
		"TestR1ReverseCompletionIdempotent",
		"TestR1AdapterContract",
		"TestR1CoreNoSDKLeakage",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("R1 fixture manifest changed: got=%v want=%v", got, want)
	}
}

// The first fixture commit deliberately has a failing assertion for every
// required scenario.  Production-backed assertions replace this red sentinel
// in the implementation commit; keeping named functions here prevents a
// zero-match go test invocation from appearing green.
func r1Red(t *testing.T, name string) {
	t.Helper()
	t.Errorf("R1 fixture assertion RED: %s has no stage2 implementation yet", name)
}

func TestR1DestinationMismatch(t *testing.T)             { r1Red(t, "destination mismatch") }
func TestR1LogicalPathTraversalRejected(t *testing.T)    { r1Red(t, "logical-path traversal") }
func TestR1DuplicateRedelivery(t *testing.T)             { r1Red(t, "duplicate/redelivery") }
func TestR1SenderCrashBeforeVsAfterPublish(t *testing.T) { r1Red(t, "sender crash windows") }
func TestR1CrashBeforeInboxWrite(t *testing.T)           { r1Red(t, "crash before inbox write") }
func TestR1CrashAfterInboxWriteBeforeAck(t *testing.T)   { r1Red(t, "crash after inbox write") }
func TestR1OfflineReceiverReconnect(t *testing.T)        { r1Red(t, "offline receiver reconnect") }
func TestR1TransportOutageBackoff(t *testing.T)          { r1Red(t, "transport outage backoff") }
func TestR1ExpiredAndOversizedPayload(t *testing.T)      { r1Red(t, "expired and oversized payload") }
func TestR1TamperedHash(t *testing.T)                    { r1Red(t, "tampered hash") }
func TestR1CompanyDataFailClosed(t *testing.T)           { r1Red(t, "classification fail-close") }
func TestR1SecretRepoLeakGuard(t *testing.T)             { r1Red(t, "secret/repository leak guard") }
func TestR1SQLiteBodyAbsence(t *testing.T)               { r1Red(t, "SQLite body absence") }
func TestR1WrkGateDenialNoBypass(t *testing.T)           { r1Red(t, "wrk gate denial") }
func TestR1ReverseCompletionIdempotent(t *testing.T)     { r1Red(t, "reverse completion") }
func TestR1AdapterContract(t *testing.T)                 { r1Red(t, "adapter contract") }
func TestR1CoreNoSDKLeakage(t *testing.T)                { r1Red(t, "core SDK leakage") }
