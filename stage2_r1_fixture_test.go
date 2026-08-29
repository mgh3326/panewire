package panewire_test

import (
	"os/exec"
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
	if len(r1FixtureNames) != 17 {
		t.Fatalf("R1 fixture manifest length=%d, want 17", len(r1FixtureNames))
	}
	output, err := exec.Command("sh", "scripts/check-r1-manifest.sh").CombinedOutput()
	if err != nil {
		t.Fatalf("R1 fixture manifest guard: %v\n%s", err, output)
	}
}

func TestR1DestinationMismatch(t *testing.T)             { r1DestinationMismatch(t) }
func TestR1LogicalPathTraversalRejected(t *testing.T)    { r1LogicalPathTraversalRejected(t) }
func TestR1DuplicateRedelivery(t *testing.T)             { r1DuplicateRedelivery(t) }
func TestR1SenderCrashBeforeVsAfterPublish(t *testing.T) { r1SenderCrashBeforeVsAfterPublish(t) }
func TestR1CrashBeforeInboxWrite(t *testing.T)           { r1CrashBeforeInboxWrite(t) }
func TestR1CrashAfterInboxWriteBeforeAck(t *testing.T)   { r1CrashAfterInboxWriteBeforeAck(t) }
func TestR1OfflineReceiverReconnect(t *testing.T)        { r1OfflineReceiverReconnect(t) }
func TestR1TransportOutageBackoff(t *testing.T)          { r1TransportOutageBackoff(t) }
func TestR1ExpiredAndOversizedPayload(t *testing.T)      { r1ExpiredAndOversizedPayload(t) }
func TestR1TamperedHash(t *testing.T)                    { r1TamperedHash(t) }
func TestR1CompanyDataFailClosed(t *testing.T)           { r1CompanyDataFailClosed(t) }
func TestR1SecretRepoLeakGuard(t *testing.T)             { r1SecretRepoLeakGuard(t) }
func TestR1SQLiteBodyAbsence(t *testing.T)               { r1SQLiteBodyAbsence(t) }
func TestR1WrkGateDenialNoBypass(t *testing.T)           { r1WrkGateDenialNoBypass(t) }
func TestR1ReverseCompletionIdempotent(t *testing.T)     { r1ReverseCompletionIdempotent(t) }
func TestR1AdapterContract(t *testing.T)                 { r1AdapterContract(t) }
func TestR1CoreNoSDKLeakage(t *testing.T)                { r1CoreNoSDKLeakage(t) }
