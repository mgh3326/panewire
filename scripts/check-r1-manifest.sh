#!/bin/sh
set -eu

expected="$(printf '%s\n' \
  TestR1DestinationMismatch \
  TestR1LogicalPathTraversalRejected \
  TestR1DuplicateRedelivery \
  TestR1SenderCrashBeforeVsAfterPublish \
  TestR1CrashBeforeInboxWrite \
  TestR1CrashAfterInboxWriteBeforeAck \
  TestR1OfflineReceiverReconnect \
  TestR1TransportOutageBackoff \
  TestR1ExpiredAndOversizedPayload \
  TestR1TamperedHash \
  TestR1CompanyDataFailClosed \
  TestR1SecretRepoLeakGuard \
  TestR1SQLiteBodyAbsence \
  TestR1WrkGateDenialNoBypass \
  TestR1ReverseCompletionIdempotent \
  TestR1AdapterContract \
  TestR1CoreNoSDKLeakage | sort)"
# The R1 family uses a letter (or underscore) immediately after its prefix.
# Keep that boundary in the discovery expression: ^TestR1 also matches future
# families such as TestR18 and made the fixed R1 manifest reject unrelated
# fixtures.
actual="$(go test ./... -list '^TestR1[A-Za-z_][A-Za-z0-9_]*$' | grep -E '^TestR1[A-Za-z_][A-Za-z0-9_]*$' | sort)"
test "$actual" = "$expected"
