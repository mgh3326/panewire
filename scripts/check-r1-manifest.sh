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
actual="$(go test ./... -list '^TestR1' | grep -E '^TestR1[A-Za-z0-9_]+$' | sort)"
test "$actual" = "$expected"
