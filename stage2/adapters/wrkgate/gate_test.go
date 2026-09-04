package wrkgate

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLookupMapsArbiterExit7ToDurableAbsence(t *testing.T) {
	gate := New(Config{
		WrkPath:         r2Stub(t, "wrk", "exit 0"),
		ArbiterPath:     r2Stub(t, "arbiter", "exit 7"),
		SpawnPolicyPath: r4PolicyFile(t, "delivery-", "fixture-model", "fixture-workspace", "T1", t.TempDir()),
	})
	// A broken external arbiter must produce a bounded, actionable test
	// failure rather than consume the package's full (historically 11 minute)
	// test timeout. The fixture itself exits immediately.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	receipt, err := gate.Lookup(ctx, "delivery-7")
	if err != nil || receipt.Found || !receipt.Durable || receipt.Accepted {
		t.Fatalf("exit 7 receipt=%+v err=%v", receipt, err)
	}
}

func TestSpawnExit74RequeriesArbiterInsteadOfDurableDeny(t *testing.T) {
	gate := New(Config{
		WrkPath: r2Stub(t, "wrk", `
[ "$1" = spawn ] && [ "${14}" = --job ] && [ "${15}" = delivery-74 ] || exit 2
exit 74`),
		ArbiterPath: r2Stub(t, "arbiter", `
[ "$1" = job-get ] && [ "$2" = --job ] && [ "$3" = delivery-74 ] && [ "$4" = --json ] || exit 2
printf '%s\n' '{"job_id":"delivery-74","receipt":"absent"}'`),
		SpawnPolicyPath: r4PolicyFile(t, "delivery-", "fixture-model", "fixture-workspace", "T1", t.TempDir()),
	})
	receipt, err := gate.Spawn(context.Background(), r4GateRequest(t, "delivery-74", "delivery-74"))
	if err != nil || !receipt.Found || receipt.Durable || receipt.Accepted {
		t.Fatalf("exit 74 receipt=%+v err=%v", receipt, err)
	}
}

func TestSpawnExit75FailsClosedWithoutReceipt(t *testing.T) {
	gate := New(Config{
		WrkPath:         r2Stub(t, "wrk", "exit 75"),
		ArbiterPath:     r2Stub(t, "arbiter", "exit 99"),
		SpawnPolicyPath: r4PolicyFile(t, "delivery-", "fixture-model", "fixture-workspace", "T1", t.TempDir()),
	})
	receipt, err := gate.Spawn(context.Background(), r4GateRequest(t, "delivery-75", "delivery-75"))
	if err == nil || receipt.Durable || receipt.Accepted {
		t.Fatalf("exit 75 receipt=%+v err=%v", receipt, err)
	}
}

func TestSpawnRequiresObjectReceipt(t *testing.T) {
	gate := New(Config{
		WrkPath: r2Stub(t, "wrk", `
[ "$1" = spawn ] && [ "${14}" = --job ] && [ "${15}" = delivery-ok ] || exit 2
exit 0`),
		ArbiterPath: r2Stub(t, "arbiter", `
[ "$1" = job-get ] && [ "$2" = --job ] && [ "$3" = delivery-ok ] && [ "$4" = --json ] || exit 2
printf '%s\n' '{"job_id":"delivery-ok","receipt":{"pane_id":"pane-fixture","spawned_at":"2026-08-29T12:00:00Z"}}'`),
		SpawnPolicyPath: r4PolicyFile(t, "delivery-", "fixture-model", "fixture-workspace", "T1", t.TempDir()),
	})
	receipt, err := gate.Spawn(context.Background(), r4GateRequest(t, "delivery-ok", "delivery-ok"))
	if err != nil || !receipt.Found || !receipt.Durable || !receipt.Accepted {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
}

func r2Stub(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\nset -eu\n"+body+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	return path
}
