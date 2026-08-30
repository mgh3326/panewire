package wrkgate

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/stage2/core"
)

func TestR4SpawnPolicyDerivesAllWrkLaunchArguments(t *testing.T) {
	cwd := t.TempDir()
	prompt := filepath.Join(t.TempDir(), "jobs", "r4", "brief.md")
	if err := os.MkdirAll(filepath.Dir(prompt), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(prompt, []byte("fixture prompt"), 0600); err != nil {
		t.Fatal(err)
	}
	capture := filepath.Join(t.TempDir(), "wrk-args")
	wrk := r2Stub(t, "wrk", fmt.Sprintf("printf '%%s\\n' \"$@\" > %q\nexit 0", capture))
	gate := New(Config{
		WrkPath:         wrk,
		ArbiterPath:     r2Stub(t, "arbiter", "printf '%s\\n' '{\"job_id\":\"delivery-context\",\"receipt\":{\"pane_id\":\"pane-fixture\",\"spawned_at\":\"2026-08-30T00:00:00Z\"}}'"),
		SpawnPolicyPath: r4PolicyFile(t, "rob1330-", "codex-terra", "workers", "T1", cwd),
	})
	receipt, err := gate.Spawn(context.Background(), core.GateSpawnRequest{DeliveryID: "delivery-context", Label: "rob1330-task", PromptPath: prompt})
	if err != nil || !receipt.Accepted || !receipt.Durable {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
	data, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	want := []string{"spawn", "-c", cwd, "-m", "codex-terra", "-p", prompt, "-w", "workers", "-l", "rob1330-task", "--t", "T1", "--job", "delivery-context"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("wrk args=%q want=%q", got, want)
	}
}

func TestR4SpawnPolicyMismatchAndMissingWrkAreTerminalDenials(t *testing.T) {
	prompt := filepath.Join(t.TempDir(), "brief.md")
	if err := os.WriteFile(prompt, []byte("fixture prompt"), 0600); err != nil {
		t.Fatal(err)
	}
	policy := r4PolicyFile(t, "allowed-", "fixture-model", "fixture-workspace", "T1", t.TempDir())
	mismatch := New(Config{WrkPath: r2Stub(t, "wrk", "exit 99"), SpawnPolicyPath: policy})
	receipt, err := mismatch.Spawn(context.Background(), core.GateSpawnRequest{DeliveryID: "delivery-mismatch", Label: "denied-task", PromptPath: prompt})
	if err != nil || !receipt.Durable || receipt.Accepted || receipt.RejectionCode != core.CodeGateDenied {
		t.Fatalf("policy mismatch receipt=%+v err=%v", receipt, err)
	}
	missing := New(Config{
		WrkPath:         filepath.Join(t.TempDir(), "missing-wrk"),
		SpawnPolicyPath: r4PolicyFile(t, "allowed-", "fixture-model", "fixture-workspace", "T1", t.TempDir()),
	})
	receipt, err = missing.Spawn(context.Background(), core.GateSpawnRequest{DeliveryID: "delivery-missing", Label: "allowed-task", PromptPath: prompt})
	if err != nil || !receipt.Durable || receipt.Accepted || receipt.RejectionCode != core.CodeGateNotInstalled {
		t.Fatalf("missing wrk receipt=%+v err=%v", receipt, err)
	}
}

func r4GateRequest(t *testing.T, deliveryID, label string) core.GateSpawnRequest {
	t.Helper()
	prompt := filepath.Join(t.TempDir(), "brief.md")
	if err := os.WriteFile(prompt, []byte("fixture prompt"), 0600); err != nil {
		t.Fatal(err)
	}
	return core.GateSpawnRequest{DeliveryID: deliveryID, Label: label, PromptPath: prompt}
}

func r4PolicyFile(t *testing.T, prefix, model, workspace, tier, cwd string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spawn-policy.json")
	content := fmt.Sprintf("{\"rules\":[{\"label_prefix\":%q,\"model\":%q,\"workspace\":%q,\"t\":%q,\"cwd\":%q}]}\n", prefix, model, workspace, tier, cwd)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0644 {
		t.Fatalf("policy mode=%v err=%v", info.Mode(), err)
	}
	return path
}
