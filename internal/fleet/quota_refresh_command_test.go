package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRefreshCommandUsesOnlyNativePools(t *testing.T) {
	for _, pool := range nativeQuotaPools {
		cmd := refreshCommand(pool, "", IsNativeQuotaPool(pool, nil))
		if cmd.Name != "panewire" || strings.Join(cmd.Args, " ") != "quota refresh "+pool {
			t.Errorf("%s refresh = %+v", pool, cmd)
		}
	}
	for _, pool := range []string{"grok", "kimi", "kiro", "clinepass", "upstage"} {
		if IsNativeQuotaPool(pool, nil) {
			t.Errorf("%s unexpectedly has a measurement source", pool)
		}
	}
}

func TestMeasureTargetRefusesUnmeasurablePoolWithoutExec(t *testing.T) {
	var calls []Command
	a := &App{Config: Config{Hosts: map[string]map[string]any{"host-b": {}}}, Runner: &fakeRunner{Do: func(c Command) Result { calls = append(calls, c); return Result{} }}}
	if err := a.measureTarget(context.Background(), "host-b", "grok"); err == nil || !strings.Contains(err.Error(), "native 측정 소스가 없습니다") {
		t.Fatalf("grok measurement error = %v", err)
	}
	if len(calls) != 0 {
		t.Fatalf("unmeasurable pool executed commands: %+v", calls)
	}
}

func TestCLIQuotaWithoutBinaryFailsClosed(t *testing.T) {
	runner := &fakeRunner{}
	q := CLIQuota{Runner: runner}
	ctx := context.Background()
	if _, err := q.Gate(ctx, QuotaRequest{Profile: "opus"}); err == nil {
		t.Fatal("gate without a configured binary should require the native adapter")
	}
	if _, err := q.Launch(ctx, QuotaRequest{Profile: "opus"}); err == nil {
		t.Fatal("launch without a configured binary should require the native adapter")
	}
	if _, err := q.Pools(ctx); err == nil {
		t.Fatal("pool read without a configured binary should require the native adapter")
	}
	if _, err := q.manualStore(ctx); err == nil {
		t.Fatal("manual-store read without a configured binary should require the shared reader")
	}
	if len(runner.Calls) != 0 {
		t.Fatalf("empty binary reached runner: %+v", runner.Calls)
	}
}

func writeHostsConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hosts.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfigIgnoresRetiredNativePools(t *testing.T) {
	for _, body := range []string{
		"[quota]\nnative_pools = [\"commandcode\"]\n",
		"[quota]\nnative_pools = []\n",
		"[quota]\nnative_pools = \"commandcode\"\n",
		"[quota]\nnative_pools = [\"claude\"]\n",
		"[quota]\nnative_pools = [1]\n",
	} {
		if _, err := LoadConfig(writeHostsConfig(t, body)); err != nil {
			t.Errorf("retired key should be ignored for %q: %v", body, err)
		}
	}
}
