package panewire

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func task67DarwinLoadOutput(name string) []byte {
	switch name {
	case "vm.loadavg":
		return []byte("{ 1.25 2.50 3.75 }")
	case "vm.swapusage":
		return []byte("total = 4096.00M  used = 1024.00M  free = 3072.00M  (encrypted)\n")
	case "hw.ncpu":
		return []byte("8\n")
	default:
		return nil
	}
}

func task67SysctlName(argv []string) (string, bool) {
	if len(argv) != 3 || argv[1] != "-n" {
		return "", false
	}
	return argv[2], true
}

func TestTask67DarwinLoadCollectorPrefersAbsoluteSysctl(t *testing.T) {
	var calls [][]string
	run := func(_ context.Context, argv ...string) ([]byte, error) {
		calls = append(calls, append([]string(nil), argv...))
		if argv[0] != "/usr/sbin/sysctl" {
			return nil, errors.New("unexpected argv0")
		}
		name, ok := task67SysctlName(argv)
		if !ok {
			return nil, errors.New("unexpected argv")
		}
		return task67DarwinLoadOutput(name), nil
	}

	load, err := collectDarwinHostLoad(t.Context(), run)
	if err != nil || load.Load1 != 1.25 || load.Load5 != 2.5 || load.Load15 == nil || *load.Load15 != 3.75 || load.NCPU == nil || *load.NCPU != 8 {
		t.Fatalf("absolute sysctl load=%+v err=%v", load, err)
	}
	want := [][]string{
		{"/usr/sbin/sysctl", "-n", "vm.loadavg"},
		{"/usr/sbin/sysctl", "-n", "vm.swapusage"},
		{"/usr/sbin/sysctl", "-n", "hw.ncpu"},
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("absolute sysctl calls=%v want=%v", calls, want)
	}
}

func TestTask67DarwinLoadCollectorFallsBackToPathSysctl(t *testing.T) {
	var calls [][]string
	run := func(_ context.Context, argv ...string) ([]byte, error) {
		calls = append(calls, append([]string(nil), argv...))
		name, ok := task67SysctlName(argv)
		if !ok {
			return nil, errors.New("unexpected argv")
		}
		if name == "vm.loadavg" && argv[0] == "/usr/sbin/sysctl" {
			return nil, errors.New("absolute sysctl unavailable")
		}
		if argv[0] != "/usr/sbin/sysctl" && argv[0] != "sysctl" {
			return nil, errors.New("unexpected argv0")
		}
		return task67DarwinLoadOutput(name), nil
	}

	load, err := collectDarwinHostLoad(t.Context(), run)
	if err != nil || load.Load1 != 1.25 || load.Load5 != 2.5 || load.Load15 == nil || *load.Load15 != 3.75 || load.NCPU == nil || *load.NCPU != 8 {
		t.Fatalf("PATH fallback load=%+v err=%v", load, err)
	}
	want := [][]string{
		{"/usr/sbin/sysctl", "-n", "vm.loadavg"},
		{"sysctl", "-n", "vm.loadavg"},
		{"/usr/sbin/sysctl", "-n", "vm.swapusage"},
		{"/usr/sbin/sysctl", "-n", "hw.ncpu"},
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("PATH fallback calls=%v want=%v", calls, want)
	}
}

func TestTask67DarwinLoadCollectorFailureExposesHeartbeatError(t *testing.T) {
	var calls [][]string
	run := func(_ context.Context, argv ...string) ([]byte, error) {
		calls = append(calls, append([]string(nil), argv...))
		if argv[0] != "/usr/sbin/sysctl" && argv[0] != "sysctl" {
			return nil, errors.New("unexpected argv0")
		}
		return nil, errors.New("sysctl unavailable")
	}
	collector := func(ctx context.Context) (HubHostLoad, error) {
		return collectDarwinHostLoad(ctx, run)
	}
	if _, err := collector(t.Context()); err == nil {
		t.Fatal("both sysctl paths unexpectedly collected load")
	}
	wantCalls := [][]string{
		{"/usr/sbin/sysctl", "-n", "vm.loadavg"},
		{"sysctl", "-n", "vm.loadavg"},
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("failed sysctl calls=%v want=%v", calls, wantCalls)
	}

	client := &HubClient{
		jobsInboxRoot:       t.TempDir(),
		assignedJobs:        map[string]uint64{},
		completedJobs:       map[string]uint64{},
		completedReports:    map[string]struct{}{},
		hostLoadCollector:   collector,
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, errors.New("memory unavailable") },
	}
	heartbeat, ok := decodeHubHeartbeatPayload(client.heartbeatEvent(t.Context()).Payload)
	if !ok || heartbeat.HostLoad != nil || heartbeat.LoadError != "host load unavailable" {
		t.Fatalf("failed load heartbeat=%+v ok=%t", heartbeat, ok)
	}
}

func TestTask67HeartbeatLoadErrorIsAdditiveAndStrict(t *testing.T) {
	withLoadError := []byte(`{"status":"alive","load_error":"host load unavailable"}`)
	heartbeat, ok := decodeHubHeartbeatPayload(withLoadError)
	if !ok || heartbeat.LoadError != "host load unavailable" {
		t.Fatalf("load_error heartbeat=%+v ok=%t", heartbeat, ok)
	}
	if _, ok := decodeHubHeartbeatPayload([]byte(`{"status":"alive","load_error":"host load unavailable","unexpected":true}`)); ok {
		t.Fatal("unknown heartbeat field was accepted")
	}
	legacy, ok := decodeHubHeartbeatPayload([]byte(`{"status":"alive","checks":{"service":"ok"}}`))
	if !ok || legacy.LoadError != "" {
		t.Fatalf("legacy heartbeat=%+v ok=%t", legacy, ok)
	}
}
