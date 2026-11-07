package agy

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/quota"
)

const appArgv = "/Applications/Antigravity.app/Contents/Resources/language_server_macos_arm --app_data_dir /Users/x/Library/Application Support/antigravity --csrf_token " + csrf + " --extension_server_port 5678"

func localConfig(t *testing.T, local, cloud Doer, procs []Proc, ports []int) Config {
	cfg := config(loginFiles(token), cloud)
	cfg.LocalHTTP = local
	cfg.Processes = func(context.Context) []Proc { return procs }
	cfg.ListenPorts = func(context.Context, int) []int { return ports }
	return cfg
}

func neverCloud(t *testing.T) Doer {
	return doer(func(*http.Request) (*http.Response, error) {
		t.Fatal("the cloud source ran")
		return nil, nil
	})
}

// The running app answers: its groups are read with no credential, and the
// cloud is never asked.
func TestMeasureLocalQuotaSummary(t *testing.T) {
	local := &server{replies: map[string]func(*http.Request) (*http.Response, error){
		"/exa.language_server_pb.LanguageServerService/RetrieveUserQuotaSummary": respond(200, fixture(t, "summary.json")),
	}}
	observed, err := Measure(context.Background(), localConfig(t, local, neverCloud(t), []Proc{{PID: 42, Argv: appArgv}}, nil))
	if err != nil {
		t.Fatal(err)
	}
	if b := group(observed.Buckets, "gemini", "5h"); b == nil || !near(b.UsedPct, 20) {
		t.Fatalf("gemini %+v", observed.Buckets)
	}
	if b := group(observed.Buckets, "3p", "7d"); b == nil || !near(b.UsedPct, 60) {
		t.Fatalf("3p %+v", observed.Buckets)
	}
	if observed.Source != quota.SourceUsageAPI {
		t.Fatalf("source %q", observed.Source)
	}
	r := local.requests[0]
	if r.URL.Host != "127.0.0.1:5678" || r.Header.Get("X-Codeium-Csrf-Token") != csrf || r.Header.Get("Connect-Protocol-Version") != "1" {
		t.Fatalf("request %s csrf %q", r.URL, r.Header.Get("X-Codeium-Csrf-Token"))
	}
	if r.Header.Get("Authorization") != "" {
		t.Fatal("the local source sent a credential")
	}
}

// The app's TLS ports are tried before its plain extension port, and the CSRF
// token goes only to 127.0.0.1.
func TestMeasureLocalTriesTLSPortsFirstAndOnlyOnLoopback(t *testing.T) {
	local := doer(func(r *http.Request) (*http.Response, error) {
		if r.URL.Port() == "9000" {
			return respond(200, `{"groups":[{"displayName":"Claude and GPT models","buckets":[{"bucketId":"3p-5h","window":"5h","remainingFraction":0.5}]}]}`)(r)
		}
		return respond(404, `{}`)(r)
	})
	var seen []string
	traced := doer(func(r *http.Request) (*http.Response, error) {
		seen = append(seen, r.URL.Scheme+"://"+r.URL.Host)
		return local(r)
	})
	observed, err := Measure(context.Background(), localConfig(t, traced, neverCloud(t), []Proc{{PID: 7, Argv: appArgv}}, []int{9000}))
	if err != nil || group(observed.Buckets, "3p", "5h") == nil {
		t.Fatalf("%+v %v", observed, err)
	}
	if len(seen) == 0 || seen[0] != "https://127.0.0.1:9000" {
		t.Fatalf("order %v", seen)
	}
	for _, u := range seen {
		if !strings.HasPrefix(u, "https://127.0.0.1:") && !strings.HasPrefix(u, "http://127.0.0.1:") {
			t.Fatalf("the CSRF token left the loopback: %s", u)
		}
	}
}

// The per-model answers (GetUserStatus) carry the groups by model label.
func TestMeasureLocalUserStatusShape(t *testing.T) {
	local := doer(func(r *http.Request) (*http.Response, error) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/RetrieveUserQuotaSummary"):
			return respond(404, `{}`)(r)
		case strings.HasSuffix(r.URL.Path, "/GetUserStatus"):
			return respond(200, fixture(t, "local_user_status.json"))(r)
		}
		return respond(404, `{}`)(r)
	})
	observed, err := Measure(context.Background(), localConfig(t, local, neverCloud(t), []Proc{{PID: 9, Argv: appArgv}}, nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(observed.Buckets) != 2 || group(observed.Buckets, "gemini", "5h") == nil || group(observed.Buckets, "3p", "5h") == nil || observed.Plan != "PRO" {
		t.Fatalf("%+v plan %q", observed.Buckets, observed.Plan)
	}
}

// No usable app — none running, no CSRF token, another program's server, the
// agy CLI itself — falls back to the cloud, and never sends a request locally.
func TestMeasureLocalSkipsProcessesThatAreNotTheApp(t *testing.T) {
	for name, procs := range map[string][]Proc{
		"none":      nil,
		"no csrf":   {{PID: 1, Argv: "/Applications/Antigravity.app/language_server --app_data_dir /antigravity/ --extension_server_port 5678"}},
		"other app": {{PID: 1, Argv: "/opt/other/language_server --csrf_token " + csrf + " --extension_server_port 5678"}},
		"agy cli":   {{PID: 1, Argv: "agy models --csrf_token " + csrf}},
	} {
		t.Run(name, func(t *testing.T) {
			local := doer(func(*http.Request) (*http.Response, error) {
				t.Fatal("a request left for a process that is not the app")
				return nil, nil
			})
			observed, err := Measure(context.Background(), localConfig(t, local, cloudServer(t), procs, nil))
			if err != nil || group(observed.Buckets, "gemini", "5h") == nil {
				t.Fatalf("cloud fallback: %+v %v", observed, err)
			}
		})
	}
}

// A refusing or unintelligible app also falls back to the cloud.
func TestMeasureLocalFailureFallsBackToTheCloud(t *testing.T) {
	for name, reply := range map[string]func(*http.Request) (*http.Response, error){
		"refused":      respond(500, `{}`),
		"unrecognised": respond(200, `{"unrelated":true}`),
		"not json":     respond(200, `<html>`),
	} {
		t.Run(name, func(t *testing.T) {
			local := doer(reply)
			cloud := cloudServer(t)
			observed, err := Measure(context.Background(), localConfig(t, local, cloud, []Proc{{PID: 3, Argv: appArgv}}, nil))
			if err != nil || group(observed.Buckets, "gemini", "5h") == nil || len(cloud.requests) == 0 {
				t.Fatalf("refused local did not reach the cloud: %+v %v", observed, err)
			}
		})
	}
}

func TestIsServer(t *testing.T) {
	for argv, want := range map[string]bool{
		appArgv: true,
		"/home/u/.antigravity/bin/language_server --csrf_token x":                       true,
		`C:\Users\x\AppData\antigravity\language_server.exe --app_data_dir antigravity`: true,
		"/usr/bin/language_server --csrf_token x":                                       false,
		"/usr/local/bin/agy models":                                                     false,
		"/opt/antigravity/tool --csrf_token x":                                          false,
	} {
		if got := isServer(argv); got != want {
			t.Errorf("isServer(%q) = %v, want %v", argv, got, want)
		}
	}
}

func TestExtensionPort(t *testing.T) {
	for argv, want := range map[string]int{
		"--extension_server_port 5678":  5678,
		"--extension_server_port=5678":  5678,
		"--extension_server_port 0":     0,
		"--extension_server_port 70000": 0,
		"--extension_server_port x":     0,
		"":                              0,
	} {
		if got := extensionPort(argv); got != want {
			t.Errorf("extensionPort(%q) = %d, want %d", argv, got, want)
		}
	}
}

// Neither the OAuth token nor the CSRF token appears in a measurement's error.
func TestMeasureNeverLeaksATokenInAnError(t *testing.T) {
	refuse := doer(func(*http.Request) (*http.Response, error) { return respond(403, `{"token":"`+token+`"}`)(nil) })
	local := doer(func(*http.Request) (*http.Response, error) { return respond(500, csrf)(nil) })
	_, err := Measure(context.Background(), localConfig(t, local, refuse, []Proc{{PID: 3, Argv: appArgv}}, nil))
	if err == nil || strings.Contains(err.Error(), token) || strings.Contains(err.Error(), csrf) {
		t.Fatalf("error %v", err)
	}
}
