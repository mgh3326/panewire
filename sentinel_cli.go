package panewire

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/mgh3326/panewire/sentinel"
	"github.com/mgh3326/panewire/stage2/adapters/supabase"
)

type sentinelCLIDeps struct {
	HTTPClient            *http.Client
	AllowInsecureForTests bool
	Now                   func() time.Time
}

func runSentinelCLI(args []string, stdout, stderr io.Writer, deps sentinelCLIDeps) int {
	if len(args) == 0 || args[0] != "status" {
		return ExitUsage
	}
	fs := flag.NewFlagSet("panewire sentinel status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	stage2ClientEnv := fs.String("stage2-client-env", "", "explicit mode-0600 enrolled client env")
	clientEnv := fs.String("client-env", "", "explicit mode-0600 enrolled client env")
	if err := fs.Parse(args[1:]); err != nil || fs.NArg() != 0 {
		return ExitUsage
	}
	credentialPath, err := singleFlagValue(*stage2ClientEnv, *clientEnv, "client env")
	if err != nil || credentialPath == "" {
		return ExitUsage
	}
	credential, err := loadStage2ClientEnv(credentialPath)
	if err != nil {
		fmt.Fprintln(stderr, "sentinel status rejected: invalid mode-0600 client credentials")
		return ExitConditionInvalid
	}
	adapter, err := supabase.New(supabase.Config{
		BaseURL: credential.URL, AccessToken: credential.AccessToken, RefreshToken: credential.RefreshToken, APIKey: credential.PublishableKey,
		ClientEnvPath: credentialPath, HTTPClient: deps.HTTPClient, AllowInsecureForTests: deps.AllowInsecureForTests,
	})
	if err != nil {
		fmt.Fprintln(stderr, "sentinel status rejected: invalid Supabase endpoint")
		return ExitConditionInvalid
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	heartbeats, err := adapter.ListHeartbeats(ctx)
	if err != nil {
		fmt.Fprintln(stderr, "sentinel status unavailable")
		return ExitInternal
	}
	now := time.Now
	if deps.Now != nil {
		now = deps.Now
	}
	renderSentinelStatus(stdout, credential.MachineID, heartbeats, now().UTC())
	return ExitOK
}

func renderSentinelStatus(writer io.Writer, localMachineID string, heartbeats []sentinel.Heartbeat, now time.Time) {
	rows := make([]sentinel.Heartbeat, 0, len(heartbeats))
	for _, heartbeat := range heartbeats {
		if heartbeat.MachineID != localMachineID {
			rows = append(rows, heartbeat)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].MachineID < rows[j].MachineID })
	fmt.Fprintln(writer, "MACHINE\tSEEN_AT\tAGE\tCHECKS\tVERSION")
	for _, heartbeat := range rows {
		age := now.Sub(heartbeat.SeenAt)
		if age < 0 {
			age = 0
		}
		fmt.Fprintf(writer, "%s\t%s\t%s\t%s\t%s\n", heartbeat.MachineID, heartbeat.SeenAt.UTC().Format(time.RFC3339), age.Truncate(time.Second), sentinel.ChecksSummary(heartbeat.Checks), heartbeat.Version)
	}
}

func buildSentinelDaemonConfig(clientEnvPath, configPath, telegramEnvPath string, watch bool, heartbeatInterval, watchInterval time.Duration, deps daemonCLIDeps) (SentinelConfig, error) {
	credential, err := loadStage2ClientEnv(clientEnvPath)
	if err != nil {
		return SentinelConfig{}, fmt.Errorf("sentinel client env must be a regular mode-0600 file")
	}
	settings, err := sentinel.LoadConfig(configPath, credential.MachineID)
	if err != nil {
		return SentinelConfig{}, fmt.Errorf("sentinel config is invalid")
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	adapter, err := supabase.New(supabase.Config{
		BaseURL: credential.URL, AccessToken: credential.AccessToken, RefreshToken: credential.RefreshToken, APIKey: credential.PublishableKey,
		ClientEnvPath: clientEnvPath, Logger: logger, HTTPClient: deps.HTTPClient, AllowInsecureForTests: deps.AllowInsecureForTests,
	})
	if err != nil {
		return SentinelConfig{}, fmt.Errorf("configure sentinel Supabase adapter")
	}
	var notifier sentinel.Notifier
	if watch {
		env, err := loadSentinelTelegramEnv(telegramEnvPath)
		if err != nil {
			return SentinelConfig{}, fmt.Errorf("sentinel Telegram env must be a regular mode-0600 file")
		}
		notifier, err = newTelegramNotifier(env, sentinelNotifierDeps{
			HTTPClient: deps.HTTPClient, BaseURL: deps.TelegramBaseURL, AllowInsecureForTests: deps.AllowInsecureForTests,
		})
		if err != nil {
			return SentinelConfig{}, fmt.Errorf("configure sentinel Telegram notifier")
		}
	}
	runner, err := sentinel.NewService(sentinel.ServiceConfig{
		MachineID: credential.MachineID, Settings: settings, Remote: adapter, Notifier: notifier,
		Warn: func(message string) { logger.Warn(message) },
	})
	if err != nil {
		return SentinelConfig{}, fmt.Errorf("configure sentinel service")
	}
	return SentinelConfig{
		Enabled: true, Watch: watch, Runner: runner, HeartbeatInterval: heartbeatInterval, WatchInterval: watchInterval,
	}, nil
}
