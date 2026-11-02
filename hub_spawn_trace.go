package panewire

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/getsentry/sentry-go"
)

var spawnStages = []string{"claim", "placement", "checkout", "launch", "landing"}

func validSpawnStage(stage string) bool {
	for _, name := range spawnStages {
		if name == stage {
			return true
		}
	}
	return false
}

func validSpawnStages(stages map[string]int64) bool {
	for stage, ms := range stages {
		if !validSpawnStage(stage) || ms < 0 || ms > 3600000 {
			return false
		}
	}
	return true
}

// This client is used only by detached spawn completion; ordinary hub traffic
// never starts transactions. WithSpanSampled forces these rare transactions in.
func newSpawnTelemetry() *sentry.Hub {
	dsn := os.Getenv(crashReportDSNEnv)
	if dsn == "" {
		return nil
	}
	client, err := sentry.NewClient(sentry.ClientOptions{Dsn: dsn, EnableTracing: true, TracesSampleRate: 1, SampleRate: 1, SendDefaultPII: false, ServerName: "panewire-hub"})
	if err != nil {
		return nil
	}
	return sentry.NewHub(client, sentry.NewScope())
}

func (h *HubServer) traceAsyncSpawn(request hubSpawnRequest, result hubSpawnResult) {
	if h.spawnTelemetry == nil {
		return
	}
	hub := h.spawnTelemetry.Clone()
	tags := map[string]string{"job": request.JobID, "host": request.Machine, "profile": "auto", "role": "worker", "rc": strconv.Itoa(result.RC)}
	for i := 0; i+1 < len(request.Args); i += 2 {
		switch request.Args[i] {
		case "-m":
			tags["profile"] = request.Args[i+1]
		case "--role":
			tags["role"] = request.Args[i+1]
		}
	}
	tx := sentry.StartTransaction(sentry.SetHubOnContext(context.Background(), hub), "runtug.spawn", sentry.WithSpanSampled(sentry.SampledTrue))
	tx.StartTime = h.now().UTC()
	h.mu.Lock()
	if record := h.spawnRecords[request.RequestID]; record != nil {
		tx.StartTime = record.created
	}
	h.mu.Unlock()
	for key, value := range tags {
		tx.SetTag(key, value)
	}
	var total time.Duration
	for _, ms := range result.StageMS {
		total += time.Duration(ms) * time.Millisecond
	}
	if earliest := h.now().UTC().Add(-total); earliest.Before(tx.StartTime) {
		tx.StartTime = earliest
	}
	cursor := tx.StartTime
	for _, stage := range spawnStages {
		ms, measured := result.StageMS[stage]
		if !measured {
			continue
		}
		span := tx.StartChild("runtug." + stage)
		span.StartTime = cursor
		span.EndTime = cursor.Add(time.Duration(ms) * time.Millisecond)
		cursor = span.EndTime
		span.Finish()
	}
	tx.EndTime = h.now().UTC()
	tx.Finish()
	if result.RC != 0 || result.Error != "" || result.JobID != request.JobID || result.Pane == "" {
		reason, stage := result.Error, result.Stage
		if reason == "" {
			reason = "invalid_spawn_receipt"
		}
		if stage == "" {
			stage = "unknown"
		}
		event := sentry.NewEvent()
		event.Message = "runtug SPAWN_FAILED"
		event.Level = sentry.LevelError
		event.Tags = tags
		event.Fingerprint = []string{"runtug.spawn", stage, reason}
		hub.CaptureEvent(event)
	}
}
