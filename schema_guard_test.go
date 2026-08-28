package panewire_test

import (
	"context"
	"strings"
	"testing"

	panewire "github.com/mgh3326/panewire"
)

func TestSchemaGuardFixtures(t *testing.T) {
	good := fixtureSchema(true, true)
	guard, err := panewire.GuardSchema(strings.NewReader(good))
	if err != nil || !guard.AgentWait || !guard.Events {
		t.Fatalf("normal schema must enable R1 capabilities: guard=%+v err=%v", guard, err)
	}

	unknown, err := panewire.GuardSchema(strings.NewReader(fixtureSchema(true, true)[:len(fixtureSchema(true, true))-1] + ",\"unknown_event\":true}"))
	if err != nil || !unknown.AgentWait || len(unknown.Warnings) == 0 {
		t.Fatalf("unknown schema fields must warn without disabling capabilities: guard=%+v err=%v", unknown, err)
	}

	missing, err := panewire.GuardSchema(strings.NewReader(fixtureSchema(false, false)))
	if err != nil || missing.AgentWait || missing.Prompt || !missing.Events {
		t.Fatalf("missing required agent contract must fail closed: guard=%+v err=%v", missing, err)
	}
}

func TestSchemaGuardReconnectIsRecorded(t *testing.T) {
	db := panewire.NewMemoryStore(t)
	d := panewire.NewDaemon(panewire.Config{Store: db})
	ctx := context.Background()
	if err := d.RecordSchemaGuard(ctx, panewire.GuardResult{AgentWait: true, Events: true}, "startup"); err != nil {
		t.Fatal(err)
	}
	if err := d.RecordSchemaGuard(ctx, panewire.GuardResult{AgentWait: false, Events: true}, "reconnect"); err != nil {
		t.Fatal(err)
	}
	if got := db.CountEventKind("herdr.schema_guard"); got != 2 {
		t.Fatalf("schema startup/reconnect rows=%d, want 2", got)
	}
}

func fixtureSchema(agentWait, prompt bool) string {
	methods := []string{"events.subscribe", "agent.read", "pane.read", "pane.send_keys"}
	if agentWait {
		methods = append(methods, "agent.wait")
	}
	if prompt {
		methods = append(methods, "agent.prompt")
	}
	return `{"protocol":20,"schema_version":1,"schemas":{"request":{"methods":` + quoteStrings(methods) + `,"agent_status":["idle","working","blocked","done","unknown"],"read_source":["visible","recent","recent_unwrapped","detection"],"subscriptions":["pane.agent_status_changed","pane.output_matched","pane.scroll_changed"]}}}`
}

func quoteStrings(values []string) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, value := range values {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('"')
		b.WriteString(value)
		b.WriteByte('"')
	}
	b.WriteByte(']')
	return b.String()
}
