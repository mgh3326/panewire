package panewire

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// seedAssistantChatRow writes the shape handoffkeep produces for an assistant
// answer: author=operator, source_channel=assistant, relay_state=stored — the
// exact orphan shape the sweep must never touch.
func seedAssistantChatRow(t *testing.T, store *fakeChatStore, body string) ChatMessage {
	t.Helper()
	message, created, err := store.CreateChatMessageExtended(context.Background(), ChatMessageCreate{
		ConversationID:  hubChatConversationID,
		Author:          "operator",
		Body:            body,
		SourceChannel:   "assistant",
		OriginEventID:   "berry-" + body,
		OriginTimestamp: time.Now().UTC(),
	})
	if err != nil || !created {
		t.Fatalf("seed assistant row: created=%v err=%v", created, err)
	}
	return message
}

// AC8a: a stored source_channel=assistant row is neither failed by the orphan
// sweep — inside or outside the grace window — nor delivered; only the
// notification_outbox is its lane notice. The operator control row keeps the
// pre-existing behavior exactly.
func TestHubChatAssistantRowSurvivesOrphanSweep(t *testing.T) {
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{}}`, nil, store)
	ctx := context.Background()

	oldAssistant := seedAssistantChatRow(t, store, "old assistant answer")
	oldOperator, err := store.CreateChatMessage(ctx, "operator", "old operator answer")
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	old := time.Now().UTC().Add(-time.Hour)
	store.messages[oldAssistant.ID].CreatedAt = old
	store.messages[oldOperator.ID].CreatedAt = old
	store.mu.Unlock()
	freshAssistant := seedAssistantChatRow(t, store, "fresh assistant answer")

	hub.drainChatOutbox(ctx)
	if got := store.messageState(oldAssistant.ID); got != "stored" {
		t.Fatalf("old assistant row state=%q, want stored (never failed by the sweep)", got)
	}
	if got := store.messageState(freshAssistant.ID); got != "stored" {
		t.Fatalf("fresh assistant row state=%q, want stored", got)
	}
	if got := store.messageState(oldOperator.ID); got != "failed" {
		t.Fatalf("old operator row state=%q, want failed — web behavior must be unchanged", got)
	}
	// A second sweep still leaves the assistant row stored: it is a
	// permanent exemption, not a grace-window deferral.
	hub.drainChatOutbox(ctx)
	if got := store.messageState(oldAssistant.ID); got != "stored" {
		t.Fatalf("assistant row failed on second sweep: %q", got)
	}
}

// AC8b: a durable chat-<id> relay row that somehow exists for an assistant
// message is retired at the replay gate, never injected — the hub never gets
// a second notice channel next to the outbox.
func TestHubChatAssistantRowReplayRetires(t *testing.T) {
	fake, relay, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, relay, store)
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 4), persisted: make(chan hubRelayPersistedEvent, 4)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: destination}
	ctx := context.Background()

	assistant := seedAssistantChatRow(t, store, "answer via berry")
	operator, err := store.CreateChatMessage(ctx, "operator", "operator relay control")
	if err != nil {
		t.Fatal(err)
	}
	// Durable rows: one naming the assistant message, one naming a live
	// operator row. Both look deliverable; only the operator one may inject.
	fake.seedUndelivered(
		handoffkeepRelayEvent{ID: 70, Kind: "lane.event", JobID: laneEventTransportID("lane-a", chatRelayEventID(assistant.ID, assistant.Body)), Epoch: 1, OwnerLane: "lane-a", EventID: chatRelayEventID(assistant.ID, assistant.Body), Text: "[chat] assistant", ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano)},
		handoffkeepRelayEvent{ID: 71, Kind: "lane.event", JobID: laneEventTransportID("lane-a", chatRelayEventID(operator.ID, operator.Body)), Epoch: 1, OwnerLane: "lane-a", EventID: chatRelayEventID(operator.ID, operator.Body), Text: "[chat] operator", ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano)},
	)

	hub.replayUndeliveredRelayEvents(ctx)

	if got := fake.deliveredToFor(70); got != "hub/chat-terminal" {
		t.Fatalf("assistant relay row delivered_to=%q, want hub/chat-terminal (retired)", got)
	}
	deadline := time.After(2 * time.Second)
	var injected []string
	for {
		select {
		case event := <-destination.relays:
			injected = append(injected, event.Text)
		case <-deadline:
			t.Fatal("operator control row was not replayed")
		default:
			goto drained
		}
	}
drained:
	for _, text := range injected {
		if strings.Contains(text, "assistant") {
			t.Fatalf("assistant chat row was injected: %q", text)
		}
	}
	found := false
	for _, text := range injected {
		if strings.Contains(text, "operator") {
			found = true
		}
	}
	if !found {
		t.Fatalf("operator relay row was not injected: %v", injected)
	}
}

// AC8c: the console retry and cancel verbs refuse assistant rows outright —
// retry would mint a second notice channel and cancel would hide a notice
// the hub does not own.
func TestHubChatAssistantRowRetryCancelRefused(t *testing.T) {
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, nil, store)

	assistant := seedAssistantChatRow(t, store, "berry answer")
	store.mu.Lock()
	store.messages[assistant.ID].RelayState = "failed"
	store.mu.Unlock()
	// Even in the failed state retry is refused — the channel check precedes
	// the state check.
	if writer := chatServe(t, hub, chatUIRequest(t, http.MethodPost, "/chat/messages/"+strconv.FormatInt(assistant.ID, 10)+"/retry", `{}`)); writer.Code != http.StatusConflict || !strings.Contains(writer.Body.String(), "chat_message_not_operator") {
		t.Fatalf("assistant retry status=%d body=%q, want 409 chat_message_not_operator", writer.Code, writer.Body.String())
	}
	store.mu.Lock()
	store.messages[assistant.ID].RelayState = "stored"
	store.mu.Unlock()
	if writer := chatServe(t, hub, chatUIRequest(t, http.MethodPost, "/chat/messages/"+strconv.FormatInt(assistant.ID, 10)+"/cancel", `{}`)); writer.Code != http.StatusConflict || !strings.Contains(writer.Body.String(), "chat_message_not_operator") {
		t.Fatalf("assistant cancel status=%d body=%q, want 409 chat_message_not_operator", writer.Code, writer.Body.String())
	}
	if got := store.messageState(assistant.ID); got != "stored" {
		t.Fatalf("assistant row state=%q after refused cancel", got)
	}

	// The data feed carries the channel so the page can label the row.
	data := chatServe(t, hub, chatUIRequest(t, http.MethodGet, "/chat/data", ""))
	if data.Code != http.StatusOK || !strings.Contains(data.Body.String(), `"source_channel":"assistant"`) {
		t.Fatalf("/chat/data lacks the assistant channel marker: %q", data.Body.String())
	}
	// And the page labels assistant rows instead of showing operator state.
	page := chatServe(t, hub, chatUIRequest(t, http.MethodGet, "/chat", ""))
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "어시스턴트") || !strings.Contains(page.Body.String(), `entry.assistant`) || !strings.Contains(page.Body.String(), `source_channel==="assistant"`) {
		t.Fatalf("/chat page lacks the assistant label")
	}
}

// Fix-round MINOR-3 (M5): the assistant exemption is exactly
// source_channel=assistant — a real web-channel operator row still fails the
// orphan sweep outside its grace window, so a mutant widening the exemption
// to web rows is caught.
func TestHubChatWebRowStillOrphanFailsR2(t *testing.T) {
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{}}`, nil, store)
	ctx := context.Background()

	web, _, err := store.CreateChatMessageExtended(ctx, ChatMessageCreate{
		ConversationID:  hubChatConversationID,
		Author:          "operator",
		Body:            "web operator row",
		SourceChannel:   "web",
		OriginEventID:   "web-1",
		OriginTimestamp: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	assistant := seedAssistantChatRow(t, store, "assistant control")
	store.mu.Lock()
	old := time.Now().UTC().Add(-time.Hour)
	store.messages[web.ID].CreatedAt = old
	store.messages[assistant.ID].CreatedAt = old
	store.mu.Unlock()

	hub.drainChatOutbox(ctx)
	if got := store.messageState(web.ID); got != "failed" {
		t.Fatalf("web row state=%q, want failed — the exemption must not cover web", got)
	}
	if got := store.messageState(assistant.ID); got != "stored" {
		t.Fatalf("assistant control row state=%q, want stored", got)
	}
}
