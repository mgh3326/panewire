package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHubChatPhase1OwnershipAndIdempotency(t *testing.T) {
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, nil, store)
	ctx := context.Background()
	if _, _, err := store.UpsertChatQuestion(ctx, ChatQuestionUpsert{ID: "Q-20260928-01", ConversationID: hubChatConversationID, Lane: "lane-a", Body: "첫 질문"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.UpsertChatQuestion(ctx, ChatQuestionUpsert{ID: "Q-20260928-02", ConversationID: hubChatConversationID, Lane: "lane-b", Body: "다른 창구"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.UpsertChatQuestion(ctx, ChatQuestionUpsert{ID: "Q-20260928-03", ConversationID: "other", Lane: "lane-a", Body: "다른 대화"}); err != nil {
		t.Fatal(err)
	}
	bad := []struct {
		name, body string
		code       int
	}{
		{"conversation", `{"conversation_id":"other","body":"답","origin_event_id":"bad-c"}`, http.StatusBadRequest},
		{"lane", `{"conversation_id":"operator-desk","lane":"lane-b","body":"답","origin_event_id":"bad-l"}`, http.StatusConflict},
		{"question lane", `{"conversation_id":"operator-desk","body":"답","question_ids":["Q-20260928-02"],"origin_event_id":"bad-q"}`, http.StatusConflict},
		{"question conversation", `{"conversation_id":"operator-desk","body":"답","question_ids":["Q-20260928-03"],"origin_event_id":"bad-q2"}`, http.StatusConflict},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			w := chatPostMessage(t, hub, tc.body)
			if w.Code != tc.code {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
	body := `{"conversation_id":"operator-desk","body":"첫 답","question_ids":["Q-20260928-01"],"origin_event_id":"browser-1"}`
	first := chatPostMessage(t, hub, body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first=%d %s", first.Code, first.Body.String())
	}
	repeat := chatPostMessage(t, hub, body)
	if repeat.Code != http.StatusOK || chatDecodeMessage(t, repeat).ID != chatDecodeMessage(t, first).ID {
		t.Fatalf("repeat=%d %s", repeat.Code, repeat.Body.String())
	}
	collision := chatPostMessage(t, hub, `{"conversation_id":"operator-desk","body":"changed","question_ids":["Q-20260928-01"],"origin_event_id":"browser-1"}`)
	if collision.Code != http.StatusConflict {
		t.Fatalf("collision=%d %s", collision.Code, collision.Body.String())
	}
	if len(store.order) != 1 {
		t.Fatalf("stored rows=%d, want 1", len(store.order))
	}
	if got := store.message(1).QuestionRelations; len(got) != 1 || got[0].QuestionText != "첫 질문" {
		t.Fatalf("relations=%+v", got)
	}
	if _, _, err := store.UpsertChatQuestion(ctx, ChatQuestionUpsert{ID: "Q-20260928-04", ConversationID: hubChatConversationID, Lane: "lane-a", Body: "넷째 질문"}); err != nil {
		t.Fatal(err)
	}
	multi := chatPostMessage(t, hub, `{"conversation_id":"operator-desk","body":"둘 다 답","question_ids":["Q-20260928-04","Q-20260928-01"],"origin_event_id":"browser-2"}`)
	if multi.Code != http.StatusCreated {
		t.Fatalf("multi=%d %s", multi.Code, multi.Body.String())
	}
	if got := chatReplyIDs(chatDecodeMessage(t, multi).QuestionRelations); len(got) != 2 || got[0] != "Q-20260928-01" || got[1] != "Q-20260928-04" {
		t.Fatalf("canonical relations=%v", got)
	}
}

func TestHubChatPhase1RelationRestartRetry(t *testing.T) {
	_, relay, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	store := newFakeChatStore()
	lanes := `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`
	hub1 := chatTestHub(t, lanes, relay, store)
	ctx := context.Background()
	for i, body := range []string{"첫 원문", "둘째 원문"} {
		id := fmt.Sprintf("Q-20260928-%02d", i+1)
		if _, _, err := store.UpsertChatQuestion(ctx, ChatQuestionUpsert{ID: id, ConversationID: hubChatConversationID, Lane: "lane-a", Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	sent := chatPostMessage(t, hub1, `{"conversation_id":"operator-desk","body":"짧은 답","question_ids":["Q-20260928-01","Q-20260928-02"],"origin_event_id":"multi-1"}`)
	if sent.Code != http.StatusCreated {
		t.Fatalf("send=%d %s", sent.Code, sent.Body.String())
	}
	hub1.drainChatOutbox(ctx) // durable row, destination disconnected
	if _, _, err := store.UpsertChatQuestion(ctx, ChatQuestionUpsert{ID: "Q-20260928-01", ConversationID: hubChatConversationID, Lane: "lane-a", Body: "나중에 바뀐 질문"}); err != nil {
		t.Fatal(err)
	}
	hub2 := chatTestHub(t, lanes, relay, store)
	data := chatServe(t, hub2, chatUIRequest(t, http.MethodGet, "/chat/data", ""))
	if data.Code != http.StatusOK {
		t.Fatalf("data=%d", data.Code)
	}
	var view hubChatData
	if err := json.Unmarshal(data.Body.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if len(view.Messages) != 1 || len(view.Messages[0].QuestionRelations) != 2 || view.Messages[0].QuestionRelations[0].QuestionText != "첫 원문" {
		t.Fatalf("restarted relations=%+v", view.Messages)
	}
	if got := store.questionState("Q-20260928-01"); got != "pending" {
		t.Fatalf("delivery resolved Q: %s", got)
	}
	if _, err := store.MarkChatMessageFailed(ctx, 1); err != nil {
		t.Fatal(err)
	}
	hub2.replayUndeliveredLaneEvents(ctx) // retire failed source's queued relay
	retry := chatServe(t, hub2, chatUIRequest(t, http.MethodPost, "/chat/messages/1/retry", `{}`))
	if retry.Code != http.StatusCreated {
		t.Fatalf("retry=%d %s", retry.Code, retry.Body.String())
	}
	if got := chatReplyIDs(chatDecodeMessage(t, retry).QuestionRelations); len(got) != 2 || got[0] != "Q-20260928-01" || got[1] != "Q-20260928-02" {
		t.Fatalf("retry IDs=%v", got)
	}
	if got := chatDecodeMessage(t, retry).OriginEventID; got != chatRetryOriginEventID(1) {
		t.Fatalf("retry event ID=%q", got)
	}
	reserved := chatPostMessage(t, hub2, `{"conversation_id":"operator-desk","body":"forged","origin_event_id":"_chat-retry-of-1"}`)
	if reserved.Code != http.StatusBadRequest {
		t.Fatalf("reserved browser event ID=%d", reserved.Code)
	}
	hub2.drainChatOutbox(ctx)
	again := chatServe(t, hub2, chatUIRequest(t, http.MethodPost, "/chat/messages/1/retry", `{}`))
	if again.Code != http.StatusOK || chatDecodeMessage(t, again).ID != 2 {
		t.Fatalf("repeat retry=%d %s", again.Code, again.Body.String())
	}
	if len(store.order) != 2 {
		t.Fatalf("retry rows=%d", len(store.order))
	}
}

func TestHubChatPhase1RelayBytesAndDeskNotSent(t *testing.T) {
	ids := []string{"Q-20260928-01", "Q-20260928-02"}
	prefix := chatRelayText(7, ids, "")
	if !strings.Contains(prefix, "message=7 reply_to=Q-20260928-01,Q-20260928-02") {
		t.Fatalf("metadata=%q", prefix)
	}
	n := (laneEventTextLimit - len(prefix)) / len("한")
	at := chatRelayText(7, ids, strings.Repeat("한", n))
	if len(at) > laneEventTextLimit || !strings.Contains(at, "한") {
		t.Fatalf("boundary bytes=%d text=%q", len(at), at)
	}
	over := chatRelayText(7, ids, strings.Repeat("한", n+2))
	if len(over) > laneEventTextLimit || strings.Contains(over, "한") || !strings.Contains(over, "reply_to=Q-20260928-01,Q-20260928-02") {
		t.Fatalf("reference bytes=%d text=%q", len(over), over)
	}
	store := newFakeChatStore()
	hub := chatTestHub(t, `{"lanes":{}}`, nil, store)
	desk, err := store.CreateChatMessage(context.Background(), "desk", "창구 본문")
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.messages[desk.ID].RelayState = "not_sent"
	store.messages[desk.ID].CreatedAt = time.Now().Add(-time.Hour)
	store.mu.Unlock()
	hub.failOrphanChatMessages(context.Background())
	if got := store.messageState(desk.ID); got != "not_sent" {
		t.Fatalf("orphan changed desk row to %s", got)
	}
	if got := hub.chatReplayDisposition(desk.ID); got != "retire" {
		t.Fatalf("desk replay=%s", got)
	}
	if response := chatServe(t, hub, chatUIRequest(t, http.MethodPost, fmt.Sprintf("/chat/messages/%d/cancel", desk.ID), `{}`)); response.Code != http.StatusConflict {
		t.Fatalf("desk cancel=%d", response.Code)
	}
	if response := chatServe(t, hub, chatUIRequest(t, http.MethodPost, fmt.Sprintf("/chat/messages/%d/retry", desk.ID), `{}`)); response.Code != http.StatusConflict {
		t.Fatalf("desk retry=%d", response.Code)
	}
	hub.drainChatOutbox(context.Background())
	if got := store.messageState(desk.ID); got != "not_sent" {
		t.Fatalf("dispatcher changed desk row to %s", got)
	}
}

func TestHubChatPhase1OldStoreFailsExplicitly(t *testing.T) {
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":1,"author":"operator","body":"answer","relay_state":"stored"}`))
	}))
	defer old.Close()
	store, err := newHandoffkeepChatStore(hubHandoffkeepEnv{URL: old.URL, Token: "test-token"}, old.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.CreateChatMessageExtended(context.Background(), ChatMessageCreate{ConversationID: hubChatConversationID, Author: "operator", Body: "answer", SourceChannel: "web", OriginEventID: "event-1", OriginTimestamp: time.Now()})
	if err != errChatStoreIncompatible {
		t.Fatalf("old store error=%v", err)
	}
	response := httptest.NewRecorder()
	(&HubServer{}).writeChatStoreError(response, err)
	if response.Code != http.StatusBadGateway || !strings.Contains(response.Body.String(), "chat_store_incompatible") {
		t.Fatalf("hub version skew response=%d %s", response.Code, response.Body.String())
	}
}

func TestHubChatPhase1QuestionLookupHandlesVariableWidthIDs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.Header.Get("Authorization") != "Bearer test-token" || request.URL.RawQuery != "" {
			t.Errorf("lookup request=%s %s auth=%q", request.Method, request.URL.String(), request.Header.Get("Authorization"))
		}
		switch request.URL.Path {
		case "/v1/chat/questions/Q-20260928-01":
			_, _ = writer.Write([]byte(`{"id":"Q-20260928-01","conversation_id":"operator-desk","lane":"lane-a","body":"wanted"}`))
		case "/v1/chat/questions/Q-20260928-02":
			writer.WriteHeader(http.StatusNotFound)
			_, _ = writer.Write([]byte(`{"error":"chat_question_not_found"}`))
		case "/v1/chat/questions/Q-20260928-03":
			_, _ = writer.Write([]byte(`{"id":"Q-20260928-04","conversation_id":"operator-desk","lane":"lane-a","body":"wrong"}`))
		case "/v1/chat/questions/Q-20260928-04":
			writer.WriteHeader(http.StatusNotFound)
			_, _ = writer.Write([]byte("404 page not found"))
		default:
			t.Errorf("unexpected lookup path=%s", request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	store, err := newHandoffkeepChatStore(hubHandoffkeepEnv{URL: server.URL, Token: "test-token"}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	question, found, err := store.GetChatQuestion(context.Background(), "Q-20260928-01")
	if err != nil || !found || question.Body != "wanted" {
		t.Fatalf("question=%+v found=%v err=%v", question, found, err)
	}
	_, found, err = store.GetChatQuestion(context.Background(), "Q-20260928-02")
	if err != nil || found {
		t.Fatalf("missing question found=%v err=%v", found, err)
	}
	_, found, err = store.GetChatQuestion(context.Background(), "Q-20260928-03")
	if !errors.Is(err, errChatStoreIncompatible) || found {
		t.Fatalf("mismatched response found=%v err=%v", found, err)
	}
	_, found, err = store.GetChatQuestion(context.Background(), "Q-20260928-04")
	if !errors.Is(err, errChatStoreIncompatible) || found {
		t.Fatalf("old route response found=%v err=%v", found, err)
	}
}
