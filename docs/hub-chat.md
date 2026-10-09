# Operator chat hub (/chat)

The hub serves one operator-desk conversation. It reads desk messages and questions from handoffkeep and sends operator replies to the configured desk lane. Every browser endpoint uses the existing operator bearer token or verified Cloudflare Access assertion. POST requests also require a same-origin Origin and Sec-Fetch-Site. The browser receives no token or lane configuration.

## Startup

Set --chat-desk-lane to the destination lane when --chat-env or --handoffkeep-env enables the chat store. The lane must be explicit and valid; hub startup fails when it is absent. Keep this value aligned with the desk question lane and the lanes route file. A question with a different lane or conversation is rejected before a reply is stored. The hub never reads live settings to infer the destination. --chat-env is an optional separate mode-0600 handoffkeep credential file; it defaults to --handoffkeep-env. The chat client has its own five-second timeout and connection pool.

Question ownership checks use handoffkeep GET /v1/chat/questions/{id}. This direct lookup retains old questions even when they are outside the hub's recent question page; the list endpoint's after_id cursor requires a complete Q ID.

Deploy the extended handoffkeep chat API before this hub version. An older store can reject the new request fields or return a message without conversation, source, event, and relation data. The hub reports chat_store_incompatible instead of accepting a write that loses links. Older hub and hook clients can continue to use the new store's legacy behavior. During the interval before the new hook deploys, desk questions from the old hook appear as standalone question cards; no desk message body is inferred.

## Browser API

GET /chat serves the page. GET /chat/data returns the bounded chronological message tail and question list. POST /chat/messages takes conversation_id=operator-desk, body, origin_event_id, and question_ids. A single question_id is accepted for transition clients. A supplied lane must match the configured desk lane exactly. The hub validates each question's conversation and lane before inserting the answer. The browser retains its event ID across a failed send. A repeated event ID with the same payload returns the stored row; a changed body or relation set returns 409.

POST /chat/questions/{id}/transition explicitly resolves or withdraws a pending question. Merely answering or delivering a reply never resolves it. POST /chat/messages/{id}/retry uses the original failed row's body and stored relations plus its original lane from hub memory or a durable relay row. Repeating the retry returns its existing row without a second directive. POST /chat/messages/{id}/cancel closes a stored operator row and retires its queued relay row. Desk not_sent rows cannot be retried, cancelled, dispatched, replayed, or failed by orphan cleanup.

The UI shows desk and operator messages in insertion order, desk question cards, quoted question snapshots on operator replies, pending questions in a side list, and multi-question selection chips. Polling runs every five seconds while retaining the draft, selection, and scroll position. It follows the tail only when the reader is near the bottom. All content is inserted as textContent.

## Delivery and failures

The hub stores each answer before queueing delivery. The background dispatcher persists a lane.event relay row, then injects it. A row that cannot yet be routed remains queued for replay. Successful direct delivery and replay mark only the message delivered. The durable relay row contains the message ID, destination lane, and Q IDs; the store message contains authoritative question text snapshots. The full UTF-8 relay text, including message and reply metadata, is limited to 2048 bytes. Longer bodies become an ID reference while their full text and question links remain in the store. The hub rejects a retry if the original route cannot be recovered rather than using browser input.

A chat store outage affects chat endpoints and the chat dispatcher, not general hub relay traffic. A chat handler panic is contained to its request. The chat page can still load during a store outage and reports the data failure.

## Assistant-channel rows

A message stored in handoffkeep with source_channel=assistant is the assistant (berry) answer path (MGH-36). The hub keeps one notice channel per row: such rows are never relayed as chat-{id} relay rows, the startup orphan sweep never marks them failed inside or outside its grace window, and the browser retry and cancel endpoints refuse them with chat_message_not_operator. Their only lane notice is the handoffkeep notification_outbox row written by the answer itself, drained by the assistant binary's PR-3 write path. The page renders them right-aligned labelled 어시스턴트 with an assistant badge and offers no retry or cancel action. Operator and desk rows are unaffected.
