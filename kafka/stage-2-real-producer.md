# Stage 2 — A real producer (HTTP API → Kafka)

**Goal:** replace the simulator with a real Fastify API, and learn the
producer-side knobs that decide whether you can lose or duplicate data.

## Concepts

### Events are facts, not commands

`message.sent` states something that **happened** — past tense, immutable.
It is not "send a notification" (a command to a specific service). Publishing
facts is what lets you add consumers later without touching the producer.
Naming convention: `domain.entity.action` → topics `chat.messages`,
`chat.typing`; event types `message.sent`, `typing.started`.

### The envelope

Every event should carry metadata besides its payload:

```ts
{
  eventId: string,      // unique per event — consumers dedupe on this (Stage 4)
  type: "message.sent",
  version: 1,           // schema version (Stage 5)
  occurredAt: string,   // when it happened (≠ when Kafka received it)
  payload: { ... }
}
```

### acks — the durability dial

When a producer sends, when is it "done"?

- `acks: 0` — fire and forget. Fastest; silently loses data. Almost never right.
- `acks: 1` — the partition leader wrote it. Lost if the leader dies before replicating.
- `acks: -1` (**all**) — all in-sync replicas wrote it. The production default for anything that matters.

On our single broker, 1 and all behave identically — but write the config as
if production: `acks: -1`.

### The idempotent producer

Retries can duplicate: broker writes the message, the *ack* gets lost, the
producer retries → written twice. Setting `idempotent: true` gives every
producer a session ID and sequence numbers so the broker drops duplicates.
Effectively free — turn it on. (kafkajs then requires `acks: -1` and caps
`maxInFlightRequests` at 5.)

### Producer lifecycle

One producer instance per process — created at startup, shared by all
requests, disconnected on shutdown. Never create a producer per request
(connection setup is expensive, and you lose batching).

## Build

Milestone 1 — the API:
```bash
npm i fastify
```
`src/api.ts`: Fastify with `POST /messages` accepting
`{conversationId, senderId, recipientId, text}`; build the envelope
(`eventId: randomUUID()`), publish to `chat.messages` keyed by
`conversationId`, reply `202 Accepted` with the eventId.

```ts
const producer = kafka.producer({ idempotent: true }); // implies acks=all
await producer.connect(); // once, at startup

app.post("/messages", async (req, reply) => {
  const event = toEnvelope(req.body);
  await producer.send({
    topic: TOPICS.chatMessages,
    messages: [{ key: event.payload.conversationId, value: JSON.stringify(event) }],
  });
  return reply.code(202).send({ eventId: event.eventId });
});
```

Milestone 2 — a second topic: `chat.typing` for `typing.started` events, 3
partitions, and set topic retention to 60s (`retention.ms: "60000"` in
`createTopics` configEntries) — typing indicators are worthless after a minute.
This teaches you retention is *per topic*.

Milestone 3 — graceful shutdown: on SIGINT, `app.close()` then
`producer.disconnect()` (flushes buffered messages). Order matters — why?

Milestone 4 — drive it:
```bash
npm i -D autocannon
curl -X POST localhost:3000/messages -H 'content-type: application/json' \
  -d '{"conversationId":"conv-1","senderId":"alice","recipientId":"bob","text":"hi"}'
npx autocannon -m POST -H content-type=application/json \
  -b '{"conversationId":"conv-1","senderId":"alice","recipientId":"bob","text":"load"}' \
  localhost:3000/messages
```
Watch throughput in Kafka UI while autocannon runs. Note requests/sec — Kafka
will not be your bottleneck.

## Break it

1. **Kafka down, API up:** `docker compose stop kafka`, POST a message. What
   does the client see, after how long? kafkajs retries internally — find the
   `retry` config, set sensible bounds, decide what your API should return
   (503?). Restart Kafka mid-retry and watch the send succeed.
2. **acks comparison:** temporarily `idempotent: false, acks: 0` and rerun
   autocannon. Compare latency. Understand what you'd be trading away.
3. **No key:** publish typing events without a key, watch round-robin in the UI.

## Self-check

1. Why `202 Accepted` and not `200 OK` with the notification result?
2. Your producer retried a send after a network blip. Under what config can that create a duplicate, and what prevents it?
3. Why is one producer per process (not per request) the rule?
4. Retention on `chat.typing` is 60s. A consumer that was down for 5 minutes comes back — what does it see, and what does its committed offset point at?
5. When would `acks: 1` actually lose a message? (You'll verify this on a real cluster in Stage 6.)

## Reading

- *Definitive Guide* ch. 3 ("Producers") — acks, retries, idempotence
- kafkajs docs: Producing messages (kafka.js.org)
