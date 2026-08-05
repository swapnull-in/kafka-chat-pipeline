# Learn Kafka: Chat Platform Event Pipeline

> **New here? Don't read this page first.** Do the 45-minute guided path in
> **[START-HERE.md](START-HERE.md)** — 5 hands-on labs, no prior knowledge
> assumed. Come back here afterwards.

A real-world style event pipeline, the kind that powers chat platforms: when a
user sends a message, the chat API publishes a `message.sent` event to Kafka,
and independent downstream services react to it — without the API knowing they
exist.

```
                                    ┌──────────────────────────┐
                              ┌────▶│ notification-service     │  (own consumer group)
┌──────────┐    ┌──────────┐  │     │ "send push to recipient" │
│ chat API │───▶│  Kafka   │──┤     └──────────────────────────┘
│(producer)│    │ 3 parts  │  │     ┌──────────────────────────┐
└──────────┘    └──────────┘  └────▶│ analytics-service        │  (own consumer group)
                topic:              │ "count msgs per convo"   │
                chat.messages       └──────────────────────────┘
```

## Run it

```bash
docker compose up -d        # Kafka broker (KRaft) + web UI
npm install
npm run topics              # create chat.messages + chat.typing (3 partitions each)

# then in separate terminals:
npm run api                 # Stage 2: the real chat API on :3000
npm run consumer:notifications
npm run consumer:analytics

# send a message through the API:
curl -X POST localhost:3000/messages -H 'content-type: application/json' \
  -d '{"conversationId":"conv-1","senderId":"alice","recipientId":"bob","text":"hi"}'
```

`npm run producer` still works as a load generator — it fakes users chatting
every second so you have a steady stream to watch during experiments.

Open **http://localhost:8081** (Kafka UI) to watch topics, partitions,
messages, and consumer-group lag live. This is the best way to build intuition.

## Experiments that teach the core concepts

1. **Partitioning & ordering** — watch the producer logs: every message for
   `conv-1` goes to the same partition (we key by `conversationId`). Change the
   key to `messageId` and watch conversations scatter across partitions —
   you've just traded per-conversation ordering for even load.

2. **Consumer group scaling** — run `npm run consumer:notifications` in **two
   terminals**. Kafka rebalances: each instance owns some of the 3 partitions.
   Start a third; each owns one. Start a fourth — it sits idle (partitions cap
   your parallelism). Kill one — its partitions fail over to the survivors.

3. **Fan-out** — notifications and analytics use *different* group IDs, so both
   receive every event. Stop analytics for a minute while the producer runs,
   then restart it — it catches up from its committed offset. Check the "lag"
   column in Kafka UI while it's stopped.

4. **Replay** — analytics subscribes with `fromBeginning: true`. Delete its
   consumer group in Kafka UI and restart it: it rebuilds all its stats from
   history. Queues can't do this; a log can.

5. **At-least-once delivery** — throw an error in `eachMessage` for a specific
   sender, watch kafkajs retry the same offset. This is why real consumers need
   idempotent handling (Stage 4).

## Roadmap: growing this into the real thing

Full study material for every stage lives in [`kafka/`](kafka/README.md) —
concepts, build milestones, break-it experiments, and self-check questions.

- **[Stage 1 — foundations](kafka/stage-1-foundations.md)** (this repo):
  producer, two consumer services, partitions, groups.
- **[Stage 2 — real producer](kafka/stage-2-real-producer.md)** (built —
  `src/api.ts`): a Fastify `POST /messages` API publishing to Kafka; the event
  envelope, acks, the idempotent producer, per-topic retention (`chat.typing`,
  60s), graceful shutdown.
- **[Stage 3 — real sinks](kafka/stage-3-real-sinks.md)** (built —
  `src/consumers/analytics-service.ts`, `src/api-outbox.ts`,
  `src/outbox-relay.ts`): analytics into Postgres with manual offset commits,
  and the transactional outbox pattern. Labs 6–7 in
  [START-HERE.md](START-HERE.md) walk the failure modes.
- **[Stage 4 — failure handling](kafka/stage-4-failure-handling.md):** retries
  with backoff, a dead-letter topic, idempotent consumers, DLQ re-drive.
- **[Stage 5 — schemas](kafka/stage-5-schemas.md):** zod-validated, versioned
  events; evolving the contract without breaking running consumers.
- **[Stage 6 — production](kafka/stage-6-production.md):** 3-broker cluster,
  replication and acks=all, compacted topics, lag monitoring, capstone design.
