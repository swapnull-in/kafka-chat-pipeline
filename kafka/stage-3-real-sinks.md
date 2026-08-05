# Stage 3 — Real sinks (Kafka → Postgres)

**Goal:** make consumers write somewhere durable, and meet the two problems
every real pipeline has: *when to commit offsets* and *the dual-write problem*.

## Concepts

### The offset-commit timing problem

A consumer does two things per message: **process** (write to DB) and
**commit** (tell Kafka "done up to here"). The order decides your semantics:

- Process **then** commit → crash in between = message processed again →
  **at-least-once** (duplicates possible, no loss). ✅ what you want
- Commit **then** process → crash in between = message skipped →
  **at-most-once** (loss possible). ❌ almost never what you want

kafkajs auto-commit is roughly "process then commit" (commits after
`eachMessage` resolves, on an interval), which is fine to start. For DB sinks
you'll take manual control to make the boundary explicit.

### The dual-write problem (the most important idea in this stage)

Your **API** (stage 2) now wants to both save the message in its own DB *and*
publish to Kafka. Two systems, no shared transaction:

- DB commit succeeds, Kafka publish fails → downstream services never hear about the message.
- Kafka publish succeeds, DB commit fails → notification sent for a message that "doesn't exist".

Neither ordering is safe. The fix is the **transactional outbox pattern**:
write the event into an `outbox` table *in the same DB transaction* as the
message row; a separate relay process reads the outbox and publishes to Kafka,
marking rows as sent. The DB transaction is the single source of truth;
delivery to Kafka becomes at-least-once (consumers dedupe — Stage 4).
This pattern appears in virtually every serious event-driven system.

### eachMessage vs eachBatch

`eachMessage` = simple, one at a time. `eachBatch` hands you all fetched
messages for a partition at once — needed when per-message DB round-trips are
too slow (batch INSERT, then commit offsets once). Rule: start with
`eachMessage`, move to `eachBatch` when lag tells you to.

### Consumers own their read models

Analytics keeps counts in Postgres; a search indexer keeps an index. Each
consumer builds **its own** representation of the same stream, and any of them
can be rebuilt by replay. This is the practical core of "event sourcing"
without the ceremony.

## Build

Milestone 1 — add Postgres to docker-compose (use port 5433; 5432 is taken on
your machine):
```yaml
  postgres:
    image: postgres:17
    ports: ["5433:5432"]
    environment: { POSTGRES_PASSWORD: kafka, POSTGRES_DB: chat }
```

Milestone 2 — persistent analytics: `npm i pg`. Rewrite analytics-service to
upsert per-conversation counts:
```sql
CREATE TABLE conversation_stats (
  conversation_id text PRIMARY KEY,
  message_count   int NOT NULL DEFAULT 0,
  last_message_at timestamptz
);
```
```ts
// in eachMessage: process THEN let the offset commit happen
await pool.query(
  `INSERT INTO conversation_stats VALUES ($1, 1, $2)
   ON CONFLICT (conversation_id)
   DO UPDATE SET message_count = conversation_stats.message_count + 1,
                 last_message_at = $2`,
  [event.payload.conversationId, event.occurredAt]
);
```
Delete the consumer group in Kafka UI, restart → the table rebuilds from
history. (Truncate it first, or you'll double-count — why? That question *is*
the point of this stage.)

Milestone 3 — manual commits: switch the consumer to
`autoCommit: false` and call `consumer.commitOffsets(...)` after the DB write
(commit `offset + 1` — the next offset to read, a classic off-by-one).

Milestone 4 — outbox: give the API from Stage 2 its own `messages` table plus
an `outbox` table, insert both in one transaction, and write
`src/outbox-relay.ts` that polls unsent rows, publishes, marks sent. Kill the
relay, POST messages, restart it — nothing is lost.

## Break it

1. **Duplicate on redelivery:** in the analytics consumer, `process.exit(1)`
   right after the DB write for a specific conversation, before offsets
   commit. Restart. The count increments twice for one message. You've now
   *seen* at-least-once. (Stage 4 fixes it.)
2. **Dual-write, no outbox:** in the API, publish to Kafka then throw before
   the DB insert. Observe: notification fires for a message that was never
   saved. Then route through the outbox and try to reproduce — you can't.

## Self-check

1. Explain at-least-once vs at-most-once purely in terms of the order of two operations.
2. Why can't a DB transaction and a Kafka publish be made atomic directly?
3. In the outbox pattern, which failure turns into duplicates, and who cleans them up?
4. Why commit `offset + 1`?
5. Your analytics table shows a count higher than Kafka UI's message count for the topic. List the plausible causes.

## Reading

- *Definitive Guide* ch. 6 ("Reliable Data Delivery")
- microservices.io: "Pattern: Transactional outbox"
- Debezium blog: "Reliable Microservices Data Exchange With the Outbox Pattern" (the production-grade version, via CDC)
