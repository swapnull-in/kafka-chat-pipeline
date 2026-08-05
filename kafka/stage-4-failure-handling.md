# Stage 4 — Failure handling (retries, DLQ, idempotency)

**Goal:** turn "it works" into "it survives": poison messages can't jam
partitions, transient failures retry themselves, and duplicates are harmless.

## Concepts

### The two kinds of failure

- **Transient** — DB briefly down, network blip, rate limit. Correct response: retry with backoff; the message will eventually succeed.
- **Permanent (poison)** — malformed JSON, data that violates an invariant, a bug. Retrying forever jams the partition (you saw this in Stage 1, experiment 7): offset never advances, everything behind it waits. Correct response: give up *on that message*, park it, move on.

You cannot tell them apart perfectly — so the policy is: retry N times with
backoff, then treat as poison.

### Dead-letter topic (DLQ)

"Parking" a poison message = publishing it to `chat.messages.dlq` with
**headers** recording what happened (original topic/partition/offset, error
message, attempt count, timestamp), then committing the original offset so the
partition moves on. The DLQ is a normal topic: you can inspect it in Kafka UI,
alert on its size, and **re-drive** messages back after fixing the bug.

### Idempotent consumers — the answer to at-least-once

You proved in Stage 3 that duplicates happen. Two ways to make them harmless:

1. **Natural idempotency** — design writes so applying them twice is a no-op
   (e.g. `INSERT ... ON CONFLICT DO NOTHING` keyed by `eventId`; or "set
   status = delivered" rather than "increment counter").
2. **Dedupe table** — record every processed `eventId`; skip if seen. Make the
   dedupe insert and the business write **one DB transaction** — that's what
   makes it airtight.

```sql
CREATE TABLE processed_events (
  event_id   text PRIMARY KEY,
  processed_at timestamptz NOT NULL DEFAULT now()
);
```

At-least-once delivery + idempotent consumer = **effectively-exactly-once**.
This combination is what ~95% of production systems actually run. (Kafka's
built-in "exactly-once" transactions mainly cover Kafka→Kafka stream
processing, not Kafka→your-database.)

### Graceful shutdown

SIGTERM arrives (deploy, k8s reschedule) mid-batch. Correct order: stop
fetching → finish in-flight messages → commit offsets → disconnect (triggers
rebalance so survivors take over). `consumer.disconnect()` does most of this —
the discipline is *waiting* for it before `process.exit`.

## Build

Milestone 1 — retry wrapper in the notification consumer:
```ts
async function withRetry(fn: () => Promise<void>, attempts = 3) {
  for (let i = 1; ; i++) {
    try { return await fn(); }
    catch (err) {
      if (i >= attempts) throw err;
      await sleep(200 * 2 ** i); // 400ms, 800ms — keep it short; you're blocking the partition
    }
  }
}
```

Milestone 2 — DLQ: catch the final failure, publish original value + error
headers to `chat.messages.dlq` (create it: 1 partition is fine), log loudly,
continue. The consumer needs its own producer for this.

Milestone 3 — dedupe: add `processed_events`; in the analytics consumer wrap
`INSERT INTO processed_events` + the stats upsert in one transaction; on
conflict, skip. Re-run Stage 3's break-it #1 — the double-count is gone.

Milestone 4 — re-drive script: `src/redrive.ts` consumes `chat.messages.dlq`
from beginning and republishes each message to its original topic (key
preserved!), then exits. Idempotency (milestone 3) is what makes re-driving safe.

## Break it

1. **Poison:** publish garbage (`echo 'not json' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic chat.messages`). Watch it retry 3×, land in the DLQ with headers intact, and — the key observation — the partition keeps flowing.
2. **Transient:** `docker compose stop postgres` while the producer runs, wait for retries/DLQ traffic, restart Postgres, then re-drive the DLQ. Final state in the stats table should be exactly as if nothing happened.
3. **kill -9** the analytics consumer mid-stream (find the pid; no graceful shutdown). Restart. Verify the dedupe table absorbed the redelivered messages.

## Self-check

1. Why must the DLQ publish happen *before* committing the original offset — what breaks with the other order?
2. Why keep in-process retry backoffs short? What is being held up while you sleep?
3. Why must the dedupe insert share a transaction with the business write? Walk the crash that breaks the two-transaction version.
4. Re-driving the DLQ replays messages out of their original order. When is that a problem, and does keying limit the damage?
5. "We need exactly-once delivery" — what do you actually build?

## Reading

- Confluent blog: "Error Handling Patterns for Apache Kafka Applications"
- *Designing Data-Intensive Applications* (Kleppmann) ch. 11 — the semantics discussion is the best in print
