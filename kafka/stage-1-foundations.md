# Stage 1 — Foundations

**Goal:** internalize the five words that everything else builds on: topic,
partition, offset, key, consumer group. The code for this stage is already
written (`src/`).

## Concepts

### Kafka is a log, not a queue

A queue deletes a message once someone consumes it. Kafka **appends every
event to a log and keeps it** (default: 7 days, configurable to forever).
Consumers don't remove anything — each consumer just remembers *how far it has
read* (its offset). Consequences, all of which you'll see first-hand:

- Many independent services can read the same stream without interfering.
- A service that was down catches up from where it left off.
- A brand-new service can replay history from the beginning.

### Topic → partitions → offsets

A **topic** (`chat.messages`) is a named stream. It's split into
**partitions** (we use 3) — independent append-only files. Every message in a
partition gets a sequential **offset** (0, 1, 2, ...). Two facts to burn in:

1. **Ordering is guaranteed only within one partition.** There is no "global
   order" across a topic — and real systems don't need one; they need
   per-entity order (per conversation, per user).
2. **Partition count = maximum consumer parallelism** for one group. 3
   partitions means at most 3 consumers doing work in a group.

### Keys route messages

When a message has a **key**, Kafka hashes it to pick the partition:
`hash(key) % numPartitions`. Same key → same partition → strict order for that
key. We key by `conversationId`, so every conversation is internally ordered.
No key → round-robin spread.

### Consumer groups

A **group** (identified by `groupId`) is one logical service scaled to N
processes. Kafka divides the partitions among the group's members
(**rebalancing** whenever a member joins or dies). Two rules:

- Same groupId → members **share** the work (each partition goes to exactly one member).
- Different groupId → each group gets the **full** stream independently.

That's why `notification-service` and `analytics-service` both see every
message, while two copies of `notification-service` split them.

### Committed offsets and lag

Each group periodically **commits** "I've processed up to offset X in
partition P" back to Kafka. After a crash/restart, the group resumes from the
committed offset. **Lag** = newest offset − committed offset = how far behind
a service is. Lag is *the* health metric for consumers.

### Delivery semantics (preview)

Commits happen *after* processing, so a crash between processing and
committing means the message is processed **again** after restart. This is
**at-least-once** delivery — Kafka's practical default. Duplicates are
possible; loss is not. Stage 4 deals with this properly.

## Build

Already built. Read every file in `src/` top to bottom — they're short and the
comments carry the concepts. Make sure you can trace one event from
`producer.ts` through the broker to both consumers.

## Break it / experiments

Work through all eight experiments in the "What all I need to test" checklist
(also summarized in the repo README):

1. Basic flow, watching messages in Kafka UI
2. Partition/key routing (then change the key to `messageId` and back)
3. Two, three, four members in one group; kill one and watch failover
4. Fan-out to a second group
5. Stop analytics, watch lag grow in Kafka UI, restart, watch catch-up
6. Delete the analytics group in Kafka UI → full replay from offset 0
7. Throw inside `eachMessage` → one partition jams, others continue
8. `docker compose restart kafka` → data survives

## Self-check (answer without looking)

1. Why does a 4th consumer in a 3-partition group sit idle?
2. A message is "in Kafka" — where exactly does it live, and when is it deleted?
3. Who stores the consumer's position — the consumer or the broker? What is it called?
4. Why did experiment 7 block only one partition?
5. You need messages for a given user processed in order. What do you do, and what are you giving up?
6. What's the difference between restarting a consumer (exp. 5) and deleting its group (exp. 6)?

## Reading

- *Definitive Guide* ch. 1 ("Meet Kafka") and ch. 4 ("Consumers") — the two most valuable chapters in the book
- Kreps, "The Log" essay — read it now, reread it after Stage 6
