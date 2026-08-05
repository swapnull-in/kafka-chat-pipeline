# Stage 6 — Production concerns

**Goal:** understand where durability actually comes from (replication), make
the sizing decisions (partitions, retention), meet log compaction, and watch
lag like an operator. Ends with the capstone design exercise.

## Concepts

### Replication — where durability really lives

Everything so far ran on one broker; if its disk died, every stage's
guarantees would be worthless. In production each partition has N **replicas**
(N=3 standard): one **leader** (all reads/writes) and followers that copy it.
Followers that are caught up form the **ISR** (in-sync replicas). Leader dies
→ a follower from the ISR is elected leader. The knobs, and how they interlock:

- `replication.factor: 3` — copies per partition.
- `acks: -1` — producer waits for all ISR members.
- `min.insync.replicas: 2` — if the ISR shrinks below 2, the broker *rejects
  writes* rather than accept under-replicated data. RF3 + acks=all + minISR 2
  is **the** production formula: survives one broker loss with zero data loss,
  stays writable.
- `unclean.leader.election.enable: false` — never elect an out-of-sync
  follower. `true` trades data loss for availability; know it exists, leave it off.

### Choosing partition counts

Partitions are your parallelism ceiling and are painful to change: you can add
(but `hash(key) % N` changes — same key maps to a *new* partition, breaking
key-ordering across the boundary) and never remove. Heuristic: estimate peak
throughput ÷ single-consumer throughput, add generous headroom (2-3×), round
up. Dozens is normal; don't create thousands "to be safe" — each partition
costs broker memory, file handles, and failover time.

### Retention vs compaction — two kinds of topic

- **Retention** (`retention.ms/bytes`): keep messages for a window, delete by
  age/size. For **event streams** — `chat.messages`, 7-30 days typically.
- **Compaction** (`cleanup.policy: compact`): keep the **latest message per
  key** forever; older values for the same key are eventually purged. For
  **state**, not events — "current presence per user", "current config per
  tenant". A compacted topic is a durable, replayable key-value snapshot: a new
  consumer reads it end-to-end and holds the full current state. (A key is
  deleted by publishing value `null` — a **tombstone**.)

### Operating consumers: lag is the alarm

Lag rising and recovering = traffic spike, fine. Lag rising monotonically =
consumer too slow / stuck / crashed — page someone. Real deployments export
lag to Prometheus/Grafana (Burrow, kafka-exporter); you'll build the toy
version with the admin API. Alert on *trend*, not absolute numbers.

### What you're not building (but must know exists)

- **Security:** production clusters run TLS + SASL auth + per-topic ACLs. Your `PLAINTEXT` listeners are dev-only.
- **Managed Kafka:** AWS MSK, Confluent Cloud, Redpanda Cloud. Most teams should buy, not run brokers — your skills transfer entirely; the client API is identical.
- **Stream processing:** Kafka Streams / Flink / ksqlDB for stateful transforms (windowed joins, aggregations) *between* topics. A separate learning track once this repo's track is done.

## Build

Milestone 1 — 3-broker cluster: `docker-compose.cluster.yml` with kafka-1/2/3
(ports 9092/9094/9095 — 9093 is each node's controller port), all three in
`KAFKA_CONTROLLER_QUORUM_VOTERS`, and the client connecting to all three
bootstrap brokers. Recreate `chat.messages` with `replicationFactor: 3` and
`min.insync.replicas: 2`. In Kafka UI, find each partition's leader and ISR.

Milestone 2 — presence on a compacted topic: create `user.presence`
(`cleanup.policy: compact`), publish `{status}` keyed by userId on every
simulated message ("online") plus periodic "offline"s. Write
`src/presence-reader.ts` that reads it from the beginning into a `Map` — the
map always converges to current state per user, no matter how long the
system's been running.

Milestone 3 — lag monitor: `src/lag-monitor.ts` using
`admin.fetchTopicOffsets()` vs `admin.fetchOffsets({groupId})`, print per
group every 5s, warn on 3 consecutive rises. This is Burrow in 40 lines.

## Break it (the payoff experiments)

1. **Kill the leader:** producer running against the cluster,
   `docker stop kafka-2` (whichever leads a partition). Watch the ISR shrink,
   a follower take over, producers/consumers ride through with zero manual
   action. Bring it back; watch it rejoin the ISR. This is the experiment the
   whole stage exists for.
2. **Break the write quorum:** stop *two* brokers. With minISR 2, producers
   now get errors — Kafka chose consistency over availability. Restart one;
   writes resume.
3. **acks=1 data loss (the Stage 2 promise):** set `acks: 1`, produce a burst,
   immediately `docker kill` (not stop) the leader before followers copy.
   Compare produced count vs surviving count. Repeat with `acks: -1` — no gap.
4. **Repartitioning pain:** add partitions to a keyed topic and show the same
   `conversationId` landing in a different partition than its history —
   per-key ordering broken at the boundary. This is why you size generously up front.

## Self-check

1. Walk a write through RF3 + acks=all + minISR2, then walk the leader dying at each step. Where can data be lost? (Answer: nowhere — prove it.)
2. Why does minISR 2 sacrifice availability, and when is that the right trade?
3. Compacted vs retention topic — pick for: chat messages, user presence, feature flags, click analytics. Justify each.
4. Why is adding partitions to a keyed topic dangerous when it "just adds capacity"?
5. What lag pattern pages a human, and what pattern doesn't?

## Capstone — the mastery test

On paper, no code: design the event architecture for a full chat platform
(CometChat-scale). Must cover: every topic with partition count, key, and
retention/compaction choice; every consumer group; the outbox decision;
DLQ + re-drive strategy; schema evolution policy; what happens when the
notification service is down for an hour; adding read-receipts with zero
changes to existing services; replaying a day into a new analytics store; and
an explicit list of things you kept OFF Kafka (and why). Then defend it —
bring it to me and I'll play the skeptical staff engineer.

## Reading

- *Definitive Guide* ch. 7 ("Replication") and ch. 13 ("Monitoring")
- Kafka docs → Design → Replication (readable, canonical)
- Kreps, "The Log" — the promised reread; it lands differently now
