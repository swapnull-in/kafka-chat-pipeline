# Kafka Mastery Curriculum

Six stages, each building on the last, using this repo as the vehicle. Every
stage has the same shape:

- **Concepts** — the theory you need, explained (read first)
- **Build** — milestones to implement (do second)
- **Break it** — deliberately trigger the failure the stage protects against
- **Self-check** — questions you must be able to answer *without looking*
- **Reading** — the best external material for depth

## Rules for getting the most out of this

1. **Don't skip Break-it.** Feeling the failure before fixing it is the whole
   method. Anyone can copy a dead-letter-queue tutorial; mastery is knowing
   exactly what goes wrong without one.
2. **Predict before you run.** Keep Kafka UI (http://localhost:8081) open and
   say out loud what the partitions/offsets/lag will do before every
   experiment. Being wrong is the lesson.
3. **Self-check gates the next stage.** If you can't answer from memory, redo
   the experiments — don't move on.

## The stages

| Stage | Title | You'll be able to... |
|-------|-------|----------------------|
| [1](stage-1-foundations.md) | Foundations | explain topics, partitions, offsets, consumer groups from experience |
| [2](stage-2-real-producer.md) | A real producer | put an HTTP API in front of Kafka with correct ack/idempotency settings |
| [3](stage-3-real-sinks.md) | Real sinks | consume into Postgres correctly and explain the dual-write problem |
| [4](stage-4-failure-handling.md) | Failure handling | build retries, dead-letter topics, and idempotent consumers |
| [5](stage-5-schemas.md) | Schemas & evolution | evolve events without breaking running consumers |
| [6](stage-6-production.md) | Production | run a replicated cluster, size topics, monitor lag, use compaction |

## The final mastery test

Design, on paper, the event architecture for a full chat platform: topics,
partition keys, consumer groups, retention per topic, what happens when the
notification service is down for an hour, how to add read-receipts without
touching existing services, and how to replay a day of history into a brand-new
analytics system. Defend every choice — including what you deliberately kept
OFF Kafka. When you can do that, you're done here.

## Canonical reading (spread across stages)

- *Kafka: The Definitive Guide*, 2nd ed. (Shapira, Palino, Sivaram, Petty) — free PDF from Confluent
- Jay Kreps, ["The Log: What every software engineer should know about real-time data"](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) — the single best "why Kafka exists" text
- [kafka.apache.org/documentation](https://kafka.apache.org/documentation/) — the Design section is genuinely readable
