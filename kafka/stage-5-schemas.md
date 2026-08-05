# Stage 5 — Schemas & evolution

**Goal:** producers and consumers deploy independently, at different times,
owned by different teams. Learn to change an event's shape without breaking
anyone — the discipline that separates "used Kafka" from "ran Kafka in prod".

## Concepts

### The topic is an API

Once another service consumes your topic, its message format is a **public
contract** — exactly like a REST API, except worse in one way: old messages
don't go away. A consumer replaying history will meet *every version you ever
published*. Schema discipline is therefore not optional hygiene; it's the
price of replay.

### Compatibility directions

- **Backward compatible:** new *consumer* reads old messages. Needed for replay/catch-up.
- **Forward compatible:** old *consumer* reads new messages. Needed because producers usually deploy first.

The rules that keep you both (worth memorizing):

1. **Adding an optional field** — safe. Old consumers ignore it; new consumers default it for old messages.
2. **Removing a field** — breaks consumers that read it. Deprecate first, verify nobody reads it, then remove.
3. **Renaming or changing a field's type/meaning** — never. A rename is rule 1 (add new) followed much later by rule 2 (remove old).
4. **Consumers must be tolerant readers:** ignore unknown fields, never crash on their presence.

### Runtime validation with zod

TypeScript types vanish at runtime; the network gives you `unknown`. Validate
at the boundary — **on consume** (protects you from bad producers *and* from
old history) and optionally on produce (fail fast in dev):

```ts
import { z } from "zod";

export const MessageSentV1 = z.object({
  eventId: z.string(),
  type: z.literal("message.sent"),
  version: z.literal(1),
  occurredAt: z.string(),
  payload: z.object({
    messageId: z.string(),
    conversationId: z.string(),
    senderId: z.string(),
    recipientId: z.string(),
    text: z.string(),
  }),
});
```

A message that fails validation is a **poison message** — your Stage 4 DLQ
already knows what to do with it. Notice how the stages compose.

### Schema Registry (know of it, don't build it yet)

At company scale, contracts move out of a shared TS file into a **Schema
Registry**: producers register Avro/Protobuf/JSON-Schema versions, the
registry *rejects incompatible changes at publish time*, and messages carry a
schema ID instead of self-describing JSON (smaller, faster). Confluent Schema
Registry is the standard; Redpanda has a compatible one. The concepts you're
practicing here transfer 1:1.

## Build

Milestone 1 — `npm i zod`; create `src/schemas.ts` with envelope + v1 schemas
for `message.sent` and `typing.started`. Validate in every consumer
(`safeParse`; failures → DLQ with the zod error in a header). Validate in the
producer too.

Milestone 2 — evolve: v2 of `message.sent` adds optional
`payload.attachments?: {url, mimeType}[]`. Producer starts emitting
`version: 2`. Consumers accept `z.union([V1, V2])` and treat missing
attachments as `[]`. Confirm replay from offset 0 still works (old + new mixed).

Milestone 3 — a consumer-driven contract test in plain vitest: fixture files
with sample v1 and v2 messages; a test asserting every consumer's parser
accepts all fixtures. This test failing in CI = you were about to break prod.

## Break it

1. **Producer deploys first:** emit v2 while a consumer still requires
   `z.literal(1)` strictly. Watch every new message land in the DLQ — a
   self-inflicted total outage, from an *additive* change, because the
   consumer wasn't tolerant. Fix with the union; re-drive the DLQ.
2. **The rename trap:** rename `text` → `body` in the producer only. Old
   consumers now read `undefined`. Notice zod catches it at the boundary
   (DLQ), whereas without validation it would have silently written garbage
   downstream — which failure would you rather debug?
3. **Replay across versions:** delete the analytics group, replay from 0 with
   mixed v1/v2 history, verify stats are correct.

## Self-check

1. Why do backward *and* forward compatibility both matter? Tie each to a concrete deploy/replay scenario.
2. What makes a rename fundamentally unsafe, and what's the safe migration sequence?
3. Where should validation live and why there rather than trusting the producer?
4. What does a Schema Registry give you that a shared zod file doesn't?
5. Your consumer meets a `version: 3` it doesn't know. What should it do — crash, skip, or DLQ? Defend your answer.

## Reading

- Confluent: "Schema Evolution and Compatibility" docs
- Kleppmann, *DDIA* ch. 4 ("Encoding and Evolution") — the definitive treatment
- Martin Fowler: "Tolerant Reader"
