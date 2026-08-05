# Start here — learn Kafka in 5 small labs (~45 min)

No Kafka knowledge assumed. Each lab is a few minutes: run a command, watch
what happens, understand one idea. Don't read ahead — do the labs in order.

## The one mental model you need

Kafka is a **shared notebook that only allows appending**.

- **Producers** write facts at the end: *"alice sent 'hi' to bob in conv-1"*.
- **Consumers** read the notebook at their own pace, each keeping a private
  bookmark (called an **offset**). Reading never removes anything.
- Because nothing is removed, five different services can read the same
  notebook independently — and a new service can start from page 1.

That's it. Partitions, groups, replay — everything below is a consequence of
"it's an append-only notebook with bookmarks".

---

## Lab 0 — Turn it on (5 min)

```bash
cd ~/Downloads/kafka
docker compose up -d     # the Kafka broker + a web UI
npm install
npm run topics           # creates our two "notebooks" (topics)
```

Now open **http://localhost:8081** in your browser and keep it open — it's
your window into Kafka for every lab. Click **Topics** → you should see
`chat.messages` with **3 partitions**.

> A **topic** is one notebook (one stream of related facts). A **partition**
> is the notebook split into 3 volumes so multiple readers can work in
> parallel. Ignore partitions until Lab 2.

## Lab 1 — One message, end to end (5 min)

Open two terminals in `~/Downloads/kafka`:

```bash
# terminal 1 — the chat API (a producer)
npm run api

# terminal 2 — the notification service (a consumer)
npm run consumer:notifications
```

Now send a chat message through the API (terminal 3):

```bash
curl -X POST localhost:3000/messages -H 'content-type: application/json' \
  -d '{"conversationId":"conv-1","senderId":"alice","recipientId":"bob","text":"hi"}'
```

**You'll see:** the API returns `202` with an `eventId`, and terminal 2
prints `push -> bob: "New message from alice"`.

**What just happened:** the API didn't call the notification service. It only
wrote a fact to the notebook. The notification service happened to be reading
the notebook and reacted. Neither knows the other exists — that decoupling is
the entire reason Kafka is popular. In Kafka UI → Topics → `chat.messages` →
**Messages** tab, find your event and look at its JSON.

## Lab 2 — Partitions: same conversation, same volume (10 min)

Stop the API (Ctrl+C in terminal 1) and start the fake-traffic generator
instead:

```bash
npm run producer
```

**Watch the log:** every line shows `[partition N]`. Notice `conv-1` is
*always* the same partition number, `conv-2` always the same, etc.

**What just happened:** we tag (key) each message with its `conversationId`,
and Kafka routes every message with the same key to the same partition. Since
a partition preserves order, each conversation stays in order — while
different conversations spread across partitions for parallelism.

**Check yourself:** if we keyed by `eventId` (unique every time) instead,
what would you gain and what would you lose? *(Answer at the bottom.)*

## Lab 3 — Consumer groups: teamwork and failover (10 min)

Keep `npm run producer` running. Now start the notification service in
**two** terminals at once:

```bash
npm run consumer:notifications   # terminal A
npm run consumer:notifications   # terminal B
```

**You'll see:** after a few seconds of rebalancing, A and B each handle
*different* partitions — the work is split, no message is handled twice.

Now **Ctrl+C terminal B** and keep watching A.

**You'll see:** A takes over B's partitions within seconds. No messages lost.

**What just happened:** both ran with the same **group id**
(`notification-service`), which tells Kafka "we're one team — divide the
3 partitions between us". That's horizontal scaling *and* failover for free.
In Kafka UI → **Consumers**, click the group to see who owns which partition.

**Check yourself:** you start a 4th consumer in this group. What does it do?

## Lab 4 — Fan-out and replay: the superpower (10 min)

With the producer still running, start the *other* service:

```bash
npm run consumer:analytics
```

**You'll see:** it counts messages per conversation — including all the OLD
messages from previous labs, replayed from the beginning of the notebook.
Meanwhile the notification service still gets every new message too.

**What just happened — two ideas:**
1. **Fan-out:** analytics uses a *different* group id, so it gets its own full
   copy of the stream. Same notebook, independent bookmark.
2. **Replay:** it subscribes `fromBeginning: true`, so on first run its
   bookmark starts at page 1 and it rebuilds stats from history. A classic
   message queue deletes messages once read — it can never do this.

Try this: stop analytics for a minute (producer keeps running), watch its
**lag** climb in Kafka UI → Consumers, then restart it and watch it catch up
from its bookmark.

## Lab 5 — Breaking it: what happens when Kafka is down? (5 min)

Start the API again (`npm run api`), then kill Kafka out from under it:

```bash
docker compose stop kafka
curl -X POST localhost:3000/messages -H 'content-type: application/json' \
  -d '{"conversationId":"conv-1","senderId":"alice","recipientId":"bob","text":"is anyone there?"}'
```

**You'll see:** the curl hangs — the producer is retrying behind the scenes.
Now, before it gives up:

```bash
docker compose start kafka
```

**You'll see:** the send succeeds once the broker is back.

**What just happened:** producers don't fail instantly; they retry with
backoff. And because our producer sets `idempotent: true`, a retry can never
create a *duplicate* message. When you're done: `docker compose up -d` to
make sure everything is running again.

---

## Where to go next

You now have the core 80%: topics, partitions, keys, offsets, consumer
groups, fan-out, replay, retries. Ready for more? Level 2 below shows the two
problems every REAL pipeline hits. For full depth, the stage guides in
[`kafka/`](kafka/README.md) pick up exactly here.

Best way to use Claude: run a lab, then explain what you saw in your own
words and ask "is my understanding right?" — or ask to be quizzed on a stage.

---

# Level 2 — the two problems every real pipeline has (~30 min)

Stage 3 is built: analytics now writes to **Postgres** (see it survive
restarts), and there's a second API that uses the **outbox pattern**. These
two labs let you *watch* the failure modes the pattern exists to prevent.

## Lab 6 — Create a duplicate with your own hands (15 min)

The analytics consumer does two things per message: **write to the DB**, then
**commit the offset** (move its bookmark). What if it crashes in between?
Let's find out. With the producer running (`npm run producer`):

```bash
CRASH_ON=conv-3 npm run consumer:analytics
```

**You'll see:** it processes messages until one arrives for `conv-3`, writes
it to Postgres, then deliberately crashes *before* committing the offset.

Now restart it normally:

```bash
npm run consumer:analytics
```

**Be patient — it takes ~30 seconds to start consuming.** The crashed
consumer died without saying goodbye, so Kafka waits for its session to time
out before giving its partitions to the new instance. (That pause IS the
failover you saw in Lab 3.) Then:

**You'll see:** the first thing it processes is that same `conv-3` message
again — and the count in Postgres goes up by 2 for 1 real message. Verify:

```bash
docker exec chat-postgres psql -U postgres -d chat \
  -c 'SELECT * FROM conversation_stats ORDER BY message_count DESC;'
```

**What just happened:** this is **at-least-once delivery** — Kafka's default
contract. "Process, then commit" means a crash in between causes a redo,
never a skip. The reverse order ("commit, then process") would *skip* the
message instead — silent data loss. Duplicates you can fix (Stage 4:
consumers dedupe on `eventId`); loss you usually can't.

**Check yourself:** why does the consumer commit `offset + 1` and not
`offset`? *(Answer at the bottom.)*

## Lab 7 — The dual-write problem and the outbox (15 min)

The Stage 2 API has a hidden bug waiting to happen: a real chat API must
*both* save the message in its own database *and* publish to Kafka. Two
systems, no shared transaction — if it crashes between the two, they
disagree forever. This is the **dual-write problem**.

The fix is in `src/api-outbox.ts` — open it next to `src/api.ts` and spot
the difference: **the outbox API never talks to Kafka**. It writes the
message row AND the event into an `outbox` table in ONE database
transaction. A separate relay process does the publishing:

```bash
# terminal 1                    # terminal 2                # terminal 3
npm run api:outbox              npm run relay               npm run consumer:notifications
```

Send a message (terminal 4) and watch it flow API → Postgres → relay →
Kafka → notification:

```bash
curl -X POST localhost:3000/messages -H 'content-type: application/json' \
  -d '{"conversationId":"conv-9","senderId":"alice","recipientId":"bob","text":"via outbox!"}'
```

Now the fun part — **kill the relay** (Ctrl+C in terminal 2) and send 3 more
messages. The API still answers 202 instantly. Where did they go?

```bash
docker exec chat-postgres psql -U postgres -d chat \
  -c 'SELECT id, key, sent_at FROM outbox ORDER BY id;'
```

**You'll see:** 3 rows with `sent_at` empty — safely parked. Restart
`npm run relay` and watch all 3 notifications fire. Nothing lost.

**What just happened:** the database transaction is the single source of
truth. Kafka being down, the relay being dead — none of it can lose a
message anymore; delivery just becomes *eventual*. This is the
**transactional outbox pattern**, and it appears in virtually every serious
event-driven system.

**Check yourself:** the relay crashes *after* publishing a row to Kafka but
*before* marking it `sent_at`. What happens on restart, and why is that OK?

### Lab answers

- **Lab 2:** keying by `eventId` spreads load perfectly evenly, but messages
  of one conversation scatter across partitions, so a consumer may see "reply"
  before "question". You trade per-conversation ordering for even load.
- **Lab 3:** the 4th consumer sits idle — 3 partitions means at most 3 active
  consumers per group. Partition count caps your parallelism; that's why you
  pick it with headroom.
- **Lab 6:** the committed offset means "the next one I want to read", not
  "the last one I finished". Commit `offset` instead of `offset + 1` and every
  restart re-processes the last message — a classic off-by-one.
- **Lab 7:** on restart the relay sees the row still unsent and publishes it
  AGAIN — a duplicate in Kafka. That's fine because the outbox promises
  at-least-once, and consumers dedupe on the envelope's `eventId` (Stage 4
  builds exactly that).
