import { createKafka, TOPICS } from "../kafka.js";
import { createPool } from "../db.js";
import type { MessageSentEvent } from "../events.js";

// Stage 3: analytics now writes to Postgres instead of in-memory Maps —
// stats survive restarts, and the whole table can be rebuilt by replaying
// the topic (delete the consumer group in Kafka UI + TRUNCATE the table).
//
// The big idea here is WHEN we tell Kafka "done" (commit the offset):
//
//   process THEN commit  → crash in between = message processed again
//                          = AT-LEAST-ONCE (duplicates possible, no loss) ✅
//   commit THEN process  → crash in between = message skipped
//                          = AT-MOST-ONCE (loss possible) ❌
//
// We take manual control (autoCommit: false) to make that boundary explicit.

async function main() {
  const pool = createPool();
  await pool.query(`
    CREATE TABLE IF NOT EXISTS conversation_stats (
      conversation_id text PRIMARY KEY,
      message_count   int NOT NULL DEFAULT 0,
      last_message_at timestamptz
    )
  `);

  const consumer = createKafka("analytics-service").consumer({
    groupId: "analytics-service",
  });

  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.chatMessages, fromBeginning: true });
  console.log("Analytics service listening (writing to Postgres)...\n");

  await consumer.run({
    autoCommit: false, // WE decide when the offset moves
    eachMessage: async ({ topic, partition, message }) => {
      const event: MessageSentEvent = JSON.parse(message.value!.toString());
      const { conversationId } = event.payload;

      // 1) PROCESS: upsert the stat row
      const res = await pool.query(
        `INSERT INTO conversation_stats (conversation_id, message_count, last_message_at)
         VALUES ($1, 1, $2)
         ON CONFLICT (conversation_id)
         DO UPDATE SET message_count   = conversation_stats.message_count + 1,
                       last_message_at = $2
         RETURNING message_count`,
        [conversationId, event.occurredAt]
      );
      console.log(
        `[p${partition} o${message.offset}] ${conversationId} -> ${res.rows[0].message_count} messages`
      );

      // Break-it experiment: CRASH_ON=conv-3 npm run consumer:analytics
      // crashes AFTER the DB write but BEFORE the commit below. On restart,
      // Kafka redelivers the same message and the count increments TWICE —
      // that's at-least-once delivery, seen with your own eyes (Stage 4
      // fixes it with idempotent consumers).
      if (process.env.CRASH_ON === conversationId) {
        console.log(`CRASH_ON=${conversationId}: exiting before offset commit!`);
        process.exit(1);
      }

      // 2) COMMIT: offset + 1 = "the NEXT offset I want to read", not the
      // one I just processed. Committing every message like this is slow but
      // explicit; production batches commits (or uses eachBatch).
      await consumer.commitOffsets([
        { topic, partition, offset: (Number(message.offset) + 1).toString() },
      ]);
    },
  });

  const shutdown = async () => {
    await consumer.disconnect();
    await pool.end();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
