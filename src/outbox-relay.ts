import { createKafka } from "./kafka.js";
import { createPool } from "./db.js";

// The other half of the outbox pattern: a tiny process that moves events
// from the `outbox` table into Kafka. Poll unsent rows → publish → mark sent.
//
// Kill this process and POST messages: nothing is lost, rows just pile up in
// the outbox. Restart it and they all flow through. Delivery becomes
// AT-LEAST-ONCE: if we crash after publishing but before marking sent, the
// row is published again on restart — consumers dedupe on eventId (Stage 4).
//
// (Production systems often replace the polling with CDC — e.g. Debezium
// tails Postgres's write-ahead log — but the pattern is identical.)

const POLL_MS = 500;
const BATCH = 100;

async function main() {
  const pool = createPool();
  const producer = createKafka("outbox-relay").producer({ idempotent: true });
  await producer.connect();
  console.log("Outbox relay running (polls every 500ms, Ctrl+C to stop)...\n");

  let running = true;

  while (running) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      // FOR UPDATE SKIP LOCKED: if you ever run two relays, they'll grab
      // disjoint rows instead of double-publishing or blocking each other.
      const { rows } = await client.query(
        `SELECT id, topic, key, value FROM outbox
         WHERE sent_at IS NULL
         ORDER BY id
         LIMIT $1
         FOR UPDATE SKIP LOCKED`,
        [BATCH]
      );

      if (rows.length > 0) {
        for (const row of rows) {
          await producer.send({
            topic: row.topic,
            messages: [{ key: row.key, value: JSON.stringify(row.value) }],
          });
        }
        await client.query(`UPDATE outbox SET sent_at = now() WHERE id = ANY($1)`, [
          rows.map((r) => r.id),
        ]);
        console.log(`relayed ${rows.length} event(s) to Kafka`);
      }
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      console.error("relay error (will retry):", err);
    } finally {
      client.release();
    }

    await new Promise((r) => setTimeout(r, POLL_MS));
  }

  process.on("SIGINT", async () => {
    running = false;
    await producer.disconnect();
    await pool.end();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
