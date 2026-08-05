import Fastify from "fastify";
import { createPool } from "./db.js";
import { toEnvelope, type MessageSentPayload } from "./events.js";
import { TOPICS } from "./kafka.js";

// Stage 3, the transactional outbox. Compare with api.ts (Stage 2):
//
// api.ts has a DUAL-WRITE problem the moment it also needs its own database:
// "save message to DB" and "publish to Kafka" are two systems with no shared
// transaction. Whichever you do first, a crash in between leaves them
// disagreeing (message saved but never announced, or announced but never
// saved).
//
// The fix: this API doesn't talk to Kafka AT ALL — notice there's no
// producer import. It writes the message row AND the event (into an `outbox`
// table) in ONE database transaction, which either fully commits or fully
// doesn't. A separate process (outbox-relay.ts) reads the outbox and does
// the actual publishing. The DB is the single source of truth.

const pool = createPool();

const app = Fastify({ logger: true });

interface SendMessageBody {
  conversationId: string;
  senderId: string;
  recipientId: string;
  text: string;
}

app.post<{ Body: SendMessageBody }>(
  "/messages",
  {
    schema: {
      body: {
        type: "object",
        required: ["conversationId", "senderId", "recipientId", "text"],
        properties: {
          conversationId: { type: "string", minLength: 1 },
          senderId: { type: "string", minLength: 1 },
          recipientId: { type: "string", minLength: 1 },
          text: { type: "string", minLength: 1 },
        },
      },
    },
  },
  async (req, reply) => {
    const event = toEnvelope<"message.sent", MessageSentPayload>("message.sent", req.body);
    const { conversationId, senderId, recipientId, text } = event.payload;

    // One transaction, two inserts: the domain row and the event to publish.
    // Crash anywhere → both roll back. Commit → both exist. No in-between.
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        `INSERT INTO messages (event_id, conversation_id, sender_id, recipient_id, text, sent_at)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [event.eventId, conversationId, senderId, recipientId, text, event.occurredAt]
      );
      await client.query(
        `INSERT INTO outbox (topic, key, value) VALUES ($1, $2, $3)`,
        [TOPICS.chatMessages, conversationId, JSON.stringify(event)]
      );
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }

    return reply.code(202).send({ eventId: event.eventId });
  }
);

async function main() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS messages (
      event_id        uuid PRIMARY KEY,
      conversation_id text NOT NULL,
      sender_id       text NOT NULL,
      recipient_id    text NOT NULL,
      text            text NOT NULL,
      sent_at         timestamptz NOT NULL
    );
    CREATE TABLE IF NOT EXISTS outbox (
      id         bigserial PRIMARY KEY,
      topic      text NOT NULL,
      key        text NOT NULL,
      value      jsonb NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now(),
      sent_at    timestamptz
    );
  `);

  await app.listen({ port: 3000 });

  const shutdown = async () => {
    await app.close();
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
