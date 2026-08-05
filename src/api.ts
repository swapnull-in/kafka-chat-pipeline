import Fastify from "fastify";
import { createKafka, TOPICS } from "./kafka.js";
import { toEnvelope, type MessageSentPayload, type TypingStartedPayload } from "./events.js";

// Stage 2: the real chat API. The Stage 1 simulator (producer.ts) is replaced
// by an HTTP endpoint — a client POSTs a message, we publish the fact to
// Kafka, and every downstream service reacts without this API knowing.

// ONE producer per process — created at startup, shared by every request,
// disconnected on shutdown. Never per request: connections are expensive and
// batching only works across requests.
//
// idempotent: true — retries can duplicate (broker writes, ack is lost,
// producer resends). Idempotence gives the producer a session + sequence
// numbers so the broker drops the duplicate. It implies acks: -1 (all
// in-sync replicas), the durability setting you'd run in production.
const producer = createKafka("chat-api").producer({ idempotent: true });

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

    await producer.send({
      topic: TOPICS.chatMessages,
      messages: [
        // Keyed by conversationId: every message of a conversation lands in
        // the same partition, so consumers see each conversation in order.
        { key: event.payload.conversationId, value: JSON.stringify(event) },
      ],
    });

    // 202: we accepted the fact into the pipeline. Whether the push
    // notification lands is some other service's business, not this request's.
    return reply.code(202).send({ eventId: event.eventId });
  }
);

interface TypingBody {
  conversationId: string;
  userId: string;
}

app.post<{ Body: TypingBody }>(
  "/typing",
  {
    schema: {
      body: {
        type: "object",
        required: ["conversationId", "userId"],
        properties: {
          conversationId: { type: "string", minLength: 1 },
          userId: { type: "string", minLength: 1 },
        },
      },
    },
  },
  async (req, reply) => {
    const event = toEnvelope<"typing.started", TypingStartedPayload>("typing.started", req.body);

    await producer.send({
      topic: TOPICS.chatTyping,
      messages: [{ key: event.payload.conversationId, value: JSON.stringify(event) }],
    });

    return reply.code(202).send({ eventId: event.eventId });
  }
);

async function main() {
  await producer.connect();
  await app.listen({ port: 3000 });

  // Shutdown order matters: stop accepting HTTP first (no new sends can
  // start), THEN disconnect the producer (flushes anything still buffered).
  // Reversed, in-flight requests would try to send on a dead producer.
  const shutdown = async () => {
    await app.close();
    await producer.disconnect();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
