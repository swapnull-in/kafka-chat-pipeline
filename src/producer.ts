import { createKafka, TOPICS } from "./kafka.js";
import { toEnvelope, type MessageSentPayload } from "./events.js";

// Stage 1's simulator, kept as a LOAD GENERATOR: it fakes users chatting so
// you have a steady stream of events to watch while experimenting. The real
// producer is now the HTTP API (api.ts) — same envelope, same topic, same key.

const USERS = ["alice", "bob", "carol", "dave", "erin"];
const CONVERSATIONS = ["conv-1", "conv-2", "conv-3", "conv-4"];
const SAMPLE_TEXTS = [
  "hey, are you free for a call?",
  "did you see the new deploy?",
  "lunch at 1?",
  "lgtm, ship it",
  "can you review my PR?",
  "the demo went great!",
];

function pick<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

function makePayload(): MessageSentPayload {
  const senderId = pick(USERS);
  return {
    conversationId: pick(CONVERSATIONS),
    senderId,
    recipientId: pick(USERS.filter((u) => u !== senderId)),
    text: pick(SAMPLE_TEXTS),
  };
}

async function main() {
  const producer = createKafka("chat-simulator").producer({ idempotent: true });
  await producer.connect();
  console.log("Simulator connected. Sending a chat message every second (Ctrl+C to stop)...\n");

  const timer = setInterval(async () => {
    const event = toEnvelope("message.sent", makePayload());
    const [meta] = await producer.send({
      topic: TOPICS.chatMessages,
      messages: [
        {
          // Keying by conversationId routes every message of a conversation
          // to the SAME partition, so consumers see each conversation in
          // order. Different conversations spread across partitions.
          key: event.payload.conversationId,
          value: JSON.stringify(event),
        },
      ],
    });
    const { conversationId, senderId, recipientId, text } = event.payload;
    console.log(
      `sent  ${conversationId}  [partition ${meta.partition}, offset ${meta.baseOffset}]  ${senderId} -> ${recipientId}: "${text}"`
    );
  }, 1000);

  const shutdown = async () => {
    clearInterval(timer);
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
