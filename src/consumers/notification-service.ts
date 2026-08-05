import { createKafka, TOPICS } from "../kafka.js";
import type { MessageSentEvent } from "../events.js";

// A downstream service that sends push notifications for new messages.
//
// Key concept — consumer groups: every consumer with the SAME groupId shares
// the topic's partitions between them (horizontal scaling + failover).
// Consumers with DIFFERENT groupIds each get ALL messages independently —
// that's why this service and analytics-service both see every event.
//
// Try it: run this file in two terminals at once. Kafka rebalances and each
// instance gets a subset of the 3 partitions. Kill one — the other takes over.

async function main() {
  const consumer = createKafka("notification-service").consumer({
    groupId: "notification-service",
  });

  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.chatMessages, fromBeginning: false });
  console.log("Notification service listening...\n");

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const event: MessageSentEvent = JSON.parse(message.value!.toString());

      // Real version: look up the recipient's device tokens, call APNs/FCM.
      console.log(
        `[p${partition}] push -> ${event.payload.recipientId}: "New message from ${event.payload.senderId}"`
      );
      // When eachMessage resolves without throwing, kafkajs marks the offset
      // as processed (auto-commit). If this service crashes mid-batch, it
      // resumes from the last committed offset — messages are never lost,
      // but some may be redelivered (at-least-once delivery).
    },
  });

  const shutdown = async () => {
    await consumer.disconnect();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
