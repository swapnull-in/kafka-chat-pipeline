import { createKafka, TOPICS } from "./kafka.js";

// Topics are usually created explicitly (auto-creation is disabled in most
// production clusters). 3 partitions = up to 3 consumers in one group can
// share the work, and messages with the same key always land in the same
// partition (preserving order per conversation).
async function main() {
  const admin = createKafka("admin").admin();
  await admin.connect();

  const created = await admin.createTopics({
    topics: [
      {
        topic: TOPICS.chatMessages,
        numPartitions: 3,
        replicationFactor: 1, // single broker; would be 3 in production
      },
      {
        topic: TOPICS.chatTyping,
        numPartitions: 3,
        replicationFactor: 1,
        // Retention is PER TOPIC. Typing indicators are worthless after a
        // minute, so keep them for 60s instead of the broker default (7 days).
        configEntries: [{ name: "retention.ms", value: "60000" }],
      },
    ],
  });

  console.log(
    created
      ? `Created topics "${TOPICS.chatMessages}" and "${TOPICS.chatTyping}" (3 partitions each)`
      : "Topics already exist"
  );

  await admin.disconnect();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
