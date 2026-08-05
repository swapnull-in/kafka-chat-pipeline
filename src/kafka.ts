import { Kafka, logLevel } from "kafkajs";

// One shared Kafka client config. Each service creates its own producer/consumer from this.
// clientId shows up in broker logs and metrics so you can tell services apart.
export function createKafka(clientId: string) {
  return new Kafka({
    clientId,
    brokers: ["localhost:9092"],
    logLevel: logLevel.WARN,
  });
}

export const TOPICS = {
  chatMessages: "chat.messages",
  chatTyping: "chat.typing",
} as const;
