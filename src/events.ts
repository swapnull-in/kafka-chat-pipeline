import { randomUUID } from "node:crypto";

// The event contract shared by the producer and every consumer.
//
// Stage 2 upgrade: events are now wrapped in an ENVELOPE. The payload says
// what happened in the domain; the envelope carries the metadata every event
// needs regardless of type:
//   eventId    — unique per event; consumers dedupe on this (Stage 4)
//   version    — schema version, so consumers can evolve (Stage 5)
//   occurredAt — when it happened (not when Kafka received it)
export interface EventEnvelope<Type extends string, Payload> {
  eventId: string;
  type: Type;
  version: 1;
  occurredAt: string; // ISO timestamp
  payload: Payload;
}

export interface MessageSentPayload {
  conversationId: string;
  senderId: string;
  recipientId: string;
  text: string;
}

export type MessageSentEvent = EventEnvelope<"message.sent", MessageSentPayload>;

export interface TypingStartedPayload {
  conversationId: string;
  userId: string;
}

export type TypingStartedEvent = EventEnvelope<"typing.started", TypingStartedPayload>;

// Events state facts, past tense — "message.sent", never "send.notification".
// The producer doesn't know or care what consumers do with the fact.
export function toEnvelope<Type extends string, Payload>(
  type: Type,
  payload: Payload
): EventEnvelope<Type, Payload> {
  return {
    eventId: randomUUID(),
    type,
    version: 1,
    occurredAt: new Date().toISOString(),
    payload,
  };
}
