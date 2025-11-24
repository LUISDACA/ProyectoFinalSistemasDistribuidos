/*
  Service: audit_bridge
  Purpose: Move the chat "Source of Truth" from RabbitMQ to Kafka for auditing.
  Flow:
    - Consume topic exchange "chat_messages" (binding chat.#)
    - Publish to Kafka topic "message_audit_log" with key = group
*/
const amqp = require('amqplib');
const { Kafka } = require('kafkajs');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const EXCHANGE_CHAT = process.env.EXCHANGE_CHAT || 'chat_messages';
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const TOPIC_AUDIT = process.env.TOPIC_AUDIT || 'message_audit_log';

const kafka = new Kafka({ brokers: [KAFKA_BROKER] });
const producer = kafka.producer();

// Initialize channels and start the bridge
async function run() {
  const conn = await amqp.connect(RABBITMQ_URL);
  const ch = await conn.createChannel();
  await ch.assertExchange(EXCHANGE_CHAT, 'topic', { durable: true });
  const q = await ch.assertQueue('audit_bridge_all', { durable: true });
  await ch.bindQueue(q.queue, EXCHANGE_CHAT, 'chat.#');
  await producer.connect();
  await ch.consume(q.queue, async msg => {
    if (!msg) return;
    try {
      const payload = JSON.parse(msg.content.toString());
      const key = String(payload.group || 'unknown');
      await producer.send({ topic: TOPIC_AUDIT, messages: [{ key, value: JSON.stringify(payload) }] });
    } catch {}
    ch.ack(msg);
  }, { noAck: false });
}

run().catch(() => process.exit(1));
