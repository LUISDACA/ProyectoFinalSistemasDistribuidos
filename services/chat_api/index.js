/*
  Service: chat_api
  Purpose: HTTP endpoint to send real chat messages.
  Flow:
    - Receives POST /message with { user, group, text }
    - Publishes persistent JSON to RabbitMQ (topic exchange "chat_messages")
    - Uses routing key "chat.<group>" to route to the group's queue
*/
const express = require('express');
const cors = require('cors');
const amqp = require('amqplib');

// Environment configuration (docker-compose sets variables pointing to RabbitMQ)
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const EXCHANGE_CHAT = process.env.EXCHANGE_CHAT || 'chat_messages';
const ROUTING_PREFIX = process.env.ROUTING_PREFIX || 'chat';
const PORT = process.env.PORT || 3001;

const app = express();
app.use(cors());
app.use(express.json());

let channel;
// Create AMQP channel and ensure durable topic exchange
async function setupRabbit() {
  const conn = await amqp.connect(RABBITMQ_URL);
  channel = await conn.createChannel();
  await channel.assertExchange(EXCHANGE_CHAT, 'topic', { durable: true });
}

// Main endpoint to publish the chat "Source of Truth"
app.post('/message', async (req, res) => {
  try {
    const { user, group, text } = req.body || {};
    if (!user || !group || !text) return res.status(400).json({ error: 'user, group, text requeridos' });
    const routingKey = `${ROUTING_PREFIX}.${String(group).toLowerCase()}`;
    const payload = { user, group, text, ts: Date.now() };
    channel.publish(EXCHANGE_CHAT, routingKey, Buffer.from(JSON.stringify(payload)), { persistent: true });
    console.log(`chat_api -> ${user}/${group}: ${text}`);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: 'error publicando mensaje' });
  }
});

// Start HTTP server after preparing RabbitMQ
setupRabbit().then(() => {
  app.listen(PORT, () => {});
}).catch(() => {
  process.exit(1);
});
