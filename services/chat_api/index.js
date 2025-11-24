/*
  Servicio: chat_api
  Propósito: Endpoint HTTP para enviar mensajes reales de chat.
  Flujo:
    - Recibe POST /message con { user, group, text }
    - Publica JSON persistente en RabbitMQ (exchange topic "chat_messages")
    - Usa routing key "chat.<grupo>" para enrutar a la cola del grupo
*/
const express = require('express');
const cors = require('cors');
const amqp = require('amqplib');

// Configuración por entorno (docker-compose define variables apuntando a rabbitmq)
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const EXCHANGE_CHAT = process.env.EXCHANGE_CHAT || 'chat_messages';
const ROUTING_PREFIX = process.env.ROUTING_PREFIX || 'chat';
const PORT = process.env.PORT || 3001;

const app = express();
app.use(cors());
app.use(express.json());

let channel;
// Crea canal AMQP y asegura el topic exchange duradero
async function setupRabbit() {
  const conn = await amqp.connect(RABBITMQ_URL);
  channel = await conn.createChannel();
  await channel.assertExchange(EXCHANGE_CHAT, 'topic', { durable: true });
}

// Endpoint principal para publicar la "Verdad" del chat
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

// Inicialización HTTP tras preparar RabbitMQ
setupRabbit().then(() => {
  app.listen(PORT, () => {});
}).catch(() => {
  process.exit(1);
});