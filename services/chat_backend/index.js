/*
  Servicio: chat_backend
  Entrega mensajes reales por WebSocket a miembros del grupo.
  - Consume RabbitMQ (exchange topic "chat_messages")
  - Colas duraderas por grupo con binding "chat.<grupo>"
  - Difunde a clientes conectados al grupo
*/
const amqp = require('amqplib');
const { WebSocketServer } = require('ws');
const { URL } = require('url');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const EXCHANGE_CHAT = process.env.EXCHANGE_CHAT || 'chat_messages';
const PORT = Number(process.env.PORT || 8081);

// Servidor WebSocket para clientes del chat
const wss = new WebSocketServer({ port: PORT });

let channel;
const groupQueues = new Map();
const clients = new Map();

// Prepara conexión y exchange en RabbitMQ
async function setupRabbit() {
  const conn = await amqp.connect(RABBITMQ_URL);
  channel = await conn.createChannel();
  await channel.assertExchange(EXCHANGE_CHAT, 'topic', { durable: true });
}

// Crea cola y consumidor por grupo bajo demanda
async function ensureGroupConsumer(group) {
  const g = String(group).toLowerCase();
  if (groupQueues.has(g)) return;
  const qname = `chat_${g}`;
  await channel.assertQueue(qname, { durable: true });
  await channel.bindQueue(qname, EXCHANGE_CHAT, `chat.${g}`);
  await channel.consume(qname, msg => {
    if (!msg) return;
    try {
      const payload = JSON.parse(msg.content.toString());
      const set = clients.get(g);
      if (set) {
        for (const ws of set) {
          if (ws.readyState === 1) ws.send(JSON.stringify(payload));
        }
        console.log(`chat_backend -> broadcast ${payload.user}/${g}: ${payload.text}`);
      }
    } catch {}
    channel.ack(msg);
  }, { noAck: false });
  groupQueues.set(g, qname);
}

wss.on('connection', async (ws, req) => {
  try {
    const u = new URL(req.url, `http://localhost:${PORT}`);
    const group = u.searchParams.get('group');
    if (!group) { ws.close(); return; }
    const g = String(group).toLowerCase();
    if (!clients.has(g)) clients.set(g, new Set());
    clients.get(g).add(ws);
    // Asegura consumidor del grupo
    await ensureGroupConsumer(g);
    ws.on('close', () => {
      const set = clients.get(g);
      if (set) {
        set.delete(ws);
        if (set.size === 0) clients.delete(g);
      }
    });
  } catch {
    ws.close();
  }
});

// Inicialización
setupRabbit().catch(() => process.exit(1));