/*
  Servicio: status_backend
  Difunde el estado "está escribiendo" por WebSocket.
  - Consume RabbitMQ (exchange fanout "status_updates")
  - No requiere enrutado por clave; todos los grupos comparten el mismo flujo
*/
const amqp = require('amqplib');
const { WebSocketServer } = require('ws');
const { URL } = require('url');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const EXCHANGE_STATUS = process.env.EXCHANGE_STATUS || 'status_updates';
const PORT = Number(process.env.PORT || 8082);

// Servidor WebSocket para estado
const wss = new WebSocketServer({ port: PORT });

let channel;
const clients = new Map();

// Prepara fanout exchange y una cola efímera (exclusive)
async function setupRabbit() {
  const conn = await amqp.connect(RABBITMQ_URL);
  channel = await conn.createChannel();
  await channel.assertExchange(EXCHANGE_STATUS, 'fanout', { durable: true });
  const q = await channel.assertQueue('', { exclusive: true });
  await channel.bindQueue(q.queue, EXCHANGE_STATUS, '');
  await channel.consume(q.queue, msg => {
    if (!msg) return;
    try {
      const payload = JSON.parse(msg.content.toString());
      const g = String(payload.group).toLowerCase();
      const set = clients.get(g);
      if (set) {
        for (const ws of set) {
          if (ws.readyState === 1) ws.send(JSON.stringify(payload));
        }
        console.log(`status_backend -> broadcast ${payload.user}/${g}=${payload.status}`);
      }
    } catch {}
  }, { noAck: true });
}

wss.on('connection', async (ws, req) => {
  try {
    const u = new URL(req.url, `http://localhost:${PORT}`);
    const group = u.searchParams.get('group');
    if (!group) { ws.close(); return; }
    const g = String(group).toLowerCase();
    if (!clients.has(g)) clients.set(g, new Set());
    clients.get(g).add(ws);
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