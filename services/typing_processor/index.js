/*
  Servicio: typing_processor
  Propósito: Consumir eventos "typing" de Kafka y deducir estado por usuario.
  Lógica:
    - Mantiene Map "last" con el último timestamp de typing por (group:user)
    - Cada 1s calcula si <3s => TYPING, >3s => STOPPED
    - Solo publica en RabbitMQ cuando el estado cambia
*/
const { Kafka } = require('kafkajs');
const amqp = require('amqplib');

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const TOPIC_TYPING = process.env.TOPIC_TYPING || 'typing_events';
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const EXCHANGE_STATUS = process.env.EXCHANGE_STATUS || 'status_updates';

// Cliente Kafka y consumer con groupId estable
const kafka = new Kafka({ brokers: [KAFKA_BROKER] });
const consumer = kafka.consumer({ groupId: 'typing-processor' });

let channel;
const last = new Map();
const state = new Map();

// Prepara canal AMQP y fanout exchange para difundir estado
async function setupRabbit() {
  const conn = await amqp.connect(RABBITMQ_URL);
  channel = await conn.createChannel();
  await channel.assertExchange(EXCHANGE_STATUS, 'fanout', { durable: true });
}

// Determina estado actual a partir de la ventana de 3s
function currentState(ms) {
  return ms < 3000 ? 'TYPING' : 'STOPPED';
}

// Publica actualización de estado en RabbitMQ (no persistente)
async function publishStatus(user, group, status) {
  const payload = { user, group, status, ts: Date.now() };
  channel.publish(EXCHANGE_STATUS, '', Buffer.from(JSON.stringify(payload)), { persistent: false });
}

// Bucle principal: consume Kafka y programa revisión periódica
async function run() {
  await setupRabbit();
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC_TYPING, fromBeginning: false });
  await consumer.run({ eachMessage: async ({ message }) => {
    try {
      const payload = JSON.parse(message.value.toString());
      const key = `${payload.group}:${payload.user}`;
      last.set(key, payload.ts || Date.now());
      console.log(`typing_processor consumed ${payload.user}/${payload.group}`);
    } catch {}
  }});

  setInterval(async () => {
    const now = Date.now();
    for (const [key, ts] of last.entries()) {
      const ms = now - ts;
      const s = currentState(ms);
      const prev = state.get(key);
      if (prev !== s) {
        state.set(key, s);
        const [group, user] = key.split(':');
        console.log(`typing_processor publish ${user}/${group}=${s}`);
        await publishStatus(user, group, s);
      }
    }
  }, 1000);
}

run().catch(() => process.exit(1));