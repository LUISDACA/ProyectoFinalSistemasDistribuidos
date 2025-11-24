/*
  Servicio: analytics_service
  Propósito: Consumir el log de auditoría desde Kafka y calcular métricas.
  Flujo:
    - Suscribe al topic "message_audit_log"
    - Acumula conteo por grupo y imprime cada 10s
*/
const { Kafka } = require('kafkajs');

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const TOPIC_AUDIT = process.env.TOPIC_AUDIT || 'message_audit_log';

const kafka = new Kafka({ brokers: [KAFKA_BROKER] });
const consumer = kafka.consumer({ groupId: 'analytics-service' });

// Acumulador de mensajes por grupo
const counts = new Map();

// Inicializa consumer y procesamiento
async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC_AUDIT, fromBeginning: false });
  await consumer.run({ eachMessage: async ({ message }) => {
    try {
      const payload = JSON.parse(message.value.toString());
      const g = String(payload.group || 'unknown').toLowerCase();
      counts.set(g, (counts.get(g) || 0) + 1);
    } catch {}
  }});
  setInterval(() => {
    for (const [g, c] of counts.entries()) {
      console.log(`Mensajes totales en ${g}: ${c}`);
    }
  }, 10000);
}

run().catch(() => process.exit(1));