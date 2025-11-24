const express = require('express');
/*
  Servicio: typing_api
  Propósito: Endpoint HTTP para eventos "está escribiendo".
  Flujo:
    - Recibe POST /typing con { user, group }
    - Publica JSON en Kafka (topic "typing_events") usando group como key
    - El procesador stateful leerá este flujo para deducir estado
*/
const { Kafka } = require('kafkajs');
const cors = require('cors');

const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const TOPIC_TYPING = process.env.TOPIC_TYPING || 'typing_events';
const PORT = process.env.PORT || 3002;

// Cliente y productor Kafka
const kafka = new Kafka({ brokers: [KAFKA_BROKER] });
const producer = kafka.producer();

const app = express();
// Habilita CORS para llamadas desde el cliente web
app.use(cors());
app.use(express.json());

// Endpoint principal de eventos de "typing"
app.post('/typing', async (req, res) => {
  try {
    const { user, group } = req.body || {};
    if (!user || !group) return res.status(400).json({ error: 'user, group requeridos' });
    const value = JSON.stringify({ user, group, ts: Date.now() });
    await producer.send({ topic: TOPIC_TYPING, messages: [{ key: String(group), value }] });
    console.log(`typing_api -> ${user}/${group}`);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: 'error publicando typing' });
  }
});

// Inicializa el productor Kafka y arranca la API
producer.connect().then(() => {
  app.listen(PORT, () => {});
}).catch(() => process.exit(1));