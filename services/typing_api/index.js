const express = require('express');
/*
  Service: typing_api
  Purpose: HTTP endpoint for "is typing" events.
  Flow:
    - Receives POST /typing with { user, group }
    - Publishes JSON to Kafka (topic "typing_events") using group as key
    - The stateful processor will read this stream to deduce status
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
// Enable CORS for calls from the web client
app.use(cors());
app.use(express.json());

// Main endpoint for "typing" events
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

// Initialize Kafka producer and start the API
producer.connect().then(() => {
  app.listen(PORT, () => {});
}).catch(() => process.exit(1));
