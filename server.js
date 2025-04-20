const express = require('express');
const http = require('http');
const { Kafka } = require('kafkajs');
const { Server } = require('socket.io');
const cors = require('cors');

// Setup server
const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(cors());
app.use(express.json());

// Setup Kafka
const kafka = new Kafka({
  clientId: 'chat-app',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'chat-group' });

//🧑 (browser)  ⇄  🌐 (Socket.IO on Node.js) ⇄  📨 (Kafka)

const topic = 'chat-room';

(async () => {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      const msg = message.value.toString();
      console.log('📩 Received from Kafka:', msg);
      io.emit('message', msg); // Emit to all clients
    },
  });
})();

// WebSocket connection
io.on('connection', (socket) => {
  console.log('🧑‍💻 User connected');
  socket.on('message', async (msg) => {
    console.log('📤 Sending to Kafka:', msg);
    await producer.send({
      topic,
      messages: [{ value: msg }],
    });
  });
});

// Start server
server.listen(3001, () => {
  console.log('🚀 Server running on http://localhost:3001');
});
