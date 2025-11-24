FROM node:18-alpine
# Backend WebSocket de chat: consume RabbitMQ y envía mensajes a clientes
WORKDIR /app
COPY package.json .
RUN npm install --production
COPY index.js .
CMD ["node", "index.js"]