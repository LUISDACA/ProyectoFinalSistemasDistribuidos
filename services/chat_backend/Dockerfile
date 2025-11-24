FROM node:18-alpine
# Chat WebSocket backend: consumes RabbitMQ and sends messages to clients
WORKDIR /app
COPY package.json .
RUN npm install --production
COPY index.js .
CMD ["node", "index.js"]
