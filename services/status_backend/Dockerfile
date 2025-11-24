FROM node:18-alpine
# Backend WebSocket de estado: consume RabbitMQ (fanout) y difunde estado
WORKDIR /app
COPY package.json .
RUN npm install --production
COPY index.js .
CMD ["node", "index.js"]