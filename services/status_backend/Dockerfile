FROM node:18-alpine
# Status WebSocket backend: consumes RabbitMQ (fanout) and broadcasts status
WORKDIR /app
COPY package.json .
RUN npm install --production
COPY index.js .
CMD ["node", "index.js"]
