FROM node:18-alpine
# Stateful processor: Kafka -> RabbitMQ, deduces TYPING/STOPPED status
WORKDIR /app
COPY package.json .
RUN npm install --production
COPY index.js .
CMD ["node", "index.js"]
