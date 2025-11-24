FROM node:18-alpine
# Procesador stateful: Kafka -> RabbitMQ, deduce estado TYPING/STOPPED
WORKDIR /app
COPY package.json .
RUN npm install --production
COPY index.js .
CMD ["node", "index.js"]