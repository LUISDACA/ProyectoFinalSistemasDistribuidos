FROM node:18-alpine
# Audit bridge: replicates RabbitMQ messages into Kafka for analytics
WORKDIR /app
COPY package.json .
RUN npm install --production
COPY index.js .
CMD ["node", "index.js"]
