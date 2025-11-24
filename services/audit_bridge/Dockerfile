FROM node:18-alpine
# Puente de auditoría: replica mensajes de RabbitMQ en Kafka para analítica
WORKDIR /app
COPY package.json .
RUN npm install --production
COPY index.js .
CMD ["node", "index.js"]