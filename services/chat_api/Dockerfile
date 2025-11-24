FROM node:18-alpine
# Chat API container: publishes messages to RabbitMQ (Source of Truth)
WORKDIR /app
COPY package.json .
RUN npm install --production
COPY index.js .
CMD ["node", "index.js"]
