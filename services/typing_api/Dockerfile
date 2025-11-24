FROM node:18-alpine
# Typing API container: publishes high-frequency events to Kafka
WORKDIR /app
COPY package.json .
RUN npm install --production
COPY index.js .
CMD ["node", "index.js"]
