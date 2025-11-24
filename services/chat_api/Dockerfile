FROM node:18-alpine
# Contenedor de la API de chat: publica mensajes en RabbitMQ (Fuente de Verdad)
WORKDIR /app
COPY package.json .
RUN npm install --production
COPY index.js .
CMD ["node", "index.js"]