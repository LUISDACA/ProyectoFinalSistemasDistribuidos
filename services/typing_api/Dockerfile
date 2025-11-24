FROM node:18-alpine
# Contenedor de la API de typing: publica eventos de alta frecuencia en Kafka
WORKDIR /app
COPY package.json .
RUN npm install --production
COPY index.js .
CMD ["node", "index.js"]