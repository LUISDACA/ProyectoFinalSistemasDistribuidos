FROM node:18-alpine
# Servicio de analítica: consume Kafka y reporta métricas básicas por consola
WORKDIR /app
COPY package.json .
RUN npm install --production
COPY index.js .
CMD ["node", "index.js"]