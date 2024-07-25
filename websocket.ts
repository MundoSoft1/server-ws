import * as WebSocket from 'ws';
import express from 'express';
import http from 'http';
import cors from 'cors';
import { consumeMessages } from './src/index.js';


const PORT = 8080;

const app = express();
app.use(cors());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

function broadcast(data: string) {
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  });
}

wss.on('connection', (ws: WebSocket) => {
  console.log('Cliente conectado');

  ws.on('message', (message: string) => {
    console.log('Mensaje recibido:', message);
  });

  broadcast('Un nuevo cliente se ha conectado.');
});

// Iniciar el consumidor de RabbitMQ una sola vez
consumeMessages((msg: string) => {
  broadcast(`Mensaje de RabbitMQ: ${msg}`);
});

server.listen(PORT, () => {
  console.log(`Servidor HTTP y WebSocket escuchando en el puerto ${PORT}`);
});
