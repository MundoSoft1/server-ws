import WebSocket from 'ws';
import jwt from 'jsonwebtoken';

const secretKey = 'token';
const server = new WebSocket.Server({ port: 8080 });

server.on('connection', (ws: WebSocket, req: any) => {
  const token = req.headers['authorization'];

  if (!token) {
    ws.close();
    return;
  }

  const tokenWithoutBearer = token.replace('Bearer ', '');
  jwt.verify(tokenWithoutBearer, secretKey, (err: any) => {
    if (err) {
      ws.close();
      return;
    }
    
    console.log('Conexión WebSocket autenticada');

    const stream = ws as unknown as NodeJS.ReadableStream;

    stream.on('data', (data: Buffer) => {
      console.log(`Datos recibidos del cliente: ${data.toString()}`);
      // Procesar los datos del cliente
    });
  });

  ws.on('close', () => {
    console.log('Conexión WebSocket cerrada');
  });
});

console.log('Servidor WebSocket escuchando en el puerto 8080');
