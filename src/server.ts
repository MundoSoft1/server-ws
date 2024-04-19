import WebSocket from 'ws';

const wss = new WebSocket.Server({ port: 8080 });

wss.on('listening', () => {
    console.log('Servidor WebSocket iniciado correctamente en el puerto 8080');
});

wss.on('connection', (ws: WebSocket) => {
    console.log('Cliente conectado');

    ws.on('message', (data: Buffer) => {
        console.log('Datos binarios recibidos');

        // Ejemplo: Guardar los datos binarios en un archivo
        const fs = require('fs');
        fs.writeFileSync('videoFrame.bin', data);

        // Respuesta al cliente WebSocket
        ws.send('Datos binarios recibidos por el servidor');
    });

    ws.on('close', () => {
        console.log('Cliente desconectado');
    });
});
