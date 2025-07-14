const axios = require('axios');
const FormData = require('form-data');
const WebSocket = require('ws');
const EventEmitter = require('events');
const { Readable } = require('stream');

// Configuración
const API_URL = 'http://54.173.247.52/images';
const WEBSOCKET_PORT = 8080;
const STREAM_URL = 'ws://your-stream-source.com/stream'; // URL del stream en vivo
const RECONNECT_INTERVAL = 5000; 
const HEARTBEAT_INTERVAL = 30000; 

// Estado del sistema
let streamConnection = null;
let connectionStatus = 'disconnected';
let messageBuffer = [];
let reconnectAttempts = 0;
const MAX_BUFFER_SIZE = 100;
const MAX_RECONNECT_ATTEMPTS = 10;

// Event emitter para comunicación interna
const streamEmitter = new EventEmitter();

// Servidor WebSocket para clientes frontend
const wss = new WebSocket.Server({ 
  port: WEBSOCKET_PORT,
  verifyClient: (info) => {
    // Implementar autenticación aquí si es necesario
    const token = new URL(info.req.url, 'http://localhost').searchParams.get('token');
    // Validar token aquí
    return true; // Por ahora acepta todas las conexiones
  }
});

// Manejo de conexiones WebSocket con clientes
const connectedClients = new Set();

wss.on('connection', (ws, req) => {
  console.log('Nueva conexión WebSocket establecida desde:', req.socket.remoteAddress);
  connectedClients.add(ws);
  
  // Enviar estado inicial al cliente
  ws.send(JSON.stringify({
    type: 'connection_status',
    status: 'connected',
    timestamp: new Date().toISOString(),
    streamStatus: connectionStatus
  }));
  
  // Enviar buffer de mensajes recientes
  if (messageBuffer.length > 0) {
    ws.send(JSON.stringify({
      type: 'buffer_data',
      data: messageBuffer.slice(-20), // Últimos 20 mensajes
      timestamp: new Date().toISOString()
    }));
  }
  
  // Manejo de mensajes del cliente
  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      handleClientMessage(ws, data);
    } catch (error) {
      console.error('Error al procesar mensaje del cliente:', error);
    }
  });
  
  // Manejo de desconexión
  ws.on('close', () => {
    console.log('Conexión WebSocket cerrada');
    connectedClients.delete(ws);
  });
  
  // Manejo de errores
  ws.on('error', (error) => {
    console.error('Error en WebSocket:', error);
    connectedClients.delete(ws);
  });
  
  // Implementar heartbeat/ping-pong
  const pingInterval = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.ping();
    } else {
      clearInterval(pingInterval);
    }
  }, HEARTBEAT_INTERVAL);
  
  ws.on('pong', () => {
    console.log('Pong recibido del cliente');
  });
});

// Función para manejar mensajes del cliente
function handleClientMessage(ws, data) {
  switch (data.type) {
    case 'request_stream_status':
      ws.send(JSON.stringify({
        type: 'stream_status',
        status: connectionStatus,
        reconnectAttempts: reconnectAttempts,
        timestamp: new Date().toISOString()
      }));
      break;
    
    case 'request_recent_data':
      const recentData = messageBuffer.slice(-data.limit || 10);
      ws.send(JSON.stringify({
        type: 'recent_data',
        data: recentData,
        timestamp: new Date().toISOString()
      }));
      break;
    
    case 'force_reconnect':
      if (data.auth === 'admin') { // Implementar autenticación adecuada
        reconnectToStream();
      }
      break;
    
    default:
      console.log('Tipo de mensaje desconocido:', data.type);
  }
}

// Función para broadcast a todos los clientes conectados
function broadcastToClients(message) {
  const messageStr = JSON.stringify({
    ...message,
    timestamp: new Date().toISOString()
  });
  
  connectedClients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) {
      try {
        ws.send(messageStr);
      } catch (error) {
        console.error('Error enviando mensaje a cliente:', error);
        connectedClients.delete(ws);
      }
    } else {
      connectedClients.delete(ws);
    }
  });
}

// Función para añadir mensajes al buffer con rate limiting
function addToBuffer(message) {
  const now = Date.now();
  const processedMessage = {
    ...message,
    timestamp: new Date().toISOString(),
    id: `${now}-${Math.random().toString(36).substr(2, 9)}`
  };
  
  messageBuffer.push(processedMessage);
  
  // Mantener el buffer dentro del límite
  if (messageBuffer.length > MAX_BUFFER_SIZE) {
    messageBuffer.shift();
  }
  
  return processedMessage;
}

// Función para procesar y filtrar datos del stream
function processStreamData(rawData) {
  try {
    // Parsear datos si vienen como JSON
    let data = typeof rawData === 'string' ? JSON.parse(rawData) : rawData;
    
    // Filtrar y procesar datos según sea necesario
    const processedData = {
      type: 'stream_data',
      data: data,
      processed: true,
      processingTime: Date.now()
    };
    
    // Filtros específicos basados en el tipo de datos
    if (data.type === 'alert' || data.priority === 'high') {
      processedData.priority = 'high';
      handleHighPriorityData(processedData);
    }
    
    // Añadir al buffer
    const bufferedMessage = addToBuffer(processedData);
    
    // Enviar a todos los clientes conectados
    broadcastToClients(bufferedMessage);
    
    return processedData;
  } catch (error) {
    console.error('Error procesando datos del stream:', error);
    broadcastToClients({
      type: 'processing_error',
      error: error.message,
      rawData: rawData
    });
  }
}

// Función para manejar datos de alta prioridad
function handleHighPriorityData(data) {
  console.log('Datos de alta prioridad recibidos:', data);
  
  // Enviar notificación inmediata
  broadcastToClients({
    type: 'high_priority_alert',
    data: data,
    urgent: true
  });
  
  // Procesar imagen si es necesario
  if (data.data.imageUrl) {
    processImageFromStream(data.data.imageUrl);
  }
}

// Función para procesar imágenes del stream
async function processImageFromStream(imageUrl) {
  try {
    broadcastToClients({
      type: 'image_processing_start',
      imageUrl: imageUrl
    });
    
    const imageStream = await getImageFromURL(imageUrl);
    await sendImageToAPI(API_URL, imageStream);
    
  } catch (error) {
    console.error('Error procesando imagen del stream:', error);
    broadcastToClients({
      type: 'image_processing_error',
      error: error.message,
      imageUrl: imageUrl
    });
  }
}

// Función para obtener imagen desde URL
async function getImageFromURL(imageUrl) {
  try {
    const response = await axios({
      url: imageUrl,
      method: 'GET',
      responseType: 'stream',
      timeout: 10000,
      headers: {
        'User-Agent': 'Stream-Consumer/1.0'
      }
    });
    return response.data;
  } catch (error) {
    console.error('Error al obtener la imagen:', error);
    throw error;
  }
}

// Función para enviar imagen a la API
async function sendImageToAPI(apiUrl, imageStream) {
  try {
    const formData = new FormData();
    formData.append('image', imageStream, { 
      filename: `stream_image_${Date.now()}.jpg`, 
      contentType: 'image/jpeg' 
    });

    const response = await axios.post(apiUrl, formData, {
      headers: {
        ...formData.getHeaders()
      },
      timeout: 15000
    });
    
    console.log('Imagen enviada a la API:', response.data);
    
    broadcastToClients({
      type: 'image_sent',
      status: 'success',
      data: response.data
    });
    
    return response.data;
  } catch (error) {
    console.error('Error al enviar imagen a la API:', error);
    
    broadcastToClients({
      type: 'image_sent',
      status: 'error',
      error: error.message
    });
    
    throw error;
  }
}

// Función para conectar al stream en vivo
function connectToStream(streamUrl) {
  try {
    console.log('Conectando al stream:', streamUrl);
    
    // Conectar al stream WebSocket
    streamConnection = new WebSocket(streamUrl);
    
    streamConnection.on('open', () => {
      console.log('Conexión al stream establecida');
      connectionStatus = 'connected';
      reconnectAttempts = 0;
      
      broadcastToClients({
        type: 'stream_connection_status',
        status: 'connected',
        message: 'Conexión establecida con el stream en vivo'
      });
    });
    
    streamConnection.on('message', (data) => {
      try {
        console.log('Datos recibidos del stream');
        processStreamData(data);
      } catch (error) {
        console.error('Error procesando mensaje del stream:', error);
      }
    });
    
    streamConnection.on('close', () => {
      console.log('Conexión al stream cerrada');
      connectionStatus = 'disconnected';
      
      broadcastToClients({
        type: 'stream_connection_status',
        status: 'disconnected',
        message: 'Conexión con el stream perdida'
      });
      
      // Intentar reconectar
      if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
        setTimeout(() => {
          reconnectToStream();
        }, RECONNECT_INTERVAL);
      }
    });
    
    streamConnection.on('error', (error) => {
      console.error('Error en la conexión del stream:', error);
      connectionStatus = 'error';
      
      broadcastToClients({
        type: 'stream_connection_error',
        error: error.message
      });
    });
    
  } catch (error) {
    console.error('Error al conectar al stream:', error);
    connectionStatus = 'error';
  }
}

// Función para reconectar al stream
function reconnectToStream() {
  reconnectAttempts++;
  console.log(`Intento de reconexión ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS}`);
  
  if (streamConnection) {
    streamConnection.close();
  }
  
  broadcastToClients({
    type: 'stream_reconnect_attempt',
    attempt: reconnectAttempts,
    maxAttempts: MAX_RECONNECT_ATTEMPTS
  });
  
  connectToStream(STREAM_URL);
}

// Función para consumir stream (API principal)
async function consumeStream(streamUrl = STREAM_URL, callback = null) {
  try {
    console.log('Iniciando consumo de stream en vivo...');
    
    // Conectar al stream
    connectToStream(streamUrl);
    
    // Configurar eventos del stream emitter
    streamEmitter.on('data', (data) => {
      if (callback) {
        callback(data);
      }
    });
    
    console.log('Sistema de streaming iniciado en puerto:', WEBSOCKET_PORT);
    
    return {
      status: 'started',
      port: WEBSOCKET_PORT,
      streamUrl: streamUrl
    };
    
  } catch (error) {
    console.error('Error iniciando el sistema de streaming:', error);
    throw error;
  }
}

// Función para detener el consumo del stream
function stopStreamConsumption() {
  if (streamConnection) {
    streamConnection.close();
    streamConnection = null;
  }
  
  connectionStatus = 'stopped';
  
  broadcastToClients({
    type: 'stream_stopped',
    message: 'Consumo de stream detenido'
  });
}

// Función para obtener estadísticas del sistema
function getSystemStats() {
  return {
    connectionStatus,
    connectedClients: connectedClients.size,
    bufferSize: messageBuffer.length,
    reconnectAttempts,
    uptime: process.uptime(),
    memoryUsage: process.memoryUsage()
  };
}

// Manejo de señales del sistema
process.on('SIGINT', () => {
  console.log('Cerrando sistema de streaming...');
  stopStreamConsumption();
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('Terminando sistema de streaming...');
  stopStreamConsumption();
  process.exit(0);
});

module.exports = { 
  consumeStream,
  stopStreamConsumption,
  getSystemStats,
  broadcastToClients // Para uso externo si es necesario
};