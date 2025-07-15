const axios = require('axios');
const FormData = require('form-data');
const WebSocket = require('ws');
const EventEmitter = require('events');
const { Readable, Transform, pipeline } = require('stream');
const { promisify } = require('util');

// MODIFICADO: Configuración de puerto desde variables de entorno
const INITIAL_WEBSOCKET_PORT = parseInt(process.env.WEBSOCKET_PORT || '8080', 10);
const MAX_WEBSOCKET_PORT = parseInt(process.env.PORT_RANGE_END || (INITIAL_WEBSOCKET_PORT + 50).toString(), 10);

// Configuración
const API_URL = 'http://54.173.247.52/images';
const STREAM_URL = 'ws://your-stream-source.com/stream';
const RECONNECT_INTERVAL = 5000;
const HEARTBEAT_INTERVAL = 30000;
const MAX_BUFFER_SIZE = 1000;
const MAX_RECONNECT_ATTEMPTS = 10;
const CHUNK_SIZE = 1024 * 64; // 64KB chunks
const BACKPRESSURE_THRESHOLD = 1024 * 1024; // 1MB

const pipelineAsync = promisify(pipeline);

// Estado del sistema mejorado
let streamConnection = null;
let connectionStatus = 'disconnected';
let messageBuffer = [];
let reconnectAttempts = 0;
let processedMessages = 0;
let droppedMessages = 0;
let lastProcessedTimestamp = null;

const streamEmitter = new EventEmitter();
streamEmitter.setMaxListeners(100);

// MODIFICADO: Declarar 'wss' pero no instanciarlo todavía.
let wss;

// Manejo de conexiones WebSocket con clientes
const connectedClients = new Map();

// ... (El resto de las funciones como StreamDataProcessor, BufferManager, etc., permanecen sin cambios)
class StreamDataProcessor extends Transform {
  constructor(options = {}) {
    super({ 
      objectMode: true,
      highWaterMark: 16,
      ...options
    });
    this.processedCount = 0;
    this.errorCount = 0;
  }

  _transform(chunk, encoding, callback) {
    try {
      const processedData = this.processChunk(chunk);
      if (processedData) {
        this.processedCount++;
        processedMessages++;
        lastProcessedTimestamp = Date.now();
        this.push(processedData);
      }
      callback();
    } catch (error) {
      this.errorCount++;
      droppedMessages++;
      console.error('Error procesando chunk:', error);
      this.push({
        type: 'processing_error',
        error: error.message,
        timestamp: new Date().toISOString(),
        chunkInfo: this.getChunkInfo(chunk)
      });
      callback();
    }
  }

  processChunk(chunk) {
    if (!chunk || (typeof chunk === 'string' && chunk.trim() === '')) {
      return null;
    }
    let data;
    try {
      data = typeof chunk === 'string' ? JSON.parse(chunk) : chunk;
    } catch (parseError) {
      data = {
        type: 'raw_data',
        content: chunk.toString(),
        parseError: parseError.message
      };
    }
    return this.transformData(data);
  }

  transformData(data) {
    const processedData = {
      type: data.type || 'stream_data',
      data: data,
      processed: true,
      processingTime: Date.now(),
      id: `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      timestamp: new Date().toISOString()
    };
    if (data.type === 'alert' || data.priority === 'high') {
      processedData.priority = 'high';
      processedData.urgent = true;
    }
    if (this.validateProcessedData(processedData)) {
      return processedData;
    }
    return null;
  }

  validateProcessedData(data) {
    return data && data.type && data.timestamp && data.id;
  }

  getChunkInfo(chunk) {
    return {
      type: typeof chunk,
      length: chunk ? chunk.length || chunk.toString().length : 0,
      isEmpty: !chunk || chunk.toString().trim() === ''
    };
  }

  getStats() {
    return {
      processedCount: this.processedCount,
      errorCount: this.errorCount,
      isActive: !this.destroyed
    };
  }
}

class BufferManager {
  constructor(maxSize = MAX_BUFFER_SIZE) {
    this.buffer = [];
    this.maxSize = maxSize;
    this.droppedCount = 0;
    this.totalAdded = 0;
  }
  add(item) {
    this.totalAdded++;
    if (this.buffer.length >= this.maxSize) {
      const dropped = this.buffer.shift();
      this.droppedCount++;
      console.warn('Buffer overflow, mensaje eliminado:', {
        droppedId: dropped?.id,
        bufferSize: this.buffer.length,
        droppedCount: this.droppedCount
      });
    }
    this.buffer.push(item);
    return true;
  }
  getRecent(limit = 20) {
    return this.buffer.slice(-limit);
  }
  getStats() {
    return {
      bufferSize: this.buffer.length,
      maxSize: this.maxSize,
      droppedCount: this.droppedCount,
      totalAdded: this.totalAdded,
      utilization: (this.buffer.length / this.maxSize) * 100
    };
  }
  clear() {
    this.buffer = [];
  }
}

const bufferManager = new BufferManager(MAX_BUFFER_SIZE);

function broadcastToClients(message, options = {}) {
  // NUEVO: Verificar si el servidor wss ya está inicializado
  if (!wss) {
    console.warn('Intento de broadcast antes de que el servidor WebSocket esté listo.');
    return { sentCount: 0, errorCount: 0 };
  }
  
  const { 
    priority = 'normal', 
    filter = null,
    retryOnError = true,
    maxRetries = 3 
  } = options;

  const messageStr = JSON.stringify({
    ...message,
    timestamp: message.timestamp || new Date().toISOString(),
    priority
  });

  let sentCount = 0;
  let errorCount = 0;
  
  const sendPromises = [];

  connectedClients.forEach((clientInfo, clientId) => {
    const ws = clientInfo.ws;
    
    if (ws.readyState === WebSocket.OPEN) {
      if (filter && !filter(clientInfo)) return;
      if (ws.bufferedAmount > BACKPRESSURE_THRESHOLD) {
        console.warn(`Cliente ${clientId} con backpressure alto:`, ws.bufferedAmount);
        return;
      }
      const sendPromise = new Promise((resolve) => {
        try {
          ws.send(messageStr, (error) => {
            if (error) {
              console.error(`Error enviando mensaje a cliente ${clientId}:`, error);
              errorCount++;
              if (!retryOnError) removeClient(clientId);
            } else {
              sentCount++;
            }
            resolve();
          });
        } catch (error) {
          console.error(`Error inmediato enviando a cliente ${clientId}:`, error);
          errorCount++;
          removeClient(clientId);
          resolve();
        }
      });
      sendPromises.push(sendPromise);
    } else {
      removeClient(clientId);
    }
  });

  Promise.all(sendPromises).then(() => {
    console.log(`Broadcast completado: ${sentCount} enviados, ${errorCount} errores`);
  });

  return { sentCount, errorCount };
}

function removeClient(clientId) {
  const clientInfo = connectedClients.get(clientId);
  if (clientInfo) {
    try {
      if (clientInfo.ws.readyState === WebSocket.OPEN) clientInfo.ws.terminate();
    } catch (error) {
      console.error('Error terminando conexión:', error);
    }
    connectedClients.delete(clientId);
    console.log(`Cliente ${clientId} removido. Clientes activos: ${connectedClients.size}`);
  }
}

// ... (El resto de las funciones como processStreamWithPipeline, connectStreamToReadable, etc., sin cambios)
async function processStreamWithPipeline(streamUrl) { /* ... */ }
function connectStreamToReadable(streamUrl, readableStream) { /* ... */ }

// MODIFICADO: La lógica de `wss.on('connection')` se mueve dentro de la función de inicio.
function setupWebSocketListeners() {
  wss.on('connection', (ws, req) => {
    const clientId = `client_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    console.log('Nueva conexión WebSocket:', clientId, 'desde:', req.socket.remoteAddress);

    const clientInfo = { ws, id: clientId, connectedAt: Date.now(), lastPing: Date.now(), messagesSent: 0, messagesReceived: 0 };
    connectedClients.set(clientId, clientInfo);

    const welcomeMessage = { type: 'connection_status', status: 'connected', clientId, timestamp: new Date().toISOString(), streamStatus: connectionStatus, bufferStats: bufferManager.getStats() };
    ws.send(JSON.stringify(welcomeMessage));

    const recentMessages = bufferManager.getRecent(20);
    if (recentMessages.length > 0) {
      ws.send(JSON.stringify({ type: 'buffer_data', data: recentMessages, timestamp: new Date().toISOString() }));
    }

    ws.on('message', (message) => {
      try {
        clientInfo.messagesReceived++;
        const data = JSON.parse(message);
        handleClientMessage(ws, clientInfo, data);
      } catch (error) {
        console.error('Error procesando mensaje del cliente:', error);
      }
    });

    ws.on('close', () => {
      console.log('Conexión WebSocket cerrada:', clientId);
      connectedClients.delete(clientId);
    });

    ws.on('error', (error) => {
      console.error('Error en WebSocket:', error);
      connectedClients.delete(clientId);
    });

    const pingInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.ping();
      } else {
        clearInterval(pingInterval);
      }
    }, HEARTBEAT_INTERVAL);

    ws.on('pong', () => {
      clientInfo.lastPing = Date.now();
    });
  });
}

// ... (handleClientMessage, handleHighPriorityData, etc. sin cambios)
function handleClientMessage(ws, clientInfo, data) { /* ... */ }
function handleHighPriorityData(data) { /* ... */ }
async function processImageFromStream(imageUrl) { /* ... */ }
async function getImageFromURL(imageUrl) { /* ... */ }
async function sendImageToAPI(apiUrl, imageStream) { /* ... */ }

// NUEVO: Función para iniciar el servidor WebSocket con fallback de puerto
function startWebSocketServer(port) {
  if (port > MAX_WEBSOCKET_PORT) {
    console.error(`Error: No se pudo encontrar un puerto disponible en el rango ${INITIAL_WEBSOCKET_PORT}-${MAX_WEBSOCKET_PORT}.`);
    process.exit(1);
    return;
  }
  
  // Intenta crear el servidor en el puerto actual
  const tempWss = new WebSocket.Server({ 
    port: port,
    maxPayload: 1024 * 1024 * 10,
    verifyClient: (info) => {
      // const token = new URL(info.req.url, 'http://localhost').searchParams.get('token');
      return true;
    }
  });

  // Manejador para el evento 'listening' (éxito)
  tempWss.on('listening', () => {
    console.log(`✅ Servidor WebSocket escuchando en el puerto ${port}`);
    wss = tempWss; // Asigna la instancia exitosa a la variable global
    
    // Ahora que el servidor está escuchando, configura los manejadores de eventos
    setupWebSocketListeners();

    // Puedes iniciar otras lógicas que dependan del servidor aquí si es necesario
    console.log('Sistema de streaming listo para aceptar conexiones.');
  });

  // Manejador para el evento 'error'
  tempWss.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
      console.log(`El puerto ${port} está ocupado. Intentando con el puerto ${port + 1}...`);
      // Cierra el servidor temporal y prueba con el siguiente puerto
      tempWss.close();
      startWebSocketServer(port + 1);
    } else {
      console.error('Error al iniciar el servidor WebSocket:', err);
      process.exit(1);
    }
  });
}

// ... (reconnectToStream, consumeStream, etc. sin cambios)
function reconnectToStream() { /* ... */ }
async function consumeStream(streamUrl = STREAM_URL) {
  try {
    console.log('Iniciando consumo de stream en vivo...');
    await processStreamWithPipeline(streamUrl);
    // La confirmación del puerto ahora se hace en startWebSocketServer
    return {
      status: 'started',
      port: wss ? wss.options.port : 'starting...', // Devuelve el puerto real si ya está listo
      streamUrl: streamUrl
    };
  } catch (error) {
    console.error('Error iniciando el sistema de streaming:', error);
    throw error;
  }
}

function stopStreamConsumption() { /* ... */ }
function getSystemStats() { /* ... */ }

// Limpieza y manejo de señales (sin cambios)
setInterval(() => { /* ... */ }, 5 * 60 * 1000);
process.on('SIGINT', () => { /* ... */ });
process.on('SIGTERM', () => { /* ... */ });

// MODIFICADO: Iniciar el servidor llamando a nuestra nueva función.
startWebSocketServer(INITIAL_WEBSOCKET_PORT);

module.exports = { 
  consumeStream,
  stopStreamConsumption,
  getSystemStats,
  broadcastToClients
};