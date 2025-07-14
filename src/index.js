const axios = require('axios');
const FormData = require('form-data');
const WebSocket = require('ws');
const EventEmitter = require('events');
const { Readable, Transform, pipeline } = require('stream');
const { promisify } = require('util');

// Configuración
const API_URL = 'http://54.173.247.52/images';
const WEBSOCKET_PORT = 8080;
const STREAM_URL = 'ws://your-stream-source.com/stream';
const RECONNECT_INTERVAL = 5000;
const HEARTBEAT_INTERVAL = 30000;
const MAX_BUFFER_SIZE = 1000;
const MAX_RECONNECT_ATTEMPTS = 10;
const CHUNK_SIZE = 1024 * 64; // 64KB chunks
const BACKPRESSURE_THRESHOLD = 1024 * 1024; // 1MB

// Pipeline promisificado para manejo de errores
const pipelineAsync = promisify(pipeline);

// Estado del sistema mejorado
let streamConnection = null;
let connectionStatus = 'disconnected';
let messageBuffer = [];
let reconnectAttempts = 0;
let processedMessages = 0;
let droppedMessages = 0;
let lastProcessedTimestamp = null;

// Event emitter para comunicación interna
const streamEmitter = new EventEmitter();

// Configurar límites de listeners para evitar memory leaks
streamEmitter.setMaxListeners(100);

// Servidor WebSocket para clientes frontend
const wss = new WebSocket.Server({ 
  port: WEBSOCKET_PORT,
  maxPayload: 1024 * 1024 * 10, // 10MB máximo
  verifyClient: (info) => {
    const token = new URL(info.req.url, 'http://localhost').searchParams.get('token');
    return true;
  }
});

// Manejo de conexiones WebSocket con clientes
const connectedClients = new Map();

// Transform stream para procesar datos del stream
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
      // Procesar chunk de datos de manera atómica
      const processedData = this.processChunk(chunk);
      
      if (processedData) {
        this.processedCount++;
        processedMessages++;
        lastProcessedTimestamp = Date.now();
        
        // Emitir datos procesados
        this.push(processedData);
      }
      
      callback();
    } catch (error) {
      this.errorCount++;
      droppedMessages++;
      
      // Log error pero no fallar el stream
      console.error('Error procesando chunk:', error);
      
      // Emitir error procesado para logging
      this.push({
        type: 'processing_error',
        error: error.message,
        timestamp: new Date().toISOString(),
        chunkInfo: this.getChunkInfo(chunk)
      });
      
      callback(); // Continuar procesando
    }
  }

  processChunk(chunk) {
    // Validar chunk
    if (!chunk || (typeof chunk === 'string' && chunk.trim() === '')) {
      return null;
    }

    let data;
    try {
      // Parsear datos de manera segura
      data = typeof chunk === 'string' ? JSON.parse(chunk) : chunk;
    } catch (parseError) {
      // Si no es JSON válido, tratarlo como texto
      data = {
        type: 'raw_data',
        content: chunk.toString(),
        parseError: parseError.message
      };
    }

    // Procesar según tipo de datos
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

    // Aplicar transformaciones específicas
    if (data.type === 'alert' || data.priority === 'high') {
      processedData.priority = 'high';
      processedData.urgent = true;
    }

    // Validar datos transformados
    if (this.validateProcessedData(processedData)) {
      return processedData;
    }

    return null;
  }

  validateProcessedData(data) {
    return data && 
           data.type && 
           data.timestamp && 
           data.id;
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

// Buffer con backpressure management
class BufferManager {
  constructor(maxSize = MAX_BUFFER_SIZE) {
    this.buffer = [];
    this.maxSize = maxSize;
    this.droppedCount = 0;
    this.totalAdded = 0;
  }

  add(item) {
    this.totalAdded++;
    
    // Implementar backpressure
    if (this.buffer.length >= this.maxSize) {
      // Remover elementos más antiguos (FIFO)
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

// Instanciar buffer manager
const bufferManager = new BufferManager(MAX_BUFFER_SIZE);

// Función mejorada para broadcast con control de flujo
function broadcastToClients(message, options = {}) {
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
  
  // Crear promesas para cada envío
  const sendPromises = [];

  connectedClients.forEach((clientInfo, clientId) => {
    const ws = clientInfo.ws;
    
    if (ws.readyState === WebSocket.OPEN) {
      // Aplicar filtro si existe
      if (filter && !filter(clientInfo)) {
        return;
      }
      
      // Verificar backpressure del cliente
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
              
              // Remover cliente si hay error persistente
              if (!retryOnError) {
                removeClient(clientId);
              }
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

  // Esperar que todos los envíos se completen
  Promise.all(sendPromises).then(() => {
    console.log(`Broadcast completado: ${sentCount} enviados, ${errorCount} errores`);
  });

  return { sentCount, errorCount };
}

// Función para remover cliente de manera segura
function removeClient(clientId) {
  const clientInfo = connectedClients.get(clientId);
  if (clientInfo) {
    try {
      if (clientInfo.ws.readyState === WebSocket.OPEN) {
        clientInfo.ws.terminate();
      }
    } catch (error) {
      console.error('Error terminando conexión:', error);
    }
    
    connectedClients.delete(clientId);
    console.log(`Cliente ${clientId} removido. Clientes activos: ${connectedClients.size}`);
  }
}

// Función para procesar stream con pipeline robusto
async function processStreamWithPipeline(streamUrl) {
  try {
    console.log('Iniciando pipeline de procesamiento de stream...');
    
    // Crear stream processor
    const processor = new StreamDataProcessor();
    
    // Crear readable stream desde WebSocket
    const readableStream = new Readable({
      objectMode: true,
      read() {}
    });

    // Crear writable stream para manejar datos procesados
    const writableStream = new Transform({
      objectMode: true,
      transform(chunk, encoding, callback) {
        try {
          // Añadir al buffer con backpressure management
          bufferManager.add(chunk);
          
          // Broadcast a clientes conectados
          const broadcastResult = broadcastToClients(chunk, {
            priority: chunk.priority || 'normal'
          });
          
          // Manejar datos de alta prioridad
          if (chunk.priority === 'high') {
            handleHighPriorityData(chunk);
          }
          
          callback();
        } catch (error) {
          console.error('Error en writable stream:', error);
          callback(); // Continuar procesando
        }
      }
    });

    // Configurar pipeline con manejo de errores
    const pipelinePromise = pipelineAsync(
      readableStream,
      processor,
      writableStream
    );

    // Conectar WebSocket al readable stream
    connectStreamToReadable(streamUrl, readableStream);

    // Manejar errores del pipeline
    pipelinePromise.catch((error) => {
      console.error('Error en pipeline de procesamiento:', error);
      
      // Notificar error a clientes
      broadcastToClients({
        type: 'pipeline_error',
        error: error.message,
        timestamp: new Date().toISOString()
      });
      
      // Intentar reconectar después de un delay
      setTimeout(() => {
        if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
          reconnectToStream();
        }
      }, RECONNECT_INTERVAL);
    });

    return { processor, readableStream, writableStream };

  } catch (error) {
    console.error('Error configurando pipeline:', error);
    throw error;
  }
}

// Función para conectar WebSocket al readable stream
function connectStreamToReadable(streamUrl, readableStream) {
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
      // Validar datos antes de procesar
      if (!data || data.length === 0) {
        return;
      }
      
      // Añadir datos al readable stream
      readableStream.push(data);
      
    } catch (error) {
      console.error('Error procesando mensaje del stream:', error);
      
      // Emitir error pero continuar procesando
      readableStream.push({
        type: 'stream_message_error',
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  });
  
  streamConnection.on('close', () => {
    console.log('Conexión al stream cerrada');
    connectionStatus = 'disconnected';
    
    // Cerrar readable stream
    readableStream.push(null);
    
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
    
    // Emitir error al readable stream
    readableStream.push({
      type: 'stream_connection_error',
      error: error.message,
      timestamp: new Date().toISOString()
    });
    
    broadcastToClients({
      type: 'stream_connection_error',
      error: error.message
    });
  });
}

// Función mejorada para manejar conexiones WebSocket
wss.on('connection', (ws, req) => {
  const clientId = `client_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  
  console.log('Nueva conexión WebSocket:', clientId, 'desde:', req.socket.remoteAddress);
  
  // Información del cliente
  const clientInfo = {
    ws,
    id: clientId,
    connectedAt: Date.now(),
    lastPing: Date.now(),
    messagesSent: 0,
    messagesReceived: 0
  };
  
  connectedClients.set(clientId, clientInfo);
  
  // Enviar estado inicial
  const welcomeMessage = {
    type: 'connection_status',
    status: 'connected',
    clientId,
    timestamp: new Date().toISOString(),
    streamStatus: connectionStatus,
    bufferStats: bufferManager.getStats()
  };
  
  ws.send(JSON.stringify(welcomeMessage));
  
  // Enviar buffer de mensajes recientes
  const recentMessages = bufferManager.getRecent(20);
  if (recentMessages.length > 0) {
    ws.send(JSON.stringify({
      type: 'buffer_data',
      data: recentMessages,
      timestamp: new Date().toISOString()
    }));
  }
  
  // Manejar mensajes del cliente
  ws.on('message', (message) => {
    try {
      clientInfo.messagesReceived++;
      const data = JSON.parse(message);
      handleClientMessage(ws, clientInfo, data);
    } catch (error) {
      console.error('Error procesando mensaje del cliente:', error);
    }
  });
  
  // Manejar desconexión
  ws.on('close', () => {
    console.log('Conexión WebSocket cerrada:', clientId);
    connectedClients.delete(clientId);
  });
  
  // Manejar errores
  ws.on('error', (error) => {
    console.error('Error en WebSocket:', error);
    connectedClients.delete(clientId);
  });
  
  // Implementar heartbeat mejorado
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

// Función para manejar mensajes del cliente
function handleClientMessage(ws, clientInfo, data) {
  switch (data.type) {
    case 'request_stream_status':
      ws.send(JSON.stringify({
        type: 'stream_status',
        status: connectionStatus,
        reconnectAttempts: reconnectAttempts,
        timestamp: new Date().toISOString(),
        stats: getSystemStats()
      }));
      break;
    
    case 'request_recent_data':
      const recentData = bufferManager.getRecent(data.limit || 10);
      ws.send(JSON.stringify({
        type: 'recent_data',
        data: recentData,
        timestamp: new Date().toISOString()
      }));
      break;
    
    case 'request_buffer_stats':
      ws.send(JSON.stringify({
        type: 'buffer_stats',
        stats: bufferManager.getStats(),
        timestamp: new Date().toISOString()
      }));
      break;
    
    case 'force_reconnect':
      if (data.auth === 'admin') {
        reconnectToStream();
      }
      break;
    
    default:
      console.log('Tipo de mensaje desconocido:', data.type);
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
  }, { priority: 'high' });
  
  // Procesar imagen si es necesario
  if (data.data?.imageUrl) {
    processImageFromStream(data.data.imageUrl);
  }
}

// Función mejorada para procesar imágenes
async function processImageFromStream(imageUrl) {
  try {
    broadcastToClients({
      type: 'image_processing_start',
      imageUrl: imageUrl
    });
    
    const imageStream = await getImageFromURL(imageUrl);
    const result = await sendImageToAPI(API_URL, imageStream);
    
    broadcastToClients({
      type: 'image_processing_complete',
      imageUrl: imageUrl,
      result: result
    });
    
  } catch (error) {
    console.error('Error procesando imagen del stream:', error);
    broadcastToClients({
      type: 'image_processing_error',
      error: error.message,
      imageUrl: imageUrl
    });
  }
}

// Función para obtener imagen desde URL con streaming
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

// Función para enviar imagen a la API con streaming
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
    return response.data;
    
  } catch (error) {
    console.error('Error al enviar imagen a la API:', error);
    throw error;
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
  
  // Reiniciar pipeline de procesamiento
  processStreamWithPipeline(STREAM_URL);
}

// Función principal para consumir stream
async function consumeStream(streamUrl = STREAM_URL) {
  try {
    console.log('Iniciando consumo de stream en vivo...');
    
    // Iniciar pipeline de procesamiento
    await processStreamWithPipeline(streamUrl);
    
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
    reconnectAttempts,
    processedMessages,
    droppedMessages,
    lastProcessedTimestamp,
    bufferStats: bufferManager.getStats(),
    uptime: process.uptime(),
    memoryUsage: process.memoryUsage()
  };
}

// Limpieza periódica de conexiones inactivas
setInterval(() => {
  const now = Date.now();
  const timeout = 60000; // 1 minuto
  
  connectedClients.forEach((clientInfo, clientId) => {
    if (now - clientInfo.lastPing > timeout) {
      console.log(`Removiendo cliente inactivo: ${clientId}`);
      removeClient(clientId);
    }
  });
}, 5 * 60 * 1000); // Cada 5 minutos

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
  broadcastToClients
};