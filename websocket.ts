import * as WebSocket from 'ws';
import express from 'express';
import http from 'http';
import cors from 'cors';

// Configuración de puerto desde variables de entorno con valores por defecto
const initialPort = parseInt(process.env.PORT || '8080', 10);
const maxPort = parseInt(process.env.PORT_RANGE_END || (initialPort + 50).toString(), 10);
const STREAM_URL = process.env.STREAM_URL || 'ws://your-stream-source.com/stream';

// --- Interfaces para TypeScript ---

// Interfaz para estadísticas del servidor local
interface SystemStats {
  uptime: number;
  memoryUsage: NodeJS.MemoryUsage;
  server: ConnectionStats;
}

interface StreamMessage {
  type: string;
  data?: any;
  timestamp?: string;
  id?: string;
  priority?: 'high' | 'normal' | 'low';
  clientId?: string;
  source?: string;
  status?: string;
  error?: string;
  message?: string;
  totalClients?: number;
  subscription?: string;
  authenticated?: boolean;
  streamUrl?: string;
}

interface ClientConnection extends WebSocket {
  id?: string;
  authenticated?: boolean;
  lastPing?: number;
  subscriptions?: Set<string>;
}

interface ConnectionStats {
  totalConnections: number;
  activeConnections: number;
  messagesSent: number;
  messagesReceived: number;
  startTime: number;
}

// --- FUNCIONES SIMULADAS/LOCALES (para que el archivo sea autónomo) ---

/**
 * Obtiene las estadísticas del sistema para este servidor.
 */
function getSystemStats(): SystemStats {
  return {
    uptime: process.uptime(),
    memoryUsage: process.memoryUsage(),
    server: connectionStats, // Incluimos las estadísticas de conexión aquí.
  };
}

/**
 * Simula el inicio del consumo de un stream externo.
 */
async function consumeStream(streamUrl: string) {
  console.log(`(Simulado) Iniciando consumo de stream desde: ${streamUrl}`);
  // En una implementación real, aquí iría la lógica de conexión al stream.
  return { status: 'simulated_active', streamUrl };
}

/**
 * Simula la detención del consumo del stream.
 */
function stopStreamConsumption() {
  console.log('(Simulado) Deteniendo el consumo de stream.');
}


const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Estado del servidor
const connectionStats: ConnectionStats = {
  totalConnections: 0,
  activeConnections: 0,
  messagesSent: 0,
  messagesReceived: 0,
  startTime: Date.now()
};

const connectedClients = new Map<string, ClientConnection>();

// Función para generar ID único
function generateClientId(): string {
  return `client_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

// Función de broadcast mejorada con filtros
function broadcast(data: StreamMessage | string, filter?: (client: ClientConnection) => boolean) {
  const message = typeof data === 'string' ? data : JSON.stringify({
    ...data,
    timestamp: data.timestamp || new Date().toISOString()
  });

  let sentCount = 0;
  
  const clients = Array.from(wss.clients) as ClientConnection[];
  
  clients.forEach((client: ClientConnection) => {
    if (client.readyState === WebSocket.OPEN) {
      if (filter && !filter(client)) {
        return;
      }
      
      try {
        client.send(message);
        sentCount++;
        connectionStats.messagesSent++;
      } catch (error) {
        console.error('Error enviando mensaje a cliente:', error);
        removeClient(client);
      }
    }
  });
  
  console.log(`Mensaje enviado a ${sentCount} clientes`);
}

// Función para remover cliente
function removeClient(client: ClientConnection) {
  if (client.id) {
    connectedClients.delete(client.id);
    connectionStats.activeConnections--;
  }
}

// Función para autenticar cliente (básica)
function authenticateClient(client: ClientConnection, token?: string): boolean {
  client.authenticated = true;
  return true;
}

// Función para manejar suscripciones
function handleSubscription(client: ClientConnection, subscription: string) {
  if (!client.subscriptions) {
    client.subscriptions = new Set();
  }
  client.subscriptions.add(subscription);
  
  const response: StreamMessage = {
    type: 'subscription_confirmed',
    subscription,
    timestamp: new Date().toISOString()
  };
  
  client.send(JSON.stringify(response));
}

// Función para manejar mensajes del cliente
function handleClientMessage(client: ClientConnection, message: any) {
  try {
    const data = typeof message === 'string' ? JSON.parse(message) : message;
    
    switch (data.type) {
      case 'auth':
        const isAuthenticated = authenticateClient(client, data.token);
        const authResponse: StreamMessage = {
          type: 'auth_response',
          authenticated: isAuthenticated,
          clientId: client.id
        };
        client.send(JSON.stringify(authResponse));
        break;
        
      case 'subscribe':
        if (client.authenticated) {
          handleSubscription(client, data.subscription);
        }
        break;
        
      case 'unsubscribe':
        if (client.authenticated && client.subscriptions) {
          client.subscriptions.delete(data.subscription);
        }
        break;
        
      case 'ping':
        client.lastPing = Date.now();
        const pongResponse: StreamMessage = {
          type: 'pong',
          timestamp: new Date().toISOString()
        };
        client.send(JSON.stringify(pongResponse));
        break;
        
      case 'request_stats':
        if (client.authenticated) {
          // CORREGIDO: Llamamos a nuestra función local. No se necesita conversión de tipo.
          const stats = getSystemStats();
          const statsResponse: StreamMessage = {
            type: 'stats_response',
            data: stats // El objeto ya tiene la estructura correcta.
          };
          client.send(JSON.stringify(statsResponse));
        }
        break;
        
      default:
        console.log('Tipo de mensaje no reconocido:', data.type);
    }
  } catch (error) {
    console.error('Error procesando mensaje del cliente:', error);
  }
}

// Configuración de conexiones WebSocket
wss.on('connection', (ws: WebSocket, req) => {
  const client = ws as ClientConnection;
  const clientId = generateClientId();
  
  client.id = clientId;
  client.authenticated = false;
  client.lastPing = Date.now();
  
  connectedClients.set(clientId, client);
  connectionStats.totalConnections++;
  connectionStats.activeConnections++;
  
  console.log(`Cliente conectado: ${clientId} desde ${req.socket.remoteAddress}`);
  
  const welcomeMessage: StreamMessage = {
    type: 'welcome',
    clientId,
    timestamp: new Date().toISOString(),
    message: 'Conexión establecida con el servidor de streaming'
  };
  client.send(JSON.stringify(welcomeMessage));
  
  const connectedMessage: StreamMessage = {
    type: 'client_connected',
    clientId,
    totalClients: connectionStats.activeConnections
  };
  broadcast(connectedMessage, (c) => c !== client && !!c.authenticated);

  client.on('message', (message: string) => {
    console.log(`Mensaje recibido de ${clientId}:`, message);
    connectionStats.messagesReceived++;
    handleClientMessage(client, message);
  });
  
  client.on('close', () => {
    console.log(`Cliente desconectado: ${clientId}`);
    removeClient(client);
    
    const disconnectedMessage: StreamMessage = {
      type: 'client_disconnected',
      clientId,
      totalClients: connectionStats.activeConnections
    };
    broadcast(disconnectedMessage, (c) => !!c.authenticated);
  });
  
  client.on('error', (error) => {
    console.error(`Error en cliente ${clientId}:`, error);
    removeClient(client);
  });
  
  const pingInterval = setInterval(() => {
    if (client.readyState === WebSocket.OPEN) {
      client.ping();
    } else {
      clearInterval(pingInterval);
    }
  }, 30000);
  
  client.on('pong', () => {
    client.lastPing = Date.now();
  });
});

// Rutas REST para monitoreo
app.get('/health', (req, res) => {
  const finalPort = (server.address() as WebSocket.AddressInfo)?.port || 'N/A';
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    connections: connectionStats.activeConnections,
    listeningOnPort: finalPort,
  });
});

app.get('/stats', (req, res) => {
  // CORREGIDO: Llamamos a nuestra función local.
  const systemStats = getSystemStats();
  res.json({
    ...systemStats,
    timestamp: new Date().toISOString()
  });
});

app.post('/broadcast', (req, res) => {
  const { message } = req.body;
  
  if (!message) {
    return res.status(400).json({ error: 'Message is required' });
  }
  
  try {
    const broadcastMessage: StreamMessage = {
      type: 'admin_broadcast',
      data: message,
      source: 'admin'
    };
    
    broadcast(broadcastMessage);
    
    res.json({ 
      success: true, 
      message: 'Broadcast sent',
      recipients: connectionStats.activeConnections 
    });
  } catch (error) {
    res.status(500).json({ error: 'Failed to broadcast message' });
  }
});

// Función para limpiar conexiones inactivas
function cleanupInactiveConnections() {
  const now = Date.now();
  const timeout = 60000;
  
  connectedClients.forEach((client, clientId) => {
    if (client.lastPing && now - client.lastPing > timeout) {
      console.log(`Removiendo cliente inactivo: ${clientId}`);
      client.terminate();
      removeClient(client);
    }
  });
}

setInterval(cleanupInactiveConnections, 5 * 60 * 1000);

// Iniciar el sistema de streaming
async function initializeStreaming() {
  try {
    console.log('Iniciando sistema de streaming...');
    
    // Llamamos a nuestra función local simulada
    const result = await consumeStream(STREAM_URL);
    
    console.log('Sistema de streaming iniciado:', result);
    
    const streamingStartedMessage: StreamMessage = {
      type: 'streaming_started',
      status: 'active',
      streamUrl: STREAM_URL
    };
    broadcast(streamingStartedMessage);
    
  } catch (error) {
    console.error('Error iniciando el sistema de streaming:', error);
    
    const errorMessage: StreamMessage = {
      type: 'streaming_error',
      error: error instanceof Error ? error.message : 'Unknown error'
    };
    broadcast(errorMessage);
  }
}

// Manejo de cierre del servidor
process.on('SIGINT', () => {
  console.log('Cerrando servidor...');
  
  const shutdownMessage: StreamMessage = {
    type: 'server_shutdown',
    message: 'Servidor cerrándose'
  };
  broadcast(shutdownMessage);
  
  stopStreamConsumption();
  
  server.close(() => {
    console.log('Servidor cerrado');
    process.exit(0);
  });
});

process.on('SIGTERM', () => {
  console.log('Terminando servidor...');
  stopStreamConsumption();
  process.exit(0);
});

// Función para iniciar el servidor con fallback de puerto
function startServer(port: number) {
  if (port > maxPort) {
    console.error(`Error: No se pudo encontrar un puerto disponible en el rango ${initialPort}-${maxPort}.`);
    process.exit(1);
  }

  server.on('error', (error: NodeJS.ErrnoException) => {
    if (error.code === 'EADDRINUSE') {
      console.log(`El puerto ${port} está ocupado. Intentando con el puerto ${port + 1}...`);
      server.close(() => {
        startServer(port + 1);
      });
    } else {
      console.error('Error al iniciar el servidor:', error);
      process.exit(1);
    }
  });
  
  server.removeAllListeners('listening');

  server.listen(port, () => {
    const finalPort = (server.address() as WebSocket.AddressInfo).port;
    console.log(`✅ Servidor HTTP y WebSocket escuchando en el puerto ${finalPort}`);
    console.log(`   Estadísticas: http://localhost:${finalPort}/stats`);
    console.log(`   Health check: http://localhost:${finalPort}/health`);
    
    initializeStreaming();
  });
}

// Iniciar el proceso del servidor.
startServer(initialPort);

// Exportar funciones para uso externo (si otros módulos TS lo necesitan)
export { broadcast, connectionStats, connectedClients };