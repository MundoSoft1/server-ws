import * as WebSocket from 'ws';
import express from 'express';
import http from 'http';
import cors from 'cors';
import { consumeStream, getSystemStats, stopStreamConsumption } from './src/index.js';

const PORT = 8080;
const STREAM_URL = process.env.STREAM_URL || 'ws://your-stream-source.com/stream';

// Interfaces para TypeScript
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
  
  // Convertir wss.clients a array para poder usar filter
  const clients = Array.from(wss.clients) as ClientConnection[];
  
  clients.forEach((client: ClientConnection) => {
    if (client.readyState === WebSocket.OPEN) {
      // Aplicar filtro si existe
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
  // Implementar lógica de autenticación aquí
  // Por ahora, acepta todos los clientes
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
          const stats = getSystemStats();
          const statsResponse: StreamMessage = {
            type: 'stats_response',
            data: {
              ...stats,
              server: connectionStats
            }
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
  
  // Enviar mensaje de bienvenida
  const welcomeMessage: StreamMessage = {
    type: 'welcome',
    clientId,
    timestamp: new Date().toISOString(),
    message: 'Conexión establecida con el servidor de streaming'
  };
  client.send(JSON.stringify(welcomeMessage));
  
  // Notificar a otros clientes (opcional)
  const connectedMessage: StreamMessage = {
    type: 'client_connected',
    clientId,
    totalClients: connectionStats.activeConnections
  };
  broadcast(connectedMessage, (c) => c !== client && !!c.authenticated);

  
  // Manejo de mensajes
  client.on('message', (message: string) => {
    console.log(`Mensaje recibido de ${clientId}:`, message);
    connectionStats.messagesReceived++;
    handleClientMessage(client, message);
  });
  
  // Manejo de cierre de conexión
  client.on('close', () => {
    console.log(`Cliente desconectado: ${clientId}`);
    removeClient(client);
    
    // Notificar a otros clientes
    const disconnectedMessage: StreamMessage = {
      type: 'client_disconnected',
      clientId,
      totalClients: connectionStats.activeConnections
    };
    broadcast(disconnectedMessage, (c) => !!c.authenticated);
  });
  
  // Manejo de errores
  client.on('error', (error) => {
    console.error(`Error en cliente ${clientId}:`, error);
    removeClient(client);
  });
  
  // Implementar ping/pong
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
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    connections: connectionStats.activeConnections
  });
});

app.get('/stats', (req, res) => {
  const systemStats = getSystemStats();
  res.json({
    server: connectionStats,
    stream: systemStats,
    timestamp: new Date().toISOString()
  });
});

app.post('/broadcast', (req, res) => {
  const { message, filter } = req.body;
  
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
  const timeout = 60000; // 1 minuto
  
  connectedClients.forEach((client, clientId) => {
    if (client.lastPing && now - client.lastPing > timeout) {
      console.log(`Removiendo cliente inactivo: ${clientId}`);
      client.terminate();
      removeClient(client);
    }
  });
}

// Ejecutar limpieza cada 5 minutos
setInterval(cleanupInactiveConnections, 5 * 60 * 1000);

// Iniciar el sistema de streaming
async function initializeStreaming() {
  try {
    console.log('Iniciando sistema de streaming...');
    
    const result = await consumeStream(STREAM_URL);
    
    console.log('Sistema de streaming iniciado:', result);
    
    // Notificar a todos los clientes que el streaming está activo
    const streamingStartedMessage: StreamMessage = {
      type: 'streaming_started',
      status: 'active',
      streamUrl: STREAM_URL
    };
    broadcast(streamingStartedMessage);
    
  } catch (error) {
    console.error('Error iniciando el sistema de streaming:', error);
    
    // Notificar error a los clientes
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
  
  // Notificar a todos los clientes
  const shutdownMessage: StreamMessage = {
    type: 'server_shutdown',
    message: 'Servidor cerrándose'
  };
  broadcast(shutdownMessage);
  
  // Detener el consumo de streams
  stopStreamConsumption();
  
  // Cerrar servidor
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

// Iniciar servidor
server.listen(PORT, () => {
  console.log(`Servidor HTTP y WebSocket escuchando en el puerto ${PORT}`);
  console.log(`Estadísticas disponibles en http://localhost:${PORT}/stats`);
  console.log(`Health check en http://localhost:${PORT}/health`);
  
  // Inicializar el sistema de streaming
  initializeStreaming();
});

// Exportar funciones para uso externo
export { broadcast, connectionStats, connectedClients };