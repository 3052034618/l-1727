import * as http from 'http';
import * as path from 'path';
import { GatewayConfig, createDefaultConfig } from './config';
import { UnifiedRouter } from './router';
import { ConnectionManager } from './connection-manager';
import { GrpcClientPool } from './grpc-client-pool';
import { HttpHandler } from './http-handler';
import { WsHandler } from './ws-handler';
import { GrpcHandler } from './grpc-handler';
import { Protocol, HttpMethod } from './types';

async function main() {
  const protoPath = path.join(__dirname, '..', 'proto', 'user.proto');
  const config = createDefaultConfig(protoPath);

  console.log('========================================');
  console.log('  Multi-Protocol Unified Gateway');
  console.log('========================================');
  console.log(`  HTTP/WS Port : ${config.httpPort}`);
  console.log(`  gRPC Port    : ${config.grpcPort}`);
  console.log(`  Backend      : ${config.grpcBackendAddress}`);
  console.log(`  Routes       : ${config.routes.length}`);
  console.log('========================================\n');

  const router = new UnifiedRouter(config.routes);
  const connectionManager = new ConnectionManager(
    config.wsMaxConnections,
    config.wsHeartbeatIntervalMs
  );
  const grpcPool = new GrpcClientPool();

  await grpcPool.initialize(protoPath);
  console.log('[Gateway] gRPC client pool initialized');

  const httpHandler = new HttpHandler(router, grpcPool, connectionManager);
  const wsHandler = new WsHandler(router, grpcPool, connectionManager);
  const grpcHandler = new GrpcHandler(router, grpcPool, connectionManager, protoPath);

  await grpcHandler.initialize();

  connectionManager.on('connected', (conn) => {
    console.log(`[Gateway] Connection opened: ${conn.id} (${conn.protocol}) from ${conn.remoteAddress}`);
  });

  connectionManager.on('disconnected', (id) => {
    console.log(`[Gateway] Connection closed: ${id}`);
  });

  connectionManager.on('error', (id, err) => {
    console.error(`[Gateway] Connection error ${id}: ${err.message}`);
  });

  const httpServer = http.createServer((req, res) => {
    const isWebSocket = req.headers.upgrade?.toLowerCase() === 'websocket';

    if (isWebSocket) {
      return;
    }

    if (req.url === '/health' && req.method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'healthy',
        connections: connectionManager.count,
        wsConnections: connectionManager.wsCount,
        protocols: ['http', 'websocket', 'grpc'],
      }));
      return;
    }

    if (req.url === '/stats' && req.method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        connections: {
          total: connectionManager.count,
          websocket: connectionManager.wsCount,
        },
        routes: config.routes.map((r) => ({
          pattern: r.pattern,
          source: r.sourceProtocol,
          target: r.targetProtocol,
          service: `${r.serviceName}/${r.methodName}`,
          streaming: r.isServerStreaming,
        })),
      }));
      return;
    }

    httpHandler.handle(req, res);
  });

  httpServer.on('upgrade', (req, socket, head) => {
    const isWebSocket = req.headers.upgrade?.toLowerCase() === 'websocket';

    if (isWebSocket) {
      console.log(`[Gateway] WebSocket upgrade request: ${req.url}`);
      wsHandler.handleUpgrade(req, socket, head);
      return;
    }

    socket.destroy();
  });

  await new Promise<void>((resolve) => {
    httpServer.listen(config.httpPort, () => {
      console.log(`[Gateway] HTTP/WS server listening on port ${config.httpPort}`);
      console.log(`  -> HTTP endpoints:`);
      console.log(`     GET  /api/v1/users       -> gateway.UserService/ListUsers (streaming)`);
      console.log(`     GET  /api/v1/users/:id   -> gateway.UserService/GetUser`);
      console.log(`     POST /api/v1/users       -> gateway.UserService/CreateUser`);
      console.log(`  -> WebSocket endpoints:`);
      console.log(`     WS   /ws/events?topic=X  -> gateway.UserService/StreamEvents`);
      console.log(`  -> Management:`);
      console.log(`     GET  /health`);
      console.log(`     GET  /stats`);
      resolve();
    });
  });

  await grpcHandler.start(config.grpcPort);
  console.log(`[Gateway] gRPC server listening on port ${config.grpcPort}`);
  console.log(`  -> gateway.UserService/GetUser`);
  console.log(`  -> gateway.UserService/CreateUser`);
  console.log(`  -> gateway.UserService/ListUsers (server streaming)`);
  console.log(`  -> gateway.UserService/StreamEvents (server streaming)`);

  console.log('\n[Gateway] All protocols active. Press Ctrl+C to shut down.\n');

  const shutdown = async () => {
    console.log('\n[Gateway] Shutting down...');

    console.log('[Gateway] Closing WebSocket handler...');
    wsHandler.shutdown();

    console.log('[Gateway] Closing HTTP server...');
    httpServer.close();

    console.log('[Gateway] Closing gRPC handler...');
    await grpcHandler.shutdown();

    console.log('[Gateway] Draining connections...');
    await connectionManager.shutdown(5000);

    console.log('[Gateway] Closing gRPC client pool...');
    grpcPool.closeAll();

    console.log('[Gateway] Shutdown complete.');
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error('[Gateway] Fatal error:', err);
  process.exit(1);
});
