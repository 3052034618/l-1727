import { WebSocketServer, WebSocket } from 'ws';
import { IncomingMessage } from 'http';
import { Protocol, Route, RequestContext, HttpMethod } from './types';
import { UnifiedRouter } from './router';
import { ProtocolConverter } from './converter';
import { GrpcClientPool } from './grpc-client-pool';
import { ConnectionManager } from './connection-manager';

interface WsRouteBinding {
  route: Route;
  grpcStream: any | null;
  wsConnection: WebSocket;
  connectionId: string | null;
  topic: string;
}

export class WsHandler {
  private wss: WebSocketServer;
  private router: UnifiedRouter;
  private grpcPool: GrpcClientPool;
  private connectionManager: ConnectionManager;
  private bindings: Map<WebSocket, WsRouteBinding> = new Map();

  constructor(
    router: UnifiedRouter,
    grpcPool: GrpcClientPool,
    connectionManager: ConnectionManager
  ) {
    this.router = router;
    this.grpcPool = grpcPool;
    this.connectionManager = connectionManager;
    this.wss = new WebSocketServer({ noServer: true });
    this.setupWss();
  }

  private setupWss(): void {
    this.wss.on('connection', (ws: WebSocket, req: IncomingMessage) => {
      this.handleConnection(ws, req);
    });
  }

  handleUpgrade(req: IncomingMessage, socket: any, head: Buffer): void {
    const url = req.url || '/';
    const path = ProtocolConverter.extractPath(url);
    const query = ProtocolConverter.extractQueryParams(url);

    const ctx: RequestContext = {
      protocol: Protocol.WEBSOCKET,
      method: HttpMethod.GET,
      path,
      headers: ProtocolConverter.extractHeaders(req),
      query,
      params: {},
      body: null,
      rawBody: null,
    };

    const route = this.router.match(ctx);
    if (!route) {
      socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
      socket.destroy();
      console.log(`[WS] Upgrade rejected: no route for ${path}`);
      return;
    }

    this.wss.handleUpgrade(req, socket, head, (ws) => {
      (ws as any).__route = route;
      (ws as any).__ctx = ctx;
      this.wss.emit('connection', ws, req);
    });
  }

  private handleConnection(ws: WebSocket, req: IncomingMessage): void {
    const route = (ws as any).__route as Route;
    const ctx = (ws as any).__ctx as RequestContext;
    const remoteAddr = req.socket.remoteAddress || '';

    const connResult = this.connectionManager.register(
      Protocol.WEBSOCKET,
      remoteAddr,
      (data: any) => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(typeof data === 'string' ? data : JSON.stringify(data));
        }
      },
      () => {
        if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CLOSING) {
          ws.close(1001, 'Server closing connection');
        }
      },
      route
    );

    if (connResult instanceof Error) {
      ws.close(1013, connResult.message);
      return;
    }

    const connectionId = connResult.id;
    const binding: WsRouteBinding = {
      route,
      grpcStream: null,
      wsConnection: ws,
      connectionId,
      topic: ctx.query.topic || 'default',
    };
    this.bindings.set(ws, binding);

    console.log(
      `[WS] Client connected: ${remoteAddr} -> ${route.serviceName}/${route.methodName} ` +
      `(topic: ${binding.topic}, connId: ${connectionId})`
    );

    if (route.targetProtocol === Protocol.GRPC && route.isServerStreaming) {
      this.bindGrpcStream(ws, binding);
    }

    ws.on('message', (data: Buffer, isBinary: boolean) => {
      this.handleMessage(ws, binding, data, isBinary);
    });

    ws.on('close', (code: number, reason: Buffer) => {
      console.log(`[WS] Client disconnected: ${connectionId}, code: ${code}`);
      this.cleanupBinding(ws);
      this.connectionManager.unregister(connectionId);
    });

    ws.on('error', (err: Error) => {
      console.error(`[WS] Error on ${connectionId}:`, err.message);
      this.cleanupBinding(ws);
      this.connectionManager.unregister(connectionId);
    });

    ws.on('pong', () => {
      this.connectionManager.updateActivity(connectionId);
    });

    ws.send(JSON.stringify({
      type: 'connected',
      data: {
        connectionId,
        topic: binding.topic,
        service: route.serviceName,
        method: route.methodName,
      },
    }));
  }

  private bindGrpcStream(ws: WebSocket, binding: WsRouteBinding): void {
    try {
      const payload = { topic: binding.topic };
      const stream = this.grpcPool.makeServerStreamingCall(
        binding.route.backendAddress,
        binding.route.serviceName,
        binding.route.methodName,
        payload
      );

      binding.grpcStream = stream;

      stream.on('data', (chunk: any) => {
        if (ws.readyState !== WebSocket.OPEN) {
          stream.cancel();
          return;
        }

        const plainData = ProtocolConverter.protobufToPlainObject(chunk);
        ws.send(JSON.stringify({
          type: 'event',
          data: plainData,
        }));

        if (binding.connectionId) {
          this.connectionManager.updateActivity(binding.connectionId);
        }
      });

      stream.on('end', () => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'stream_end', data: { reason: 'grpc_stream_completed' } }));
        }
      });

      stream.on('error', (err: any) => {
        console.error(`[WS] gRPC stream error for ${binding.connectionId}:`, err.message);
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'error', data: { message: err.message } }));
        }
      });

      console.log(`[WS] gRPC stream bound: ${binding.route.serviceName}/${binding.route.methodName}`);
    } catch (err: any) {
      console.error(`[WS] Failed to bind gRPC stream:`, err.message);
      ws.send(JSON.stringify({ type: 'error', data: { message: 'Failed to connect to backend stream' } }));
    }
  }

  private handleMessage(ws: WebSocket, binding: WsRouteBinding, data: Buffer, isBinary: boolean): void {
    if (isBinary) {
      console.warn(`[WS] Binary message not supported from ${binding.connectionId}`);
      return;
    }

    const message = data.toString('utf-8');
    let parsed: any;
    try {
      parsed = JSON.parse(message);
    } catch {
      parsed = { data: message };
    }

    if (parsed.type === 'pong') {
      if (binding.connectionId) {
        this.connectionManager.updateActivity(binding.connectionId);
      }
      return;
    }

    if (parsed.type === 'subscribe' && parsed.topic) {
      binding.topic = parsed.topic;
      if (binding.grpcStream) {
        binding.grpcStream.cancel();
      }
      this.bindGrpcStream(ws, binding);
      return;
    }

    if (parsed.type === 'unsubscribe') {
      if (binding.grpcStream) {
        binding.grpcStream.cancel();
        binding.grpcStream = null;
        ws.send(JSON.stringify({ type: 'unsubscribed', data: { topic: binding.topic } }));
      }
      return;
    }

    if (binding.route.targetProtocol === Protocol.GRPC && !binding.route.isServerStreaming) {
      this.forwardWsToGrpcUnary(ws, binding, parsed);
    }
  }

  private async forwardWsToGrpcUnary(ws: WebSocket, binding: WsRouteBinding, message: any): Promise<void> {
    try {
      const payload = ProtocolConverter.wsMessageToGrpcPayload(message, binding.route);
      const response = await this.grpcPool.makeUnaryCall(
        binding.route.backendAddress,
        binding.route.serviceName,
        binding.route.methodName,
        payload
      );

      if (ws.readyState === WebSocket.OPEN) {
        const plainResponse = ProtocolConverter.protobufToPlainObject(response);
        ws.send(JSON.stringify({ type: 'response', data: plainResponse }));
      }
    } catch (err: any) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'error', data: { message: err.message, code: err.code } }));
      }
    }
  }

  private cleanupBinding(ws: WebSocket): void {
    const binding = this.bindings.get(ws);
    if (binding) {
      if (binding.grpcStream) {
        try { binding.grpcStream.cancel(); } catch {}
        binding.grpcStream = null;
      }
      this.bindings.delete(ws);
    }
  }

  get activeConnections(): number {
    return this.bindings.size;
  }

  broadcast(topic: string, data: any): void {
    for (const [, binding] of this.bindings) {
      if (binding.topic === topic && binding.wsConnection.readyState === WebSocket.OPEN) {
        binding.wsConnection.send(JSON.stringify({ type: 'broadcast', data }));
      }
    }
  }

  shutdown(): void {
    for (const [ws, binding] of this.bindings) {
      this.cleanupBinding(ws);
      if (ws.readyState === WebSocket.OPEN) {
        ws.close(1001, 'Server shutting down');
      }
    }
    this.wss.close();
  }
}
