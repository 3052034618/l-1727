import { WebSocketServer, WebSocket } from 'ws';
import { IncomingMessage } from 'http';
import { Protocol, Route, RequestContext, HttpMethod } from './types';
import { UnifiedRouter } from './router';
import { ProtocolConverter } from './converter';
import { GrpcClientPool } from './grpc-client-pool';
import { ConnectionManager } from './connection-manager';
import { RouteRegistry } from './route-registry';

interface WsRouteBinding {
  route: Route;
  grpcStream: any | null;
  wsConnection: WebSocket;
  connectionId: string | null;
  topic: string;
  startTime: number;
}

export class WsHandler {
  private wss: WebSocketServer;
  private router: UnifiedRouter;
  private grpcPool: GrpcClientPool;
  private connectionManager: ConnectionManager;
  private registry: RouteRegistry;
  private bindings: Map<WebSocket, WsRouteBinding> = new Map();

  constructor(
    router: UnifiedRouter,
    grpcPool: GrpcClientPool,
    connectionManager: ConnectionManager,
    registry: RouteRegistry
  ) {
    this.router = router;
    this.grpcPool = grpcPool;
    this.connectionManager = connectionManager;
    this.registry = registry;
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
      socket.write('HTTP/1.1 404 Not Found\r\nX-Gateway-Error: 1\r\nContent-Type: application/json\r\n\r\n{"error":{"code":"ROUTE_NOT_FOUND","message":"No route found for WebSocket","path":"' + path + '"}}');
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
      const limits = this.connectionManager.getLimitsByProtocol();
      ws.close(1013, JSON.stringify({
        type: 'error',
        error: {
          code: 'WS_QUOTA_EXCEEDED',
          message: connResult.message,
          limits: limits.websocket,
          retryAfterSeconds: 10,
        },
      }));
      return;
    }

    const connectionId = connResult.id;
    this.registry.incrementActive(route.ruleId, Protocol.WEBSOCKET);

    const topicParam = route.topicQueryParam || 'topic';
    const binding: WsRouteBinding = {
      route,
      grpcStream: null,
      wsConnection: ws,
      connectionId,
      topic: ctx.query[topicParam] || 'default',
      startTime: Date.now(),
    };
    this.bindings.set(ws, binding);

    console.log(
      `[WS] Connected: ${remoteAddr} -> rule=${route.ruleId} connId=${connectionId} topic=${binding.topic}`
    );

    if (route.targetProtocol === Protocol.GRPC && route.isServerStreaming) {
      this.bindGrpcStream(ws, binding);
    }

    ws.on('message', (data: Buffer, isBinary: boolean) => {
      this.handleMessage(ws, binding, data, isBinary);
    });

    ws.on('close', (code: number, reason: Buffer) => {
      const duration = Date.now() - binding.startTime;
      this.registry.recordRequest(route.ruleId, Protocol.WEBSOCKET, duration, false);
      console.log(`[WS] Disconnected: ${connectionId}, code=${code}, duration=${duration}ms`);
      this.cleanupBinding(ws);
      this.registry.decrementActive(route.ruleId, Protocol.WEBSOCKET);
      this.connectionManager.unregister(connectionId);
    });

    ws.on('error', (err: Error) => {
      console.error(`[WS] Error on ${connectionId}:`, err.message);
      this.registry.recordRequest(route.ruleId, Protocol.WEBSOCKET, Date.now() - binding.startTime, true, err.message);
      this.cleanupBinding(ws);
      this.registry.decrementActive(route.ruleId, Protocol.WEBSOCKET);
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
        ruleId: route.ruleId,
        ruleName: route.ruleName,
        service: route.serviceName,
        method: route.methodName,
        isStreaming: route.isServerStreaming,
        connectedAt: Date.now(),
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
          meta: {
            ruleId: binding.route.ruleId,
            topic: binding.topic,
            timestamp: Date.now(),
          },
        }));

        if (binding.connectionId) {
          this.connectionManager.updateActivity(binding.connectionId);
        }
      });

      stream.on('end', () => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({
            type: 'stream_end',
            data: { reason: 'grpc_stream_completed', topic: binding.topic },
          }));
        }
      });

      stream.on('error', (err: any) => {
        console.error(`[WS] gRPC stream error for ${binding.connectionId}:`, err.code, err.message);
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({
            type: 'error',
            error: {
              code: `GRPC_${err.code ?? 'STREAM_ERROR'}`,
              grpcCode: err.code,
              message: err.message,
              details: err.details,
              ruleId: binding.route.ruleId,
              backend: binding.route.backendAddress,
            },
          }));
        }
        binding.grpcStream = null;
      });

      console.log(`[WS] gRPC stream bound: ${binding.route.ruleId}/${binding.route.methodName}, topic=${binding.topic}`);
    } catch (err: any) {
      console.error(`[WS] Failed to bind gRPC stream:`, err.message);
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          type: 'error',
          error: {
            code: 'BACKEND_CONNECT_FAILED',
            message: 'Failed to connect to backend stream',
            details: err.message,
          },
        }));
      }
    }
  }

  private handleMessage(ws: WebSocket, binding: WsRouteBinding, data: Buffer, isBinary: boolean): void {
    if (isBinary) {
      console.warn(`[WS] Binary message not supported from ${binding.connectionId}`);
      ws.send(JSON.stringify({
        type: 'error',
        error: { code: 'BINARY_NOT_SUPPORTED', message: 'Binary messages are not supported' },
      }));
      return;
    }

    const message = data.toString('utf-8');
    let parsed: any;
    try {
      parsed = JSON.parse(message);
    } catch {
      parsed = { type: 'raw', data: message };
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
        try { binding.grpcStream.cancel(); } catch {}
        binding.grpcStream = null;
      }
      if (binding.route.isServerStreaming) {
        this.bindGrpcStream(ws, binding);
      }
      ws.send(JSON.stringify({
        type: 'subscribed',
        data: { topic: binding.topic, ruleId: binding.route.ruleId },
      }));
      return;
    }

    if (parsed.type === 'unsubscribe') {
      if (binding.grpcStream) {
        try { binding.grpcStream.cancel(); } catch {}
        binding.grpcStream = null;
        ws.send(JSON.stringify({
          type: 'unsubscribed',
          data: { topic: binding.topic, ruleId: binding.route.ruleId },
        }));
      }
      return;
    }

    if (parsed.type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong', serverTime: Date.now() }));
      return;
    }

    if (binding.route.targetProtocol === Protocol.GRPC && !binding.route.isServerStreaming) {
      this.forwardWsToGrpcUnary(ws, binding, parsed);
    }
  }

  private async forwardWsToGrpcUnary(ws: WebSocket, binding: WsRouteBinding, message: any): Promise<void> {
    const reqStart = Date.now();
    let isError = false;
    let errMsg: string | undefined;

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
        ws.send(JSON.stringify({
          type: 'response',
          data: plainResponse,
          meta: {
            ruleId: binding.route.ruleId,
            method: binding.route.methodName,
            latencyMs: Date.now() - reqStart,
          },
        }));
      }
    } catch (err: any) {
      isError = true;
      errMsg = err.message;
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          type: 'error',
          error: {
            code: `GRPC_${err.code ?? 'CALL_ERROR'}`,
            grpcCode: err.code,
            message: err.message,
            details: err.details,
            ruleId: binding.route.ruleId,
            backend: binding.route.backendAddress,
          },
        }));
      }
    } finally {
      this.registry.recordRequest(
        binding.route.ruleId,
        Protocol.WEBSOCKET,
        Date.now() - reqStart,
        isError,
        errMsg
      );
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

  getBindingsByRuleId(ruleId: string): WsRouteBinding[] {
    const result: WsRouteBinding[] = [];
    for (const binding of this.bindings.values()) {
      if (binding.route.ruleId === ruleId) {
        result.push(binding);
      }
    }
    return result;
  }

  getBindingStats(): Array<{
    ruleId: string;
    ruleName: string;
    topic: string;
    durationMs: number;
    connectionId: string;
  }> {
    const now = Date.now();
    const result: Array<{
      ruleId: string;
      ruleName: string;
      topic: string;
      durationMs: number;
      connectionId: string;
    }> = [];
    for (const binding of this.bindings.values()) {
      result.push({
        ruleId: binding.route.ruleId,
        ruleName: binding.route.ruleName,
        topic: binding.topic,
        durationMs: now - binding.startTime,
        connectionId: binding.connectionId || 'unknown',
      });
    }
    return result;
  }

  broadcast(topic: string, data: any): void {
    for (const [, binding] of this.bindings) {
      if (binding.topic === topic && binding.wsConnection.readyState === WebSocket.OPEN) {
        binding.wsConnection.send(JSON.stringify({
          type: 'broadcast',
          data,
          meta: { topic, broadcastAt: Date.now() },
        }));
      }
    }
  }

  shutdown(): void {
    for (const [ws, binding] of this.bindings) {
      this.cleanupBinding(ws);
      if (ws.readyState === WebSocket.OPEN) {
        ws.close(1001, JSON.stringify({
          type: 'close',
          reason: 'server_shutdown',
          message: 'Gateway is shutting down gracefully',
        }));
      }
    }
    this.wss.close();
  }
}
