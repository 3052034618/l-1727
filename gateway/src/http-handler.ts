import * as http from 'http';
import { ServerResponse, IncomingMessage } from 'http';
import { Protocol, Route, RequestContext, HttpMethod } from './types';
import { UnifiedRouter } from './router';
import { ProtocolConverter } from './converter';
import { GrpcClientPool } from './grpc-client-pool';
import { ConnectionManager } from './connection-manager';
import { RouteRegistry } from './route-registry';

interface StreamState {
  connId: string | null;
  cancelled: boolean;
  headersSent: boolean;
  streamRef: any;
  reqRef: IncomingMessage;
  resRef: ServerResponse;
}

export class HttpHandler {
  private router: UnifiedRouter;
  private grpcPool: GrpcClientPool;
  private connectionManager: ConnectionManager;
  private registry: RouteRegistry;

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
  }

  async handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const startTime = Date.now();
    let ruleId: string | undefined;
    let isError = false;
    let errorMsg: string | undefined;

    try {
      const { body, rawBody } = await this.readBody(req);
      const headers = ProtocolConverter.extractHeaders(req);
      const ctx = ProtocolConverter.buildRequestContext(
        Protocol.HTTP,
        req.method || 'GET',
        req.url || '/',
        headers,
        body,
        rawBody
      );

      const route = this.router.match(ctx);
      if (!route) {
        this.sendError(res, 404, {
          code: 'ROUTE_NOT_FOUND',
          message: `No route found for ${ctx.method} ${ctx.path}`,
          path: ctx.path,
          method: ctx.method,
        });
        return;
      }

      ruleId = route.ruleId;
      ctx.matchedRuleId = ruleId;

      const safeHeader = (v: string) => /[^\x20-\x7E]/.test(v) ? encodeURIComponent(v) : v;
      res.setHeader('X-Gateway-Rule-Id', ruleId);
      res.setHeader('X-Gateway-Rule-Name', safeHeader(route.ruleName));
      res.setHeader('X-Gateway-Route', safeHeader(`${route.serviceName}/${route.methodName}`));
      res.setHeader('X-Gateway-Conversion', `${route.sourceProtocol}->${route.targetProtocol}`);

      if (route.targetProtocol === Protocol.GRPC) {
        await this.handleHttpToGrpc(req, res, ctx, route);
      } else if (route.targetProtocol === Protocol.HTTP) {
        this.sendError(res, 501, {
          code: 'NOT_IMPLEMENTED',
          message: 'HTTP-to-HTTP proxying not yet implemented',
        });
      } else {
        this.sendError(res, 502, {
          code: 'UNSUPPORTED_TARGET_PROTOCOL',
          message: `Unsupported target protocol: ${route.targetProtocol}`,
          protocol: route.targetProtocol,
        });
      }
    } catch (err: any) {
      isError = true;
      errorMsg = err?.message || 'Unknown error';
      console.error('[HTTP] Handler error:', err);
      this.sendError(res, 500, {
        code: err?.code ?? 'INTERNAL_ERROR',
        message: err?.message || 'Internal gateway error',
      });
    } finally {
      if (ruleId) {
        const duration = Date.now() - startTime;
        this.registry.recordRequest(ruleId, Protocol.HTTP, duration, isError, errorMsg);
        if (!isError) {
          console.log(
            `[HTTP] ${(req.method || '').padEnd(4)} ${req.url} -> ` +
            `${ruleId} ${duration}ms`
          );
        }
      }
    }
  }

  private async handleHttpToGrpc(
    req: IncomingMessage,
    res: ServerResponse,
    ctx: RequestContext,
    route: Route
  ): Promise<void> {
    const payload = ProtocolConverter.buildGrpcPayload(route, ctx);

    if (route.isServerStreaming) {
      await this.handleStreamingGrpcResponse(req, res, route, payload);
    } else {
      await this.handleUnaryGrpcResponse(res, route, payload);
    }
  }

  private async handleUnaryGrpcResponse(
    res: ServerResponse,
    route: Route,
    payload: any
  ): Promise<void> {
    try {
      const response = await this.grpcPool.makeUnaryCall(
        route.backendAddress,
        route.serviceName,
        route.methodName,
        payload
      );

      const httpResponse = ProtocolConverter.grpcResponseToHttp(response, false);
      res.writeHead(httpResponse.statusCode, {
        ...httpResponse.headers,
        'X-Gateway-Stream-Mode': 'unary',
      });
      res.end(JSON.stringify(httpResponse.body));
    } catch (err: any) {
      this.handleGrpcError(err, res, route);
    }
  }

  private async handleStreamingGrpcResponse(
    req: IncomingMessage,
    res: ServerResponse,
    route: Route,
    payload: any
  ): Promise<void> {
    const remoteAddr = req.socket.remoteAddress || '';

    const connResult = this.connectionManager.register(
      Protocol.HTTP,
      remoteAddr,
      (data: any) => {
        if (!res.writableEnded && res.headersSent) {
          res.write(JSON.stringify(data) + '\n');
        }
      },
      () => {
        if (!res.writableEnded) {
          try { res.end(); } catch {}
        }
      },
      route
    );

    if (connResult instanceof Error) {
      const limits = this.connectionManager.getLimitsByProtocol();
      this.sendError(res, 503, {
        code: 'STREAM_QUOTA_EXCEEDED',
        message: connResult.message,
        details: {
          ruleId: route.ruleId,
          ruleName: route.ruleName,
          protocol: Protocol.HTTP,
          limits: limits.http,
          retryAfterSeconds: 5,
        },
      });
      return;
    }

    const connId = connResult.id;
    this.registry.incrementActive(route.ruleId, Protocol.HTTP);

    const state: StreamState = {
      connId,
      cancelled: false,
      headersSent: false,
      streamRef: null,
      reqRef: req,
      resRef: res,
    };

    let stream: any;
    try {
      stream = this.grpcPool.makeServerStreamingCall(
        route.backendAddress,
        route.serviceName,
        route.methodName,
        payload
      );
      state.streamRef = stream;
    } catch (grpcErr: any) {
      this.connectionManager.unregister(connId);
      this.registry.decrementActive(route.ruleId, Protocol.HTTP);
      this.handleGrpcError(grpcErr, res, route);
      return;
    }

    try {
      res.writeHead(200, {
        'Content-Type': 'application/x-ndjson',
        'Transfer-Encoding': 'chunked',
        'Cache-Control': 'no-cache, no-transform',
        'Connection': 'keep-alive',
        'X-Gateway-Stream': 'grpc-server-streaming-to-ndjson',
        'X-Gateway-Stream-Id': connId,
        'X-Gateway-Rule-Id': route.ruleId,
      });
      state.headersSent = true;
    } catch (headerErr: any) {
      stream.cancel();
      this.connectionManager.unregister(connId);
      this.registry.decrementActive(route.ruleId, Protocol.HTTP);
      return;
    }

    stream.on('data', (chunk: any) => {
      if (state.cancelled) return;

      try {
        const plainData = ProtocolConverter.protobufToPlainObject(chunk);
        res.write(JSON.stringify({ type: 'data', data: plainData, timestamp: Date.now() }) + '\n');
        this.connectionManager.updateActivity(connId);
      } catch (writeErr: any) {
        console.error('[HTTP] Stream write error:', writeErr.message);
        state.cancelled = true;
        stream.cancel();
      }
    });

    stream.on('end', () => {
      if (state.cancelled) return;
      state.cancelled = true;

      try {
        if (!res.writableEnded) {
          res.write(JSON.stringify({
            type: 'stream_end',
            meta: {
              ruleId: route.ruleId,
              streamId: connId,
              completedAt: Date.now(),
            },
          }) + '\n');
          res.end();
        }
      } finally {
        this.connectionManager.unregister(connId);
        this.registry.decrementActive(route.ruleId, Protocol.HTTP);
      }
    });

    stream.on('error', (err: any) => {
      if (state.cancelled) return;
      state.cancelled = true;

      console.error('[HTTP] gRPC stream error:', err.code, err.message);

      if (!res.writableEnded) {
        try {
          if (!state.headersSent) {
            this.handleGrpcError(err, res, route);
          } else {
            res.write(JSON.stringify({
              type: 'error',
              error: {
                code: err.code ?? 'GRPC_STREAM_ERROR',
                message: err.message,
                grpcCode: err.code,
                grpcDetails: err.details,
                timestamp: Date.now(),
              },
            }) + '\n');
            res.end();
          }
        } catch {}
      }

      this.connectionManager.unregister(connId);
      this.registry.decrementActive(route.ruleId, Protocol.HTTP);
    });

    const handleClientClose = () => {
      if (state.cancelled) return;
      state.cancelled = true;

      try {
        stream.cancel();
        if (!res.writableEnded) {
          if (!state.headersSent) {
            this.sendError(res, 499, {
              code: 'CLIENT_CLOSED',
              message: 'Client closed connection before response completed',
              ruleId: route.ruleId,
              streamId: connId,
            });
          } else {
            try {
              res.write(JSON.stringify({
                type: 'stream_end',
                meta: {
                  ruleId: route.ruleId,
                  streamId: connId,
                  completedAt: Date.now(),
                  reason: 'client_disconnected',
                },
              }) + '\n');
              res.end();
            } catch {}
          }
        }
      } finally {
        this.connectionManager.unregister(connId);
        this.registry.decrementActive(route.ruleId, Protocol.HTTP);
      }
    };

    req.on('close', handleClientClose);
    req.on('aborted', handleClientClose);
  }

  private handleGrpcError(err: any, res: ServerResponse, route: Route): void {
    const httpStatus = this.grpcCodeToHttpStatus(err.code);
    const errorBody = {
      error: {
        code: `GRPC_${err.code ?? 'UNKNOWN'}`,
        grpcCode: err.code ?? 2,
        message: err.message,
        details: err.details || undefined,
        ruleId: route.ruleId,
        ruleName: route.ruleName,
        backend: route.backendAddress,
        method: `${route.serviceName}/${route.methodName}`,
        timestamp: Date.now(),
      },
    };
    this.sendError(res, httpStatus, errorBody.error);
  }

  private grpcCodeToHttpStatus(code: number): number {
    const map: Record<number, number> = {
      0: 200, 1: 499, 2: 500, 3: 400, 4: 504, 5: 404,
      6: 409, 7: 403, 8: 429, 9: 412, 10: 409, 11: 400,
      12: 501, 13: 500, 14: 503, 15: 500, 16: 401,
    };
    return map[code] || 500;
  }

  private sendError(res: ServerResponse, statusCode: number, errorInfo: any): void {
    if (!res.headersSent) {
      res.writeHead(statusCode, {
        'Content-Type': 'application/json',
        'X-Gateway-Error': '1',
      });
    }
    const body = typeof errorInfo === 'string'
      ? { error: { code: statusCode, message: errorInfo } }
      : { error: errorInfo };
    res.end(JSON.stringify(body, null, 2));
  }

  private readBody(req: IncomingMessage): Promise<{ body: any; rawBody: Buffer }> {
    return new Promise((resolve, reject) => {
      const chunks: Buffer[] = [];
      req.on('data', (chunk: Buffer) => chunks.push(chunk));
      req.on('end', () => {
        const rawBody = Buffer.concat(chunks);
        if (rawBody.length === 0) {
          resolve({ body: null, rawBody });
          return;
        }
        try {
          const body = JSON.parse(rawBody.toString('utf-8'));
          resolve({ body, rawBody });
        } catch {
          resolve({ body: null, rawBody });
        }
      });
      req.on('error', reject);
    });
  }
}
