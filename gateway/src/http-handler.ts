import * as http from 'http';
import { ServerResponse, IncomingMessage } from 'http';
import { Protocol, Route, RequestContext, HttpMethod } from './types';
import { UnifiedRouter } from './router';
import { ProtocolConverter } from './converter';
import { GrpcClientPool } from './grpc-client-pool';
import { ConnectionManager } from './connection-manager';
import { RouteRegistry, DEFAULT_TARGET_NAME } from './route-registry';

interface StreamState {
  connId: string | null;
  cancelled: boolean;
  headersSent: boolean;
  streamRef: any;
  reqRef: IncomingMessage;
  resRef: ServerResponse;
  startTime: number;
  targetName: string;
  recorded: boolean;
  hasError: boolean;
  errorMessage?: string;
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
    let targetName: string = DEFAULT_TARGET_NAME;
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
      targetName = ctx.grayTargetName || DEFAULT_TARGET_NAME;
      ctx.matchedRuleId = ruleId;

      const safeHeader = (v: string) => /[^\x20-\x7E]/.test(v) ? encodeURIComponent(v) : v;
      res.setHeader('X-Gateway-Rule-Id', ruleId);
      res.setHeader('X-Gateway-Rule-Name', safeHeader(route.ruleName));
      res.setHeader('X-Gateway-Route', safeHeader(`${route.serviceName}/${route.methodName}`));
      res.setHeader('X-Gateway-Conversion', `${route.sourceProtocol}->${route.targetProtocol}`);
      res.setHeader('X-Gateway-Target', targetName);

      if (route.targetProtocol === Protocol.GRPC) {
        await this.handleHttpToGrpc(req, res, ctx, route, targetName, startTime);
      } else if (route.targetProtocol === Protocol.HTTP) {
        isError = true;
        errorMsg = 'HTTP-to-HTTP proxying not yet implemented';
        this.sendError(res, 501, {
          code: 'NOT_IMPLEMENTED',
          message: errorMsg,
        });
      } else {
        isError = true;
        errorMsg = `Unsupported target protocol: ${route.targetProtocol}`;
        this.sendError(res, 502, {
          code: 'UNSUPPORTED_TARGET_PROTOCOL',
          message: errorMsg,
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
        this.registry.recordRequest(ruleId, Protocol.HTTP, duration, isError, errorMsg, targetName);
        if (isError) {
          console.error(
            `[HTTP] ${(req.method || '').padEnd(4)} ${req.url} -> ` +
            `${ruleId}[${targetName}] ERROR ${errorMsg} ${duration}ms`
          );
        } else {
          console.log(
            `[HTTP] ${(req.method || '').padEnd(4)} ${req.url} -> ` +
            `${ruleId}[${targetName}] ${duration}ms`
          );
        }
      }
    }
  }

  private async handleHttpToGrpc(
    req: IncomingMessage,
    res: ServerResponse,
    ctx: RequestContext,
    route: Route,
    targetName: string,
    startTime: number
  ): Promise<void> {
    const target = this.registry.getTargetForRoute(route, targetName);
    const effectiveRoute: Route = {
      ...route,
      backendAddress: target.backendAddress,
      serviceName: target.serviceName,
      methodName: target.methodName,
    };
    const payload = ProtocolConverter.buildGrpcPayload(effectiveRoute, ctx);

    if (route.isServerStreaming) {
      await this.handleStreamingGrpcResponse(req, res, effectiveRoute, payload, targetName, startTime);
    } else {
      await this.handleUnaryGrpcResponse(res, effectiveRoute, payload, targetName, startTime);
    }
  }

  private async handleUnaryGrpcResponse(
    res: ServerResponse,
    route: Route,
    payload: any,
    targetName: string,
    startTime: number
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
        'X-Gateway-Target': targetName,
      });
      res.end(JSON.stringify(httpResponse.body));
    } catch (err: any) {
      this.handleGrpcError(err, res, route);
      const duration = Date.now() - startTime;
      this.registry.recordRequest(
        route.ruleId,
        Protocol.HTTP,
        duration,
        true,
        `${err.code}: ${err.message}`,
        targetName
      );
      (res as any)._errorRecorded = true;
    }
  }

  private async handleStreamingGrpcResponse(
    req: IncomingMessage,
    res: ServerResponse,
    route: Route,
    payload: any,
    targetName: string,
    startTime: number
  ): Promise<void> {
    const remoteAddr = req.socket.remoteAddress || '';

    const state: StreamState = {
      connId: null,
      cancelled: false,
      headersSent: false,
      streamRef: null,
      reqRef: req,
      resRef: res,
      startTime,
      targetName,
      recorded: false,
      hasError: false,
    };

    const finalize = (isError: boolean, errorMsg?: string) => {
      if (state.recorded) return;
      state.recorded = true;
      state.hasError = isError;
      state.errorMessage = errorMsg;

      const duration = Date.now() - state.startTime;
      this.registry.recordRequest(
        route.ruleId,
        Protocol.HTTP,
        duration,
        isError,
        errorMsg,
        state.targetName
      );

      if (isError) {
        console.error(
          `[HTTP] STREAM ${route.ruleId}[${state.targetName}] ERROR ${errorMsg} ${duration}ms`
        );
      } else {
        console.log(
          `[HTTP] STREAM ${route.ruleId}[${state.targetName}] completed ${duration}ms`
        );
      }
    };

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
      const errMsg = connResult.message;
      this.sendError(res, 503, {
        code: 'STREAM_QUOTA_EXCEEDED',
        message: errMsg,
        details: {
          ruleId: route.ruleId,
          ruleName: route.ruleName,
          protocol: Protocol.HTTP,
          limits: limits.http,
          retryAfterSeconds: 5,
        },
      });
      finalize(true, `STREAM_QUOTA_EXCEEDED: ${errMsg}`);
      return;
    }

    state.connId = connResult.id;
    this.registry.incrementActive(route.ruleId, Protocol.HTTP, targetName);

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
      this.connectionManager.unregister(state.connId!);
      this.registry.decrementActive(route.ruleId, Protocol.HTTP, targetName);
      this.handleGrpcError(grpcErr, res, route);
      finalize(true, `${grpcErr.code}: ${grpcErr.message}`);
      return;
    }

    try {
      res.writeHead(200, {
        'Content-Type': 'application/x-ndjson',
        'Transfer-Encoding': 'chunked',
        'Cache-Control': 'no-cache, no-transform',
        'Connection': 'keep-alive',
        'X-Gateway-Stream': 'grpc-server-streaming-to-ndjson',
        'X-Gateway-Stream-Id': state.connId!,
        'X-Gateway-Rule-Id': route.ruleId,
        'X-Gateway-Target': targetName,
      });
      state.headersSent = true;
    } catch (headerErr: any) {
      stream.cancel();
      this.connectionManager.unregister(state.connId!);
      this.registry.decrementActive(route.ruleId, Protocol.HTTP, targetName);
      finalize(true, `header_write_error: ${headerErr.message}`);
      return;
    }

    stream.on('data', (chunk: any) => {
      if (state.cancelled) return;

      try {
        const plainData = ProtocolConverter.protobufToPlainObject(chunk);
        res.write(JSON.stringify({ type: 'data', data: plainData, timestamp: Date.now() }) + '\n');
        this.connectionManager.updateActivity(state.connId!);
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
              streamId: state.connId,
              targetName: state.targetName,
              completedAt: Date.now(),
            },
          }) + '\n');
          res.end();
        }
      } finally {
        this.connectionManager.unregister(state.connId!);
        this.registry.decrementActive(route.ruleId, Protocol.HTTP, state.targetName);
        finalize(false);
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

      this.connectionManager.unregister(state.connId!);
      this.registry.decrementActive(route.ruleId, Protocol.HTTP, state.targetName);
      finalize(true, `${err.code}: ${err.message}`);
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
              streamId: state.connId,
            });
          } else {
            try {
              res.write(JSON.stringify({
                type: 'stream_end',
                meta: {
                  ruleId: route.ruleId,
                  streamId: state.connId,
                  targetName: state.targetName,
                  completedAt: Date.now(),
                  reason: 'client_disconnected',
                },
              }) + '\n');
              res.end();
            } catch {}
          }
        }
      } finally {
        this.connectionManager.unregister(state.connId!);
        this.registry.decrementActive(route.ruleId, Protocol.HTTP, state.targetName);
        finalize(false, 'client_disconnected');
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
