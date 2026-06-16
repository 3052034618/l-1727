import * as http from 'http';
import { ServerResponse, IncomingMessage } from 'http';
import { Protocol, Route, RequestContext } from './types';
import { UnifiedRouter } from './router';
import { ProtocolConverter } from './converter';
import { GrpcClientPool } from './grpc-client-pool';
import { ConnectionManager } from './connection-manager';

export class HttpHandler {
  private router: UnifiedRouter;
  private grpcPool: GrpcClientPool;
  private connectionManager: ConnectionManager;

  constructor(
    router: UnifiedRouter,
    grpcPool: GrpcClientPool,
    connectionManager: ConnectionManager
  ) {
    this.router = router;
    this.grpcPool = grpcPool;
    this.connectionManager = connectionManager;
  }

  async handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const startTime = Date.now();

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
        this.sendError(res, 404, `No route found for ${ctx.method} ${ctx.path}`);
        return;
      }

      res.setHeader('X-Gateway-Route', `${route.serviceName}/${route.methodName}`);
      res.setHeader('X-Gateway-Conversion', `${route.sourceProtocol}->${route.targetProtocol}`);

      if (route.targetProtocol === Protocol.GRPC) {
        await this.handleHttpToGrpc(req, ctx, route, res);
      } else if (route.targetProtocol === Protocol.HTTP) {
        this.sendError(res, 501, 'HTTP-to-HTTP proxying not yet implemented');
      } else {
        this.sendError(res, 502, `Unsupported target protocol: ${route.targetProtocol}`);
      }

      const duration = Date.now() - startTime;
      console.log(
        `[HTTP] ${ctx.method} ${ctx.path} -> ${route.serviceName}/${route.methodName} ` +
        `(${route.sourceProtocol}->${route.targetProtocol}) ${duration}ms`
      );
    } catch (err: any) {
      console.error('[HTTP] Handler error:', err);
      this.sendError(res, 500, err.message || 'Internal gateway error');
    }
  }

  private async handleHttpToGrpc(
    req: IncomingMessage,
    ctx: RequestContext,
    route: Route,
    res: ServerResponse
  ): Promise<void> {
    const payload = ProtocolConverter.buildGrpcPayload(route, ctx);

    if (route.isServerStreaming) {
      await this.handleStreamingGrpcResponse(req, route, payload, res);
    } else {
      await this.handleUnaryGrpcResponse(route, payload, res);
    }
  }

  private async handleUnaryGrpcResponse(
    route: Route,
    payload: any,
    res: ServerResponse
  ): Promise<void> {
    try {
      const response = await this.grpcPool.makeUnaryCall(
        route.backendAddress,
        route.serviceName,
        route.methodName,
        payload
      );

      const httpResponse = ProtocolConverter.grpcResponseToHttp(response, false);
      res.writeHead(httpResponse.statusCode, httpResponse.headers);
      res.end(JSON.stringify(httpResponse.body));
    } catch (err: any) {
      this.handleGrpcError(err, res);
    }
  }

  private async handleStreamingGrpcResponse(
    req: IncomingMessage,
    route: Route,
    payload: any,
    res: ServerResponse
  ): Promise<void> {
    try {
      const stream = this.grpcPool.makeServerStreamingCall(
        route.backendAddress,
        route.serviceName,
        route.methodName,
        payload
      );

      res.writeHead(200, {
        'Content-Type': 'application/x-ndjson',
        'Transfer-Encoding': 'chunked',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'X-Gateway-Stream': 'grpc-server-streaming-to-ndjson',
      });

      const remoteAddr = req.socket.remoteAddress || '';
      const connResult = this.connectionManager.register(
        Protocol.HTTP,
        remoteAddr,
        (data: any) => {
          res.write(JSON.stringify(data) + '\n');
        },
        () => {
          stream.cancel();
          try { res.end(); } catch {}
        },
        route
      );

      if (connResult instanceof Error) {
        stream.cancel();
        this.sendError(res, 503, connResult.message);
        return;
      }

      const connId = connResult.id;

      stream.on('data', (chunk: any) => {
        const plainData = ProtocolConverter.protobufToPlainObject(chunk);
        const ndjsonLine = JSON.stringify(plainData) + '\n';
        res.write(ndjsonLine);
        this.connectionManager.updateActivity(connId);
      });

      stream.on('end', () => {
        this.connectionManager.unregister(connId);
        res.end();
      });

      stream.on('error', (err: any) => {
        console.error('[HTTP] Stream error:', err);
        this.connectionManager.unregister(connId);
        if (!res.writableEnded) {
          const errorLine = JSON.stringify({ error: err.message }) + '\n';
          res.write(errorLine);
          res.end();
        }
      });

      req.on('close', () => {
        stream.cancel();
        this.connectionManager.unregister(connId);
      });
    } catch (err: any) {
      this.handleGrpcError(err, res);
    }
  }

  private handleGrpcError(err: any, res: ServerResponse): void {
    const httpStatus = this.grpcCodeToHttpStatus(err.code);
    const errorBody = {
      error: {
        code: err.code,
        message: err.message,
        details: err.details || undefined,
      },
    };
    this.sendError(res, httpStatus, JSON.stringify(errorBody));
  }

  private grpcCodeToHttpStatus(code: number): number {
    const map: Record<number, number> = {
      0: 200, 1: 499, 2: 500, 3: 400, 4: 504, 5: 404,
      6: 409, 7: 403, 8: 429, 9: 412, 10: 409, 11: 400,
      12: 501, 13: 500, 14: 503, 15: 500, 16: 401,
    };
    return map[code] || 500;
  }

  private sendError(res: ServerResponse, statusCode: number, message: string): void {
    if (!res.headersSent) {
      res.writeHead(statusCode, { 'Content-Type': 'application/json' });
    }
    res.end(JSON.stringify({ error: { code: statusCode, message } }));
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
