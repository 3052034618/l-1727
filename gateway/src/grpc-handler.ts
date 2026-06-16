import * as grpc from '@grpc/grpc-js';
import { Protocol, Route, RequestContext } from './types';
import { UnifiedRouter } from './router';
import { GrpcClientPool } from './grpc-client-pool';
import { ConnectionManager } from './connection-manager';
import { ProtocolConverter } from './converter';

export class GrpcHandler {
  private server: grpc.Server;
  private router: UnifiedRouter;
  private grpcPool: GrpcClientPool;
  private connectionManager: ConnectionManager;
  private protoPath: string;

  constructor(
    router: UnifiedRouter,
    grpcPool: GrpcClientPool,
    connectionManager: ConnectionManager,
    protoPath: string
  ) {
    this.router = router;
    this.grpcPool = grpcPool;
    this.connectionManager = connectionManager;
    this.protoPath = protoPath;
    this.server = new grpc.Server({
      'grpc.max_receive_message_length': 4 * 1024 * 1024,
      'grpc.max_send_message_length': 4 * 1024 * 1024,
    });
  }

  async initialize(): Promise<void> {
    const protoLoader = require('@grpc/proto-loader');
    const packageDefinition = await protoLoader.load(this.protoPath, {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
    });

    const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
    const gatewayPkg = this.getPackage(protoDescriptor, 'gateway');

    if (!gatewayPkg || !(gatewayPkg as any).UserService) {
      throw new Error('gateway.UserService not found in proto definition');
    }

    const UserService = (gatewayPkg as any).UserService;
    const serviceDef = UserService.service;

    const implementations: Record<string, grpc.handleUnaryCall<any, any> | grpc.handleServerStreamingCall<any, any>> = {};

    for (const [methodName, methodDef] of Object.entries(serviceDef)) {
      const fullMethodName = (methodDef as any).originalName || methodName;
      const serviceName = 'gateway.UserService';

      const route = this.router.findRouteByServiceMethod(serviceName, methodName);
      if (!route) {
        console.warn(`[gRPC] No route found for ${serviceName}/${methodName}, using pass-through`);
      }

      if ((methodDef as any).requestStream) {
        continue;
      }

      if ((methodDef as any).responseStream) {
        implementations[methodName] = this.createServerStreamingHandler(serviceName, methodName, route);
      } else {
        implementations[methodName] = this.createUnaryHandler(serviceName, methodName, route);
      }
    }

    this.server.addService(serviceDef, implementations);
    console.log('[gRPC] Service handlers registered');
  }

  private createUnaryHandler(
    serviceName: string,
    methodName: string,
    route: Route | null
  ): grpc.handleUnaryCall<any, any> {
    return async (call: grpc.ServerUnaryCall<any, any>, callback: grpc.sendUnaryData<any>) => {
      const startTime = Date.now();
      const grpcPath = `/${serviceName}/${methodName}`;

      try {
        console.log(`[gRPC] Incoming: ${grpcPath}`);

        if (!route) {
          callback({
            code: grpc.status.UNIMPLEMENTED,
            message: `Method ${grpcPath} not routed`,
          } as grpc.ServiceError);
          return;
        }

        const payload = call.request;

        if (route.targetProtocol === Protocol.GRPC) {
          const response = await this.grpcPool.makeUnaryCall(
            route.backendAddress,
            route.serviceName,
            route.methodName,
            payload
          );

          const duration = Date.now() - startTime;
          console.log(`[gRPC] ${grpcPath} -> ${route.backendAddress} ${duration}ms`);
          callback(null, response);
        } else {
          callback({
            code: grpc.status.UNIMPLEMENTED,
            message: `Target protocol ${route.targetProtocol} not supported for gRPC source`,
          } as grpc.ServiceError);
        }
      } catch (err: any) {
        console.error(`[gRPC] Error handling ${grpcPath}:`, err.message);
        callback({
          code: err.code || grpc.status.INTERNAL,
          message: err.message,
        } as grpc.ServiceError);
      }
    };
  }

  private createServerStreamingHandler(
    serviceName: string,
    methodName: string,
    route: Route | null
  ): grpc.handleServerStreamingCall<any, any> {
    return (call: grpc.ServerWritableStream<any, any>) => {
      const startTime = Date.now();
      const grpcPath = `/${serviceName}/${methodName}`;

      console.log(`[gRPC] Incoming stream: ${grpcPath}`);

      if (!route) {
        call.emit('error', {
          code: grpc.status.UNIMPLEMENTED,
          message: `Method ${grpcPath} not routed`,
        });
        return;
      }

      const payload = call.request;
      const remoteAddr = call.getPeer();

      const connResult = this.connectionManager.register(
        Protocol.GRPC,
        remoteAddr,
        (data: any) => {
          call.write(data);
        },
        () => {
          call.end();
        },
        route
      );

      if (connResult instanceof Error) {
        call.emit('error', {
          code: grpc.status.RESOURCE_EXHAUSTED,
          message: connResult.message,
        });
        return;
      }

      const connId = connResult.id;

      try {
        const backendStream = this.grpcPool.makeServerStreamingCall(
          route.backendAddress,
          route.serviceName,
          route.methodName,
          payload
        );

        backendStream.on('data', (chunk: any) => {
          call.write(chunk);
          this.connectionManager.updateActivity(connId);
        });

        backendStream.on('end', () => {
          this.connectionManager.unregister(connId);
          call.end();
          const duration = Date.now() - startTime;
          console.log(`[gRPC] Stream ${grpcPath} completed ${duration}ms`);
        });

        backendStream.on('error', (err: any) => {
          console.error(`[gRPC] Stream error ${grpcPath}:`, err.message);
          this.connectionManager.unregister(connId);
          call.emit('error', {
            code: err.code || grpc.status.INTERNAL,
            message: err.message,
          });
        });

        call.on('cancelled', () => {
          backendStream.cancel();
          this.connectionManager.unregister(connId);
        });
      } catch (err: any) {
        this.connectionManager.unregister(connId);
        call.emit('error', {
          code: grpc.status.INTERNAL,
          message: err.message,
        });
      }
    };
  }

  private getPackage(protoDescriptor: any, packageName: string): any {
    const parts = packageName.split('.');
    let current = protoDescriptor;
    for (const part of parts) {
      if (!current[part]) return null;
      current = current[part];
    }
    return current;
  }

  async start(port: number): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server.bindAsync(
        `0.0.0.0:${port}`,
        grpc.ServerCredentials.createInsecure(),
        (err, port) => {
          if (err) {
            reject(err);
            return;
          }
          this.server.start();
          resolve();
        }
      );
    });
  }

  async shutdown(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server.tryShutdown((err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }
}
