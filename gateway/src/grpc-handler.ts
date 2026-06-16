import * as grpc from '@grpc/grpc-js';
import { Protocol, Route } from './types';
import { UnifiedRouter } from './router';
import { GrpcClientPool } from './grpc-client-pool';
import { ConnectionManager } from './connection-manager';
import { RouteRegistry } from './route-registry';

export class GrpcHandler {
  private server: grpc.Server;
  private router: UnifiedRouter;
  private grpcPool: GrpcClientPool;
  private connectionManager: ConnectionManager;
  private registry: RouteRegistry;
  private protoPath: string;

  constructor(
    router: UnifiedRouter,
    grpcPool: GrpcClientPool,
    connectionManager: ConnectionManager,
    registry: RouteRegistry,
    protoPath: string
  ) {
    this.router = router;
    this.grpcPool = grpcPool;
    this.connectionManager = connectionManager;
    this.registry = registry;
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
      if ((methodDef as any).requestStream) continue;

      if ((methodDef as any).responseStream) {
        implementations[methodName] = this.createServerStreamingHandler(methodName);
      } else {
        implementations[methodName] = this.createUnaryHandler(methodName);
      }
    }

    this.server.addService(serviceDef, implementations);
    console.log('[gRPC] Service handlers registered for gateway.UserService');
  }

  private createUnaryHandler(
    methodName: string
  ): grpc.handleUnaryCall<any, any> {
    return async (call: grpc.ServerUnaryCall<any, any>, callback: grpc.sendUnaryData<any>) => {
      const startTime = Date.now();
      const serviceName = 'gateway.UserService';
      const grpcPath = `/${serviceName}/${methodName}`;

      const route = this.router.findRouteByServiceMethod(serviceName, methodName);
      if (!route) {
        this.registry.recordRequest('__unrouted__', Protocol.GRPC, Date.now() - startTime, true, `No route for ${grpcPath}`);
        callback({
          code: grpc.status.UNIMPLEMENTED,
          message: `Method ${grpcPath} not routed`,
        } as grpc.ServiceError);
        return;
      }

      if (route.targetProtocol !== Protocol.GRPC) {
        this.registry.recordRequest(route.ruleId, Protocol.GRPC, Date.now() - startTime, true,
          `Target protocol ${route.targetProtocol} not supported for gRPC source`);
        callback({
          code: grpc.status.UNIMPLEMENTED,
          message: `Target protocol ${route.targetProtocol} not supported for gRPC source`,
        } as grpc.ServiceError);
        return;
      }

      const payload = call.request;
      let isError = false;
      let errMsg: string | undefined;

      try {
        this.registry.incrementActive(route.ruleId, Protocol.GRPC);
        const response = await this.grpcPool.makeUnaryCall(
          route.backendAddress,
          route.serviceName,
          route.methodName,
          payload
        );

        const duration = Date.now() - startTime;
        console.log(`[gRPC] ${grpcPath} -> ${route.ruleId} ${duration}ms`);
        callback(null, response);
      } catch (err: any) {
        isError = true;
        errMsg = err.message;
        console.error(`[gRPC] Error handling ${grpcPath}:`, err.code, err.message);
        callback({
          code: err.code || grpc.status.INTERNAL,
          message: err.message,
          details: err.details,
        } as grpc.ServiceError);
      } finally {
        this.registry.decrementActive(route.ruleId, Protocol.GRPC);
        this.registry.recordRequest(route.ruleId, Protocol.GRPC, Date.now() - startTime, isError, errMsg);
      }
    };
  }

  private createServerStreamingHandler(
    methodName: string
  ): grpc.handleServerStreamingCall<any, any> {
    return (call: grpc.ServerWritableStream<any, any>) => {
      const startTime = Date.now();
      const serviceName = 'gateway.UserService';
      const grpcPath = `/${serviceName}/${methodName}`;
      const peer = call.getPeer();

      const route = this.router.findRouteByServiceMethod(serviceName, methodName);
      if (!route) {
        this.registry.recordRequest('__unrouted__', Protocol.GRPC, Date.now() - startTime, true, `No route for ${grpcPath}`);
        call.emit('error', {
          code: grpc.status.UNIMPLEMENTED,
          message: `Method ${grpcPath} not routed`,
        });
        return;
      }

      const connResult = this.connectionManager.register(
        Protocol.GRPC,
        peer,
        (data: any) => {
          try { call.write(data); } catch {}
        },
        () => {
          try { call.end(); } catch {}
        },
        route
      );

      if (connResult instanceof Error) {
        this.registry.recordRequest(route.ruleId, Protocol.GRPC, Date.now() - startTime, true, connResult.message);
        call.emit('error', {
          code: grpc.status.RESOURCE_EXHAUSTED,
          message: connResult.message,
        });
        return;
      }

      const connectionId = connResult.id;
      this.registry.incrementActive(route.ruleId, Protocol.GRPC);

      let completed = false;
      let isError = false;
      let errMsg: string | undefined;

      const finalize = () => {
        if (completed) return;
        completed = true;
        this.registry.decrementActive(route.ruleId, Protocol.GRPC);
        this.connectionManager.unregister(connectionId);
        this.registry.recordRequest(route.ruleId, Protocol.GRPC, Date.now() - startTime, isError, errMsg);
      };

      try {
        console.log(`[gRPC] Stream started: ${grpcPath} for rule=${route.ruleId}`);

        const backendStream = this.grpcPool.makeServerStreamingCall(
          route.backendAddress,
          route.serviceName,
          route.methodName,
          call.request
        );

        backendStream.on('data', (chunk: any) => {
          try {
            call.write(chunk);
            this.connectionManager.updateActivity(connectionId);
          } catch (writeErr: any) {
            isError = true;
            errMsg = `Write error: ${writeErr.message}`;
            backendStream.cancel();
          }
        });

        backendStream.on('end', () => {
          if (!completed) {
            const duration = Date.now() - startTime;
            console.log(`[gRPC] Stream completed: ${grpcPath}, duration=${duration}ms`);
            try { call.end(); } catch {}
            finalize();
          }
        });

        backendStream.on('error', (err: any) => {
          if (!completed) {
            isError = true;
            errMsg = err.message;
            console.error(`[gRPC] Stream error ${grpcPath}: code=${err.code} msg=${err.message}`);
            try {
              call.emit('error', {
                code: err.code || grpc.status.INTERNAL,
                message: err.message,
                details: err.details,
              });
            } catch {}
            finalize();
          }
        });

        call.on('cancelled', () => {
          if (!completed) {
            console.log(`[gRPC] Stream cancelled: ${grpcPath}`);
            try { backendStream.cancel(); } catch {}
            finalize();
          }
        });
      } catch (err: any) {
        isError = true;
        errMsg = err.message;
        console.error(`[gRPC] Failed to start stream ${grpcPath}:`, err.message);
        try {
          call.emit('error', {
            code: grpc.status.INTERNAL,
            message: err.message,
          });
        } catch {}
        finalize();
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
        (err, boundPort) => {
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
