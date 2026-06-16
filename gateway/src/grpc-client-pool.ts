import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { ServiceClientConstructor } from '@grpc/grpc-js/build/src/make-client';

interface CachedClient {
  client: any;
  cachedAt: number;
}

export class GrpcClientPool {
  private clients: Map<string, CachedClient> = new Map();
  private packageDefinition: protoLoader.PackageDefinition | null = null;
  private protoDescriptor: any = null;
  private serviceConstructors: Map<string, ServiceClientConstructor> = new Map();
  private idleTimeoutMs: number = 300000;

  async initialize(protoPath: string): Promise<void> {
    this.packageDefinition = await protoLoader.load(protoPath, {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
    });

    this.protoDescriptor = grpc.loadPackageDefinition(this.packageDefinition);

    this.extractServiceConstructors(this.protoDescriptor, '');

    this.startIdleSweeper();
  }

  private extractServiceConstructors(pkg: any, prefix: string): void {
    for (const key of Object.keys(pkg)) {
      const value = pkg[key];
      const fullName = prefix ? `${prefix}.${key}` : key;

      if (value && (typeof value === 'object' || typeof value === 'function') && value.service) {
        this.serviceConstructors.set(fullName, value as ServiceClientConstructor);
      }

      if (value && typeof value === 'object' && !value.service) {
        this.extractServiceConstructors(value, fullName);
      }
    }
  }

  getClient(address: string, serviceName: string): any {
    const cacheKey = `${address}::${serviceName}`;
    const cached = this.clients.get(cacheKey);

    if (cached) {
      cached.cachedAt = Date.now();
      return cached.client;
    }

    const ServiceConstructor = this.serviceConstructors.get(serviceName);
    if (!ServiceConstructor) {
      throw new Error(`gRPC service not found: ${serviceName}. Available: ${Array.from(this.serviceConstructors.keys()).join(', ')}`);
    }

    const client = new ServiceConstructor(
      address,
      grpc.credentials.createInsecure(),
      {
        'grpc.keepalive_time_ms': 30000,
        'grpc.keepalive_timeout_ms': 10000,
        'grpc.keepalive_permit_without_calls': 1,
      }
    );

    this.clients.set(cacheKey, { client, cachedAt: Date.now() });
    return client;
  }

  async makeUnaryCall(
    address: string,
    serviceName: string,
    methodName: string,
    payload: any
  ): Promise<any> {
    const client = this.getClient(address, serviceName);

    return new Promise((resolve, reject) => {
      if (typeof client[methodName] !== 'function') {
        reject(new Error(`Method ${methodName} not found on service ${serviceName}`));
        return;
      }

      client[methodName](payload, (err: grpc.ServiceError | null, response: any) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(response);
      });
    });
  }

  makeServerStreamingCall(
    address: string,
    serviceName: string,
    methodName: string,
    payload: any
  ): grpc.ClientReadableStream<any> {
    const client = this.getClient(address, serviceName);

    if (typeof client[methodName] !== 'function') {
      throw new Error(`Method ${methodName} not found on service ${serviceName}`);
    }

    return client[methodName](payload);
  }

  getServiceDefinition(serviceName: string): any {
    const ServiceConstructor = this.serviceConstructors.get(serviceName);
    if (!ServiceConstructor) return null;
    return (ServiceConstructor as any).service;
  }

  private startIdleSweeper(): void {
    setInterval(() => {
      const now = Date.now();
      for (const [key, cached] of this.clients.entries()) {
        if (now - cached.cachedAt > this.idleTimeoutMs) {
          try {
            (cached.client as grpc.Client).close();
          } catch {}
          this.clients.delete(key);
        }
      }
    }, 60000);
  }

  closeAll(): void {
    for (const [, cached] of this.clients) {
      try {
        (cached.client as grpc.Client).close();
      } catch {}
    }
    this.clients.clear();
  }
}
