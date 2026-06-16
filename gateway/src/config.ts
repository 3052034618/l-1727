import { Protocol, HttpMethod, Route } from './types';

export interface GatewayConfig {
  httpPort: number;
  grpcPort: number;
  wsHeartbeatIntervalMs: number;
  wsMaxConnections: number;
  grpcBackendAddress: string;
  routes: Route[];
  protoPath: string;
  grpcKeepaliveTimeMs: number;
  grpcKeepaliveTimeoutMs: number;
}

export function createDefaultConfig(protoPath: string): GatewayConfig {
  return {
    httpPort: 8080,
    grpcPort: 9090,
    wsHeartbeatIntervalMs: 30000,
    wsMaxConnections: 1000,
    grpcBackendAddress: 'localhost:50051',
    protoPath,
    grpcKeepaliveTimeMs: 30000,
    grpcKeepaliveTimeoutMs: 10000,
    routes: buildDefaultRoutes(),
  };
}

function buildDefaultRoutes(): Route[] {
  return [
    {
      pattern: '/api/v1/users/:id',
      methods: [HttpMethod.GET],
      sourceProtocol: Protocol.HTTP,
      targetProtocol: Protocol.GRPC,
      backendAddress: 'localhost:50051',
      serviceName: 'gateway.UserService',
      methodName: 'GetUser',
      stripPrefix: '/api/v1',
      pathToFieldMap: { ':id': 'id' },
      isServerStreaming: false,
    },
    {
      pattern: '/api/v1/users',
      methods: [HttpMethod.POST],
      sourceProtocol: Protocol.HTTP,
      targetProtocol: Protocol.GRPC,
      backendAddress: 'localhost:50051',
      serviceName: 'gateway.UserService',
      methodName: 'CreateUser',
      stripPrefix: '/api/v1',
      pathToFieldMap: {},
      isServerStreaming: false,
    },
    {
      pattern: '/api/v1/users',
      methods: [HttpMethod.GET],
      sourceProtocol: Protocol.HTTP,
      targetProtocol: Protocol.GRPC,
      backendAddress: 'localhost:50051',
      serviceName: 'gateway.UserService',
      methodName: 'ListUsers',
      stripPrefix: '/api/v1',
      pathToFieldMap: {},
      isServerStreaming: true,
    },
    {
      pattern: '/ws/events',
      methods: [HttpMethod.GET],
      sourceProtocol: Protocol.WEBSOCKET,
      targetProtocol: Protocol.GRPC,
      backendAddress: 'localhost:50051',
      serviceName: 'gateway.UserService',
      methodName: 'StreamEvents',
      stripPrefix: '/ws',
      pathToFieldMap: {},
      isServerStreaming: true,
    },
    {
      pattern: '/gateway.UserService/GetUser',
      methods: [],
      sourceProtocol: Protocol.GRPC,
      targetProtocol: Protocol.GRPC,
      backendAddress: 'localhost:50051',
      serviceName: 'gateway.UserService',
      methodName: 'GetUser',
      stripPrefix: '',
      pathToFieldMap: {},
      isServerStreaming: false,
    },
    {
      pattern: '/gateway.UserService/ListUsers',
      methods: [],
      sourceProtocol: Protocol.GRPC,
      targetProtocol: Protocol.GRPC,
      backendAddress: 'localhost:50051',
      serviceName: 'gateway.UserService',
      methodName: 'ListUsers',
      stripPrefix: '',
      pathToFieldMap: {},
      isServerStreaming: true,
    },
    {
      pattern: '/gateway.UserService/StreamEvents',
      methods: [],
      sourceProtocol: Protocol.GRPC,
      targetProtocol: Protocol.GRPC,
      backendAddress: 'localhost:50051',
      serviceName: 'gateway.UserService',
      methodName: 'StreamEvents',
      stripPrefix: '',
      pathToFieldMap: {},
      isServerStreaming: true,
    },
    {
      pattern: '/gateway.UserService/CreateUser',
      methods: [],
      sourceProtocol: Protocol.GRPC,
      targetProtocol: Protocol.GRPC,
      backendAddress: 'localhost:50051',
      serviceName: 'gateway.UserService',
      methodName: 'CreateUser',
      stripPrefix: '',
      pathToFieldMap: {},
      isServerStreaming: false,
    },
  ];
}
