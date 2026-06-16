import { Protocol, HttpMethod, BusinessRule } from './types';

export interface GatewayConfig {
  httpPort: number;
  grpcPort: number;
  wsHeartbeatIntervalMs: number;
  wsMaxConnections: number;
  httpMaxConcurrentStreams: number;
  grpcBackendAddress: string;
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
    httpMaxConcurrentStreams: 500,
    grpcBackendAddress: 'localhost:50051',
    protoPath,
    grpcKeepaliveTimeMs: 30000,
    grpcKeepaliveTimeoutMs: 10000,
  };
}

export function createDefaultBusinessRules(): BusinessRule[] {
  const now = new Date().toISOString();
  return [
    {
      id: 'rule-get-user',
      name: 'GetUser - 获取用户详情',
      description: '根据用户ID获取用户详情，支持 HTTP、gRPC 两种入口',
      enabled: true,
      createdAt: now,
      updatedAt: now,
      target: {
        protocol: Protocol.GRPC,
        backendAddress: 'localhost:50051',
        serviceName: 'gateway.UserService',
        methodName: 'GetUser',
        isServerStreaming: false,
      },
      endpoints: [
        {
          protocol: Protocol.HTTP,
          pattern: '/api/v1/users/:id',
          methods: [HttpMethod.GET],
          stripPrefix: '/api/v1',
          pathToFieldMap: { ':id': 'id' },
        },
        {
          protocol: Protocol.GRPC,
          serviceName: 'gateway.UserService',
          methodName: 'GetUser',
        },
      ],
    },
    {
      id: 'rule-create-user',
      name: 'CreateUser - 创建用户',
      description: '通过 HTTP 或 gRPC 创建新用户',
      enabled: true,
      createdAt: now,
      updatedAt: now,
      target: {
        protocol: Protocol.GRPC,
        backendAddress: 'localhost:50051',
        serviceName: 'gateway.UserService',
        methodName: 'CreateUser',
        isServerStreaming: false,
      },
      endpoints: [
        {
          protocol: Protocol.HTTP,
          pattern: '/api/v1/users',
          methods: [HttpMethod.POST],
          stripPrefix: '/api/v1',
        },
        {
          protocol: Protocol.GRPC,
          serviceName: 'gateway.UserService',
          methodName: 'CreateUser',
        },
      ],
    },
    {
      id: 'rule-list-users',
      name: 'ListUsers - 用户列表流式',
      description: '以流式方式返回所有用户。HTTP 入口返回 NDJSON，gRPC 原生流',
      enabled: true,
      createdAt: now,
      updatedAt: now,
      target: {
        protocol: Protocol.GRPC,
        backendAddress: 'localhost:50051',
        serviceName: 'gateway.UserService',
        methodName: 'ListUsers',
        isServerStreaming: true,
      },
      endpoints: [
        {
          protocol: Protocol.HTTP,
          pattern: '/api/v1/users',
          methods: [HttpMethod.GET],
          stripPrefix: '/api/v1',
        },
        {
          protocol: Protocol.GRPC,
          serviceName: 'gateway.UserService',
          methodName: 'ListUsers',
        },
      ],
    },
    {
      id: 'rule-stream-events',
      name: 'StreamEvents - 事件流推送',
      description: '按主题订阅事件。WebSocket 入口提供长连接推送，gRPC 入口原生流式',
      enabled: true,
      createdAt: now,
      updatedAt: now,
      target: {
        protocol: Protocol.GRPC,
        backendAddress: 'localhost:50051',
        serviceName: 'gateway.UserService',
        methodName: 'StreamEvents',
        isServerStreaming: true,
      },
      endpoints: [
        {
          protocol: Protocol.WEBSOCKET,
          pattern: '/ws/events',
          stripPrefix: '/ws',
          topicQueryParam: 'topic',
        },
        {
          protocol: Protocol.GRPC,
          serviceName: 'gateway.UserService',
          methodName: 'StreamEvents',
        },
      ],
    },
  ];
}
