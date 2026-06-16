export enum Protocol {
  HTTP = 'http',
  WEBSOCKET = 'websocket',
  GRPC = 'grpc',
}

export enum HttpMethod {
  GET = 'GET',
  POST = 'POST',
  PUT = 'PUT',
  DELETE = 'DELETE',
  PATCH = 'PATCH',
}

export interface Route {
  pattern: string;
  methods: HttpMethod[];
  sourceProtocol: Protocol;
  targetProtocol: Protocol;
  backendAddress: string;
  serviceName: string;
  methodName: string;
  stripPrefix: string;
  pathToFieldMap: Record<string, string>;
  isServerStreaming: boolean;
}

export interface PathParams {
  [key: string]: string;
}

export interface RequestContext {
  protocol: Protocol;
  method: HttpMethod;
  path: string;
  headers: Record<string, string>;
  params: PathParams;
  query: Record<string, string>;
  body: any;
  rawBody: Buffer | null;
}

export interface BackendResponse {
  statusCode: number;
  headers: Record<string, string>;
  body: any;
  stream?: AsyncIterable<any>;
  metadata?: Record<string, string>;
}

export interface GrpcCallConfig {
  address: string;
  serviceName: string;
  methodName: string;
  requestPayload: any;
  isServerStreaming: boolean;
}

export type ConnectionId = string;

export interface ManagedConnection {
  id: ConnectionId;
  protocol: Protocol;
  remoteAddress: string;
  connectedAt: Date;
  lastActivity: Date;
  route: Route | null;
  send: (data: any) => void;
  close: () => void;
}
