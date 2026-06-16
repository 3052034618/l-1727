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

export interface HttpEndpoint {
  protocol: Protocol.HTTP;
  pattern: string;
  methods: HttpMethod[];
  stripPrefix?: string;
  pathToFieldMap?: Record<string, string>;
}

export interface WsEndpoint {
  protocol: Protocol.WEBSOCKET;
  pattern: string;
  stripPrefix?: string;
  pathToFieldMap?: Record<string, string>;
  topicQueryParam?: string;
}

export interface GrpcEndpoint {
  protocol: Protocol.GRPC;
  serviceName: string;
  methodName: string;
}

export type ProtocolEndpoint = HttpEndpoint | WsEndpoint | GrpcEndpoint;

export interface Route {
  ruleId: string;
  ruleName: string;
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
  topicQueryParam?: string;
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
  matchedRuleId?: string;
  grayTargetName?: string;
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
  ruleId?: string;
  send: (data: any) => void;
  close: () => void;
}

export interface RouteMetrics {
  ruleId: string;
  ruleName: string;
  protocol: Protocol;
  requests: number;
  errors: number;
  totalLatencyMs: number;
  activeConnections: number;
  p99LatencyMs?: number;
  lastRequestAt?: string;
  lastErrorAt?: string;
  lastErrorMessage?: string;
}

export interface RuleCreateInput {
  id?: string;
  name: string;
  description?: string;
  enabled?: boolean;
  target: {
    protocol: Protocol;
    backendAddress: string;
    serviceName: string;
    methodName: string;
    isServerStreaming: boolean;
  };
  endpoints: ProtocolEndpoint[];
}

export interface RuleUpdateInput {
  name?: string;
  description?: string;
  enabled?: boolean;
  target?: {
    protocol: Protocol;
    backendAddress: string;
    serviceName: string;
    methodName: string;
    isServerStreaming: boolean;
  };
  endpoints?: ProtocolEndpoint[];
}

export interface GrayTarget {
  name: string;
  weight: number;
  backendAddress: string;
  serviceName?: string;
  methodName?: string;
}

export interface BusinessRule {
  id: string;
  name: string;
  description?: string;
  enabled: boolean;
  createdAt: string;
  updatedAt: string;
  version: string;
  target: {
    protocol: Protocol;
    backendAddress: string;
    serviceName: string;
    methodName: string;
    isServerStreaming: boolean;
  };
  grayTargets?: GrayTarget[];
  endpoints: ProtocolEndpoint[];
}

export interface RuleVersion {
  id: string;
  ruleId: string;
  version: string;
  ruleSnapshot: BusinessRule;
  createdAt: string;
  createdBy: string;
  note?: string;
}

export enum AuditAction {
  RULE_CREATED = 'rule.created',
  RULE_UPDATED = 'rule.updated',
  RULE_DELETED = 'rule.deleted',
  RULE_ENABLED = 'rule.enabled',
  RULE_DISABLED = 'rule.disabled',
  RULE_ROLLED_BACK = 'rule.rolled_back',
  VERSION_CREATED = 'version.created',
  GRAY_UPDATED = 'gray.updated',
}

export interface AuditLogEntry {
  id: string;
  timestamp: string;
  action: AuditAction;
  actor: string;
  ruleId: string;
  ruleName: string;
  before?: any;
  after?: any;
  metadata?: Record<string, any>;
  ip?: string;
}

export interface AdminConfig {
  enabled: boolean;
  tokens: string[];
  tokenHeader: string;
  auditLogMaxEntries: number;
}
