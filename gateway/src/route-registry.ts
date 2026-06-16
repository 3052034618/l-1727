import { EventEmitter } from 'events';
import { v4 as uuidv4 } from 'uuid';
import {
  BusinessRule,
  Route,
  RouteMetrics,
  RuleCreateInput,
  RuleUpdateInput,
  Protocol,
  HttpEndpoint,
  WsEndpoint,
  GrpcEndpoint,
  ProtocolEndpoint,
  HttpMethod,
} from './types';

interface CompiledRoute {
  route: Route;
  regex: RegExp;
  paramNames: string[];
  metricKey: string;
}

interface MetricsStore {
  [key: string]: RouteMetrics & { latencySamples: number[] };
}

type RegistryEvents = {
  'routes:changed': (rules: BusinessRule[]) => void;
  'rule:added': (rule: BusinessRule) => void;
  'rule:updated': (rule: BusinessRule, prevRule: BusinessRule) => void;
  'rule:deleted': (ruleId: string) => void;
  'metrics:updated': (metrics: RouteMetrics[]) => void;
};

declare interface RouteRegistry {
  on<U extends keyof RegistryEvents>(event: U, listener: RegistryEvents[U]): this;
  emit<U extends keyof RegistryEvents>(event: U, ...args: Parameters<RegistryEvents[U]>): boolean;
}

class RouteRegistry extends EventEmitter {
  private rules: Map<string, BusinessRule> = new Map();
  private compiledRoutes: CompiledRoute[] = [];
  private metrics: MetricsStore = {};
  private readonly P99_SAMPLE_SIZE = 1000;

  constructor() {
    super();
  }

  loadRules(rules: BusinessRule[]): void {
    this.rules.clear();
    for (const rule of rules) {
      this.rules.set(rule.id, rule);
    }
    this.recompileRoutes();
    this.emit('routes:changed', Array.from(this.rules.values()));
  }

  addRule(input: RuleCreateInput): BusinessRule {
    const id = input.id || uuidv4();
    if (this.rules.has(id)) {
      throw new Error(`Rule with id '${id}' already exists`);
    }

    this.validateRuleInput(input);

    const now = new Date().toISOString();
    const rule: BusinessRule = {
      id,
      name: input.name,
      description: input.description,
      enabled: input.enabled !== false,
      createdAt: now,
      updatedAt: now,
      target: { ...input.target },
      endpoints: [...input.endpoints],
    };

    this.rules.set(id, rule);
    this.recompileRoutes();
    this.emit('rule:added', rule);
    this.emit('routes:changed', Array.from(this.rules.values()));
    return rule;
  }

  updateRule(id: string, input: RuleUpdateInput): BusinessRule {
    const existing = this.rules.get(id);
    if (!existing) {
      throw new Error(`Rule with id '${id}' not found`);
    }

    const updated: BusinessRule = {
      ...existing,
      updatedAt: new Date().toISOString(),
    };

    if (input.name !== undefined) updated.name = input.name;
    if (input.description !== undefined) updated.description = input.description;
    if (input.enabled !== undefined) updated.enabled = input.enabled;
    if (input.target !== undefined) {
      updated.target = { ...input.target };
    }
    if (input.endpoints !== undefined) {
      this.validateRuleInput({ ...(updated as unknown as RuleCreateInput), endpoints: input.endpoints });
      updated.endpoints = [...input.endpoints];
    }

    this.rules.set(id, updated);
    this.recompileRoutes();
    this.emit('rule:updated', updated, existing);
    this.emit('routes:changed', Array.from(this.rules.values()));
    return updated;
  }

  deleteRule(id: string): boolean {
    const existing = this.rules.get(id);
    if (!existing) return false;

    this.rules.delete(id);
    this.recompileRoutes();
    this.emit('rule:deleted', id);
    this.emit('routes:changed', Array.from(this.rules.values()));
    return true;
  }

  getRule(id: string): BusinessRule | undefined {
    return this.rules.get(id);
  }

  listRules(): BusinessRule[] {
    return Array.from(this.rules.values());
  }

  setRuleEnabled(id: string, enabled: boolean): BusinessRule {
    return this.updateRule(id, { enabled });
  }

  recordRequest(ruleId: string, protocol: Protocol, latencyMs: number, isError: boolean, errorMsg?: string): void {
    const key = this.metricKey(ruleId, protocol);
    if (!this.metrics[key]) {
      const rule = this.rules.get(ruleId);
      this.metrics[key] = {
        ruleId,
        ruleName: rule?.name || ruleId,
        protocol,
        requests: 0,
        errors: 0,
        totalLatencyMs: 0,
        activeConnections: 0,
        latencySamples: [],
      };
    }

    const m = this.metrics[key];
    m.requests++;
    m.totalLatencyMs += latencyMs;
    m.lastRequestAt = new Date().toISOString();

    if (isError) {
      m.errors++;
      m.lastErrorAt = m.lastRequestAt;
      m.lastErrorMessage = errorMsg;
    }

    m.latencySamples.push(latencyMs);
    if (m.latencySamples.length > this.P99_SAMPLE_SIZE) {
      m.latencySamples.shift();
    }
  }

  incrementActive(ruleId: string, protocol: Protocol): void {
    const key = this.metricKey(ruleId, protocol);
    if (!this.metrics[key]) {
      const rule = this.rules.get(ruleId);
      this.metrics[key] = {
        ruleId,
        ruleName: rule?.name || ruleId,
        protocol,
        requests: 0,
        errors: 0,
        totalLatencyMs: 0,
        activeConnections: 0,
        latencySamples: [],
      };
    }
    this.metrics[key].activeConnections++;
  }

  decrementActive(ruleId: string, protocol: Protocol): void {
    const key = this.metricKey(ruleId, protocol);
    if (this.metrics[key] && this.metrics[key].activeConnections > 0) {
      this.metrics[key].activeConnections--;
    }
  }

  getMetrics(): RouteMetrics[] {
    return Object.values(this.metrics).map((m) => {
      const samples = [...m.latencySamples].sort((a, b) => a - b);
      let p99: number | undefined;
      if (samples.length > 0) {
        const idx = Math.ceil(samples.length * 0.99) - 1;
        p99 = samples[Math.max(0, idx)];
      }
      const { latencySamples, ...rest } = m;
      return { ...rest, p99LatencyMs: p99 };
    });
  }

  getMetricsByRuleId(ruleId: string): RouteMetrics[] {
    return this.getMetrics().filter((m) => m.ruleId === ruleId);
  }

  getAggregatedMetricsByRule() {
    const byRule: Record<string, {
      ruleId: string;
      ruleName: string;
      totalRequests: number;
      totalErrors: number;
      avgLatencyMs: number;
      protocols: Record<string, RouteMetrics>;
      endpoints: Array<{ protocol: string; pattern?: string; service?: string }>;
      enabled: boolean;
    }> = {};

    for (const rule of this.rules.values()) {
      byRule[rule.id] = {
        ruleId: rule.id,
        ruleName: rule.name,
        totalRequests: 0,
        totalErrors: 0,
        avgLatencyMs: 0,
        protocols: {},
        endpoints: rule.endpoints.map((ep) => {
          if (ep.protocol === Protocol.HTTP || ep.protocol === Protocol.WEBSOCKET) {
            return { protocol: ep.protocol, pattern: ep.pattern };
          } else {
            return { protocol: ep.protocol, service: `${(ep as GrpcEndpoint).serviceName}/${(ep as GrpcEndpoint).methodName}` };
          }
        }),
        enabled: rule.enabled,
      };
    }

    let totalLatency = 0;
    let totalCount = 0;

    for (const m of this.getMetrics()) {
      const r = byRule[m.ruleId];
      if (r) {
        r.totalRequests += m.requests;
        r.totalErrors += m.errors;
        r.protocols[m.protocol] = m;
        totalLatency += m.totalLatencyMs;
        totalCount += m.requests;
      }
    }

    for (const r of Object.values(byRule)) {
      if (r.totalRequests > 0) {
        let sum = 0;
        let cnt = 0;
        for (const m of Object.values(r.protocols)) {
          sum += m.totalLatencyMs;
          cnt += m.requests;
        }
        r.avgLatencyMs = cnt > 0 ? Math.round(sum / cnt) : 0;
      }
    }

    return Object.values(byRule);
  }

  matchRoute(
    protocol: Protocol,
    method: HttpMethod,
    path: string,
    paramsOut: { params: Record<string, string> },
    ruleIdOut: { id?: string }
  ): Route | null {
    for (const compiled of this.compiledRoutes) {
      const { route, regex, paramNames } = compiled;

      if (route.sourceProtocol !== protocol) continue;

      if (protocol === Protocol.HTTP || protocol === Protocol.WEBSOCKET) {
        if (route.methods.length > 0 && !route.methods.includes(method)) continue;
        const match = path.match(regex);
        if (!match) continue;
        const params: Record<string, string> = {};
        for (let i = 0; i < paramNames.length; i++) {
          params[paramNames[i]] = match[i + 1];
        }
        paramsOut.params = params;
        ruleIdOut.id = route.ruleId;
        return route;
      }

      if (protocol === Protocol.GRPC) {
        const grpcPath = `/${route.serviceName}/${route.methodName}`;
        if (path === grpcPath) {
          ruleIdOut.id = route.ruleId;
          return route;
        }
      }
    }
    return null;
  }

  findRouteByServiceMethod(serviceName: string, methodName: string): Route | null {
    for (const compiled of this.compiledRoutes) {
      if (
        compiled.route.sourceProtocol === Protocol.GRPC &&
        compiled.route.serviceName === serviceName &&
        compiled.route.methodName === methodName
      ) {
        return compiled.route;
      }
    }
    return null;
  }

  getCompiledRoutes(): Route[] {
    return this.compiledRoutes.map((c) => c.route);
  }

  resetMetrics(): void {
    this.metrics = {};
  }

  private metricKey(ruleId: string, protocol: Protocol): string {
    return `${ruleId}::${protocol}`;
  }

  private recompileRoutes(): void {
    const routes: Route[] = [];

    for (const rule of this.rules.values()) {
      if (!rule.enabled) continue;

      for (const ep of rule.endpoints) {
        const base: Partial<Route> = {
          ruleId: rule.id,
          ruleName: rule.name,
          targetProtocol: rule.target.protocol,
          backendAddress: rule.target.backendAddress,
          serviceName: rule.target.serviceName,
          methodName: rule.target.methodName,
          isServerStreaming: rule.target.isServerStreaming,
        };

        if (ep.protocol === Protocol.HTTP) {
          routes.push({
            ...base,
            pattern: ep.pattern,
            methods: ep.methods,
            sourceProtocol: Protocol.HTTP,
            stripPrefix: ep.stripPrefix || '',
            pathToFieldMap: ep.pathToFieldMap || {},
          } as Route);
        } else if (ep.protocol === Protocol.WEBSOCKET) {
          routes.push({
            ...base,
            pattern: ep.pattern,
            methods: [HttpMethod.GET],
            sourceProtocol: Protocol.WEBSOCKET,
            stripPrefix: ep.stripPrefix || '',
            pathToFieldMap: ep.pathToFieldMap || {},
            topicQueryParam: ep.topicQueryParam || 'topic',
          } as Route);
        } else if (ep.protocol === Protocol.GRPC) {
          routes.push({
            ...base,
            pattern: `/${ep.serviceName}/${ep.methodName}`,
            methods: [],
            sourceProtocol: Protocol.GRPC,
            stripPrefix: '',
            pathToFieldMap: {},
          } as Route);
        }
      }
    }

    const compiled: CompiledRoute[] = [];
    for (const route of routes) {
      const { regex, paramNames } = this.compilePattern(route.pattern);
      compiled.push({
        route,
        regex,
        paramNames,
        metricKey: this.metricKey(route.ruleId, route.sourceProtocol),
      });
    }

    this.compiledRoutes = compiled;
  }

  private compilePattern(pattern: string): { regex: RegExp; paramNames: string[] } {
    const paramNames: string[] = [];
    let regexStr = '^';

    const parts = pattern.split('/');
    for (const part of parts) {
      if (!part) continue;
      regexStr += '\\/';
      if (part.startsWith(':')) {
        const name = part.slice(1);
        paramNames.push(name);
        regexStr += '([^\\/]+)';
      } else {
        regexStr += part.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      }
    }

    regexStr += '$';
    return { regex: new RegExp(regexStr), paramNames };
  }

  private validateRuleInput(input: RuleCreateInput): void {
    if (!input.name || input.name.trim().length === 0) {
      throw new Error('Rule name is required');
    }
    if (!input.target) {
      throw new Error('Rule target is required');
    }
    if (!input.target.serviceName || !input.target.methodName) {
      throw new Error('Target serviceName and methodName are required');
    }
    if (!input.target.backendAddress) {
      throw new Error('Target backendAddress is required');
    }
    if (!Array.isArray(input.endpoints) || input.endpoints.length === 0) {
      throw new Error('At least one endpoint is required');
    }
    for (const ep of input.endpoints) {
      if (ep.protocol === Protocol.HTTP) {
        const h = ep as HttpEndpoint;
        if (!h.pattern || !h.methods || h.methods.length === 0) {
          throw new Error('HTTP endpoint requires pattern and methods');
        }
      } else if (ep.protocol === Protocol.WEBSOCKET) {
        const w = ep as WsEndpoint;
        if (!w.pattern) {
          throw new Error('WebSocket endpoint requires pattern');
        }
      } else if (ep.protocol === Protocol.GRPC) {
        const g = ep as GrpcEndpoint;
        if (!g.serviceName || !g.methodName) {
          throw new Error('gRPC endpoint requires serviceName and methodName');
        }
      } else {
        throw new Error(`Unknown endpoint protocol: ${(ep as any).protocol}`);
      }
    }
  }
}

export { RouteRegistry };
