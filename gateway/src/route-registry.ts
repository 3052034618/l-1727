import { EventEmitter } from 'events';
import { v4 as uuidv4 } from 'uuid';
import {
  BusinessRule,
  Route,
  RouteMetrics,
  RuleCreateInput,
  RuleUpdateInput,
  RuleVersion,
  GrayTarget,
  AuditLogEntry,
  AuditAction,
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
}

interface MetricsStore {
  [key: string]: RouteMetrics & { latencySamples: number[] };
}

interface GrayPickState {
  [key: string]: { currentIndex: number; currentWeight: number };
}

type RegistryEvents = {
  'routes:changed': (rules: BusinessRule[]) => void;
  'rule:added': (rule: BusinessRule) => void;
  'rule:updated': (rule: BusinessRule, prevRule: BusinessRule) => void;
  'rule:deleted': (ruleId: string) => void;
  'rule:rolled-back': (rule: BusinessRule, version: RuleVersion) => void;
  'version:created': (version: RuleVersion) => void;
  'audit:entry': (entry: AuditLogEntry) => void;
  'metrics:updated': (metrics: RouteMetrics[]) => void;
};

declare interface RouteRegistry {
  on<U extends keyof RegistryEvents>(event: U, listener: RegistryEvents[U]): this;
  emit<U extends keyof RegistryEvents>(event: U, ...args: Parameters<RegistryEvents[U]>): boolean;
}

const DEFAULT_TARGET_NAME = 'default';

class RouteRegistry extends EventEmitter {
  private rules: Map<string, BusinessRule> = new Map();
  private compiledRoutes: CompiledRoute[] = [];
  private metrics: MetricsStore = {};
  private versions: Map<string, RuleVersion[]> = new Map();
  private auditLog: AuditLogEntry[] = [];
  private auditLogMaxEntries = 1000;
  private grayPickState: GrayPickState = {};
  private readonly P99_SAMPLE_SIZE = 1000;

  constructor() {
    super();
  }

  setAuditLogMaxEntries(max: number): void {
    this.auditLogMaxEntries = max;
  }

  loadRules(rules: BusinessRule[]): void {
    this.rules.clear();
    for (const rule of rules) {
      rule.version = rule.version || 'v1';
      this.rules.set(rule.id, rule);
    }
    this.recompileRoutes();
    this.emit('routes:changed', Array.from(this.rules.values()));
  }

  addRule(input: RuleCreateInput, actor = 'system', ip?: string): BusinessRule {
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
      version: 'v1',
      createdAt: now,
      updatedAt: now,
      target: { ...input.target },
      grayTargets: [],
      endpoints: [...input.endpoints],
    };

    this.rules.set(id, rule);
    this.recompileRoutes();

    this.addAuditLog({
      action: AuditAction.RULE_CREATED,
      actor,
      ruleId: rule.id,
      ruleName: rule.name,
      after: { ...rule },
      ip,
    });

    this.emit('rule:added', rule);
    this.emit('routes:changed', Array.from(this.rules.values()));
    return rule;
  }

  updateRule(id: string, input: RuleUpdateInput, actor = 'system', ip?: string): BusinessRule {
    const existing = this.rules.get(id);
    if (!existing) {
      throw new Error(`Rule with id '${id}' not found`);
    }

    const updated: BusinessRule = {
      ...existing,
      updatedAt: new Date().toISOString(),
      version: this.bumpVersion(existing.version),
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

    this.addAuditLog({
      action: AuditAction.RULE_UPDATED,
      actor,
      ruleId: updated.id,
      ruleName: updated.name,
      before: { ...existing },
      after: { ...updated },
      ip,
    });

    this.emit('rule:updated', updated, existing);
    this.emit('routes:changed', Array.from(this.rules.values()));
    return updated;
  }

  deleteRule(id: string, actor = 'system', ip?: string): boolean {
    const existing = this.rules.get(id);
    if (!existing) return false;

    this.rules.delete(id);
    this.versions.delete(id);
    this.recompileRoutes();

    this.addAuditLog({
      action: AuditAction.RULE_DELETED,
      actor,
      ruleId: id,
      ruleName: existing.name,
      before: { ...existing },
      ip,
    });

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

  setRuleEnabled(id: string, enabled: boolean, actor = 'system', ip?: string): BusinessRule {
    const result = this.updateRule(id, { enabled }, actor, ip);
    this.addAuditLog({
      action: enabled ? AuditAction.RULE_ENABLED : AuditAction.RULE_DISABLED,
      actor,
      ruleId: id,
      ruleName: result.name,
      after: { enabled },
      ip,
    });
    return result;
  }

  createVersion(ruleId: string, actor = 'system', note?: string, ip?: string): RuleVersion {
    const rule = this.rules.get(ruleId);
    if (!rule) {
      throw new Error(`Rule with id '${ruleId}' not found`);
    }

    const version: RuleVersion = {
      id: uuidv4(),
      ruleId,
      version: rule.version,
      ruleSnapshot: JSON.parse(JSON.stringify(rule)),
      createdAt: new Date().toISOString(),
      createdBy: actor,
      note,
    };

    if (!this.versions.has(ruleId)) {
      this.versions.set(ruleId, []);
    }
    this.versions.get(ruleId)!.push(version);

    this.addAuditLog({
      action: AuditAction.VERSION_CREATED,
      actor,
      ruleId,
      ruleName: rule.name,
      after: { version: version.version, versionId: version.id, note },
      ip,
    });

    this.emit('version:created', version);
    return version;
  }

  listVersions(ruleId: string): RuleVersion[] {
    return this.versions.get(ruleId) || [];
  }

  getVersion(ruleId: string, versionId: string): RuleVersion | undefined {
    return this.versions.get(ruleId)?.find((v) => v.id === versionId);
  }

  rollbackToVersion(ruleId: string, versionId: string, actor = 'system', ip?: string): BusinessRule {
    const version = this.getVersion(ruleId, versionId);
    if (!version) {
      throw new Error(`Version '${versionId}' not found for rule '${ruleId}'`);
    }

    const existing = this.rules.get(ruleId);
    if (!existing) {
      throw new Error(`Rule with id '${ruleId}' not found`);
    }

    const newVersion = this.bumpVersion(existing.version);
    const rolledBack: BusinessRule = {
      ...JSON.parse(JSON.stringify(version.ruleSnapshot)),
      id: ruleId,
      version: newVersion,
      createdAt: existing.createdAt,
      updatedAt: new Date().toISOString(),
    };

    this.rules.set(ruleId, rolledBack);
    this.recompileRoutes();

    this.addAuditLog({
      action: AuditAction.RULE_ROLLED_BACK,
      actor,
      ruleId,
      ruleName: rolledBack.name,
      before: { ...existing },
      after: { ...rolledBack },
      metadata: { fromVersion: version.version, toVersion: newVersion, versionId },
      ip,
    });

    this.emit('rule:rolled-back', rolledBack, version);
    this.emit('routes:changed', Array.from(this.rules.values()));
    return rolledBack;
  }

  updateGrayTargets(ruleId: string, grayTargets: GrayTarget[], actor = 'system', ip?: string): BusinessRule {
    const existing = this.rules.get(ruleId);
    if (!existing) {
      throw new Error(`Rule with id '${ruleId}' not found`);
    }

    const totalWeight = grayTargets.reduce((sum, g) => sum + g.weight, 0);
    if (totalWeight > 100) {
      throw new Error(`Total gray target weight (${totalWeight}) exceeds 100`);
    }
    if (grayTargets.some((g) => g.weight < 0)) {
      throw new Error('Gray target weight cannot be negative');
    }
    if (new Set(grayTargets.map((g) => g.name)).size !== grayTargets.length) {
      throw new Error('Gray target names must be unique');
    }

    const updated: BusinessRule = {
      ...existing,
      version: this.bumpVersion(existing.version),
      updatedAt: new Date().toISOString(),
      grayTargets: [...grayTargets],
    };

    this.rules.set(ruleId, updated);
    delete this.grayPickState[ruleId];
    this.recompileRoutes();

    this.addAuditLog({
      action: AuditAction.GRAY_UPDATED,
      actor,
      ruleId,
      ruleName: updated.name,
      before: { grayTargets: existing.grayTargets || [] },
      after: { grayTargets: updated.grayTargets || [] },
      ip,
    });

    this.emit('rule:updated', updated, existing);
    this.emit('routes:changed', Array.from(this.rules.values()));
    return updated;
  }

  pickGrayTarget(ruleId: string): string {
    const rule = this.rules.get(ruleId);
    if (!rule || !rule.grayTargets || rule.grayTargets.length === 0) {
      return DEFAULT_TARGET_NAME;
    }

    const state = this.grayPickState[ruleId] || { currentIndex: -1, currentWeight: 0 };
    const targets = rule.grayTargets;
    const defaultWeight = 100 - targets.reduce((sum, g) => sum + g.weight, 0);

    if (state.currentIndex < 0) {
      state.currentIndex = 0;
      state.currentWeight = targets[0]?.weight || defaultWeight;
    }

    const allTargets = [...targets, { name: DEFAULT_TARGET_NAME, weight: defaultWeight }];
    const idx = state.currentIndex % allTargets.length;
    state.currentWeight--;

    if (state.currentWeight <= 0) {
      state.currentIndex = (state.currentIndex + 1) % allTargets.length;
      const next = allTargets[state.currentIndex];
      state.currentWeight = Math.max(1, next.weight);
    }

    this.grayPickState[ruleId] = state;
    return allTargets[idx].name;
  }

  getTargetForRoute(route: Route, grayTargetName: string): { backendAddress: string; serviceName: string; methodName: string } {
    const rule = this.rules.get(route.ruleId);
    if (!rule) {
      return {
        backendAddress: route.backendAddress,
        serviceName: route.serviceName,
        methodName: route.methodName,
      };
    }

    if (grayTargetName === DEFAULT_TARGET_NAME || !rule.grayTargets || rule.grayTargets.length === 0) {
      return {
        backendAddress: rule.target.backendAddress,
        serviceName: rule.target.serviceName,
        methodName: rule.target.methodName,
      };
    }

    const gray = rule.grayTargets.find((g) => g.name === grayTargetName);
    if (!gray) {
      return {
        backendAddress: rule.target.backendAddress,
        serviceName: rule.target.serviceName,
        methodName: rule.target.methodName,
      };
    }

    return {
      backendAddress: gray.backendAddress,
      serviceName: gray.serviceName || rule.target.serviceName,
      methodName: gray.methodName || rule.target.methodName,
    };
  }

  recordRequest(ruleId: string, protocol: Protocol, latencyMs: number, isError: boolean, errorMsg?: string, targetName = DEFAULT_TARGET_NAME): void {
    const key = this.metricKey(ruleId, protocol, targetName);
    if (!this.metrics[key]) {
      const rule = this.rules.get(ruleId);
      this.metrics[key] = {
        ruleId,
        ruleName: `${rule?.name || ruleId}[${targetName}]`,
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

  recordErrorOnly(ruleId: string, protocol: Protocol, errorMsg: string, latencyMs = 0): void {
    this.recordRequest(ruleId, protocol, latencyMs, true, errorMsg);
  }

  incrementActive(ruleId: string, protocol: Protocol, targetName = DEFAULT_TARGET_NAME): void {
    const key = this.metricKey(ruleId, protocol, targetName);
    if (!this.metrics[key]) {
      const rule = this.rules.get(ruleId);
      this.metrics[key] = {
        ruleId,
        ruleName: `${rule?.name || ruleId}[${targetName}]`,
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

  decrementActive(ruleId: string, protocol: Protocol, targetName = DEFAULT_TARGET_NAME): void {
    const key = this.metricKey(ruleId, protocol, targetName);
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
      version: string;
      totalRequests: number;
      totalErrors: number;
      avgLatencyMs: number;
      byProtocol: Record<string, {
        requests: number;
        errors: number;
        avgLatencyMs: number;
        p99LatencyMs: number;
        activeConnections: number;
        lastRequestAt?: string;
        lastError?: { at: string; message: string } | null;
        byTarget: Record<string, {
          targetName: string;
          requests: number;
          errors: number;
          avgLatencyMs: number;
          p99LatencyMs: number;
          activeConnections: number;
        }>;
      }>;
      endpoints: Array<{ protocol: string; pattern?: string; service?: string }>;
      enabled: boolean;
      grayTargets: GrayTarget[];
    }> = {};

    for (const rule of this.rules.values()) {
      byRule[rule.id] = {
        ruleId: rule.id,
        ruleName: rule.name,
        version: rule.version,
        totalRequests: 0,
        totalErrors: 0,
        avgLatencyMs: 0,
        byProtocol: {},
        endpoints: rule.endpoints.map((ep) => {
          if (ep.protocol === Protocol.HTTP || ep.protocol === Protocol.WEBSOCKET) {
            return { protocol: ep.protocol, pattern: ep.pattern };
          } else {
            return { protocol: ep.protocol, service: `${(ep as GrpcEndpoint).serviceName}/${(ep as GrpcEndpoint).methodName}` };
          }
        }),
        enabled: rule.enabled,
        grayTargets: rule.grayTargets || [],
      };
    }

    for (const mRaw of this.getMetrics()) {
      const r = byRule[mRaw.ruleId];
      if (!r) continue;

      const targetNameMatch = mRaw.ruleName.match(/\[(.+)\]$/);
      const targetName = targetNameMatch ? targetNameMatch[1] : DEFAULT_TARGET_NAME;

      if (!r.byProtocol[mRaw.protocol]) {
        r.byProtocol[mRaw.protocol] = {
          requests: 0,
          errors: 0,
          avgLatencyMs: 0,
          p99LatencyMs: 0,
          activeConnections: 0,
          lastError: null,
          byTarget: {},
        };
      }

      const protoEntry = r.byProtocol[mRaw.protocol];
      protoEntry.requests += mRaw.requests;
      protoEntry.errors += mRaw.errors;
      protoEntry.activeConnections += mRaw.activeConnections;

      if (mRaw.lastRequestAt) {
        protoEntry.lastRequestAt = mRaw.lastRequestAt;
      }
      if (mRaw.lastErrorAt && mRaw.lastErrorMessage) {
        if (!protoEntry.lastError || mRaw.lastErrorAt > protoEntry.lastError.at) {
          protoEntry.lastError = { at: mRaw.lastErrorAt, message: mRaw.lastErrorMessage };
        }
      }

      protoEntry.byTarget[targetName] = {
        targetName,
        requests: mRaw.requests,
        errors: mRaw.errors,
        avgLatencyMs: mRaw.requests > 0 ? Math.round(mRaw.totalLatencyMs / mRaw.requests) : 0,
        p99LatencyMs: mRaw.p99LatencyMs || 0,
        activeConnections: mRaw.activeConnections,
      };

      r.totalRequests += mRaw.requests;
      r.totalErrors += mRaw.errors;
    }

    for (const r of Object.values(byRule)) {
      for (const p of Object.values(r.byProtocol)) {
        let totalLatency = 0;
        let totalCount = 0;
        let maxP99 = 0;
        for (const t of Object.values(p.byTarget)) {
          totalLatency += t.avgLatencyMs * t.requests;
          totalCount += t.requests;
          maxP99 = Math.max(maxP99, t.p99LatencyMs);
        }
        p.avgLatencyMs = totalCount > 0 ? Math.round(totalLatency / totalCount) : 0;
        p.p99LatencyMs = maxP99;
      }

      let ruleTotalLatency = 0;
      let ruleTotalCount = 0;
      for (const p of Object.values(r.byProtocol)) {
        ruleTotalLatency += p.avgLatencyMs * p.requests;
        ruleTotalCount += p.requests;
      }
      r.avgLatencyMs = ruleTotalCount > 0 ? Math.round(ruleTotalLatency / ruleTotalCount) : 0;
    }

    return Object.values(byRule);
  }

  addAuditLog(entry: Omit<AuditLogEntry, 'id' | 'timestamp'>): AuditLogEntry {
    const fullEntry: AuditLogEntry = {
      id: uuidv4(),
      timestamp: new Date().toISOString(),
      ...entry,
    };
    this.auditLog.unshift(fullEntry);
    if (this.auditLog.length > this.auditLogMaxEntries) {
      this.auditLog = this.auditLog.slice(0, this.auditLogMaxEntries);
    }
    this.emit('audit:entry', fullEntry);
    return fullEntry;
  }

  getAuditLogs(ruleId?: string, action?: string, limit = 100): AuditLogEntry[] {
    let logs = this.auditLog;
    if (ruleId) {
      logs = logs.filter((l) => l.ruleId === ruleId);
    }
    if (action) {
      logs = logs.filter((l) => l.action === action);
    }
    return logs.slice(0, limit);
  }

  matchRoute(
    protocol: Protocol,
    method: HttpMethod,
    path: string,
    paramsOut: { params: Record<string, string> },
    ruleIdOut: { id?: string },
    targetNameOut?: { name?: string }
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

        if (targetNameOut) {
          targetNameOut.name = this.pickGrayTarget(route.ruleId);
        }

        return route;
      }

      if (protocol === Protocol.GRPC) {
        const grpcPath = `/${route.serviceName}/${route.methodName}`;
        if (path === grpcPath) {
          ruleIdOut.id = route.ruleId;
          if (targetNameOut) {
            targetNameOut.name = this.pickGrayTarget(route.ruleId);
          }
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

  private metricKey(ruleId: string, protocol: Protocol, targetName = DEFAULT_TARGET_NAME): string {
    return `${ruleId}::${protocol}::${targetName}`;
  }

  private bumpVersion(current: string): string {
    const match = current.match(/^v(\d+)$/);
    if (match) {
      return `v${parseInt(match[1], 10) + 1}`;
    }
    return `${current}-${Date.now()}`;
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

export { RouteRegistry, DEFAULT_TARGET_NAME };
