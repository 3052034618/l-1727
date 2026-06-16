import * as http from 'http';
import { IncomingMessage, ServerResponse } from 'http';
import * as path from 'path';
import { GatewayConfig, createDefaultConfig, createDefaultBusinessRules } from './config';
import { UnifiedRouter } from './router';
import { ConnectionManager } from './connection-manager';
import { GrpcClientPool } from './grpc-client-pool';
import { HttpHandler } from './http-handler';
import { WsHandler } from './ws-handler';
import { GrpcHandler } from './grpc-handler';
import { RouteRegistry } from './route-registry';
import { AdminAuth } from './admin-auth';
import { Protocol, RuleCreateInput, RuleUpdateInput, BusinessRule, GrayTarget, AdminConfig } from './types';

const ADMIN_TOKEN = process.env.GATEWAY_ADMIN_TOKEN || 'admin-secret-token';
const ADMIN_AUTH_ENABLED = process.env.GATEWAY_ADMIN_AUTH_ENABLED === 'true';

async function main() {
  const protoPath = path.join(__dirname, '..', '..', 'proto', 'user.proto');
  const config: GatewayConfig = createDefaultConfig(protoPath);

  const adminConfig: AdminConfig = {
    enabled: ADMIN_AUTH_ENABLED,
    tokens: [ADMIN_TOKEN],
    tokenHeader: 'x-admin-token',
    auditLogMaxEntries: 1000,
  };
  const adminAuth = new AdminAuth(adminConfig);

  console.log('========================================');
  console.log('  Multi-Protocol Unified Gateway (v3)  ');
  console.log('========================================');
  console.log(`  HTTP/WS Port : ${config.httpPort}`);
  console.log(`  gRPC Port    : ${config.grpcPort}`);
  console.log(`  Backend      : ${config.grpcBackendAddress}`);
  console.log(`  WS Max Conn  : ${config.wsMaxConnections}`);
  console.log(`  HTTP Streams : ${config.httpMaxConcurrentStreams}`);
  console.log(`  Admin Auth   : ${adminAuth.isEnabled() ? 'ON (token required)' : 'OFF (open access)'}`);
  console.log('========================================\n');

  const registry = new RouteRegistry();
  registry.setAuditLogMaxEntries(adminAuth.getAuditLogMaxEntries());
  registry.loadRules(createDefaultBusinessRules());

  const router = new UnifiedRouter(registry);
  const connectionManager = new ConnectionManager({
    wsMaxConnections: config.wsMaxConnections,
    httpMaxStreams: config.httpMaxConcurrentStreams,
    heartbeatIntervalMs: config.wsHeartbeatIntervalMs,
  });
  const grpcPool = new GrpcClientPool();

  await grpcPool.initialize(protoPath);
  console.log('[Gateway] gRPC client pool initialized');

  const httpHandler = new HttpHandler(router, grpcPool, connectionManager, registry);
  const wsHandler = new WsHandler(router, grpcPool, connectionManager, registry);
  const grpcHandler = new GrpcHandler(router, grpcPool, connectionManager, registry, protoPath);

  await grpcHandler.initialize();

  setupRegistryListeners(registry);
  setupConnectionListeners(connectionManager, registry);

  const httpServer = http.createServer(async (req, res) => {
    const isWebSocket = req.headers.upgrade?.toLowerCase() === 'websocket';
    if (isWebSocket) return;

    const url = req.url || '/';

    if (url.startsWith('/admin/rules')) {
      await handleAdminRules(req, res, registry, adminAuth);
      return;
    }

    if (url.startsWith('/admin/metrics')) {
      adminAuth.middleware(req, res, () => {
        handleAdminMetrics(req, res, registry, connectionManager, wsHandler);
      });
      return;
    }

    if (url.startsWith('/admin/connections')) {
      adminAuth.middleware(req, res, () => {
        handleAdminConnections(req, res, connectionManager, wsHandler, registry);
      });
      return;
    }

    if (url.startsWith('/admin/audit-logs')) {
      adminAuth.middleware(req, res, () => {
        handleAdminAuditLogs(req, res, registry);
      });
      return;
    }

    if (url === '/health' && req.method === 'GET') {
      sendJson(res, 200, {
        status: 'healthy',
        uptimeSec: Math.floor(process.uptime()),
        connections: connectionManager.getCountsByProtocol(),
        limits: connectionManager.getLimitsByProtocol(),
        protocols: ['http', 'websocket', 'grpc'],
        rules: {
          total: registry.listRules().length,
          enabled: registry.listRules().filter(r => r.enabled).length,
        },
        adminAuth: adminAuth.isEnabled(),
      });
      return;
    }

    if (url === '/stats' && req.method === 'GET') {
      handleStats(req, res, registry, connectionManager, wsHandler);
      return;
    }

    httpHandler.handle(req, res);
  });

  httpServer.on('upgrade', (req, socket, head) => {
    const isWebSocket = req.headers.upgrade?.toLowerCase() === 'websocket';

    if (isWebSocket) {
      wsHandler.handleUpgrade(req, socket, head);
      return;
    }

    socket.destroy();
  });

  await new Promise<void>((resolve) => {
    httpServer.listen(config.httpPort, () => {
      console.log(`[Gateway] HTTP/WS server listening on port ${config.httpPort}`);
      printEndpoints(registry);
      resolve();
    });
  });

  await grpcHandler.start(config.grpcPort);
  console.log(`[Gateway] gRPC server listening on port ${config.grpcPort}`);
  console.log(`  -> gateway.UserService methods proxied via registered rules\n`);

  console.log('========================================');
  console.log('  Admin API (port 8080):');
  console.log('    Rules:');
  console.log('      GET    /admin/rules');
  console.log('      GET    /admin/rules/:id');
  console.log('      POST   /admin/rules');
  console.log('      PUT    /admin/rules/:id');
  console.log('      DELETE /admin/rules/:id');
  console.log('      PATCH  /admin/rules/:id/enable');
  console.log('      PATCH  /admin/rules/:id/disable');
  console.log('    Versions:');
  console.log('      GET    /admin/rules/:id/versions');
  console.log('      POST   /admin/rules/:id/versions');
  console.log('      POST   /admin/rules/:id/rollback/:versionId');
  console.log('    Gray release:');
  console.log('      GET    /admin/rules/:id/gray-targets');
  console.log('      PUT    /admin/rules/:id/gray-targets');
  console.log('    Metrics & Stats:');
  console.log('      GET    /admin/metrics');
  console.log('      GET    /admin/connections');
  console.log('      GET    /admin/audit-logs');
  console.log('      GET    /stats');
  console.log('      GET    /health');
  if (adminAuth.isEnabled()) {
    console.log(`  * Admin auth enabled, use header: ${adminAuth.getTokenHeader()}`);
  }
  console.log('========================================');
  console.log('\n[Gateway] All protocols active. Press Ctrl+C to shut down.\n');

  const shutdown = async () => {
    console.log('\n[Gateway] Shutting down...');

    console.log('[Gateway] Closing WebSocket handler...');
    wsHandler.shutdown();

    console.log('[Gateway] Closing HTTP server...');
    httpServer.close();

    console.log('[Gateway] Closing gRPC handler...');
    try { await grpcHandler.shutdown(); } catch (e: any) { console.log(e.message); }

    console.log('[Gateway] Draining connections...');
    await connectionManager.shutdown(5000);

    console.log('[Gateway] Closing gRPC client pool...');
    grpcPool.closeAll();

    console.log('[Gateway] Shutdown complete.');
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

function printEndpoints(registry: RouteRegistry): void {
  const byProtocol: Record<string, string[]> = { http: [], websocket: [], grpc: [] };
  for (const rule of registry.listRules()) {
    for (const ep of rule.endpoints) {
      if (ep.protocol === Protocol.HTTP) {
        const methods = ep.methods.map(m => m.padEnd(4)).join(',');
        byProtocol.http.push(`     ${methods} ${ep.pattern}`);
      } else if (ep.protocol === Protocol.WEBSOCKET) {
        byProtocol.websocket.push(`     WS   ${ep.pattern}`);
      } else if (ep.protocol === Protocol.GRPC) {
        byProtocol.grpc.push(`          ${ep.serviceName}/${ep.methodName}`);
      }
    }
  }
  console.log('  -> HTTP endpoints:');
  byProtocol.http.forEach(l => console.log(l));
  console.log('  -> WebSocket endpoints:');
  byProtocol.websocket.forEach(l => console.log(l));
  console.log('  -> gRPC endpoints:');
  byProtocol.grpc.forEach(l => console.log(l));
}

function setupRegistryListeners(registry: RouteRegistry): void {
  registry.on('rule:added', (rule) => {
    console.log(`[Registry] Rule ADDED: ${rule.id} ("${rule.name}") v${rule.version} with ${rule.endpoints.length} endpoints`);
  });
  registry.on('rule:updated', (rule, prev) => {
    const changes: string[] = [];
    if (prev.enabled !== rule.enabled) changes.push(`enabled: ${prev.enabled}->${rule.enabled}`);
    if (prev.endpoints.length !== rule.endpoints.length) changes.push(`endpoints: ${prev.endpoints.length}->${rule.endpoints.length}`);
    if (prev.version !== rule.version) changes.push(`version: ${prev.version}->${rule.version}`);
    console.log(`[Registry] Rule UPDATED: ${rule.id} (${changes.join(', ') || 'metadata changed'})`);
  });
  registry.on('rule:deleted', (id) => {
    console.log(`[Registry] Rule DELETED: ${id}`);
  });
  registry.on('rule:rolled-back', (rule, version) => {
    console.log(`[Registry] Rule ROLLED BACK: ${rule.id} to version ${version.version} (now v${rule.version})`);
  });
  registry.on('version:created', (version) => {
    console.log(`[Registry] Version CREATED: ${version.ruleId}@${version.version} (${version.id})`);
  });
  registry.on('audit:entry', (entry) => {
    console.log(`[Audit] ${entry.action} | ${entry.actor} | ${entry.ruleId} | ${entry.timestamp}`);
  });
  registry.on('routes:changed', (rules) => {
    const totalEp = rules.reduce((s, r) => s + r.endpoints.length, 0);
    console.log(`[Registry] Routes recompiled: ${rules.length} rules -> ${totalEp} endpoints`);
  });
}

function setupConnectionListeners(
  connectionManager: ConnectionManager,
  registry: RouteRegistry
): void {
  connectionManager.on('connected', (conn) => {
    console.log(`[Connections] + ${conn.protocol} | ${conn.id} | ${conn.remoteAddress} | rule=${conn.ruleId || 'n/a'}`);
  });
  connectionManager.on('disconnected', (id) => {
    console.log(`[Connections] - ${id}`);
  });
  connectionManager.on('error', (id, err) => {
    console.error(`[Connections] Error ${id}: ${err.message}`);
  });
  connectionManager.on('limitExceeded', (protocol, limit, count) => {
    console.error(`[Connections] LIMIT EXCEEDED: ${protocol} | count=${count} | limit=${limit}`);
  });
}

function sendJson(res: ServerResponse, status: number, body: any): void {
  if (!res.headersSent) {
    res.writeHead(status, {
      'Content-Type': 'application/json; charset=utf-8',
    });
  }
  res.end(JSON.stringify(body, null, 2));
}

async function readJsonBody(req: IncomingMessage): Promise<any> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (c: Buffer) => chunks.push(c));
    req.on('end', () => {
      const raw = Buffer.concat(chunks);
      if (raw.length === 0) { resolve({}); return; }
      try { resolve(JSON.parse(raw.toString('utf-8'))); }
      catch (e: any) { reject(new Error(`Invalid JSON: ${e.message}`)); }
    });
    req.on('error', reject);
  });
}

function extractPath(url: string): string {
  const idx = url.indexOf('?');
  return idx === -1 ? url : url.slice(0, idx);
}

function requireAuth(
  req: IncomingMessage,
  res: ServerResponse,
  adminAuth: AdminAuth,
  handler: (actor: string, ip: string) => void
): void {
  adminAuth.middleware(req, res, handler);
}

async function handleAdminRules(
  req: IncomingMessage,
  res: ServerResponse,
  registry: RouteRegistry,
  adminAuth: AdminAuth
): Promise<void> {
  const url = req.url || '';
  const path = extractPath(url);
  const method = req.method || 'GET';

  const matchId = path.match(/^\/admin\/rules\/([^\/]+)$/);
  const matchEnable = path.match(/^\/admin\/rules\/([^\/]+)\/(enable|disable)$/);
  const matchVersions = path.match(/^\/admin\/rules\/([^\/]+)\/versions$/);
  const matchRollback = path.match(/^\/admin\/rules\/([^\/]+)\/rollback\/([^\/]+)$/);
  const matchGray = path.match(/^\/admin\/rules\/([^\/]+)\/gray-targets$/);

  if (path === '/admin/rules' && method === 'GET') {
    requireAuth(req, res, adminAuth, () => {
      sendJson(res, 200, {
        total: registry.listRules().length,
        data: registry.listRules().map(summarizeRule),
      });
    });
    return;
  }

  if (matchId && method === 'GET') {
    requireAuth(req, res, adminAuth, () => {
      const rule = registry.getRule(matchId[1]);
      if (!rule) { sendJson(res, 404, { error: { code: 'NOT_FOUND', message: 'Rule not found' } }); return; }
      sendJson(res, 200, { data: rule });
    });
    return;
  }

  if (path === '/admin/rules' && method === 'POST') {
    requireAuth(req, res, adminAuth, async (actor, ip) => {
      try {
        const input = (await readJsonBody(req)) as RuleCreateInput;
        const rule = registry.addRule(input, actor, ip);
        sendJson(res, 201, {
          message: 'Rule created successfully. New requests will immediately use this rule.',
          data: summarizeRule(rule),
        });
      } catch (e: any) {
        sendJson(res, 400, { error: { code: 'VALIDATION_ERROR', message: e.message } });
      }
    });
    return;
  }

  if (matchId && method === 'PUT') {
    requireAuth(req, res, adminAuth, async (actor, ip) => {
      try {
        const input = (await readJsonBody(req)) as RuleUpdateInput;
        const rule = registry.updateRule(matchId[1], input, actor, ip);
        sendJson(res, 200, {
          message: 'Rule updated. New requests use new rules; existing WebSocket connections are NOT interrupted.',
          data: summarizeRule(rule),
          note: 'Active WebSocket streams continue with old routing until reconnected. HTTP/gRPC are stateless and pick up changes immediately.',
        });
      } catch (e: any) {
        sendJson(res, e.message.includes('not found') ? 404 : 400,
          { error: { code: e.message.includes('not found') ? 'NOT_FOUND' : 'VALIDATION_ERROR', message: e.message } });
      }
    });
    return;
  }

  if (matchId && method === 'DELETE') {
    requireAuth(req, res, adminAuth, (actor, ip) => {
      const deleted = registry.deleteRule(matchId[1], actor, ip);
      if (!deleted) { sendJson(res, 404, { error: { code: 'NOT_FOUND', message: 'Rule not found' } }); return; }
      sendJson(res, 200, {
        message: 'Rule deleted. Existing connections will continue until they close.',
        deletedRuleId: matchId[1],
      });
    });
    return;
  }

  if (matchEnable && method === 'PATCH') {
    requireAuth(req, res, adminAuth, (actor, ip) => {
      try {
        const enabled = matchEnable[2] === 'enable';
        const rule = registry.setRuleEnabled(matchEnable[1], enabled, actor, ip);
        sendJson(res, 200, {
          message: `Rule ${enabled ? 'enabled' : 'disabled'}`,
          data: summarizeRule(rule),
          note: enabled
            ? 'New requests will now match this rule.'
            : 'Existing connections (incl. WebSocket) will continue until they close.',
        });
      } catch (e: any) {
        sendJson(res, 404, { error: { code: 'NOT_FOUND', message: e.message } });
      }
    });
    return;
  }

  if (matchVersions && method === 'GET') {
    requireAuth(req, res, adminAuth, () => {
      const ruleId = matchVersions[1];
      const rule = registry.getRule(ruleId);
      if (!rule) { sendJson(res, 404, { error: { code: 'NOT_FOUND', message: 'Rule not found' } }); return; }
      const versions = registry.listVersions(ruleId);
      sendJson(res, 200, {
        ruleId,
        currentVersion: rule.version,
        total: versions.length,
        versions: versions.map((v) => ({
          id: v.id,
          version: v.version,
          createdAt: v.createdAt,
          createdBy: v.createdBy,
          note: v.note,
        })),
      });
    });
    return;
  }

  if (matchVersions && method === 'POST') {
    requireAuth(req, res, adminAuth, async (actor, ip) => {
      try {
        const ruleId = matchVersions[1];
        const body = await readJsonBody(req);
        const version = registry.createVersion(ruleId, actor, body?.note, ip);
        sendJson(res, 201, {
          message: 'Version created',
          data: {
            id: version.id,
            version: version.version,
            createdAt: version.createdAt,
            createdBy: version.createdBy,
            note: version.note,
          },
        });
      } catch (e: any) {
        sendJson(res, e.message.includes('not found') ? 404 : 400,
          { error: { code: e.message.includes('not found') ? 'NOT_FOUND' : 'VALIDATION_ERROR', message: e.message } });
      }
    });
    return;
  }

  if (matchRollback && method === 'POST') {
    requireAuth(req, res, adminAuth, (actor, ip) => {
      try {
        const ruleId = matchRollback[1];
        const versionId = matchRollback[2];
        const rule = registry.rollbackToVersion(ruleId, versionId, actor, ip);
        sendJson(res, 200, {
          message: 'Rule rolled back. New requests immediately use the rolled-back configuration.',
          data: summarizeRule(rule),
          note: 'Existing connections (incl. WebSocket) will continue until they close with old routing.',
        });
      } catch (e: any) {
        sendJson(res, e.message.includes('not found') ? 404 : 400,
          { error: { code: e.message.includes('not found') ? 'NOT_FOUND' : 'VALIDATION_ERROR', message: e.message } });
      }
    });
    return;
  }

  if (matchGray && method === 'GET') {
    requireAuth(req, res, adminAuth, () => {
      const rule = registry.getRule(matchGray[1]);
      if (!rule) { sendJson(res, 404, { error: { code: 'NOT_FOUND', message: 'Rule not found' } }); return; }
      sendJson(res, 200, {
        ruleId: rule.id,
        defaultTarget: {
          backendAddress: rule.target.backendAddress,
          serviceName: rule.target.serviceName,
          methodName: rule.target.methodName,
        },
        grayTargets: rule.grayTargets || [],
      });
    });
    return;
  }

  if (matchGray && method === 'PUT') {
    requireAuth(req, res, adminAuth, async (actor, ip) => {
      try {
        const ruleId = matchGray[1];
        const body = await readJsonBody(req);
        const grayTargets = (body.grayTargets || []) as GrayTarget[];
        const rule = registry.updateGrayTargets(ruleId, grayTargets, actor, ip);
        sendJson(res, 200, {
          message: 'Gray targets updated. Traffic will be split according to weights.',
          data: {
            ruleId: rule.id,
            version: rule.version,
            grayTargets: rule.grayTargets || [],
          },
          note: 'Weight is integer percentage. Total of all gray target weights + default = 100%.',
        });
      } catch (e: any) {
        sendJson(res, e.message.includes('not found') ? 404 : 400,
          { error: { code: e.message.includes('not found') ? 'NOT_FOUND' : 'VALIDATION_ERROR', message: e.message } });
      }
    });
    return;
  }

  sendJson(res, 405, { error: { code: 'METHOD_NOT_ALLOWED', message: 'Check supported methods in README' } });
}

function summarizeRule(r: BusinessRule): any {
  return {
    id: r.id,
    name: r.name,
    description: r.description,
    version: r.version,
    enabled: r.enabled,
    createdAt: r.createdAt,
    updatedAt: r.updatedAt,
    target: {
      backendAddress: r.target.backendAddress,
      service: `${r.target.serviceName}/${r.target.methodName}`,
      protocol: r.target.protocol,
      isServerStreaming: r.target.isServerStreaming,
    },
    grayTargets: r.grayTargets || [],
    endpoints: r.endpoints.map((ep) => {
      if (ep.protocol === Protocol.HTTP) {
        return { protocol: 'http', pattern: ep.pattern, methods: ep.methods };
      }
      if (ep.protocol === Protocol.WEBSOCKET) {
        return { protocol: 'websocket', pattern: ep.pattern, topicParam: ep.topicQueryParam || 'topic' };
      }
      return { protocol: 'grpc', service: ep.serviceName, method: ep.methodName };
    }),
  };
}

function handleAdminMetrics(
  req: IncomingMessage,
  res: ServerResponse,
  registry: RouteRegistry,
  connMgr: ConnectionManager,
  wsHandler: WsHandler
): void {
  const byRule = registry.getAggregatedMetricsByRule();
  const summaryByProtocol: Record<string, any> = {};

  for (const rule of byRule) {
    for (const [proto, m] of Object.entries(rule.byProtocol)) {
      if (!summaryByProtocol[proto]) {
        summaryByProtocol[proto] = { requests: 0, errors: 0, avgLatencyMs: 0, active: 0 };
      }
      const p = summaryByProtocol[proto];
      p.requests += m.requests;
      p.errors += m.errors;
      p.active += m.activeConnections;
    }
  }

  for (const p of Object.values(summaryByProtocol)) {
    p.avgLatencyMs = p.requests > 0
      ? Math.round(byRule.reduce((sum, r) => {
          const protoM = r.byProtocol[Object.keys(summaryByProtocol).find(k => summaryByProtocol[k] === p) || ''];
          return sum + (protoM ? protoM.avgLatencyMs * protoM.requests : 0);
        }, 0) / p.requests)
      : 0;
  }

  const output = {
    generatedAt: new Date().toISOString(),
    uptimeSec: Math.floor(process.uptime()),
    summary: {
      totalRequests: byRule.reduce((s, r) => s + r.totalRequests, 0),
      totalErrors: byRule.reduce((s, r) => s + r.totalErrors, 0),
      byProtocol: summaryByProtocol,
      connections: connMgr.getCountsByProtocol(),
      limits: connMgr.getLimitsByProtocol(),
      webSocketConnections: wsHandler.getBindingStats(),
    },
    rules: byRule.map((r) => ({
      rule: {
        id: r.ruleId,
        name: r.ruleName,
        version: r.version,
        enabled: r.enabled,
        endpoints: r.endpoints,
        grayTargets: r.grayTargets,
      },
      metrics: {
        totalRequests: r.totalRequests,
        totalErrors: r.totalErrors,
        avgLatencyMs: r.avgLatencyMs,
        byProtocol: Object.fromEntries(
          Object.entries(r.byProtocol).map(([proto, m]) => [proto, {
            requests: m.requests,
            errors: m.errors,
            avgLatencyMs: m.avgLatencyMs,
            p99LatencyMs: m.p99LatencyMs,
            activeConnections: m.activeConnections,
            lastRequestAt: m.lastRequestAt,
            lastError: m.lastError,
            byTarget: Object.values(m.byTarget),
          }])
        ),
      },
    })),
  };

  sendJson(res, 200, output);
}

function handleStats(
  req: IncomingMessage,
  res: ServerResponse,
  registry: RouteRegistry,
  connMgr: ConnectionManager,
  wsHandler: WsHandler
): void {
  const byRule = registry.getAggregatedMetricsByRule();
  sendJson(res, 200, {
    generatedAt: new Date().toISOString(),
    connections: {
      total: connMgr.count,
      byProtocol: connMgr.getCountsByProtocol(),
      limits: connMgr.getLimitsByProtocol(),
      activeWebSockets: wsHandler.getBindingStats().length,
    },
    routes: byRule.map((r) => ({
      ruleId: r.ruleId,
      ruleName: r.ruleName,
      version: r.version,
      enabled: r.enabled,
      grayTargets: r.grayTargets,
      endpoints: r.endpoints,
      metrics: {
        totalRequests: r.totalRequests,
        totalErrors: r.totalErrors,
        avgLatencyMs: r.avgLatencyMs,
        byProtocol: Object.fromEntries(
          Object.entries(r.byProtocol).map(([proto, m]) => [proto, {
            requests: m.requests,
            errors: m.errors,
            avgLatencyMs: m.avgLatencyMs,
            p99LatencyMs: m.p99LatencyMs,
            activeConnections: m.activeConnections,
          }])
        ),
      },
    })),
  });
}

function handleAdminConnections(
  req: IncomingMessage,
  res: ServerResponse,
  connMgr: ConnectionManager,
  wsHandler: WsHandler,
  registry: RouteRegistry
): void {
  const url = req.url || '';
  const path = extractPath(url);
  const matchRule = path.match(/^\/admin\/connections\/rule\/(.+)$/);

  if (matchRule) {
    const ruleId = matchRule[1];
    const rule = registry.getRule(ruleId);
    if (!rule) { sendJson(res, 404, { error: { code: 'NOT_FOUND', message: 'Rule not found' } }); return; }

    const conns = connMgr.getConnectionsByRuleId(ruleId);
    const wsBindings = wsHandler.getBindingsByRuleId(ruleId);

    sendJson(res, 200, {
      ruleId,
      ruleName: rule.name,
      totalActiveConnections: conns.length,
      byProtocol: {
        [Protocol.HTTP]: conns.filter(c => c.protocol === Protocol.HTTP).length,
        [Protocol.WEBSOCKET]: conns.filter(c => c.protocol === Protocol.WEBSOCKET).length,
        [Protocol.GRPC]: conns.filter(c => c.protocol === Protocol.GRPC).length,
      },
      webSocketTopics: wsBindings.map(b => ({
        topic: b.topic,
        connectionId: b.connectionId,
        durationMs: Date.now() - b.startTime,
      })),
      connections: conns.map(c => ({
        id: c.id,
        protocol: c.protocol,
        remoteAddress: c.remoteAddress,
        connectedAt: c.connectedAt,
        lastActivity: c.lastActivity,
        durationMs: Date.now() - c.connectedAt.getTime(),
      })),
    });
    return;
  }

  const result: any[] = [];
  for (const protocol of Object.values(Protocol)) {
    for (const c of connMgr.getByProtocol(protocol)) {
      result.push({
        id: c.id,
        protocol: c.protocol,
        remoteAddress: c.remoteAddress,
        ruleId: c.ruleId,
        connectedAt: c.connectedAt,
        lastActivity: c.lastActivity,
        durationMs: Date.now() - c.connectedAt.getTime(),
      });
    }
  }

  sendJson(res, 200, {
    total: connMgr.count,
    byProtocol: connMgr.getCountsByProtocol(),
    limits: connMgr.getLimitsByProtocol(),
    summary: {
      [Protocol.HTTP]: connMgr.httpCount,
      [Protocol.WEBSOCKET]: connMgr.wsCount,
      [Protocol.GRPC]: connMgr.grpcCount,
    },
    webSocketConnections: wsHandler.getBindingStats(),
    connections: result,
  });
}

function handleAdminAuditLogs(
  req: IncomingMessage,
  res: ServerResponse,
  registry: RouteRegistry
): void {
  const url = req.url || '';
  const path = extractPath(url);
  
  const urlObj = new URL(url, 'http://localhost');
  const ruleId = urlObj.searchParams.get('ruleId') || undefined;
  const action = urlObj.searchParams.get('action') || undefined;
  const limit = parseInt(urlObj.searchParams.get('limit') || '100', 10);

  const logs = registry.getAuditLogs(ruleId, action, Math.min(limit, 500));
  
  sendJson(res, 200, {
    total: logs.length,
    data: logs.map((log) => ({
      id: log.id,
      timestamp: log.timestamp,
      action: log.action,
      actor: log.actor,
      ruleId: log.ruleId,
      ruleName: log.ruleName,
      ip: log.ip,
      metadata: log.metadata,
      hasBefore: !!log.before,
      hasAfter: !!log.after,
    })),
  });
}

main().catch((err) => {
  console.error('[Gateway] Fatal error:', err);
  process.exit(1);
});
