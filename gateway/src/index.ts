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
import { Protocol, RuleCreateInput, RuleUpdateInput, BusinessRule } from './types';

async function main() {
  const protoPath = path.join(__dirname, '..', 'proto', 'user.proto');
  const config: GatewayConfig = createDefaultConfig(protoPath);

  console.log('========================================');
  console.log('  Multi-Protocol Unified Gateway (v2)  ');
  console.log('========================================');
  console.log(`  HTTP/WS Port : ${config.httpPort}`);
  console.log(`  gRPC Port    : ${config.grpcPort}`);
  console.log(`  Backend      : ${config.grpcBackendAddress}`);
  console.log(`  WS Max Conn  : ${config.wsMaxConnections}`);
  console.log(`  HTTP Streams : ${config.httpMaxConcurrentStreams}`);
  console.log('========================================\n');

  const registry = new RouteRegistry();
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
      await handleAdminRules(req, res, registry);
      return;
    }

    if (url.startsWith('/admin/metrics')) {
      handleAdminMetrics(req, res, registry, connectionManager, wsHandler);
      return;
    }

    if (url.startsWith('/admin/connections')) {
      handleAdminConnections(req, res, connectionManager, wsHandler, registry);
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
  console.log('    GET    /admin/rules              List rules');
  console.log('    GET    /admin/rules/:id          Get rule');
  console.log('    POST   /admin/rules              Create rule');
  console.log('    PUT    /admin/rules/:id          Update rule');
  console.log('    DELETE /admin/rules/:id          Delete rule');
  console.log('    PATCH  /admin/rules/:id/enable   Enable rule');
  console.log('    PATCH  /admin/rules/:id/disable  Disable rule');
  console.log('    GET    /admin/metrics            Full metrics');
  console.log('    GET    /admin/connections        All connections');
  console.log('    GET    /stats                    Aggregated view');
  console.log('    GET    /health                   Health check');
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
    console.log(`[Registry] Rule ADDED: ${rule.id} ("${rule.name}") with ${rule.endpoints.length} endpoints`);
  });
  registry.on('rule:updated', (rule, prev) => {
    const changes: string[] = [];
    if (prev.enabled !== rule.enabled) changes.push(`enabled: ${prev.enabled}->${rule.enabled}`);
    if (prev.endpoints.length !== rule.endpoints.length) changes.push(`endpoints: ${prev.endpoints.length}->${rule.endpoints.length}`);
    console.log(`[Registry] Rule UPDATED: ${rule.id} (${changes.join(', ') || 'metadata changed'})`);
  });
  registry.on('rule:deleted', (id) => {
    console.log(`[Registry] Rule DELETED: ${id}`);
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

async function handleAdminRules(
  req: IncomingMessage,
  res: ServerResponse,
  registry: RouteRegistry
): Promise<void> {
  const url = req.url || '';
  const path = extractPath(url);
  const method = req.method || 'GET';

  const matchId = path.match(/^\/admin\/rules\/([^\/]+)$/);
  const matchEnable = path.match(/^\/admin\/rules\/([^\/]+)\/(enable|disable)$/);

  if (path === '/admin/rules' && method === 'GET') {
    sendJson(res, 200, {
      total: registry.listRules().length,
      data: registry.listRules().map(summarizeRule),
    });
    return;
  }

  if (matchId && method === 'GET') {
    const rule = registry.getRule(matchId[1]);
    if (!rule) { sendJson(res, 404, { error: { code: 'NOT_FOUND', message: 'Rule not found' } }); return; }
    sendJson(res, 200, { data: rule });
    return;
  }

  if (path === '/admin/rules' && method === 'POST') {
    try {
      const input = (await readJsonBody(req)) as RuleCreateInput;
      const rule = registry.addRule(input);
      sendJson(res, 201, {
        message: 'Rule created successfully. New requests will immediately use this rule.',
        data: summarizeRule(rule),
      });
    } catch (e: any) {
      sendJson(res, 400, { error: { code: 'VALIDATION_ERROR', message: e.message } });
    }
    return;
  }

  if (matchId && method === 'PUT') {
    try {
      const input = (await readJsonBody(req)) as RuleUpdateInput;
      const rule = registry.updateRule(matchId[1], input);
      sendJson(res, 200, {
        message: 'Rule updated. New requests use new rules; existing WebSocket connections are NOT interrupted.',
        data: summarizeRule(rule),
        note: 'Active WebSocket streams continue with old routing until reconnected. HTTP/gRPC are stateless and pick up changes immediately.',
      });
    } catch (e: any) {
      sendJson(res, e.message.includes('not found') ? 404 : 400,
        { error: { code: e.message.includes('not found') ? 'NOT_FOUND' : 'VALIDATION_ERROR', message: e.message } });
    }
    return;
  }

  if (matchId && method === 'DELETE') {
    const deleted = registry.deleteRule(matchId[1]);
    if (!deleted) { sendJson(res, 404, { error: { code: 'NOT_FOUND', message: 'Rule not found' } }); return; }
    sendJson(res, 200, {
      message: 'Rule deleted. Existing connections will continue until they close.',
      deletedRuleId: matchId[1],
    });
    return;
  }

  if (matchEnable && method === 'PATCH') {
    try {
      const enabled = matchEnable[2] === 'enable';
      const rule = registry.setRuleEnabled(matchEnable[1], enabled);
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
    return;
  }

  sendJson(res, 405, { error: { code: 'METHOD_NOT_ALLOWED', message: 'Check supported methods in README' } });
}

function summarizeRule(r: BusinessRule): any {
  return {
    id: r.id,
    name: r.name,
    description: r.description,
    enabled: r.enabled,
    createdAt: r.createdAt,
    updatedAt: r.updatedAt,
    target: {
      backendAddress: r.target.backendAddress,
      service: `${r.target.serviceName}/${r.target.methodName}`,
      protocol: r.target.protocol,
      isServerStreaming: r.target.isServerStreaming,
    },
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
  const url = req.url || '';
  const byRule = registry.getAggregatedMetricsByRule();
  const byProtocol = registry.getMetrics().reduce((acc, m) => {
    if (!acc[m.protocol]) acc[m.protocol] = { requests: 0, errors: 0, avgLatencyMs: 0, active: 0 };
    const p = acc[m.protocol];
    p.requests += m.requests;
    p.errors += m.errors;
    p.active += m.activeConnections;
    p.avgLatencyMs = Math.round((p.avgLatencyMs * (p.requests - m.requests) + m.totalLatencyMs) / Math.max(1, p.requests));
    return acc;
  }, {} as Record<string, any>);

  const output = {
    generatedAt: new Date().toISOString(),
    uptimeSec: Math.floor(process.uptime()),
    summary: {
      totalRequests: byRule.reduce((s, r) => s + r.totalRequests, 0),
      totalErrors: byRule.reduce((s, r) => s + r.totalErrors, 0),
      byProtocol,
      connections: connMgr.getCountsByProtocol(),
      limits: connMgr.getLimitsByProtocol(),
      webSocketConnections: wsHandler.getBindingStats(),
    },
    rules: byRule.map((r) => {
      const metric = registry.getMetricsByRuleId(r.ruleId);
      return {
        rule: {
          id: r.ruleId,
          name: r.ruleName,
          enabled: r.enabled,
          endpoints: r.endpoints,
        },
        metrics: {
          totalRequests: r.totalRequests,
          totalErrors: r.totalErrors,
          avgLatencyMs: r.avgLatencyMs,
          byProtocol: Object.fromEntries(
            Object.entries(r.protocols).map(([p, m]) => [p, {
              requests: m.requests,
              errors: m.errors,
              avgLatencyMs: m.requests > 0 ? Math.round(m.totalLatencyMs / m.requests) : 0,
              p99LatencyMs: m.p99LatencyMs,
              activeConnections: m.activeConnections,
              lastRequestAt: m.lastRequestAt,
              lastError: m.errors > 0 ? { at: m.lastErrorAt, message: m.lastErrorMessage } : null,
            }])
          ),
          perEndpoint: metric.map(m => ({
            protocol: m.protocol,
            requests: m.requests,
            errors: m.errors,
            avgLatencyMs: m.requests > 0 ? Math.round(m.totalLatencyMs / m.requests) : 0,
            p99LatencyMs: m.p99LatencyMs,
            activeConnections: m.activeConnections,
          })),
        },
      };
    }),
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
      enabled: r.enabled,
      endpoints: r.endpoints,
      metrics: {
        totalRequests: r.totalRequests,
        totalErrors: r.totalErrors,
        avgLatencyMs: r.avgLatencyMs,
        byProtocol: Object.fromEntries(
          Object.entries(r.protocols).map(([p, m]) => [p, {
            requests: m.requests,
            errors: m.errors,
            avgLatencyMs: m.requests > 0 ? Math.round(m.totalLatencyMs / m.requests) : 0,
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

  const allConns = Array.from({ length: 0 });
  const wsStats = wsHandler.getBindingStats();

  sendJson(res, 200, {
    total: connMgr.count,
    byProtocol: connMgr.getCountsByProtocol(),
    limits: connMgr.getLimitsByProtocol(),
    summary: {
      [Protocol.HTTP]: connMgr.httpCount,
      [Protocol.WEBSOCKET]: connMgr.wsCount,
      [Protocol.GRPC]: connMgr.grpcCount,
    },
    webSocketConnections: wsStats,
    connections: Array.from(
      (() => {
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
        return result;
      })()
    ),
  });
}

main().catch((err) => {
  console.error('[Gateway] Fatal error:', err);
  process.exit(1);
});
