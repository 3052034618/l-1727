import { EventEmitter } from 'events';
import { Protocol, ConnectionId, ManagedConnection, Route } from './types';
import { v4 as uuidv4 } from 'uuid';

interface ConnectionEvents {
  connected: (conn: ManagedConnection) => void;
  disconnected: (id: ConnectionId) => void;
  message: (id: ConnectionId, data: any) => void;
  error: (id: ConnectionId, err: Error) => void;
}

declare interface ConnectionManager {
  on<U extends keyof ConnectionEvents>(event: U, listener: ConnectionEvents[U]): this;
  emit<U extends keyof ConnectionEvents>(event: U, ...args: Parameters<ConnectionEvents[U]>): boolean;
}

class ConnectionManager extends EventEmitter {
  private connections: Map<ConnectionId, ManagedConnection> = new Map();
  private heartbeatTimers: Map<ConnectionId, NodeJS.Timeout> = new Map();
  private maxConnections: number;
  private heartbeatIntervalMs: number;
  private shuttingDown: boolean = false;

  constructor(maxConnections: number = 1000, heartbeatIntervalMs: number = 30000) {
    super();
    this.maxConnections = maxConnections;
    this.heartbeatIntervalMs = heartbeatIntervalMs;
  }

  register(
    protocol: Protocol,
    remoteAddress: string,
    send: (data: any) => void,
    close: () => void,
    route?: Route
  ): ManagedConnection | Error {
    if (this.shuttingDown) {
      return new Error('Gateway is shutting down, refusing new connections');
    }

    if (this.connections.size >= this.maxConnections) {
      return new Error(`Maximum connections (${this.maxConnections}) reached`);
    }

    const id: ConnectionId = uuidv4();
    const conn: ManagedConnection = {
      id,
      protocol,
      remoteAddress,
      connectedAt: new Date(),
      lastActivity: new Date(),
      route: route ?? null,
      send,
      close,
    };

    this.connections.set(id, conn);
    this.startHeartbeat(id);
    this.emit('connected', conn);

    return conn;
  }

  unregister(id: ConnectionId): void {
    const conn = this.connections.get(id);
    if (!conn) return;

    this.stopHeartbeat(id);
    this.connections.delete(id);
    this.emit('disconnected', id);
  }

  updateActivity(id: ConnectionId): void {
    const conn = this.connections.get(id);
    if (conn) {
      conn.lastActivity = new Date();
    }
  }

  get(id: ConnectionId): ManagedConnection | undefined {
    return this.connections.get(id);
  }

  getByProtocol(protocol: Protocol): ManagedConnection[] {
    const result: ManagedConnection[] = [];
    for (const conn of this.connections.values()) {
      if (conn.protocol === protocol) {
        result.push(conn);
      }
    }
    return result;
  }

  get count(): number {
    return this.connections.size;
  }

  get wsCount(): number {
    let count = 0;
    for (const conn of this.connections.values()) {
      if (conn.protocol === Protocol.WEBSOCKET) count++;
    }
    return count;
  }

  broadcast(protocol: Protocol, data: any, filter?: (conn: ManagedConnection) => boolean): void {
    for (const conn of this.connections.values()) {
      if (conn.protocol !== protocol) continue;
      if (filter && !filter(conn)) continue;
      try {
        conn.send(data);
      } catch {
        this.unregister(conn.id);
      }
    }
  }

  private startHeartbeat(id: ConnectionId): void {
    const conn = this.connections.get(id);
    if (!conn || conn.protocol !== Protocol.WEBSOCKET) return;

    const timer = setInterval(() => {
      if (!this.connections.has(id)) {
        this.stopHeartbeat(id);
        return;
      }

      const now = Date.now();
      const lastActivity = conn.lastActivity.getTime();
      if (now - lastActivity > this.heartbeatIntervalMs * 3) {
        this.emit('error', id, new Error('Connection heartbeat timeout'));
        this.unregister(id);
        try { conn.close(); } catch {}
        return;
      }

      try {
        conn.send({ type: 'ping', timestamp: now });
      } catch {
        this.unregister(id);
      }
    }, this.heartbeatIntervalMs);

    this.heartbeatTimers.set(id, timer);
  }

  private stopHeartbeat(id: ConnectionId): void {
    const timer = this.heartbeatTimers.get(id);
    if (timer) {
      clearInterval(timer);
      this.heartbeatTimers.delete(id);
    }
  }

  async shutdown(timeoutMs: number = 10000): Promise<void> {
    this.shuttingDown = true;

    const deadline = Date.now() + timeoutMs;
    const connections = Array.from(this.connections.values());

    for (const conn of connections) {
      try {
        conn.send({ type: 'close', reason: 'server_shutdown' });
      } catch {}
    }

    await new Promise<void>((resolve) => {
      const check = () => {
        if (this.connections.size === 0 || Date.now() > deadline) {
          for (const conn of this.connections.values()) {
            try { conn.close(); } catch {}
            this.stopHeartbeat(conn.id);
          }
          this.connections.clear();
          resolve();
        } else {
          setTimeout(check, 200);
        }
      };
      check();
    });
  }
}

export { ConnectionManager };
