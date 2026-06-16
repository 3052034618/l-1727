import { IncomingMessage, ServerResponse } from 'http';
import { AdminConfig } from './types';

const DEFAULT_ADMIN_CONFIG: AdminConfig = {
  enabled: false,
  tokens: [],
  tokenHeader: 'x-admin-token',
  auditLogMaxEntries: 1000,
};

export class AdminAuth {
  private config: AdminConfig;

  constructor(config?: Partial<AdminConfig>) {
    this.config = { ...DEFAULT_ADMIN_CONFIG, ...config };
  }

  isEnabled(): boolean {
    return this.config.enabled && this.config.tokens.length > 0;
  }

  getTokenHeader(): string {
    return this.config.tokenHeader;
  }

  getAuditLogMaxEntries(): number {
    return this.config.auditLogMaxEntries;
  }

  private extractToken(req: IncomingMessage): string | null {
    const header = req.headers[this.config.tokenHeader.toLowerCase()];
    if (Array.isArray(header)) {
      return header[0] || null;
    }
    return header || null;
  }

  private isValidToken(token: string | null): boolean {
    if (!this.isEnabled()) return true;
    if (!token) return false;
    return this.config.tokens.includes(token);
  }

  extractActor(req: IncomingMessage): string {
    const token = this.extractToken(req);
    if (!this.isEnabled()) return 'system';
    if (!token) return 'anonymous';
    const idx = this.config.tokens.indexOf(token);
    if (idx < 0) return 'unknown';
    return `admin-${idx + 1}`;
  }

  extractIp(req: IncomingMessage): string {
    const forwarded = req.headers['x-forwarded-for'];
    if (forwarded) {
      return Array.isArray(forwarded) ? forwarded[0] : forwarded;
    }
    return (req.socket as any)?.remoteAddress || '';
  }

  middleware(
    req: IncomingMessage,
    res: ServerResponse,
    next: (actor: string, ip: string) => void
  ): void {
    const token = this.extractToken(req);
    if (!this.isValidToken(token)) {
      res.writeHead(401, {
        'Content-Type': 'application/json',
        'WWW-Authenticate': this.config.tokenHeader,
      });
      res.end(JSON.stringify({
        error: {
          code: 'UNAUTHORIZED',
          message: 'Admin access denied: valid admin token required',
        },
      }));
      return;
    }

    const actor = this.extractActor(req);
    const ip = this.extractIp(req);
    next(actor, ip);
  }
}
