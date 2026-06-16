import { IncomingMessage } from 'http';
import { Route, RequestContext, HttpMethod, Protocol, PathParams } from './types';

export class ProtocolConverter {
  static httpPathToGrpcMethod(route: Route, path: string): string {
    return `/${route.serviceName}/${route.methodName}`;
  }

  static httpMethodToGrpcAction(httpMethod: HttpMethod): string {
    const mapping: Record<string, string> = {
      [HttpMethod.GET]: 'Get',
      [HttpMethod.POST]: 'Create',
      [HttpMethod.PUT]: 'Update',
      [HttpMethod.DELETE]: 'Delete',
      [HttpMethod.PATCH]: 'Patch',
    };
    return mapping[httpMethod] || 'Get';
  }

  static buildGrpcPayload(
    route: Route,
    ctx: RequestContext
  ): any {
    const payload: any = {};

    if (ctx.body && typeof ctx.body === 'object') {
      Object.assign(payload, ctx.body);
    }

    if (ctx.params) {
      for (const [patternKey, fieldName] of Object.entries(route.pathToFieldMap)) {
        const paramName = patternKey.startsWith(':') ? patternKey.slice(1) : patternKey;
        const value = ctx.params[paramName];
        if (value !== undefined) {
          payload[fieldName] = value;
        }
      }
    }

    if (ctx.query) {
      for (const [key, value] of Object.entries(ctx.query)) {
        if (!(key in payload)) {
          payload[key] = this.coerceType(value);
        }
      }
    }

    return payload;
  }

  private static coerceType(value: string): any {
    if (value === 'true') return true;
    if (value === 'false') return false;
    if (/^-?\d+$/.test(value)) return parseInt(value, 10);
    if (/^-?\d+\.\d+$/.test(value)) return parseFloat(value);
    return value;
  }

  static grpcResponseToHttp(response: any, isStreaming: boolean): {
    statusCode: number;
    headers: Record<string, string>;
    body: any;
  } {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      'X-Protocol-Converted': 'grpc-to-http',
    };

    if (isStreaming) {
      headers['Transfer-Encoding'] = 'chunked';
      headers['X-Stream-Mode'] = 'ndjson';
    }

    return {
      statusCode: 200,
      headers,
      body: this.protobufToPlainObject(response),
    };
  }

  static protobufToPlainObject(obj: any): any {
    if (obj === null || obj === undefined) return obj;
    if (typeof obj !== 'object') return obj;

    if (Array.isArray(obj)) {
      return obj.map((item) => this.protobufToPlainObject(item));
    }

    const result: any = {};
    for (const key of Object.keys(obj)) {
      if (key.startsWith('_') || typeof obj[key] === 'function') continue;
      result[key] = this.protobufToPlainObject(obj[key]);
    }
    return result;
  }

  static extractQueryParams(url: string): Record<string, string> {
    const queryIndex = url.indexOf('?');
    if (queryIndex === -1) return {};

    const searchParams = new URLSearchParams(url.slice(queryIndex + 1));
    const params: Record<string, string> = {};
    searchParams.forEach((value, key) => {
      params[key] = value;
    });
    return params;
  }

  static extractPath(url: string): string {
    const queryIndex = url.indexOf('?');
    return queryIndex === -1 ? url : url.slice(0, queryIndex);
  }

  static extractHeaders(req: IncomingMessage): Record<string, string> {
    const headers: Record<string, string> = {};
    for (const [key, value] of Object.entries(req.headers)) {
      if (typeof value === 'string') {
        headers[key] = value;
      } else if (Array.isArray(value)) {
        headers[key] = value.join(', ');
      }
    }
    return headers;
  }

  static buildRequestContext(
    protocol: Protocol,
    method: string,
    url: string,
    headers: Record<string, string>,
    body: any,
    rawBody: Buffer | null
  ): RequestContext {
    return {
      protocol,
      method: method.toUpperCase() as HttpMethod,
      path: this.extractPath(url),
      headers,
      query: this.extractQueryParams(url),
      params: {},
      body,
      rawBody,
    };
  }

  static wsMessageToGrpcPayload(message: any, route: Route): any {
    const payload: any = {};

    if (typeof message === 'string') {
      try {
        Object.assign(payload, JSON.parse(message));
      } catch {
        payload.data = message;
      }
    } else if (typeof message === 'object' && message !== null) {
      if ('payload' in message) {
        Object.assign(payload, message.payload);
      } else {
        Object.assign(payload, message);
      }
    }

    return payload;
  }
}
