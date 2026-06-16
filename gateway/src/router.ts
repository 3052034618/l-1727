import { Protocol, HttpMethod, Route, RequestContext, PathParams } from './types';

interface CompiledRoute {
  route: Route;
  regex: RegExp;
  paramNames: string[];
}

export class UnifiedRouter {
  private compiledRoutes: CompiledRoute[] = [];

  constructor(routes: Route[]) {
    for (const route of routes) {
      const { regex, paramNames } = this.compilePattern(route.pattern);
      this.compiledRoutes.push({ route, regex, paramNames });
    }
  }

  match(ctx: RequestContext): Route | null {
    for (const compiled of this.compiledRoutes) {
      const { route, regex, paramNames } = compiled;

      if (route.sourceProtocol !== ctx.protocol) {
        continue;
      }

      if (ctx.protocol === Protocol.HTTP || ctx.protocol === Protocol.WEBSOCKET) {
        if (!this.matchHttpRoute(route, ctx, regex, paramNames)) {
          continue;
        }
        return route;
      }

      if (ctx.protocol === Protocol.GRPC) {
        if (this.matchGrpcRoute(route, ctx)) {
          return route;
        }
      }
    }
    return null;
  }

  private matchHttpRoute(
    route: Route,
    ctx: RequestContext,
    regex: RegExp,
    paramNames: string[]
  ): boolean {
    if (route.methods.length > 0 && !route.methods.includes(ctx.method)) {
      return false;
    }

    const match = ctx.path.match(regex);
    if (!match) {
      return false;
    }

    const params: PathParams = {};
    for (let i = 0; i < paramNames.length; i++) {
      params[paramNames[i]] = match[i + 1];
    }
    ctx.params = params;
    return true;
  }

  private matchGrpcRoute(route: Route, ctx: RequestContext): boolean {
    const grpcPath = `/${route.serviceName}/${route.methodName}`;
    return ctx.path === grpcPath;
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

  findRouteByServiceMethod(serviceName: string, methodName: string): Route | null {
    for (const compiled of this.compiledRoutes) {
      if (
        compiled.route.serviceName === serviceName &&
        compiled.route.methodName === methodName
      ) {
        return compiled.route;
      }
    }
    return null;
  }
}
