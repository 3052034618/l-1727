import { Protocol, HttpMethod, Route, RequestContext, PathParams } from './types';
import { RouteRegistry } from './route-registry';

export class UnifiedRouter {
  private registry: RouteRegistry;

  constructor(registry: RouteRegistry) {
    this.registry = registry;
  }

  match(ctx: RequestContext): Route | null {
    const paramsOut: { params: Record<string, string> } = { params: {} };
    const ruleIdOut: { id?: string } = {};
    const targetNameOut: { name?: string } = {};

    const route = this.registry.matchRoute(
      ctx.protocol,
      ctx.method,
      ctx.path,
      paramsOut,
      ruleIdOut,
      targetNameOut
    );

    if (route) {
      ctx.params = paramsOut.params;
      ctx.matchedRuleId = ruleIdOut.id;
      ctx.grayTargetName = targetNameOut.name;
    }

    return route;
  }

  findRouteByServiceMethod(serviceName: string, methodName: string): Route | null {
    return this.registry.findRouteByServiceMethod(serviceName, methodName);
  }

  getAllRoutes(): Route[] {
    return this.registry.getCompiledRoutes();
  }
}
