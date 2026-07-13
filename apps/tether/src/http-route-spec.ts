/** HTTP methods currently served by the Tether REST boundary. */
export type HttpRouteMethod = "DELETE" | "GET" | "PATCH" | "POST" | "PUT";

/** Named route spec used to keep path matching out of handler logic. */
export interface HttpRouteSpec<TName extends string = string> {
  /** Stable route name for diagnostics and tests. */
  readonly name: TName;
  /** Required HTTP method. */
  readonly method: HttpRouteMethod;
  /** Pathname matcher with capture groups for route params. */
  readonly pattern: RegExp;
}

/** Defines a typed HTTP route spec. */
export function defineHttpRoute<TName extends string>(
  spec: HttpRouteSpec<TName>,
): HttpRouteSpec<TName> {
  return spec;
}

/** Matches a request method and pathname against one route spec. */
export function matchHttpRoute(
  spec: HttpRouteSpec,
  method: string | undefined,
  pathname: string,
): RegExpMatchArray | null {
  if (method !== spec.method) {
    return null;
  }
  return pathname.match(spec.pattern);
}
