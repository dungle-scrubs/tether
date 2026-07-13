type ServiceAuthEnv = Readonly<Record<string, string | undefined>>;

/** Reads the first configured Tether service auth token from an explicit environment map. */
export function readServiceAuthToken(env: ServiceAuthEnv = readProcessEnv()): string | null {
  const serviceToken = readNonEmptyEnv(env.SERVICE_AUTH_TOKEN);
  if (serviceToken) {
    return serviceToken;
  }
  return readNonEmptyEnv(env.TETHER_AUTH_TOKEN);
}

/** Resolves an explicit token value, falling back to service environment names. */
export function resolveServiceAuthToken(
  authToken: string | null | undefined,
  env: ServiceAuthEnv = readProcessEnv(),
): string | null {
  if (authToken !== undefined) {
    return authToken?.trim() || null;
  }
  return readServiceAuthToken(env);
}

/** Reads process.env without requiring Node ambient types in downstream packages. */
function readProcessEnv(): ServiceAuthEnv {
  const maybeGlobal = globalThis as typeof globalThis & {
    readonly process?: { readonly env?: ServiceAuthEnv };
  };
  return maybeGlobal.process?.env ?? {};
}

/** Reads a trimmed environment value, treating blanks as omitted. */
function readNonEmptyEnv(value: string | undefined): string | null {
  const trimmed = value?.trim();
  return trimmed ? trimmed : null;
}
