export function validateDependencyPolicy(
  manifest: Record<string, unknown>,
  lock: Record<string, unknown>,
): { packageCount: number };
export function checkDependencies(options?: { root?: string }): { packageCount: number };
