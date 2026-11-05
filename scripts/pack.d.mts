// SPDX-License-Identifier: MIT
export interface PackOptions {
  root?: string;
  nativeBundle?: boolean;
  offline?: boolean;
  output?: string;
  keepStaging?: boolean;
  nativeFeatures?: string;
}
export function pack(options?: PackOptions): Promise<{ output: string; staging: string }>;
