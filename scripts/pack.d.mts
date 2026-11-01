// SPDX-License-Identifier: GPL-2.0-only
export interface PackOptions {
  root?: string;
  nativeBundle?: boolean;
  offline?: boolean;
  output?: string;
  keepStaging?: boolean;
}
export function pack(options?: PackOptions): Promise<{ output: string; staging: string }>;
