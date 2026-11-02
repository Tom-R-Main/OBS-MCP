// SPDX-License-Identifier: GPL-2.0-only
export interface NativeManifest {
  server: {type: 'binary'; entry_point: string; mcp_config: {command: string; args: string[]; env: Record<string,string>}};
  compatibility: {platforms: NodeJS.Platform[]};
  user_config: Record<string,{type: string; title: string; description: string; required?: boolean; default?: string | number | boolean}>;
  tools: Array<{name: string; description?: string}>;
  long_description: string;
}
export function nativeManifest(root: string, platform?: NodeJS.Platform, arch?: string): NativeManifest;
export function nativeBundleFiles(root: string): string[];
