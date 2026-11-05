// SPDX-License-Identifier: MIT
export interface NativeManifest {
  server: {type: 'binary'; entry_point: string; mcp_config: {command: string; args: string[]; env: Record<string,string>}};
  compatibility: {platforms: NodeJS.Platform[]};
  user_config: Record<string,{type: string; title: string; description: string; required?: boolean; default?: string | number | boolean}>;
  tools: Array<{name: string; description?: string}>;
  long_description: string;
}
export function nativeManifest(root: string, platform?: NodeJS.Platform, arch?: string, buildFeatures?: string): NativeManifest;
export function nativeBundleFiles(root: string): string[];
export function nativeBuildFeatures(value?: string): string[];
export function nativeBuildSource(root: string): {sha256: string; files: Record<string,string>};

export interface NativeArtifactOwner {path:string;dev:number;ino:number}
export function nativeArtifactOwner(root:string,prefix?:string):NativeArtifactOwner;
export function nativeArtifactCleanup(owner:NativeArtifactOwner):void;
