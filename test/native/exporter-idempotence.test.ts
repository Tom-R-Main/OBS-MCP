/* SPDX-License-Identifier: GPL-2.0-only */
import { execFileSync } from "node:child_process";
import { readFileSync, statSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";

it("preserves embedded asset bytes and modification times across unchanged actual SDK exports", () => {
  const root=fileURLToPath(new URL("../../",import.meta.url));
  for(const [script,asset] of [["scripts/export-native-contract.mjs","native/assets/tools.json"],["scripts/export-native-prompts.mjs","native/assets/prompts.json"]] as const) {
    const path=fileURLToPath(new URL(`../../${asset}`,import.meta.url));
    const before=readFileSync(path);const stamp=statSync(path,{bigint:true}).mtimeNs;
    const first=execFileSync(process.execPath,[script],{cwd:root,encoding:"utf8"});
    const second=execFileSync(process.execPath,[script],{cwd:root,encoding:"utf8"});
    expect(first).toBe(second);expect(readFileSync(path)).toEqual(before);expect(statSync(path,{bigint:true}).mtimeNs).toBe(stamp);
  }
});
