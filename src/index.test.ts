/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { describe, it, expect } from 'vitest';
import fs from 'fs';
import path from 'path';

const cliPath = path.resolve(__dirname, '../build/index.js');

describe('CLI entry (build/index.js)', () => {
  it('should exist after build', () => {
    expect(fs.existsSync(cliPath)).toBe(true);
  });

  it('should be executable', () => {
    const stat = fs.statSync(cliPath);
    // Check owner execute bit
    expect(stat.mode & 0o100).toBeTruthy();
  });
});