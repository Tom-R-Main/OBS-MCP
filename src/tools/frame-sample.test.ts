/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { describe, expect, it } from "vitest";
import { decodePpm, encodePpm, isBlank, isSameFrame } from "./frame-sample.js";

const dataUri = (buffer: Buffer) => `data:image/ppm;base64,${buffer.toString("base64")}`;

describe("frame samples", () => {
  it("decodes a binary PPM from a data URI, including header comments", () => {
    const pixels = Buffer.from([255, 0, 0, 0, 255, 0, 0, 0, 255, 255, 255, 255]);
    const ppm = Buffer.concat([Buffer.from("P6\n# made by a test\n2 2\n255\n"), pixels]);

    const sample = decodePpm(dataUri(ppm));

    expect(sample).toMatchObject({ width: 2, height: 2 });
    expect([...sample.luma]).toEqual([54, 182, 18, 255]);
  });

  it("decodes 16-bit PPMs", () => {
    const ppm = Buffer.concat([Buffer.from("P6 1 1 65535\n"), Buffer.from([0xff, 0xff, 0xff, 0xff, 0xff, 0xff])]);

    expect([...decodePpm(ppm).luma]).toEqual([255]);
  });

  it("rejects other formats and truncated data", () => {
    expect(() => decodePpm(Buffer.from("\x89PNG...."))).toThrow("Not a binary PPM");
    expect(() => decodePpm(Buffer.from("P6\n4 4\n255\n\x00\x00"))).toThrow("Truncated");
  });

  it("recognizes black frames and unchanged frames", () => {
    const black = decodePpm(encodePpm(4, 4, () => [8, 8, 8]));
    const page = decodePpm(encodePpm(4, 4, (pixel) => (pixel % 2 ? [240, 240, 240] : [20, 20, 20])));
    const scrolled = decodePpm(encodePpm(4, 4, (pixel) => (pixel % 2 ? [20, 20, 20] : [240, 240, 240])));

    expect(isBlank(black)).toBe(true);
    expect(isBlank(page)).toBe(false);
    expect(isSameFrame(page, decodePpm(encodePpm(4, 4, (pixel) => (pixel % 2 ? [240, 240, 240] : [20, 20, 20]))))).toBe(true);
    expect(isSameFrame(page, scrolled)).toBe(false);
  });
});
