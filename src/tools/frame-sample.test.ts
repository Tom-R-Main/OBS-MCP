/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { describe, expect, it } from "vitest";
import { decodePpm, encodePpm, isBlank, isSameFrame, StillTracker } from "./frame-sample.js";

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

  it("ignores slight changes everywhere but sees a few strongly changed pixels", () => {
    const page = decodePpm(encodePpm(96, 54, () => [30, 30, 30]));
    const noisy = decodePpm(encodePpm(96, 54, (pixel) => (pixel % 2 ? [36, 36, 36] : [26, 26, 26])));
    const typed = decodePpm(encodePpm(96, 54, (pixel) => (pixel === 500 || pixel === 501 ? [200, 200, 200] : [30, 30, 30])));
    const oneSpeck = decodePpm(encodePpm(96, 54, (pixel) => (pixel === 500 ? [200, 200, 200] : [30, 30, 30])));

    expect(isSameFrame(page, noisy)).toBe(true);
    expect(isSameFrame(page, typed)).toBe(false);
    expect(isSameFrame(page, oneSpeck)).toBe(true);
  });
});

describe("StillTracker", () => {
  const size = 96 * 54;
  const frame = (lit: number[] = []) => {
    const luma = new Uint8Array(size).fill(20);
    for (const pixel of lit) luma[pixel] = 220;
    return luma;
  };

  it("treats a shimmer that comes and goes as still", () => {
    const tracker = new StillTracker(3);
    for (let second = 0; second < 10; second++) {
      // A highlight sweeps along a label: different pixels each sample, none for long.
      tracker.push(second, frame([100 + (second % 4) * 3, 101 + (second % 4) * 3]));
    }

    expect(tracker.finish(10)).toEqual([{ start: 0, end: 10 }]);
  });

  it("ends a still stretch when text appears and stays", () => {
    const tracker = new StillTracker(3);
    const text: number[] = [];
    for (let second = 0; second < 6; second++) tracker.push(second, frame());
    for (let second = 6; second < 12; second++) {
      text.push(200 + second * 2, 201 + second * 2);
      tracker.push(second, frame(text));
    }
    for (let second = 12; second < 18; second++) tracker.push(second, frame(text));

    expect(tracker.finish(18)).toEqual([{ start: 0, end: 6 }, { start: 11, end: 18 }]);
  });

  it("reports the stretch in progress once it is long enough", () => {
    const tracker = new StillTracker(3);
    tracker.push(0, frame());
    tracker.push(2, frame());
    expect(tracker.stillSince()).toBeNull();
    tracker.push(3, frame());
    expect(tracker.stillSince()).toBe(0);
  });
});
