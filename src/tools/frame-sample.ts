/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */

/**
 * A tiny grayscale copy of a frame. OBS can return screenshots as binary PPM
 * (Qt's "ppm" format), which decodes without an image library.
 */
export type FrameSample = { width: number; height: number; luma: Uint8Array };

/** Brightest pixel below this means the frame is black. */
const BLANK_MAX_LUMA = 16;
/** Mean per-pixel change below this means the frame did not change. */
const STILL_MEAN_DELTA = 1;

/** Decodes a binary (P6) PPM, as a buffer or a base64 data URI, into luma. */
export function decodePpm(input: Buffer | string): FrameSample {
  const buffer = typeof input === "string"
    ? Buffer.from(input.slice(input.indexOf(",") + 1), "base64")
    : input;

  // Header: "P6", width, height, maxval, separated by whitespace and comments,
  // then exactly one whitespace byte before the pixels.
  const fields: number[] = [];
  let offset = 0;
  const magic = buffer.subarray(0, 2).toString("ascii");
  if (magic !== "P6") throw new Error(`Not a binary PPM (starts with ${JSON.stringify(magic)})`);
  offset = 2;
  while (fields.length < 3) {
    const byte = buffer[offset];
    if (byte === undefined) throw new Error("Truncated PPM header");
    if (byte === 0x23) { // "#": comment to end of line
      while (buffer[offset] !== undefined && buffer[offset] !== 0x0a) offset++;
    } else if (byte >= 0x30 && byte <= 0x39) {
      let end = offset;
      while ((buffer[end] ?? 0) >= 0x30 && (buffer[end] ?? 0) <= 0x39) end++;
      fields.push(Number(buffer.subarray(offset, end).toString("ascii")));
      offset = end;
      continue;
    }
    offset++;
  }
  offset++; // the single whitespace byte after maxval

  const [width = 0, height = 0, maxValue = 255] = fields;
  const bytesPerSample = maxValue > 255 ? 2 : 1;
  const pixels = width * height;
  if (buffer.length < offset + pixels * 3 * bytesPerSample) throw new Error("Truncated PPM pixel data");

  const luma = new Uint8Array(pixels);
  const read = bytesPerSample === 1
    ? (index: number) => buffer[offset + index] ?? 0
    : (index: number) => buffer.readUInt16BE(offset + index * 2);
  const scale = 255 / maxValue;
  for (let pixel = 0; pixel < pixels; pixel++) {
    const r = read(pixel * 3), g = read(pixel * 3 + 1), b = read(pixel * 3 + 2);
    luma[pixel] = Math.round((0.2126 * r + 0.7152 * g + 0.0722 * b) * scale);
  }
  return { width, height, luma };
}

export function isBlank(sample: FrameSample): boolean {
  return sample.luma.every((value) => value < BLANK_MAX_LUMA);
}

export function isSameFrame(a: FrameSample, b: FrameSample): boolean {
  if (a.width !== b.width || a.height !== b.height || a.luma.length === 0) return false;
  let total = 0;
  for (let index = 0; index < a.luma.length; index++) total += Math.abs((a.luma[index] ?? 0) - (b.luma[index] ?? 0));
  return total / a.luma.length < STILL_MEAN_DELTA;
}

/** Builds a binary PPM filled with one color, or with a color per pixel. For tests and fakes. */
export function encodePpm(width: number, height: number, color: (pixel: number) => [number, number, number]): Buffer {
  const header = Buffer.from(`P6\n${width} ${height}\n255\n`, "ascii");
  const pixels = Buffer.alloc(width * height * 3);
  for (let pixel = 0; pixel < width * height; pixel++) pixels.set(color(pixel), pixel * 3);
  return Buffer.concat([header, pixels]);
}
