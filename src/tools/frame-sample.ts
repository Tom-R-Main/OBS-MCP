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
/**
 * A frame changed when at least CHANGED_PIXELS pixels moved by more than
 * CHANGE_LUMA levels. Tuned on a 1080p screen recording sampled 96px wide:
 * streaming text changes a few pixels strongly, while encoder noise and a
 * blinking cursor change many pixels slightly. A mean difference missed
 * streaming text entirely.
 */
const CHANGE_LUMA = 12;
const CHANGED_PIXELS = 2;

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
  if (a.width !== b.width || a.height !== b.height) return false;
  return isSameLuma(a.luma, b.luma);
}

/** isSameFrame for two equally sized luma buffers. */
export function isSameLuma(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length === 0 || a.length !== b.length) return false;
  let changed = 0;
  for (let index = 0; index < a.length; index++) {
    if (Math.abs((a[index] ?? 0) - (b[index] ?? 0)) > CHANGE_LUMA && ++changed >= CHANGED_PIXELS) return false;
  }
  return true;
}

export type StillSpan = { start: number; end: number };

/** Samples compared on each side of a moment: the median of three ignores a one-sample blip. */
const STEP_WINDOW = 3;

function median3(a: number, b: number, c: number): number {
  return Math.max(Math.min(a, b), Math.min(Math.max(a, b), c));
}

/**
 * Finds stretches where the picture holds still, from samples pushed in time
 * order. The picture changes at a moment when, for at least CHANGED_PIXELS
 * pixels, the median of the three samples before it and the median of the
 * three after it differ by more than CHANGE_LUMA. Text that appears and stays
 * is such a step; a shimmering "thinking" label, a spinner, or a blinking
 * cursor is a blip, so waiting on one still counts as still. Decisions lag
 * the newest sample by three samples.
 */
export class StillTracker {
  readonly spans: StillSpan[] = [];
  private samples: Sample[] = [];
  private stillFrom: number | null = null;
  private lastTime: number | null = null;

  constructor(private readonly minStillSeconds: number) {}

  /** Start of the stretch the picture has held still since, once it is long enough to report. */
  stillSince(): number | null {
    return this.stillFrom !== null && this.lastTime !== null && this.lastTime - this.stillFrom >= this.minStillSeconds
      ? this.stillFrom
      : null;
  }

  push(time: number, luma: Uint8Array): void {
    if (this.samples.length > 0 && this.samples[0]!.luma.length !== luma.length) this.samples = [];
    this.stillFrom ??= time;
    this.lastTime = time;
    this.samples.push({ time, luma });
    if (this.samples.length < STEP_WINDOW * 2) return;

    const [b0, b1, b2, a0, a1, a2] = this.samples as [Sample, Sample, Sample, Sample, Sample, Sample];
    const before = medianLuma(b0.luma, b1.luma, b2.luma);
    const after = medianLuma(a0.luma, a1.luma, a2.luma);
    if (!isSameLuma(before, after)) {
      // The medians place the step within a sample or so; pin it to the first
      // sample that left the old picture and the first that shows the new one.
      const changedAt = [b1, b2, a0, a1].find((sample) => !isSameLuma(before, sample.luma))?.time ?? a0.time;
      let settled = a0;
      for (const sample of [b2, b1]) {
        if (!isSameLuma(after, sample.luma)) break;
        settled = sample;
      }
      this.close(changedAt);
      this.stillFrom = Math.max(changedAt, settled.time);
    }
    this.samples.shift();
  }

  /** Ends the current stretch at `time`, e.g. at the end of the video or a scene switch. */
  finish(time: number): StillSpan[] {
    this.close(time);
    this.stillFrom = null;
    this.samples = [];
    return this.spans;
  }

  private close(time: number): void {
    if (this.stillFrom !== null && time - this.stillFrom >= this.minStillSeconds) {
      this.spans.push({ start: this.stillFrom, end: time });
    }
  }
}

type Sample = { time: number; luma: Uint8Array };

function medianLuma(a: Uint8Array, b: Uint8Array, c: Uint8Array): Uint8Array {
  const median = new Uint8Array(a.length);
  for (let index = 0; index < a.length; index++) median[index] = median3(a[index] ?? 0, b[index] ?? 0, c[index] ?? 0);
  return median;
}

/** Builds a binary PPM filled with one color, or with a color per pixel. For tests and fakes. */
export function encodePpm(width: number, height: number, color: (pixel: number) => [number, number, number]): Buffer {
  const header = Buffer.from(`P6\n${width} ${height}\n255\n`, "ascii");
  const pixels = Buffer.alloc(width * height * 3);
  for (let pixel = 0; pixel < width * height; pixel++) pixels.set(color(pixel), pixel * 3);
  return Buffer.concat([header, pixels]);
}
