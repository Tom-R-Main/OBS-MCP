/*
 * SPDX-License-Identifier: MIT
 *
 * Pure signal checks for scenario tests. They judge what a recording actually
 * contains, independent of what OBS reported while it was being written.
 */

/** Level of one frequency in decibels relative to a full-scale sine. */
export function toneLevelDb(samples: Float32Array, sampleRate: number, frequency: number): number {
  if (samples.length === 0) return -Infinity;
  // Goertzel filter: the power of one DFT bin without a full FFT.
  const coefficient = 2 * Math.cos((2 * Math.PI * frequency) / sampleRate);
  let previous = 0;
  let beforePrevious = 0;
  for (const sample of samples) {
    const current = sample + coefficient * previous - beforePrevious;
    beforePrevious = previous;
    previous = current;
  }
  const power = previous * previous + beforePrevious * beforePrevious - coefficient * previous * beforePrevious;
  // A full-scale sine of N samples gives power (N/2)^2.
  const amplitude = (2 * Math.sqrt(Math.max(power, 0))) / samples.length;
  return amplitude > 0 ? 20 * Math.log10(amplitude) : -Infinity;
}

export function toneLevels(
  samples: Float32Array,
  sampleRate: number,
  frequencies: readonly number[],
): Record<number, number> {
  return Object.fromEntries(frequencies.map((frequency) => [frequency, toneLevelDb(samples, sampleRate, frequency)]));
}

/**
 * Start times, in seconds from the first sample, of bursts whose short-window
 * peak rises above `threshold` after at least `minGapSeconds` below it.
 */
export function audioOnsets(
  samples: Float32Array,
  sampleRate: number,
  { threshold = 0.1, windowSeconds = 0.002, minGapSeconds = 0.3 } = {},
): number[] {
  const window = Math.max(1, Math.round(windowSeconds * sampleRate));
  const onsets: number[] = [];
  let lastLoud = -Infinity;
  for (let start = 0; start < samples.length; start += window) {
    let peak = 0;
    const end = Math.min(samples.length, start + window);
    for (let index = start; index < end; index += 1) peak = Math.max(peak, Math.abs(samples[index]!));
    if (peak < threshold) continue;
    const time = start / sampleRate;
    if (time - lastLoud >= minGapSeconds) {
      // Refine to the first loud sample inside the window.
      let first = start;
      while (first < end && Math.abs(samples[first]!) < threshold) first += 1;
      onsets.push(first / sampleRate);
    }
    lastLoud = time;
  }
  return onsets;
}

export interface LumaFrame {
  time: number;
  luma: number;
}

/** Times of frames where luma first crosses halfway between the darkest and brightest frame. */
export function flashTimes(frames: readonly LumaFrame[], { minGapSeconds = 0.3, minContrast = 64 } = {}): number[] {
  if (frames.length === 0) return [];
  const lumas = frames.map(({ luma }) => luma);
  const low = Math.min(...lumas);
  const high = Math.max(...lumas);
  if (high - low < minContrast) return [];
  const midpoint = (low + high) / 2;
  const times: number[] = [];
  let wasBright = true; // ignore a flash already on screen at the first frame
  let lastFlash = -Infinity;
  for (const { time, luma } of frames) {
    const bright = luma >= midpoint;
    if (bright && !wasBright && time - lastFlash >= minGapSeconds) {
      times.push(time);
      lastFlash = time;
    }
    wasBright = bright;
  }
  return times;
}

export interface SyncMeasurement {
  /** Median of audio onset minus video flash, in seconds. Positive means audio is late. */
  medianOffset: number;
  /** Largest absolute offset of any pair. A median alone hides pairs that disagree. */
  worstOffset: number;
  pairs: number;
  spread: number;
}

/** Pairs each flash with the nearest beep within `maxGapSeconds` and summarizes audio minus video. */
export function measureSync(
  flashes: readonly number[],
  onsets: readonly number[],
  maxGapSeconds = 0.45,
): SyncMeasurement | undefined {
  const offsets: number[] = [];
  for (const flash of flashes) {
    let best: number | undefined;
    for (const onset of onsets) {
      const offset = onset - flash;
      if (Math.abs(offset) <= maxGapSeconds && (best === undefined || Math.abs(offset) < Math.abs(best))) best = offset;
    }
    if (best !== undefined) offsets.push(best);
  }
  if (offsets.length === 0) return undefined;
  const sorted = [...offsets].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  const medianOffset = sorted.length % 2 === 1 ? sorted[middle]! : (sorted[middle - 1]! + sorted[middle]!) / 2;
  const worstOffset = Math.max(...offsets.map(Math.abs));
  return { medianOffset, worstOffset, pairs: offsets.length, spread: sorted.at(-1)! - sorted[0]! };
}
