/* SPDX-License-Identifier: GPL-2.0-only */
import { spawn } from "node:child_process";

type ProcessOptions = { signal?: AbortSignal; timeoutMs?: number };
export const RECORDING_FORMATS = "matroska,webm,mov,mp4,m4a,3gp,3g2,mj2,wav,avi,flv,mpegts,ogg,nut,yuv4mpegpipe";
/** Apply immediately before EACH input. Metadata/images are private work files. */
export function localMediaInputOptions(kind: "recording" | "metadata" | "images" = "recording"): string[] {
  const format = kind === "recording" ? RECORDING_FORMATS
    : kind === "metadata" ? "ffmetadata"
    : kind === "images" ? "image2" : undefined;
  if (!format) throw new Error("Unqualified local media input kind");
  return ["-protocol_whitelist", "file,pipe", "-format_whitelist", format];
}

/** Wait for close even on cancellation: an aborted promise must not orphan a decoder. */
export function runBoundedMediaProcess(
  command: string,
  args: string[],
  options: ProcessOptions = {},
): Promise<void> {
  return runProcess(command, args, options);
}

/** The consumer must retain bounded state and must not perform external effects. */
export function runStreamingMediaProcess(
  command: string,
  args: string[],
  consume: (chunk: Buffer) => void,
  options: ProcessOptions = {},
): Promise<void> {
  return runProcess(command, args, options, consume);
}

function runProcess(command: string, args: string[], { signal, timeoutMs = 60_000 }: ProcessOptions, consume?: (chunk: Buffer) => void): Promise<void> {
  signal?.throwIfAborted();
  return new Promise((resolve, reject) => {
    const child = consume
      ? spawn(command, args, { stdio: ["ignore", "pipe", "pipe"] })
      : spawn(command, args, { stdio: ["ignore", "ignore", "pipe"] });
    let stderr = "";
    let failure: Error | undefined;
    let escalation: NodeJS.Timeout | undefined;
    let drain: NodeJS.Timeout | undefined;
    let exited = false;
    const stop = (reason: Error) => {
      if (failure) return;
      failure = reason;
      if (!exited) {
        child.kill("SIGTERM");
        escalation = setTimeout(() => { if (!exited) child.kill("SIGKILL"); }, 500);
      }
    };
    const abort = () => stop(new Error("Media verification cancelled"));
    const timer = setTimeout(() => stop(new Error("Media verification timed out")), timeoutMs);
    child.stderr.on("data", (chunk: Buffer) => { stderr = (stderr + chunk.toString()).slice(-4000); });
    child.stderr.on("error", (error) => stop(new Error(`Media stderr read failed: ${error.message}`)));
    child.stdout?.on("error", (error) => stop(new Error(`Media stdout read failed: ${error.message}`)));
    child.stdout?.on("data", (chunk: Buffer) => {
      if (!failure && consume) {
        try { consume(chunk); }
        catch (error) { stop(error instanceof Error ? error : new Error(String(error))); }
      }
    });
    child.on("error", (error) => { failure ??= error; });
    child.on("exit", (code, closedBy) => {
      exited = true;
      if (escalation) clearTimeout(escalation);
      // A descendant may hold a pipe after the direct child was reaped. Bound
      // our drain and report uncertainty; never signal that now-reusable PID.
      drain = setTimeout(() => {
        const primary = failure ?? (code !== 0 ? new Error(`Media decode failed (${code ?? closedBy}): ${stderr.trim()}`) : undefined);
        const issue = "cleanup unconfirmed: media pipe did not close after direct child exit";
        failure = new Error(primary ? `${primary.message}; ${issue}` : `Media verification ${issue}`);
        child.stdout?.destroy();
        child.stderr.destroy();
      }, 500);
    });
    child.on("close", (code, closedBy) => {
      clearTimeout(timer);
      if (escalation) clearTimeout(escalation);
      if (drain) clearTimeout(drain);
      signal?.removeEventListener("abort", abort);
      if (failure) reject(failure);
      else if (code !== 0) reject(new Error(`Media decode failed (${code ?? closedBy}): ${stderr.trim()}`));
      else resolve();
    });
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) abort();
  });
}

/** Same s16 peak and one-decimal projection as FFmpeg af_volumedetect. */
export async function measurePcmPeak(command: string, args: string[], options: ProcessOptions = {}): Promise<number | undefined> {
  let trailing: number | undefined;
  let peak = 0;
  let samples = 0;
  const sample = (low: number, high: number) => {
    const encoded = low | (high << 8);
    const signed = encoded >= 0x8000 ? encoded - 0x10000 : encoded;
    peak = Math.max(peak, Math.abs(signed));
    samples += 1;
    if (!Number.isSafeInteger(samples)) throw new Error("PCM sample count exhausted");
  };
  await runStreamingMediaProcess(command, args, (chunk) => {
    let offset = 0;
    if (trailing !== undefined && chunk.length > 0) {
      sample(trailing, chunk[0]!);
      trailing = undefined;
      offset = 1;
    }
    while (offset + 1 < chunk.length) { sample(chunk[offset]!, chunk[offset + 1]!); offset += 2; }
    if (offset < chunk.length) trailing = chunk[offset];
  }, options);
  if (trailing !== undefined) throw new Error("Incomplete s16 PCM sample at EOF");
  if (samples === 0) return undefined;
  const level = peak === 0 ? -91 : -(-Math.log10((peak * peak) / (0x8000 * 0x8000)) * 10);
  const projected = Number(level.toFixed(1));
  return projected === 0 ? 0 : projected;
}

/** Audio evidence comes from PCM, never filenames or metadata in diagnostics. */
export function measureAudioPeak(path: string, track: number, options: ProcessOptions = {}): Promise<number | undefined> {
  return measurePcmPeak("ffmpeg", [
    "-v", "error", "-hide_banner", "-nostats", ...localMediaInputOptions(),
    "-i", path, "-map", `0:a:${track}`,
    "-c:a", "pcm_s16le", "-f", "s16le", "-",
  ], options);
}

export function strictDecode(path: string, signal?: AbortSignal): Promise<void> {
  return runBoundedMediaProcess("ffmpeg", [
    "-nostdin", "-v", "error", "-xerror", "-err_detect", "explode", ...localMediaInputOptions(), "-i", path,
    "-map", "0:v:0", "-map", "0:a?", "-f", "null", "-",
  ], { signal });
}
