#!/usr/bin/env node
// Inspect the final local artifact. This command never connects to OBS.
import { execFileSync, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync, openSync, readSync, closeSync } from "node:fs";
import { resolve, parse } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

const recipe = JSON.parse(readFileSync(new URL("../docs/recipes/software-demo.v1.json", import.meta.url), "utf8"));
const numeric = (value) => typeof value === "string" && value.trim() !== "" ? Number(value) : NaN;
function fileHash(path) {
  const hash = createHash("sha256");
  const fd = openSync(path, "r");
  try {
    const buffer = Buffer.alloc(64 * 1024);
    let count;
    while ((count = readSync(fd, buffer)) > 0) hash.update(buffer.subarray(0, count));
    return hash.digest("hex");
  } finally { closeSync(fd); }
}
const frame = (path, at = 0) => execFileSync("ffmpeg", [
  "-v", "error", "-ss", String(at), "-i", path, "-frames:v", "1", "-vf", "scale=160:90,format=gray", "-f", "rawvideo", "pipe:1",
], { timeout: 15_000, maxBuffer: 1024 * 1024, stdio: ["ignore", "pipe", "pipe"] });

export function verifyDemo({ file, reference, seconds, chapters = [], checkpoints = [], decodeTimeoutMs = 60_000 }) {
  const problems = [];
  const unavailable = [];
  const frames = [];
  let sha256;
  let info;
  let maxVolumeDb;
  const checks = { metadata: "unavailable", strictDecode: "unavailable", sampledContent: "unavailable" };
  let format;
  try {
    if (!Number.isFinite(decodeTimeoutMs) || decodeTimeoutMs <= 0 || decodeTimeoutMs > 60_000) throw new Error("Decode timeout must be between 1 and 60000ms");
    sha256 = fileHash(file);
    info = JSON.parse(execFileSync("ffprobe", ["-v", "error", "-show_format", "-show_streams", "-show_chapters", "-of", "json", file], { timeout: 15_000, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }));
    if (!(Number.isFinite(seconds) && seconds > 0)) throw new Error("Expected duration must be a positive number");
    const duration = Number(info.format?.duration);
    if (!Number.isFinite(duration) || Math.abs(duration - seconds) > recipe.delivery.durationToleranceSeconds) problems.push("Duration is outside the expected tolerance");
    const video = info.streams?.find((s) => s.codec_type === "video");
    if (!video) problems.push("No video stream");
    format = video ? { codec: video.codec_name ?? "unknown", pixelFormat: video.pix_fmt ?? "unknown", colorTransfer: video.color_transfer ?? "unknown", colorPrimaries: video.color_primaries ?? "unknown", colorSpace: video.color_space ?? "unknown", colorFidelity: "unverified" } : undefined;
    checks.metadata = problems.length ? "fail" : "pass";
  } catch (error) { unavailable.push(error instanceof Error ? error.message : String(error)); }
  // Use spawnSync for measurements written on stderr.
  if (info) {
    // A metadata probe and a sampled frame do not establish full decodability.
    // spawnSync waits for child closure; SIGKILL bounds even a decoder ignoring TERM.
    const decoded = spawnSync("ffmpeg", ["-nostdin", "-v", "error", "-xerror", "-err_detect", "explode", "-i", file, "-map", "0:v:0", "-map", "0:a?", "-f", "null", "-"], {
      timeout: decodeTimeoutMs, killSignal: "SIGKILL", encoding: "utf8", maxBuffer: 1024 * 1024,
    });
    if (decoded.error) unavailable.push(`Strict decode unavailable: ${decoded.error.message}`);
    else if (decoded.status !== 0) {
      checks.strictDecode = "fail";
      problems.push("Strict full decode failed");
    } else checks.strictDecode = "pass";
    try {
      for (let track = 0; track < (info.streams ?? []).filter((s) => s.codec_type === "audio").length; track++) {
        const measured = measureAudio(file, track);
        maxVolumeDb = Math.max(maxVolumeDb ?? -Infinity, measured);
      }
      if (maxVolumeDb > recipe.delivery.maxSilentPeakDb) problems.push("Audio exceeds the silence threshold");
      for (const checkpoint of [{ at: 0, reference }, ...checkpoints]) {
        if (!checkpoint.reference || !Number.isFinite(checkpoint.at) || checkpoint.at < 0 || checkpoint.at >= Number(info.format?.duration)) throw new Error("Checkpoint requires a reference and a time within the recording");
        const actual = frame(file, checkpoint.at);
        const expected = frame(checkpoint.reference);
        if (actual.length !== 14400 || expected.length !== actual.length) throw new Error("Could not decode checkpoint frames");
        const error = actual.reduce((sum, value, i) => sum + Math.abs(value - expected[i]), 0) / actual.length;
        frames.push({ at: checkpoint.at, reference: resolve(checkpoint.reference), meanAbsoluteError: error });
        if (error > recipe.delivery.maxFrameMeanAbsoluteError) problems.push(`Frame at ${checkpoint.at}s differs from its reference`);
      }
      checks.sampledContent = frames.some(({ meanAbsoluteError }) => meanAbsoluteError > recipe.delivery.maxFrameMeanAbsoluteError) ? "fail" : "pass";
      if (chapters.length > 0) {
        let recorded = (info.chapters ?? []).map((c) => ({ name: c.tags?.title, at: Number(c.start_time) }));
        if (recorded.length === 0) {
          const p = parse(file);
          const sidecar = JSON.parse(readFileSync(resolve(p.dir, `${p.name}.take.json`), "utf8"));
          recorded = (sidecar.chapters ?? []).map((c) => ({ name: c.name, at: c.atSeconds }));
        }
        for (const name of chapters) if (!recorded.some((c) => c.name === name && Number.isFinite(c.at) && c.at >= 0 && c.at < Number(info.format?.duration))) problems.push(`Missing or invalid chapter: ${name}`);
      }
    } catch (error) { unavailable.push(error instanceof Error ? error.message : String(error)); }
  }
  if (sha256) {
    try { if (fileHash(file) !== sha256) problems.push("Artifact changed during verification"); }
    catch { unavailable.push("Artifact identity unavailable after verification"); }
  }
  return { recipe: `${recipe.id}/v${recipe.version}`, scope: "Metadata, bounded strict full decode and sampled grayscale similarity; expected content still requires visual review", contentVerification: "requires-review", checks, format, decodeTimeoutMs, file: resolve(file), sha256, status: problems.length ? "fail" : unavailable.length ? "unavailable" : "pass", expectedDurationSeconds: seconds, durationSeconds: Number(info?.format?.duration) || undefined, maxVolumeDb: maxVolumeDb === -Infinity ? null : maxVolumeDb, frames, problems, unavailable };
}

function measureAudio(file, track) {
  const result = spawnSync("ffmpeg", ["-hide_banner", "-nostats", "-i", file, "-map", `0:a:${track}`, "-af", "volumedetect", "-f", "null", "-"], { timeout: 60_000, encoding: "utf8" });
  if (result.error || result.status !== 0) throw new Error("Audio measurement unavailable");
  const match = /max_volume:\s*(-?[\d.]+|-inf) dB/.exec(result.stderr);
  if (!match) throw new Error("Audio measurement did not report a peak");
  return match[1] === "-inf" ? -Infinity : Number(match[1]);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const { values } = parseArgs({ options: { file: { type: "string" }, reference: { type: "string" }, seconds: { type: "string" }, chapter: { type: "string", multiple: true }, checkpoint: { type: "string", multiple: true }, report: { type: "string" } } });
  if (!values.file || !values.reference || !values.seconds) throw new Error("Usage: node scripts/verify-demo.mjs --file video --reference image --seconds duration [--chapter name] [--checkpoint seconds=image] [--report path]");
  const result = verifyDemo({ file: values.file, reference: values.reference, seconds: numeric(values.seconds), chapters: values.chapter, checkpoints: (values.checkpoint ?? []).map((value) => { const split = value.indexOf("="); return { at: numeric(value.slice(0, split)), reference: value.slice(split + 1) }; }) });
  const output = `${JSON.stringify(result, null, 2)}\n`;
  if (values.report) {
    if ([values.file, values.reference, ...(values.checkpoint ?? []).map((c) => c.slice(c.indexOf("=") + 1))].some((p) => resolve(p) === resolve(values.report))) throw new Error("Report must not overwrite an input artifact");
    writeFileSync(values.report, output, { flag: "wx" });
  }
  process.stdout.write(output);
  process.exitCode = result.status === "pass" ? 0 : 1;
}
