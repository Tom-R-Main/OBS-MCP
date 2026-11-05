/* SPDX-License-Identifier: MIT */
import { execFileSync } from "node:child_process";
import { join } from "node:path";
import { GAME_TONE_HZ, MIC_TONE_HZ } from "./scenario-media.js";

/** Stand-ins for a persona's camera, game, slides, and overlay art. Colors identify each one in a frame. */
export interface PersonaFixtures {
  camera: string;
  game: string;
  slides: [string, string, string];
  alert: string;
}

/** Colors as OBS renders them (RGB), for region checks. */
export const PERSONA_COLORS = {
  camera: [16, 64, 192],
  game: [16, 160, 64],
  slides: [[192, 48, 48], [192, 160, 32], [112, 48, 160]],
  alert: [255, 0, 255],
} as const;

function ffmpeg(args: string[]): void {
  execFileSync("ffmpeg", ["-v", "error", "-y", ...args], { stdio: ["ignore", "ignore", "pipe"], timeout: 60_000 });
}

const hex = ([r, g, b]: readonly number[]) => `0x${[r, g, b].map((value) => value!.toString(16).padStart(2, "0")).join("")}`;

/**
 * A moving box keeps each clip from reading as a frozen picture. `overlay`
 * evaluates its position every frame; `drawbox` evaluates `t` only once.
 */
function movingClip(file: string, size: string, rate: number, color: readonly number[], box: string, speed: string, toneHz: number, seconds: number): void {
  ffmpeg([
    "-f", "lavfi", "-i", `color=c=${hex(color)}:s=${size}:r=${rate}:d=${seconds}`,
    "-f", "lavfi", "-i", `color=c=${box}:s=80x80:r=${rate}:d=${seconds}`,
    "-f", "lavfi", "-i", `sine=frequency=${toneHz}:sample_rate=48000:duration=${seconds}`,
    "-filter_complex", `[0][1]overlay=${speed}`, "-map", "2:a", "-af", "volume=4",
    "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", "-shortest", file,
  ]);
}

export function generatePersonaFixtures(dir: string, seconds = 20): PersonaFixtures {
  const camera = join(dir, "camera.mp4");
  const game = join(dir, "game.mp4");
  movingClip(camera, "640x360", 30, PERSONA_COLORS.camera, "white", "x='mod(t*80,560)':y=140", MIC_TONE_HZ, seconds);
  movingClip(game, "1280x720", 60, PERSONA_COLORS.game, "yellow", "x='mod(t*400,1180)':y='mod(t*230,620)'", GAME_TONE_HZ, seconds);
  const slides = PERSONA_COLORS.slides.map((color, index) => {
    const file = join(dir, `slide${index + 1}.png`);
    ffmpeg(["-f", "lavfi", "-i", `color=c=${hex(color)}:s=1920x1080`, "-frames:v", "1", "-update", "1", file]);
    return file;
  }) as [string, string, string];
  const alert = join(dir, "alert.png");
  ffmpeg(["-f", "lavfi", "-i", `color=c=${hex(PERSONA_COLORS.alert)}:s=600x200`, "-frames:v", "1", "-update", "1", alert]);
  return { camera, game, slides, alert };
}

/** Mean RGB of a region (in output pixels) of the frame at `seconds`. */
export function regionColor(file: string, seconds: number, region: { x: number; y: number; width: number; height: number }): [number, number, number] {
  const pixel = execFileSync("ffmpeg", [
    "-v", "error", "-ss", String(seconds), "-i", file, "-frames:v", "1",
    "-vf", `crop=${region.width}:${region.height}:${region.x}:${region.y},scale=1:1:flags=area,format=rgb24`, "-f", "rawvideo", "-",
  ], { timeout: 30_000 });
  if (pixel.byteLength < 3) throw new Error(`No frame at ${seconds}s in ${file}`);
  return [pixel[0]!, pixel[1]!, pixel[2]!];
}

/** True when every channel is within `tolerance` of the expected color. */
export function colorNear(actual: readonly number[], expected: readonly number[], tolerance = 40): boolean {
  return actual.every((value, index) => Math.abs(value - expected[index]!) <= tolerance);
}

export function chapterTitles(file: string): { start: number; title: string }[] {
  const output = execFileSync("ffprobe", [
    "-v", "error", "-show_entries", "chapter=start_time:chapter_tags=title", "-of", "json", file,
  ], { encoding: "utf8", timeout: 30_000 });
  const { chapters = [] } = JSON.parse(output) as { chapters?: { start_time: string; tags?: { title?: string } }[] };
  return chapters.map(({ start_time, tags }) => ({ start: Number(start_time), title: tags?.title ?? "" }));
}
