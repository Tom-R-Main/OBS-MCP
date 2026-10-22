import { execFileSync, spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { verifyDemo } from "../../scripts/verify-demo.mjs";
import { demoRecipe } from "../../src/tools/prompts.js";
import { startMcpHarness } from "../support/mcp-harness.js";

const hasMedia = !spawnSync("ffmpeg", ["-version"]).error && !spawnSync("ffprobe", ["-version"]).error;
describe("software demo discovery", () => {
  it("resolves every recipe tool through MCP and renders the bounded prompt", async () => {
    const harness = await startMcpHarness();
    try {
      const recipe = JSON.parse(readFileSync("docs/recipes/software-demo.v1.json", "utf8")) as { tools: string[] };
      const names = new Set((await harness.mcpClient.listTools()).tools.map((tool) => tool.name));
      expect(recipe.tools.filter((name) => !names.has(name))).toEqual([]);
      expect(demoRecipe.version).toBe(1);
      const prompt = await harness.mcpClient.getPrompt({ name: "software-demo", arguments: { sceneCollection: "Test", sceneName: "Demo", inputName: "Chrome" } });
      expect(JSON.stringify(prompt)).toContain("maxDurationSeconds:120");
      expect(JSON.stringify(prompt)).toContain("do not substitute the legacy record-demo");
      await expect(harness.mcpClient.getPrompt({ name: "software-demo", arguments: { sceneCollection: "Test", sceneName: "Demo", inputName: "Chrome", maxDurationSeconds: "NaN" } })).rejects.toThrow();
    } finally { await harness.close(); }
  });
});

describe.skipIf(!hasMedia)("final recording artifact verification (synthetic media, no OBS)", () => {
  let dir: string;
  let video: string;
  let reference: string;
  let wrong: string;
  let stepTwo: string;
  beforeAll(() => {
    dir = mkdtempSync(join(tmpdir(), "obs-demo-media-"));
    video = join(dir, "demo.mp4"); reference = join(dir, "expected.png"); wrong = join(dir, "wrong.png");
    stepTwo = join(dir, "step-two.png");
    execFileSync("ffmpeg", ["-v", "error", "-f", "lavfi", "-i", "color=white:s=160x90:r=10:d=0.5", "-f", "lavfi", "-i", "color=blue:s=160x90:r=10:d=0.5", "-filter_complex", "[0:v][1:v]concat=n=2:v=1:a=0[out]", "-map", "[out]", "-c:v", "mpeg4", video]);
    for (const [color, file] of [["white", reference], ["red", wrong], ["blue", stepTwo]]) execFileSync("ffmpeg", ["-v", "error", "-f", "lavfi", "-i", `color=${color}:s=160x90`, "-frames:v", "1", file!]);
    writeFileSync(join(dir, "demo.take.json"), JSON.stringify({ chapters: [{ name: "Intro", atSeconds: 0 }] }));
  });
  afterAll(() => rmSync(dir, { recursive: true, force: true }));
  it("checks final file identity, duration, reference frame, and sidecar chapter", () => {
    const result = verifyDemo({ file: video, reference, seconds: 1, chapters: ["Intro"], checkpoints: [{ at: 0.75, reference: stepTwo }] });
    expect(result.status).toBe("pass"); expect(result.sha256).toMatch(/^[a-f0-9]{64}$/);
  });
  it("runs the public verifier CLI and writes a new report without overwriting artifacts", () => {
    const report = join(dir, "verification.json");
    const args = ["scripts/verify-demo.mjs", "--file", video, "--reference", reference, "--seconds", "1", "--chapter", "Intro", "--checkpoint", `0.75=${stepTwo}`, "--report", report];
    const result = spawnSync(process.execPath, args, { encoding: "utf8" });
    expect(result.status, result.stderr).toBe(0);
    expect(JSON.parse(readFileSync(report, "utf8"))).toMatchObject({ status: "pass", contentVerification: "requires-review" });
    const again = spawnSync(process.execPath, args, { encoding: "utf8" });
    expect(again.status).not.toBe(0);
    const refusesInput = spawnSync(process.execPath, [...args.slice(0, -2), "--report", video], { encoding: "utf8" });
    expect(refusesInput.status).not.toBe(0);
    expect(verifyDemo({ file: video, reference, seconds: 1 }).status).toBe("pass");
  });
  it("checks the later state rather than accepting a matching first frame", () => {
    expect(verifyDemo({ file: video, reference, seconds: 1, checkpoints: [{ at: 0.75, reference }] }).status).toBe("fail");
  });
  it("rejects a wrong nonblack frame and missing chapter", () => {
    expect(verifyDemo({ file: video, reference: wrong, seconds: 1 }).status).toBe("fail");
    expect(verifyDemo({ file: video, reference, seconds: 1, chapters: ["Absent"] }).status).toBe("fail");
  });
  it("does not call absent evidence verified", () => {
    expect(verifyDemo({ file: video, reference: join(dir, "missing.png"), seconds: 1 }).status).toBe("unavailable");
  });
  it("rejects audible audio in the final file", () => {
    const audible = join(dir, "audible.mp4");
    execFileSync("ffmpeg", ["-v", "error", "-i", video, "-f", "lavfi", "-i", "sine=frequency=440:duration=1", "-c:v", "copy", "-c:a", "aac", "-shortest", audible]);
    expect(verifyDemo({ file: audible, reference, seconds: 1 }).problems).toContain("Audio exceeds the silence threshold");
  });
});
