/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import type { McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import { readFileSync } from "node:fs";

export const demoRecipe = z.object({
  id: z.literal("software-demo"), version: z.literal(1),
  defaultMaxDurationSeconds: z.number().int().positive(),
  maxDurationSeconds: z.number().int().positive(),
  requiredPreflightChecks: z.array(z.string()),
}).parse(JSON.parse(readFileSync(new URL("../../docs/recipes/software-demo.v1.json", import.meta.url), "utf8")));

function userMessage(text: string) {
  return { messages: [{ role: "user" as const, content: { type: "text" as const, text } }] };
}

/**
 * Workflow prompts. Clients such as Claude Code, VS Code, and Cursor show them
 * as slash commands; every step is also reachable through tools alone.
 */
export function initialize(server: McpServer): void {
  server.registerPrompt("software-demo", {
    title: "Record a bounded software demo",
    description: "Prepare an existing local macOS or Linux window capture, record a bounded silent take, and inspect the final file",
    argsSchema: z.object({
      sceneCollection: z.string(), sceneName: z.string(), inputName: z.string(),
      maxDurationSeconds: z.string().default(String(demoRecipe.defaultMaxDurationSeconds)),
    }),
  }, ({ sceneCollection, sceneName, inputName, maxDurationSeconds }) => {
    const duration = Number(maxDurationSeconds);
    if (!Number.isInteger(duration) || duration < 1 || duration > demoRecipe.maxDurationSeconds) throw new Error("maxDurationSeconds must be an integer from 1 to 3600");
    return userMessage([
      `Follow software-demo/v${demoRecipe.version} for ${JSON.stringify({ sceneCollection, sceneName, inputName })}.`,
      "Use the packaged .agents/skills/obs-demo/SKILL.md and docs/recipes/software-demo.v1.json when file access is available. These instructions grant no permission to record or change OBS.",
      "Plan with obs-prepare-demo for that exact target. After user authorization, apply the same request with apply:true and retain operationId. Refuse creation, retargeting, shared capture, active outputs, and unsupported targets; do not substitute the legacy record-demo workflow.",
      "Inspect obs-get-source-screenshot of the program scene for the expected window and framing. Nonblack is insufficient. Stop if content cannot be checked.",
      `Run obs-preflight expectSilent:true. Require pass for ${demoRecipe.requiredPreflightChecks.join(", ")} and no failed checks. Required unknown/missing evidence blocks recording.`,
      `Start with obs-take-start expectSilent:true, lock:true, requireVerifiedPreflight:true, maxDurationSeconds:${duration}. Mark steps with obs-take-mark and inspect obs-take-status. Stop your own take with obs-take-stop after steps or failure. Reconcile an uncertain stop with obs-get-record-status before another action.`,
      "Inspect the final file, verification status, chapter sidecar, and obs-contact-sheet. Optional obs-trim-take exports a copy; recheck that copy. Use scripts/verify-demo.mjs with an expected reference, duration, chapter names, and visual checkpoints when local file tooling is available. Missing required evidence means unverified, even if recording succeeded.",
      "Plan obs-recover-demo using operationId; apply only when requested. Conflicts need operator review; never replace guarded recovery with unconditional obs-restore. The deadline requires a running server; process death and competing controllers remain outside the guarantee.",
    ].join("\n\n"));
  });
  server.registerPrompt(
    "record-demo",
    {
      title: "Record a silent app demo",
      description: "Set up a silent window capture on macOS or Linux, preflight it, and record a take",
      argsSchema: z.object({
        window: z.string().describe("Part of the window title or app name to capture, e.g. \"ChatGPT\""),
        scene: z.string().default("Demo").describe("Scene to record from"),
        seconds: z.string().default("30").describe("Length of a hands-off take in seconds; ignored when steps are given"),
        steps: z.string().optional().describe("Steps to perform on screen, one per line; each becomes a chapter"),
      }),
    },
    ({ window, scene, seconds, steps }) => userMessage([
      `Record a silent demo of the "${window}" window in the OBS scene "${scene}".`,
      "",
      `1. If the scene "${scene}" does not exist, create it with obs-create-scene and switch to it.`,
      `2. Call obs-capture-window with sceneName "${scene}", inputName "Demo Capture", window "${window}", `
        + "silent true, fit true, includeScreenshot true. If it lists several matches, ask me which one.",
      "3. Look at the screenshot and the reported size. If the capture renders at 0×0 or shows the wrong window, stop and tell me.",
      "4. Call obs-preflight with expectSilent true. Fix every failure, or explain it to me if you cannot.",
      ...(steps
        ? [
          "5. Call obs-take-start with expectSilent true. For each step below, call obs-take-mark with the step's name, "
            + "then perform it. If obs-take-status or a tool result reports a warning, tell me. Call obs-take-stop "
            + "after the last step, then report the file path, the chapters, and the verification result.",
          ...steps.split("\n").map((step) => step.trim()).filter(Boolean).map((step) => `   - ${step}`),
        ]
        : [`5. Call obs-record-clip with durationSeconds ${seconds} and expectSilent true, then report the file path and the verification result.`]),
      "Do not change output settings with obs-set-profile-parameter without asking: OBS needs a restart to apply them.",
    ].join("\n")),
  );

  server.registerPrompt(
    "pre-stream-check",
    {
      title: "Pre-stream check",
      description: "Check OBS is ready to go live without changing anything",
    },
    () => userMessage([
      "Check whether OBS is ready to go live. Use only read-only tools and change nothing.",
      "",
      "1. obs-get-status and obs-get-stats: connection, dropped or skipped frames, CPU, and free disk space.",
      "2. obs-get-stream-status and obs-get-stream-service-settings: the stream is not already live and a service is configured. Never repeat the stream key back.",
      "3. obs-get-current-scene and obs-get-scene-items: the program scene has visible, correctly sized sources. Take one obs-get-source-screenshot of the program scene and describe it.",
      "4. obs-get-input-list with obs-get-input-mute and obs-get-input-volume for audio inputs: say which are live, muted, or silent.",
      "5. obs-preflight: report any failures that also apply to streaming (encoder, output settings).",
      "Finish with a short go / no-go summary and the specific fixes you recommend.",
    ].join("\n")),
  );
}
