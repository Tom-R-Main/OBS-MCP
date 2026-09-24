/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import type { McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";

function userMessage(text: string) {
  return { messages: [{ role: "user" as const, content: { type: "text" as const, text } }] };
}

/**
 * Workflow prompts. Clients such as Claude Code, VS Code, and Cursor show them
 * as slash commands; every step is also reachable through tools alone.
 */
export function initialize(server: McpServer): void {
  server.registerPrompt(
    "record-demo",
    {
      title: "Record a silent app demo",
      description: "Set up a silent, window-only capture on macOS, preflight it, and record a take",
      argsSchema: z.object({
        window: z.string().describe("Part of the window title or app name to capture, e.g. \"ChatGPT\""),
        scene: z.string().default("Demo").describe("Scene to record from"),
        seconds: z.string().default("30").describe("Length of the take in seconds (up to 50 in one call)"),
      }),
    },
    ({ window, scene, seconds }) => userMessage([
      `Record a silent demo of the "${window}" window in the OBS scene "${scene}".`,
      "",
      `1. If the scene "${scene}" does not exist, create it with obs-create-scene and switch to it.`,
      `2. Call obs-capture-window with sceneName "${scene}", inputName "Demo Capture", window "${window}", `
        + "silent true, fit true, includeScreenshot true. If it lists several matches, ask me which one.",
      "3. Look at the screenshot and the reported size. If the capture renders at 0×0 or shows the wrong window, stop and tell me.",
      "4. Call obs-preflight with expectSilent true. Fix every failure, or explain it to me if you cannot.",
      `5. Call obs-record-clip with durationSeconds ${seconds} and expectSilent true, then report the file path and the verification result.`,
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
