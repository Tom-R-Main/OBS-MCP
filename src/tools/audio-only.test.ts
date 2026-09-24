/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { describe, expect, it } from "vitest";
import { rendersNoVideo } from "./audio-only.js";

describe("rendersNoVideo", () => {
  it.each([
    "coreaudio_input_capture", "coreaudio_output_capture", "sck_audio_capture",
    "wasapi_input_capture", "wasapi_output_capture", "wasapi_process_output_capture",
    "pulse_input_capture", "pulse_output_capture", "alsa_input_capture", "jack_output_capture",
  ])("treats %s as audio-only", (kind) => {
    expect(rendersNoVideo(kind)).toBe(true);
  });

  it.each(["screen_capture", "window_capture", "display_capture", "game_capture", "av_capture_input", "browser_source", "image_source"])(
    "treats %s as visual",
    (kind) => expect(rendersNoVideo(kind)).toBe(false),
  );

  it("treats a media source as audio-only only when it plays an audio file", () => {
    expect(rendersNoVideo("ffmpeg_source", { local_file: "/music/Tone.WAV" })).toBe(true);
    expect(rendersNoVideo("vlc_source", { local_file: "/clips/intro.m4a" })).toBe(true);
    expect(rendersNoVideo("ffmpeg_source", { local_file: "/clips/intro.mp4" })).toBe(false);
    expect(rendersNoVideo("ffmpeg_source")).toBe(false);
  });
});
