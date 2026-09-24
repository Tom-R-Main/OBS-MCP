/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import type { OBSWebSocketClient } from "../client.js";

type JsonObject = Record<string, unknown>;

/** Audio capture kinds on every platform: always 0×0, by design. */
const AUDIO_CAPTURE_KIND = /(^|_)(input|output|process_output)_capture$|^sck_audio_capture$|^jack_/;
/** Media sources render nothing when their file has no video. */
const MEDIA_KINDS = new Set(["ffmpeg_source", "vlc_source"]);
const AUDIO_FILE = /\.(wav|mp3|m4a|aac|flac|ogg|oga|opus|aiff?|wma|caf)$/i;

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Whether a source has no picture by nature, so a 0×0 size is not a
 * problem: an audio capture, or a media source playing an audio file.
 */
export function rendersNoVideo(inputKind: unknown, inputSettings?: JsonObject): boolean {
  if (typeof inputKind !== "string") return false;
  if (AUDIO_CAPTURE_KIND.test(inputKind)) return true;
  const file = inputSettings?.local_file;
  return MEDIA_KINDS.has(inputKind) && typeof file === "string" && AUDIO_FILE.test(file);
}

/** Drops the 0×0 items that are audio-only; reads settings only for media sources. */
export async function withoutAudioOnly(
  client: OBSWebSocketClient,
  blank: { name: string; inputKind: unknown }[],
): Promise<string[]> {
  const media = blank.filter(({ inputKind }) => typeof inputKind === "string" && MEDIA_KINDS.has(inputKind));
  const results = await client.sendBatch(media.map(({ name }) => ({ requestType: "GetInputSettings", requestData: { inputName: name } })))
    .catch(() => []);
  const settings = new Map(media.map(({ name }, index) => {
    const result = results[index];
    return [name, result?.ok && isObject(result.responseData) && isObject(result.responseData.inputSettings) ? result.responseData.inputSettings : undefined];
  }));
  return blank.filter(({ name, inputKind }) => !rendersNoVideo(inputKind, settings.get(name))).map(({ name }) => name);
}
