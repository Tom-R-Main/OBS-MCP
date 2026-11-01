/* SPDX-License-Identifier: GPL-2.0-only */
/** OBS platform strings vary with Linux packaging. Select by advertised source kinds,
 * never the MCP host's DISPLAY/WAYLAND_DISPLAY (OBS may be remote). */
export const VIDEO_ONLY_CAPTURE_KINDS = new Set(["xcomposite_input", "pipewire-window-capture-source", "pipewire-screen-capture-source"]);
export const WINDOW_CAPTURE_KINDS = new Set(["screen_capture", ...VIDEO_ONLY_CAPTURE_KINDS]);
export function configuredWindow(kind: string, settings: Record<string, unknown>): boolean {
  if (kind === "screen_capture") return settings.type === 1 && typeof settings.window === "number" && settings.window > 0;
  if (kind === "xcomposite_input") return typeof settings.capture_window === "string" && Number(settings.capture_window.split("\r\n", 1)[0]) > 0 && /^\d+(?:\r\n|$)/.test(settings.capture_window);
  // A persisted portal token identifies an existing user-authorized selection.
  // Nonzero video dimensions are checked separately; token alone proves no pixels.
  return kind.startsWith("pipewire-") && typeof settings.RestoreToken === "string" && settings.RestoreToken.length > 0;
}
