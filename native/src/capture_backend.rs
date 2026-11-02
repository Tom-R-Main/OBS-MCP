// SPDX-License-Identifier: GPL-2.0-only
//! Classify the OBS-advertised source, never the MCP host's display variables.
use serde_json::{Map, Value};
pub const VIDEO_ONLY_CAPTURE_KINDS: &[&str] = &[
    "xcomposite_input",
    "pipewire-window-capture-source",
    "pipewire-screen-capture-source",
];
pub const WINDOW_CAPTURE_KINDS: &[&str] = &[
    "screen_capture",
    "xcomposite_input",
    "pipewire-window-capture-source",
    "pipewire-screen-capture-source",
];
pub fn video_only(kind: &str) -> bool {
    VIDEO_ONLY_CAPTURE_KINDS.contains(&kind)
}
pub fn window_capture(kind: &str) -> bool {
    WINDOW_CAPTURE_KINDS.contains(&kind)
}
pub fn configured_window(kind: &str, settings: &Map<String, Value>) -> bool {
    if kind == "screen_capture" {
        return settings.get("type").and_then(Value::as_f64) == Some(1.0)
            && settings
                .get("window")
                .and_then(Value::as_f64)
                .is_some_and(|window| window > 0.0);
    }
    if kind == "xcomposite_input" {
        return settings
            .get("capture_window")
            .and_then(Value::as_str)
            .is_some_and(|window| {
                let first = window.split("\r\n").next().unwrap_or("");
                !first.is_empty()
                    && first.bytes().all(|byte| byte.is_ascii_digit())
                    && first.bytes().any(|byte| byte != b'0')
            });
    }
    // A retained portal token establishes a persisted selection, not pixels.
    kind.starts_with("pipewire-")
        && settings
            .get("RestoreToken")
            .and_then(Value::as_str)
            .is_some_and(|token| !token.is_empty())
}
