// SPDX-License-Identifier: GPL-2.0-only
//! Static prompts exported from the actual SDK registration. Invalid or
//! unsupported arguments retain SDK interpretation before any native effect.
use obs_mcp_native::config::trim;
use serde_json::{Value, json};
use std::sync::OnceLock;
pub(crate) fn asset() -> &'static Value {
    static ASSET: OnceLock<Value> = OnceLock::new();
    ASSET.get_or_init(|| {
        serde_json::from_str(include_str!("../assets/prompts.json"))
            .expect("checked prompt catalog")
    })
}
pub(crate) fn list() -> Value {
    asset()["listing"].clone()
}
pub(crate) fn resource_templates() -> Value {
    asset()["resourceTemplates"].clone()
}
fn integer(value: &str, max: u64) -> Option<u64> {
    let value = trim(value);
    let prefix = value.get(..2).unwrap_or("").to_ascii_lowercase();
    let result = match prefix.as_str() {
        "0x" => radix(&value[2..], 16)?,
        "0o" => radix(&value[2..], 8)?,
        "0b" => radix(&value[2..], 2)?,
        _ => {
            let number = value.parse::<f64>().ok()?;
            if !number.is_finite() || number.fract() != 0.0 || number < 1.0 || number > max as f64 {
                return None;
            }
            number as u64
        }
    };
    (result > 0 && result <= max).then_some(result)
}
fn radix(value: &str, base: u32) -> Option<u64> {
    (!value.is_empty() && value.chars().all(|ch| ch.is_ascii() && ch.is_digit(base)))
        .then(|| u64::from_str_radix(value, base).ok())
        .flatten()
}
fn expand(template: &str, replacements: &[(&str, String)]) -> String {
    let mut output = String::with_capacity(template.len());
    let mut remaining = template;
    loop {
        let next = replacements
            .iter()
            .filter_map(|(marker, replacement)| {
                remaining.find(marker).map(|at| (at, *marker, replacement))
            })
            .min_by_key(|entry| entry.0);
        let Some((at, marker, replacement)) = next else {
            output.push_str(remaining);
            break;
        };
        output.push_str(&remaining[..at]);
        output.push_str(replacement);
        remaining = &remaining[at + marker.len()..];
    }
    output
}
fn message(text: String) -> Value {
    json!({"messages":[{"role":"user","content":{"type":"text","text":text}}]})
}
pub(crate) fn get(name: &str, args: &Value) -> Option<Value> {
    let arguments = args.as_object()?;
    if arguments.values().any(|value| !value.is_string()) {
        return None;
    }
    let a = asset();
    let markers = &a["replacements"];
    let text = |name: &str| arguments.get(name).and_then(Value::as_str);
    let result = match name {
        "pre-stream-check" => a["templates"]["preStream"].clone(),
        "software-demo" => {
            let maximum = a["recipe"]["maxDurationSeconds"].as_u64()?;
            let fallback = a["recipe"]["defaultMaxDurationSeconds"]
                .as_u64()?
                .to_string();
            let duration = integer(text("maxDurationSeconds").unwrap_or(&fallback), maximum)?;
            let replacements = [
                (
                    markers["software"]["collection"].as_str()?,
                    serde_json::to_string(text("sceneCollection")?).ok()?,
                ),
                (
                    markers["software"]["scene"].as_str()?,
                    serde_json::to_string(text("sceneName")?).ok()?,
                ),
                (
                    markers["software"]["input"].as_str()?,
                    serde_json::to_string(text("inputName")?).ok()?,
                ),
                (
                    markers["software"]["duration"].as_str()?,
                    format!("maxDurationSeconds:{duration}"),
                ),
            ];
            message(expand(a["templates"]["software"].as_str()?, &replacements))
        }
        "record-demo" => {
            let steps = text("steps").filter(|s| !s.is_empty());
            let step_lines = steps
                .map(|s| {
                    s.split('\n')
                        .map(trim)
                        .filter(|s| !s.is_empty())
                        .map(|s| format!("   - {s}\n"))
                        .collect::<String>()
                })
                .unwrap_or_default();
            let replacements = [
                (
                    markers["record"]["window"].as_str()?,
                    text("window")?.to_owned(),
                ),
                (
                    markers["record"]["scene"].as_str()?,
                    text("scene").unwrap_or("Demo").to_owned(),
                ),
                (
                    markers["record"]["seconds"].as_str()?,
                    text("seconds").unwrap_or("30").to_owned(),
                ),
                (markers["record"]["steps"].as_str()?, step_lines),
            ];
            message(expand(
                a["templates"][if steps.is_some() { "steps" } else { "record" }].as_str()?,
                &replacements,
            ))
        }
        _ => return None,
    };
    // The read-only SDK lane retains oversized prompt interpretation. This cap
    // is a native admission bound, not a claim about total process memory.
    (serde_json::to_vec(&result).ok()?.len() <= 4 * 1024 * 1024).then_some(result)
}
