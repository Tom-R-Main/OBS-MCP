// SPDX-License-Identifier: GPL-2.0-only
//! Pure Source-shaped bodies for the deferred Record/named Output callbacks.
//!
//! These functions own no transport, policy, journal, timer or output identity.
//! Callers must preserve actual transport disposition and establish durable
//! Accepted custody before formatting an ACK. A formatting error never turns
//! that known ACK into NotDispatched, Rejected, Unknown or replay authority.
//! No route is admitted by exporting this module.
use crate::generic_requests::{JsStringError, append_js_string};
use serde_json::{Map, Value};

/// Whole UTF-8 text budget, including the Record success prefix. It is not a
/// bound on the full encoded MCP body, duplicate message fields or process RSS.
pub const RECORD_TEXT_BYTES: usize = 8 * 1024 * 1024;
const RECORD_PREFIX: &str = "Recording stopped, saved to: ";
const TO_PRIMITIVE: &str = "Cannot convert object to primitive value";

fn content(text: String) -> Value {
    let mut block = Map::new();
    block.insert("type".into(), Value::String("text".into()));
    block.insert("text".into(), Value::String(text));
    Value::Array(vec![Value::Object(block)])
}
fn success(text: String) -> Value {
    let mut message = Map::new();
    // Exact Source body contains the same message twice. Clone only this
    // required result field; no selected or whole response Value is cloned.
    message.insert("message".into(), Value::String(text.clone()));
    let mut body = Map::new();
    body.insert("content".into(), content(text));
    body.insert("structuredContent".into(), Value::Object(message));
    Value::Object(body)
}
fn failure(text: String) -> Value {
    let mut body = Map::new();
    body.insert("content".into(), content(text));
    body.insert("isError".into(), Value::Bool(true));
    Value::Object(body)
}

/// Format a known StopRecord ACK. Session's nullish responseData normalization
/// is retained; optional property access also yields undefined for primitive
/// and array data. An own noncallable toString anywhere in the selected array
/// projection reproduces Source's caught TypeError. Over-limit text refuses
/// after ACK without truncation or any assertion that the recording stopped.
pub fn record_ack(response_data: &Value) -> Value {
    record_ack_with_limit(response_data, RECORD_TEXT_BYTES)
}
fn record_ack_with_limit(response_data: &Value, max_bytes: usize) -> Value {
    let mut text = String::from(RECORD_PREFIX);
    match append_js_string(&mut text, response_data.get("outputPath"), max_bytes) {
        Ok(()) => success(text),
        Err(JsStringError::ToPrimitive) => record_error(TO_PRIMITIVE),
        Err(JsStringError::ByteCapacity) => record_error(&format!(
            "StopRecord was acknowledged, but its response text exceeds the native {max_bytes}-byte limit"
        )),
    }
}

/// Named Output's ACK ignores responseData; only the schema-validated name is
/// interpolated. Empty/Unicode strings stay intact. Request/name admission
/// bounds remain the caller's responsibility, separate from Record text.
pub fn named_ack(output_name: &str) -> Value {
    success(format!("Output '{output_name}' stopped"))
}

/// Only a body builder: the caller selects an authorized public error message
/// while retaining the actual transport/receipt fate independently.
pub fn record_error(message: &str) -> Value {
    failure(format!("Error stopping recording: {message}"))
}
/// Pure body builder for the existing request-tool callback catch prefix.
pub fn named_error(message: &str) -> Value {
    failure(format!("Stop Output failed: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn exact_budget_covers_prefix_utf8_and_join_separators() {
        let data = json!({"outputPath":["λ",null,["x",null]]});
        let expected = "Recording stopped, saved to: λ,,x,";
        let result = record_ack_with_limit(&data, expected.len());
        assert_eq!(result["content"][0]["text"], expected);
        let refused = record_ack_with_limit(&data, expected.len() - 1);
        assert_eq!(refused["isError"], true);
        assert!(refused.get("structuredContent").is_none());
        assert!(
            !refused["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("λ,,x,")
        );
    }

    #[test]
    fn numeric_decimal_expansion_is_charged_before_success() {
        let data = json!({"outputPath":1e20});
        let expected = "Recording stopped, saved to: 100000000000000000000";
        assert_eq!(
            record_ack_with_limit(&data, expected.len())["content"][0]["text"],
            expected
        );
        assert_eq!(
            record_ack_with_limit(&data, RECORD_PREFIX.len() + 4)["isError"],
            true
        );
    }
}
