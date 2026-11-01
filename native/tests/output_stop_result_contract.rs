// SPDX-License-Identifier: GPL-2.0-only
//! Pure formatter evidence. No route, transport or receipt fate is admitted here.
use obs_mcp_native::output_stop_result::{
    RECORD_TEXT_BYTES, named_ack, named_error, record_ack, record_error,
};
use serde_json::{Map, Value, json};
use std::{path::PathBuf, process::Command};

#[test]
fn actual_source_callback_bodies_match_for_46_real_client_wire_cases() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_owned();
    let output = Command::new("node")
        .arg(root.join("native/tests/fixtures/output_stop_source_oracle.mjs"))
        .current_dir(&root)
        .output()
        .expect("existing Node and copied compiled Source fixture");
    assert!(
        output.status.success(),
        "Source fixture failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let source: Value =
        serde_json::from_slice(&output.stdout).expect("one complete Source fixture report");
    assert_eq!(source["cases"], 46);
    assert_eq!(source["recordCases"], 39);
    assert_eq!(source["namedCases"], 7);
    assert_eq!(source["knownAcceptedFormattingErrors"], 8);
    assert_eq!(source["connections"], 1);
    assert_eq!(
        source["cleanup"],
        json!({"clientDisconnected":true,"openSockets":0,"listenerClosed":true})
    );
    let mut accepted_errors = 0;
    for case in source["results"].as_array().unwrap() {
        let record = case["tool"] == "obs-stop-record";
        let known_accepted = case["knownTransportAccepted"].as_bool().unwrap();
        let actual = if known_accepted {
            if record {
                let raw = case["responseDataRaw"].as_str().unwrap();
                // Session and the actual Source client normalize nullish data;
                // optional property selection also gives undefined for null.
                let data = if raw == "<absent>" {
                    Value::Null
                } else {
                    serde_json::from_str(raw).unwrap()
                };
                record_ack(&data)
            } else {
                named_ack(case["args"]["outputName"].as_str().unwrap())
            }
        } else {
            // The real client recorded this caught rejection message. Only the
            // callback body prefix is supplied here; fate stays caller-owned.
            let message = case["transportErrorMessage"].as_str().unwrap();
            if record {
                record_error(message)
            } else {
                named_error(message)
            }
        };
        assert_eq!(
            actual, case["structuredResult"],
            "Source callback {}",
            case["name"]
        );
        if known_accepted && actual["isError"] == true {
            accepted_errors += 1;
            assert!(actual.get("structuredContent").is_none());
        }
        assert_eq!(case["trace"].as_array().unwrap().len(), 1);
    }
    assert_eq!(
        accepted_errors, 8,
        "known ACK formatting errors must not become successful messages"
    );
}

#[test]
fn record_whole_text_budget_accepts_exact_utf8_boundary_then_refuses_without_truncation() {
    let prefix = "Recording stopped, saved to: ";
    let mut data = Map::new();
    data.insert(
        "outputPath".into(),
        Value::String("x".repeat(RECORD_TEXT_BYTES - prefix.len())),
    );
    let mut data = Value::Object(data);
    let accepted = record_ack(&data);
    assert!(accepted.get("isError").is_none());
    assert_eq!(
        accepted["content"][0]["text"].as_str().unwrap().len(),
        RECORD_TEXT_BYTES
    );
    assert_eq!(
        accepted["content"][0]["text"],
        accepted["structuredContent"]["message"]
    );
    drop(accepted);
    if let Value::String(path) = &mut data["outputPath"] {
        path.push('λ');
    } else {
        unreachable!();
    }
    let refused = record_ack(&data);
    assert_eq!(refused["isError"], true);
    assert!(refused.get("structuredContent").is_none());
    let message = refused["content"][0]["text"].as_str().unwrap();
    assert_eq!(
        message,
        "Error stopping recording: StopRecord was acknowledged, but its response text exceeds the native 8388608-byte limit"
    );
    assert!(
        !message.contains(&"x".repeat(128)),
        "partial successful path must not escape into the refusal"
    );
}

#[test]
fn wide_array_streaming_keeps_empty_and_nested_null_join_slots() {
    let mut values = vec![Value::Null; 4096];
    values[0] = json!([null, "λ", null]);
    values[4095] = json!([[], null]);
    let result = record_ack(&json!({"outputPath":values}));
    let text = result["content"][0]["text"].as_str().unwrap();
    assert_eq!(
        text,
        format!("Recording stopped, saved to: ,λ,{},", ",".repeat(4095))
    );
    assert_eq!(result["structuredContent"]["message"], text);
}

#[test]
fn selected_object_fields_are_not_recursively_json_serialized() {
    let ordinary = json!({"outputPath":{"child":{"toString":null}}});
    assert_eq!(
        record_ack(&ordinary)["content"][0]["text"],
        "Recording stopped, saved to: [object Object]"
    );
    let broken = json!({"outputPath":["partial",[{"toString":false}]]});
    assert_eq!(
        record_ack(&broken),
        json!({"content":[{"type":"text","text":"Error stopping recording: Cannot convert object to primitive value"}],"isError":true})
    );
    assert_eq!(
        named_ack(""),
        json!({"content":[{"type":"text","text":"Output '' stopped"}],"structuredContent":{"message":"Output '' stopped"}})
    );
}
