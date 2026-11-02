// SPDX-License-Identifier: GPL-2.0-only
//! Advanced-only opaque effects. Fixed catalog membership is not settlement
//! evidence: one retained request/batch owns an identity-only incompatibility
//! receipt until its actual transport observer settles. No replay or Take grant.
use crate::{
    control::Disposition,
    custody::{Clock, SharedControl},
    generic_requests::{self, Call},
    operation_store::{GenericKind as K, GenericPhase as P, OperationStore},
    session::{BatchOptions, BatchRequest, Execution, RequestError, SessionHandle},
    stage_transport::Cancellation,
};
use serde_json::{Map, Value, json};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use tokio::time::Instant;

const ENUMERATION: &str = "GetInputPropertiesListPropertyItems";
const BATCH_ENUMERATION: &str = "Capture property enumeration must run separately through obs-get-input-property-list-items so its crash guard can inspect current state after any changes.";
const PROBE_FAILED: &str = "Capture properties could not be checked safely; read obs-get-input-settings before listing them.";
const KIND_MISSING: &str = "Capture properties could not be checked safely; inspect obs-get-input-settings for the source kind.";
const XCOMPOSITE: &str = "Listing windows on an unconfigured XComposite input can crash OBS. Select a window in OBS first, then inspect or plan its binding; discovery never seeds this property.";
const SCREEN_CAPTURE: &str = "Listing ScreenCaptureKit display_uuid or application string properties can crash OBS even after a display is selected. Use the integer window list for window capture, or configure the intended application capture in OBS and bind that existing source.";

pub fn is_name(name: &str) -> bool {
    matches!(
        name,
        "obs-call-request"
            | "obs-batch"
            | "obs-call-vendor-request"
            | "obs-trigger-hotkey-by-name"
            | "obs-trigger-hotkey-by-key-sequence"
    )
}
pub(crate) fn is_kind(kind: K) -> bool {
    matches!(
        kind,
        K::OpaqueRequest | K::OpaqueBatch | K::VendorRequest | K::HotkeyName | K::HotkeySequence
    )
}
fn known(method: &str) -> bool {
    static NAMES: OnceLock<HashSet<String>> = OnceLock::new();
    NAMES
        .get_or_init(|| {
            let source: Value = serde_json::from_str(include_str!("../../docs/protocol.json"))
                .expect("checked pinned request catalog");
            let names = source["requests"]
                .as_array()
                .expect("checked requests")
                .iter()
                .map(|row| {
                    row["requestType"]
                        .as_str()
                        .expect("checked request name")
                        .to_owned()
                })
                .collect::<HashSet<_>>();
            assert_eq!(names.len(), 147, "pinned request-name authority changed");
            names
        })
        .contains(method)
}
fn string(
    input: &Map<String, Value>,
    output: &mut Map<String, Value>,
    key: &str,
    required: bool,
) -> Option<()> {
    match input.get(key) {
        Some(value) => {
            output.insert(key.into(), json!(value.as_str()?));
        }
        None if required => return None,
        None => {}
    }
    Some(())
}
fn record(input: &Map<String, Value>, output: &mut Map<String, Value>, key: &str) -> Option<()> {
    if let Some(value) = input.get(key) {
        let mut fields = value.as_object()?.clone();
        // Zod record owns this level and drops the prototype setter key;
        // unknown values beneath it remain opaque, including their own keys.
        fields.remove("__proto__");
        output.insert(key.into(), Value::Object(fields));
    }
    Some(())
}
fn single(input: &Value) -> Option<Value> {
    let input = input.as_object()?;
    let mut output = Map::new();
    string(input, &mut output, "requestType", true)?;
    record(input, &mut output, "requestData")?;
    Some(Value::Object(output))
}
pub(crate) fn parse(name: &str, args: &Value) -> Option<(K, &'static str, &'static str, Value)> {
    let input = args.as_object()?;
    let mut output = Map::new();
    let (kind, name, title) = match name {
        "obs-call-request" => {
            output = single(args)?.as_object()?.clone();
            (K::OpaqueRequest, "obs-call-request", "Call OBS Request")
        }
        "obs-batch" => {
            let requests = input.get("requests")?.as_array()?;
            if requests.is_empty() || requests.len() > 100 {
                return None;
            }
            output.insert(
                "requests".into(),
                Value::Array(requests.iter().map(single).collect::<Option<Vec<_>>>()?),
            );
            let execution = input
                .get("executionType")
                .map(Value::as_str)
                .unwrap_or(Some("serial-realtime"))?;
            if !matches!(execution, "serial-realtime" | "serial-frame") {
                return None;
            }
            output.insert("executionType".into(), json!(execution));
            output.insert(
                "haltOnFailure".into(),
                json!(
                    input
                        .get("haltOnFailure")
                        .map(Value::as_bool)
                        .unwrap_or(Some(false))?
                ),
            );
            (K::OpaqueBatch, "obs-batch", "Run OBS Requests in a Batch")
        }
        "obs-call-vendor-request" => {
            string(input, &mut output, "vendorName", true)?;
            string(input, &mut output, "requestType", true)?;
            record(input, &mut output, "requestData")?;
            (
                K::VendorRequest,
                "obs-call-vendor-request",
                "Call Vendor Request",
            )
        }
        "obs-trigger-hotkey-by-name" => {
            string(input, &mut output, "hotkeyName", true)?;
            string(input, &mut output, "contextName", false)?;
            (
                K::HotkeyName,
                "obs-trigger-hotkey-by-name",
                "Trigger Hotkey by Name",
            )
        }
        "obs-trigger-hotkey-by-key-sequence" => {
            string(input, &mut output, "keyId", false)?;
            if let Some(modifiers) = input.get("keyModifiers") {
                let mut keys = Map::new();
                let modifiers = modifiers.as_object()?;
                for key in ["shift", "control", "alt", "command"] {
                    if let Some(value) = modifiers.get(key) {
                        keys.insert(key.into(), json!(value.as_bool()?));
                    }
                }
                output.insert("keyModifiers".into(), Value::Object(keys));
            }
            (
                K::HotkeySequence,
                "obs-trigger-hotkey-by-key-sequence",
                "Trigger Hotkey by Key Sequence",
            )
        }
        _ => return None,
    };
    Some((kind, name, title, Value::Object(output)))
}
fn error(message: impl Into<String>) -> Value {
    json!({"content":[{"type":"text","text":message.into()}],"isError":true})
}
fn args(call: &Call) -> &Value {
    call.arguments_ref()
}
fn method(call: &Call) -> &str {
    if call.kind() == K::OpaqueRequest {
        args(call)["requestType"]
            .as_str()
            .expect("validated method")
    } else {
        call.kind().request_type()
    }
}
fn guarded(value: Option<&Value>) -> Result<bool, crate::generic_requests::JsStringError> {
    let mut text = String::new();
    crate::generic_requests::append_js_string(&mut text, value, 8 * 1024 * 1024)?;
    Ok(matches!(
        text.as_str(),
        "display_uuid" | "application" | "capture_window"
    ))
}
pub(crate) fn refusal(call: &Call) -> Option<Value> {
    if call.kind() == K::OpaqueRequest && !known(method(call)) {
        return Some(error(format!(
            "Unknown OBS WebSocket request: {}",
            method(call)
        )));
    }
    if call.kind() == K::OpaqueBatch {
        let requests = args(call)["requests"].as_array().expect("validated batch");
        let unknown = requests
            .iter()
            .filter_map(|row| row["requestType"].as_str())
            .filter(|name| !known(name))
            .collect::<Vec<_>>();
        if !unknown.is_empty() {
            return Some(error(format!(
                "Unknown OBS WebSocket request: {}",
                unknown.join(", ")
            )));
        }
        for row in requests {
            if row["requestType"] == ENUMERATION {
                match guarded(row.pointer("/requestData/propertyName")) {
                    Ok(true) => return Some(error(BATCH_ENUMERATION)),
                    Ok(false) => {}
                    Err(_) => {
                        return Some(failure(call, "Cannot convert object to primitive value"));
                    }
                }
            }
        }
        if budget(call).is_none() {
            return Some(error(
                "OBS request batch failed: Original batch deadline exceeds the native 600-second qualification bound",
            ));
        }
    }
    None
}
fn batch_requests(call: &Call) -> Vec<BatchRequest> {
    args(call)["requests"]
        .as_array()
        .expect("validated batch")
        .iter()
        .map(|row| BatchRequest {
            request_type: row["requestType"]
                .as_str()
                .expect("validated method")
                .into(),
            request_data: row.get("requestData").cloned(),
        })
        .collect()
}
pub(crate) fn budget(call: &Call) -> Option<Duration> {
    if call.kind() != K::OpaqueBatch {
        return Some(Duration::from_secs(10));
    }
    BatchOptions::default()
        .effective_timeout(&batch_requests(call))
        .ok()
        .filter(|duration| *duration <= Duration::from_secs(600))
}
pub(crate) fn failure(call: &Call, message: impl AsRef<str>) -> Value {
    let prefix = match call.kind() {
        K::OpaqueRequest => format!("OBS request {} failed", method(call)),
        K::OpaqueBatch => "OBS request batch failed".into(),
        K::VendorRequest => "Error calling vendor request".into(),
        K::HotkeyName => "Error triggering hotkey".into(),
        K::HotkeySequence => "Error triggering hotkey by key sequence".into(),
        _ => unreachable!("closed opaque kind"),
    };
    error(format!("{prefix}: {}", message.as_ref()))
}
fn json_result(response: Value) -> Value {
    json!({"content":[{"type":"text","text":serde_json::to_string_pretty(&response).expect("bounded JSON value")} ]})
}
fn success(call: &Call, response: Value) -> Value {
    match call.kind() {
        K::HotkeyName => {
            json!({"content":[{"type":"text","text":format!("Successfully triggered hotkey: {}",args(call)["hotkeyName"].as_str().expect("validated hotkey"))}]})
        }
        K::HotkeySequence => {
            json!({"content":[{"type":"text","text":"Hotkey triggered by key sequence successfully"}]})
        }
        _ => json_result(response),
    }
}
fn batch_result(call: &Call, results: Vec<crate::session::BatchResult>) -> Value {
    let total = args(call)["requests"]
        .as_array()
        .expect("validated batch")
        .len();
    let failed = results.iter().filter(|row| !row.ok).count();
    let succeeded = results.len() - failed;
    let skipped = total - results.len();
    let mut summary = vec![format!("{succeeded} of {total} request(s) succeeded")];
    for (index, row) in results.iter().enumerate().filter(|(_, row)| !row.ok) {
        summary.push(format!(
            "#{index} {} failed with code {}{}",
            row.request_type,
            row.code,
            row.comment
                .as_ref()
                .filter(|comment| !comment.is_empty())
                .map(|comment| format!(": {comment}"))
                .unwrap_or_default()
        ));
    }
    if skipped > 0 {
        summary.push(format!("{skipped} request(s) skipped after the failure"));
    }
    let results = results
        .into_iter()
        .enumerate()
        .map(|(index, row)| {
            let mut row = serde_json::to_value(row).expect("batch serialization");
            row["index"] = json!(index);
            row
        })
        .collect::<Vec<_>>();
    let mut value = json!({"content":[{"type":"text","text":format!("{}\n{}",summary.join("\n"),serde_json::to_string_pretty(&results).expect("batch JSON"))}],"structuredContent":{"results":results,"succeeded":succeeded,"failed":failed,"skipped":skipped}});
    if failed > 0 {
        value["isError"] = json!(true);
    }
    value
}
fn missing(capabilities: &Option<Arc<HashSet<String>>>, method: &str) -> bool {
    capabilities
        .as_ref()
        .is_some_and(|set| !set.contains(method))
}
fn unsupported(method: &str) -> String {
    format!("OBS WebSocket does not advertise support for request '{method}'")
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute(
    mut store: OperationStore,
    call: Call,
    session: SessionHandle,
    control: SharedControl,
    clock: Clock,
    caller: String,
    cancel: Cancellation,
    id: String,
    timeout: Duration,
    admitted_at: Instant,
    capabilities: Option<Arc<HashSet<String>>>,
) -> Value {
    let call = Arc::new(call);
    let Some(deadline) = admitted_at
        .checked_add(timeout)
        .filter(|_| !timeout.is_zero())
    else {
        return error("Invalid operation deadline");
    };
    match generic_requests::policy(&call, &caller, &control, &clock) {
        Ok(Disposition::Allow) => {}
        Ok(refusal) => {
            return refusal
                .tool_result()
                .unwrap_or_else(|| error("Control policy refused dispatch"));
        }
        Err(()) => return error("Control policy is unavailable"),
    }
    if let Some(result) = refusal(&call) {
        return result;
    }
    if store
        .read_view()
        .admission()
        .map_or(true, |admission| admission.blocking)
    {
        return error("Unresolved receipt custody prevents dispatch");
    }
    if cancel.is_cancelled() || Instant::now() >= deadline {
        return failure(&call, "Operation cancelled or expired before dispatch");
    }
    // Preserve the Source single-property guard before its actual request,
    // using the same retained session and original deadline. These reads grant
    // no effect authority and do not create a new socket or replayable subcall.
    let needs_capture_guard = if call.kind() == K::OpaqueRequest && method(&call) == ENUMERATION {
        match guarded(args(&call).pointer("/requestData/propertyName")) {
            Ok(value) => value,
            Err(_) => return failure(&call, "Cannot convert object to primitive value"),
        }
    } else {
        false
    };
    if needs_capture_guard {
        let data = &args(&call)["requestData"];
        let mut selectors = Map::new();
        for key in ["inputName", "inputUuid"] {
            if let Some(value) = data.get(key).filter(|value| value.is_string()) {
                selectors.insert(key.into(), value.clone());
            }
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return failure(&call, "Operation cancelled or expired before dispatch");
        };
        let settings = if missing(&capabilities, "GetInputSettings") {
            None
        } else {
            let c = call.clone();
            let ctl = control.clone();
            let clk = clock.clone();
            let who = caller.clone();
            let cancellation = cancel.clone();
            session
                .request_checked(
                    session.token(),
                    "GetInputSettings",
                    Value::Object(selectors),
                    remaining,
                    move || {
                        if cancellation.is_cancelled()
                            || Instant::now() >= deadline
                            || generic_requests::policy(&c, &who, &ctl, &clk)
                                != Ok(Disposition::Allow)
                        {
                            return Err("Property enumeration guard authority changed".into());
                        }
                        Ok(())
                    },
                )
                .await
                .ok()
        };
        let Some(settings) = settings else {
            return error(PROBE_FAILED);
        };
        let Some(kind) = settings["inputKind"]
            .as_str()
            .filter(|kind| !kind.is_empty())
        else {
            return error(KIND_MISSING);
        };
        if data["propertyName"] == "capture_window" {
            if kind == "xcomposite_input"
                && settings
                    .pointer("/inputSettings/capture_window")
                    .and_then(Value::as_str)
                    .is_none_or(|value| value.is_empty())
            {
                return error(XCOMPOSITE);
            }
        } else if kind == "screen_capture" {
            return error(SCREEN_CAPTURE);
        }
    }
    let required = if call.kind() == K::OpaqueBatch {
        args(&call)["requests"]
            .as_array()
            .expect("validated batch")
            .iter()
            .map(|row| row["requestType"].as_str().expect("validated request"))
            .collect::<Vec<_>>()
    } else {
        vec![method(&call)]
    };
    if let Some(method) = required
        .into_iter()
        .find(|method| missing(&capabilities, method))
    {
        return failure(&call, unsupported(method));
    }
    if cancel.is_cancelled() || Instant::now() >= deadline {
        return failure(&call, "Operation cancelled or expired before dispatch");
    }
    let receipt = match store
        .prepare_generic(&id, call.kind(), session.token().generation(), &clock().utc)
        .and_then(|receipt| store.update_generic(&receipt, P::Dispatching, &clock().utc))
    {
        Ok(receipt) => receipt,
        Err(_) => {
            return error(
                "Operation receipt could not be persisted or inspected; inspect obs-observe before any further production change",
            );
        }
    };
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        let _ = store.update_generic(&receipt, P::NotDispatched, &clock().utc);
        return failure(&call, "Operation deadline expired before dispatch");
    };
    let view = store.read_view();
    let fence = store.dispatch_fence();
    let checked = call.clone();
    let clk = clock.clone();
    let refused = Arc::new(Mutex::new(None));
    let observed = refused.clone();
    let guard = move || {
        let decision = generic_requests::policy(&checked, &caller, &control, &clk)
            .map_err(|_| "Control policy unavailable".to_owned())?;
        if decision != Disposition::Allow {
            let _ = observed.lock().map(|mut value| *value = Some(decision));
            return Err("Control policy changed before dispatch".into());
        }
        if cancel.is_cancelled() || Instant::now() >= deadline {
            return Err("Operation cancelled or expired before dispatch".into());
        }
        if fence.as_ref().map_or_else(
            || {
                !view.is_dispatching(&id).unwrap_or(false)
                    || view
                        .admission_except(Some(&id))
                        .map_or(true, |value| value.blocking)
            },
            |fence| !fence.permits(&id).unwrap_or(false),
        ) {
            return Err("Unresolved receipt custody prevents dispatch".into());
        }
        Ok(())
    };
    let response = if call.kind() == K::OpaqueBatch {
        session
            .batch_checked(
                session.token(),
                batch_requests(&call),
                BatchOptions {
                    execution: if args(&call)["executionType"] == "serial-frame" {
                        Execution::SerialFrame
                    } else {
                        Execution::SerialRealtime
                    },
                    halt_on_failure: args(&call)["haltOnFailure"] == true,
                    timeout: Some(remaining),
                },
                guard,
            )
            .await
            .map(|results| batch_result(&call, results))
    } else {
        let data = if call.kind() == K::OpaqueRequest {
            args(&call)
                .get("requestData")
                .cloned()
                .unwrap_or(Value::Null)
        } else {
            args(&call).clone()
        };
        session
            .request_checked(session.token(), method(&call), data, remaining, guard)
            .await
            .map(|response| success(&call, response))
    };
    let phase = match &response {
        Ok(_) => P::Accepted,
        Err(RequestError::BeforeDispatch(_)) => P::NotDispatched,
        Err(error) if error.rejection_code().is_some() => P::Rejected,
        Err(_) => P::Unknown,
    };
    if store.update_generic(&receipt, phase, &clock().utc).is_err() {
        return error(
            "Operation receipt outcome is uncertain; inspect obs-observe before any further production change",
        );
    }
    match response {
        Ok(result) => result,
        Err(reason) => {
            if let Some(result) = refused
                .lock()
                .ok()
                .and_then(|mut value| value.take())
                .and_then(|value| value.tool_result())
            {
                return result;
            }
            let message=reason.obs_failure_message(method(&call)).or_else(||session.authorized_disconnect_message(&reason)).unwrap_or_else(||match reason{RequestError::BeforeDispatch(crate::session::BeforeDispatch::GuardRefused(message))=>message,RequestError::Unknown(crate::session::Invalidation::DeadlineAfterDispatch)=>format!("Request {} timed out after {}ms",method(&call),timeout.as_millis()),_=>"OBS request outcome is uncertain; inspect obs-observe before another change".into()});
            failure(&call, message)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_strip_opaque_records_nested_hotkey_keys_and_js_numbers() {
        let raw=Call::parse("obs-call-request",&json!({"requestType":"SetInputSettings","discard":true,"requestData":{"foreign":true,"value":9007199254740993_u64,"nested":{"keep":[null,"x",3.5]}}})).unwrap();
        assert_eq!(
            raw.arguments()["requestData"]["value"],
            json!(9007199254740992_u64)
        );
        assert!(raw.arguments().get("discard").is_none());
        assert_eq!(
            raw.arguments()["requestData"]["nested"],
            json!({"keep":[null,"x",3.5]})
        );
        assert_eq!(Call::parse("obs-trigger-hotkey-by-key-sequence",&json!({"keyId":"OBS_KEY_A","keyModifiers":{"shift":false,"command":true,"discard":true},"discard":true})).unwrap().arguments(),json!({"keyId":"OBS_KEY_A","keyModifiers":{"shift":false,"command":true}}));
        assert!(Call::parse("obs-batch", &json!({"requests":[]})).is_none());
        assert!(
            Call::parse(
                "obs-batch",
                &json!({"requests":[{"requestType":"GetVersion"}],"executionType":"parallel"})
            )
            .is_none()
        );
        assert!(
            Call::parse(
                "obs-call-request",
                &json!({"requestType":"GetVersion","requestData":null})
            )
            .is_none()
        );
    }
    #[test]
    fn pinned_names_and_batch_guard_refuse_before_transport() {
        let raw =
            Call::parse("obs-call-request", &json!({"requestType":"WhateverPlugin"})).unwrap();
        assert_eq!(refusal(&raw).unwrap()["isError"], true);
        assert!(known("GetVersion"));
        assert!(!known("RequestBatch"));
        let batch=Call::parse("obs-batch",&json!({"requests":[{"requestType":"SetInputSettings"},{"requestType":ENUMERATION,"requestData":{"propertyName":["application"]}}]})).unwrap();
        assert_eq!(refusal(&batch).unwrap(), error(BATCH_ENUMERATION));
    }
    #[test]
    fn one_source_sleep_formula_both_fields_30fps_and_bound_without_waiting() {
        let make = |data: Value, mode: &str| {
            Call::parse("obs-batch",&json!({"requests":[{"requestType":"Sleep","requestData":data}],"executionType":mode})).unwrap()
        };
        for mode in ["serial-realtime", "serial-frame"] {
            assert_eq!(
                budget(&make(json!({"sleepMillis":590000}), mode)),
                Some(Duration::from_secs(600))
            );
            assert_eq!(
                budget(&make(json!({"sleepFrames":17700}), mode)),
                Some(Duration::from_secs(600))
            );
            assert_eq!(
                budget(&make(json!({"sleepMillis":1000,"sleepFrames":30}), mode)),
                Some(Duration::from_secs(12))
            );
            assert!(refusal(&make(json!({"sleepMillis":590001}), mode)).is_some());
            assert!(refusal(&make(json!({"sleepMillis":-10000}), mode)).is_some());
            let call = make(json!({"sleepMillis":40000}), mode);
            assert_eq!(
                call.execution_budget(Duration::from_secs(30)),
                Duration::from_secs(50)
            );
        }
    }
}
