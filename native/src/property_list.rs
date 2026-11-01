// SPDX-License-Identifier: GPL-2.0-only
//! Closed, source-aware property enumeration on an existing physical session.
//! The service retains this future until settlement; no task or socket is made.
use crate::{
    observation::{ObservationError, ReadAdapter, ReadRequest},
    session::{ALL_EVENTS, BeforeDispatch, RequestError, SessionHandle},
    session_observation::{ObservationFence, SessionReader},
    stage_transport::Cancellation,
};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{collections::HashSet, fmt, sync::Arc};
use tokio::time::Instant;

const METHOD: &str = "GetInputPropertiesListPropertyItems";
const PROBE_FAILED: &str = "Capture properties could not be checked safely; read obs-get-input-settings before listing them.";
const KIND_MISSING: &str = "Capture properties could not be checked safely; inspect obs-get-input-settings for the source kind.";
const XCOMPOSITE: &str = "Listing windows on an unconfigured XComposite input can crash OBS. Select a window in OBS first, then inspect or plan its binding; discovery never seeds this property.";
const SCREEN_CAPTURE: &str = "Listing ScreenCaptureKit display_uuid or application string properties can crash OBS even after a display is selected. Use the integer window list for window capture, or configure the intended application capture in OBS and bind that existing source.";

/// Only this method's known selectors can enter the adapter. Debug omits names.
pub struct Request {
    selectors: Map<String, Value>,
    property_name: String,
}
impl fmt::Debug for Request {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("PropertyListRequest(..)")
    }
}
impl Request {
    pub fn parse(args: &Value) -> Result<Self, ObservationError> {
        let invalid =
            || ObservationError::new("invalid_arguments", "Invalid property-list arguments");
        let object = args.as_object().ok_or_else(invalid)?;
        let property_name = object
            .get("propertyName")
            .and_then(Value::as_str)
            .ok_or_else(invalid)?
            .to_owned();
        let mut selectors = Map::new();
        for key in ["inputName", "inputUuid"] {
            if let Some(value) = object.get(key) {
                selectors.insert(key.into(), json!(value.as_str().ok_or_else(invalid)?));
            }
        }
        Ok(Self {
            selectors,
            property_name,
        })
    }
}

pub struct Context {
    pub handle: SessionHandle,
    pub reader: SessionReader,
    pub fence: ObservationFence,
    pub endpoint: String,
    /// Actual same-session GetVersion metadata; None means unknown, not empty.
    pub capabilities: Option<Arc<HashSet<String>>>,
    pub cancellation: Cancellation,
    pub deadline: Instant,
}
fn invalidated() -> ObservationError {
    ObservationError::new(
        "property_list_invalidated",
        "Property-list source evidence changed; inspect OBS before retrying",
    )
}
fn live(context: &Context) -> Result<(), ObservationError> {
    if context.cancellation.is_cancelled() {
        return Err(ObservationError::new(
            "property_list_cancelled",
            "Property-list request cancelled",
        ));
    }
    if Instant::now() >= context.deadline {
        return Err(ObservationError::new(
            "property_list_deadline",
            "Property-list deadline elapsed",
        ));
    }
    if context.handle.invalidation().is_some() {
        return Err(invalidated());
    }
    Ok(())
}
fn text_error(disposition: &str, message: &str) -> Value {
    json!({"content":[{"type":"text","text":format!("Get Input Property List Items {disposition}: {message}")}],"isError":true})
}
fn unsupported(context: &Context, method: &str) -> bool {
    context
        .capabilities
        .as_ref()
        .is_some_and(|set| !set.contains(method))
}
fn unsupported_text(method: &str) -> String {
    format!("OBS WebSocket does not advertise support for request '{method}'")
}

fn json_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Exact owning output schema: optional nullable array of string-keyed records.
/// The SDK validates but does not replace valid plugin records or extra fields.
fn output_error(response: &Value) -> Result<Option<Value>, ObservationError> {
    let Some(items) = response.get("propertyItems").filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    let mut issues = String::new();
    if let Some(items) = items.as_array() {
        for (index, value) in items.iter().enumerate() {
            if value.is_object() {
                continue;
            }
            if !issues.is_empty() {
                issues.push_str(", ");
            }
            use std::fmt::Write;
            write!(
                issues,
                "propertyItems.{index}: Invalid input: expected record, received {}",
                json_kind(value)
            )
            .expect("String write");
            if issues.len() > 32 * 1024 {
                return Err(ObservationError::new(
                    "property_list_output_limit",
                    "Property-list output validation exceeded its diagnostic bound",
                ));
            }
        }
    } else {
        issues = format!(
            "propertyItems: Invalid input: expected array, received {}",
            json_kind(items)
        );
    }
    Ok((!issues.is_empty()).then(|| json!({"content":[{"type":"text","text":format!("Output validation error: Invalid structured content for tool obs-get-input-property-list-items: {issues}")}],"isError":true})))
}

/// Run with the sole observer's reader/fence and a cached capability snapshot.
/// Cancellation is checked at admission and actual send. An admitted RPC is
/// always awaited, including its causal event barrier; it is never replayed.
pub async fn run(mut context: Context, request: Request) -> Result<Value, ObservationError> {
    let hash = format!("{:x}", Sha256::digest(context.endpoint.as_bytes()));
    if context.handle.token() != context.reader.token()
        || context.handle.token() != context.fence.token()
        || hash != context.handle.endpoint_hash()
        || hash != context.reader.endpoint_hash()
        || hash != context.fence.endpoint_hash()
        || !context.fence.full_profile()
        || context.handle.event_subscriptions() & ALL_EVENTS != ALL_EVENTS
    {
        return Err(invalidated());
    }
    live(&context)?;
    context.reader.synchronize(context.deadline).await?;
    let evidence = context.reader.evidence();
    if !context
        .fence
        .ready_at_revision(evidence.generation, evidence.revision)
    {
        return Err(invalidated());
    }

    if matches!(
        request.property_name.as_str(),
        "display_uuid" | "application" | "capture_window"
    ) {
        if unsupported(&context, "GetInputSettings") {
            return Ok(text_error("refused", PROBE_FAILED));
        }
        let settings = match context
            .reader
            .read(
                ReadRequest::InputSettings,
                Value::Object(request.selectors.clone()),
                context.deadline,
            )
            .await
        {
            Ok(value) => value,
            Err(_) => {
                live(&context)?;
                return Ok(text_error("refused", PROBE_FAILED));
            }
        };
        live(&context)?;
        if !context
            .fence
            .ready_at_revision(evidence.generation, evidence.revision)
        {
            return Err(invalidated());
        }
        let Some(kind) = settings
            .as_object()
            .and_then(|s| s.get("inputKind"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        else {
            return Ok(text_error("refused", KIND_MISSING));
        };
        if request.property_name == "capture_window" {
            if kind == "xcomposite_input"
                && settings
                    .get("inputSettings")
                    .and_then(Value::as_object)
                    .and_then(|s| s.get("capture_window"))
                    .and_then(Value::as_str)
                    .is_none_or(|s| s.is_empty())
            {
                return Ok(text_error("refused", XCOMPOSITE));
            }
        } else if kind == "screen_capture" {
            return Ok(text_error("refused", SCREEN_CAPTURE));
        }
    }
    if unsupported(&context, METHOD) {
        return Ok(text_error("failed", &unsupported_text(METHOD)));
    }
    live(&context)?;
    let remaining = context
        .deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(invalidated)?;
    let fence = context.fence.clone();
    let cancellation = context.cancellation.clone();
    let deadline = context.deadline;
    let mut data = request.selectors;
    data.insert("propertyName".into(), json!(request.property_name));
    let result = context
        .handle
        .request_checked(
            context.handle.token(),
            METHOD,
            Value::Object(data),
            remaining,
            move || {
                if cancellation.is_cancelled()
                    || Instant::now() >= deadline
                    || !fence.ready_at_revision(evidence.generation, evidence.revision)
                {
                    Err("Property-list source evidence changed; inspect OBS before retrying".into())
                } else {
                    Ok(())
                }
            },
        )
        .await;
    // A rejected reply may have preceding events too. Always settle those before
    // returning the exact authorized body or any metadata-only failure.
    context.reader.synchronize(context.deadline).await?;
    live(&context)?;
    if !context
        .fence
        .ready_at_revision(evidence.generation, evidence.revision)
    {
        return Err(invalidated());
    }
    match result {
        Ok(response) => {
            let structured = if response.is_object() {
                response
            } else {
                json!({})
            };
            if let Some(error) = output_error(&structured)? {
                return Ok(error);
            }
            Ok(
                json!({"content":[{"type":"text","text":serde_json::to_string_pretty(&structured).expect("JSON value")}],"structuredContent":structured}),
            )
        }
        Err(RequestError::BeforeDispatch(BeforeDispatch::GuardRefused(_))) => Err(invalidated()),
        Err(error) => match error.obs_failure_message(METHOD) {
            Some(message) => Ok(text_error("failed", &message)),
            None => Err(invalidated()),
        },
    }
}
