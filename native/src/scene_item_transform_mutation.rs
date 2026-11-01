// SPDX-License-Identifier: GPL-2.0-only
//! Closed transform projection and read-only follow-up. The enclosing owner
//! alone dispatches the mutation and durably records its fate before finish.
//! Transport text permits do not cover callback or frontend output copies.
use crate::{
    js_json::{self, Limits},
    ordered_transform::{SceneItemSelectors, SceneItemTransformReadback, TransformReadbackError},
    session::{BeforeDispatch, RequestError, SessionHandle},
    stage_transport::Cancellation,
};
use serde_json::{Map, Value, json};
use std::{collections::HashSet, future::Future, pin::Pin, sync::Arc};
use tokio::time::{Instant, timeout_at};

const SELECTORS: &[&str] = &["canvasUuid", "sceneName", "sceneUuid"];
const NUMBERS: &[&str] = &[
    "positionX",
    "positionY",
    "rotation",
    "scaleX",
    "scaleY",
    "cropTop",
    "cropBottom",
    "cropLeft",
    "cropRight",
];
const INVISIBLE: &str =
    "The source renders at 0×0, so the item is invisible whatever its transform";
const UNAVAILABLE: &str = "Transform readback cancelled or original operation deadline expired";

pub struct Request {
    arguments: Value,
}
impl std::fmt::Debug for Request {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.debug_struct("SceneItemTransformMutation")
            .finish_non_exhaustive()
    }
}
/// Metadata beside one retained normalized public argument Value. This grants
/// neither mutation, receipt publication nor postcondition-observation authority.
#[derive(Clone, Copy, Debug)]
pub struct FollowUp;
impl Request {
    pub fn parse(arguments: &Value) -> Option<Self> {
        Self::parse_owned(arguments.clone())
    }
    pub fn parse_owned(arguments: Value) -> Option<Self> {
        let Value::Object(mut source) = arguments else {
            return None;
        };
        let id = source.get("sceneItemId")?.as_f64()?;
        if !id.is_finite() || id < 0.0 || id.fract() != 0.0 || id > 9_007_199_254_740_991.0 {
            return None;
        }
        for field in SELECTORS {
            if source.get(*field).is_some_and(|value| !value.is_string()) {
                return None;
            }
        }
        for field in NUMBERS {
            if source
                .get(*field)
                .is_some_and(|value| value.as_f64().is_none_or(|n| !n.is_finite()))
            {
                return None;
            }
        }
        if source
            .get("sceneItemTransform")
            .is_some_and(|value| !value.is_object())
        {
            return None;
        }
        let mut public = Map::new();
        for field in SELECTORS
            .iter()
            .chain([&"sceneItemId", &"sceneItemTransform"])
            .chain(NUMBERS)
        {
            if let Some(value) = source.remove(*field) {
                public.insert((*field).into(), value);
            }
        }
        // Zod record strips only this record's own prototype setter key. Unknown
        // nested values and literal private serde-like keys remain ordinary data.
        if let Some(Value::Object(transform)) = public.get_mut("sceneItemTransform") {
            transform.remove("__proto__");
        }
        let mut arguments = Value::Object(public);
        js_json::normalize_with_limits(&mut arguments, Limits::default()).ok()?;
        Some(Self { arguments })
    }
    pub fn arguments(&self) -> &Value {
        &self.arguments
    }
    pub fn request_data(&self) -> Value {
        FollowUp.request_data(&self.arguments)
    }
    pub fn into_parts(self) -> (Value, FollowUp) {
        (self.arguments, FollowUp)
    }
}
impl FollowUp {
    /// Active-only wire projection from the validated public owner returned by
    /// Request. Callers must not mutate those arguments between parse and use.
    pub fn request_data(self, arguments: &Value) -> Value {
        let mut transform = arguments
            .get("sceneItemTransform")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        for pair in [["positionX", "positionY"], ["scaleX", "scaleY"]] {
            if pair.iter().any(|field| arguments.get(*field).is_some()) {
                for field in pair {
                    // Source assigns BOTH fields when either is supplied.
                    // Assignment of undefined removes an existing opaque partner
                    // from JSON.stringify's wire output.
                    transform.remove(field);
                    if let Some(value) = arguments.get(field) {
                        transform.insert(field.into(), value.clone());
                    }
                }
            }
        }
        for field in ["rotation", "cropTop", "cropBottom", "cropLeft", "cropRight"] {
            if let Some(value) = arguments.get(field) {
                transform.insert(field.into(), value.clone());
            }
        }
        let mut wire = Map::new();
        for field in SELECTORS.iter().chain([&"sceneItemId"]) {
            if let Some(value) = arguments.get(*field) {
                wire.insert((*field).into(), value.clone());
            }
        }
        wire.insert("sceneItemTransform".into(), Value::Object(transform));
        Value::Object(wire)
    }
    pub fn selectors(self, arguments: &Value) -> SceneItemSelectors {
        let string = |field: &str| {
            arguments
                .get(field)
                .and_then(Value::as_str)
                .map(str::to_owned)
        };
        SceneItemSelectors {
            canvas_uuid: string("canvasUuid"),
            scene_name: string("sceneName"),
            scene_uuid: string("sceneUuid"),
            scene_item_id: arguments["sceneItemId"]
                .as_u64()
                .expect("validated normalized safe integer"),
        }
    }
    /// Only after durable Accepted. The existing physical reader owns an
    /// admitted read through the original deadline; cancellation never drops an
    /// in-flight Session reply or grants a mutation replay permission.
    pub async fn finish(
        self,
        arguments: &Value,
        reader: &mut impl ReadAdapter,
        cancellation: &Cancellation,
        deadline: Instant,
    ) -> Value {
        let scene = arguments
            .get("sceneName")
            .or_else(|| arguments.get("sceneUuid"));
        let message = format!(
            "Successfully updated transform for item with ID {} in {}",
            crate::observation::js_string(arguments.get("sceneItemId")),
            crate::observation::js_string(scene)
        );
        let reply = if cancellation.is_cancelled() || Instant::now() >= deadline {
            Err(UNAVAILABLE.into())
        } else if reader.supports() == Some(false) {
            Err(
                "OBS WebSocket does not advertise support for request 'GetSceneItemTransform'"
                    .into(),
            )
        } else {
            timeout_at(deadline, reader.read(self.selectors(arguments), deadline))
                .await
                .map_err(|_| UNAVAILABLE.to_owned())
                .and_then(|result| result.map_err(|reason| reason.tool_message()))
        };
        match reply {
            Ok(SceneItemTransformReadback {
                mut response_data,
                applied_json,
            }) => {
                let mut transform = response_data
                    .as_object_mut()
                    .and_then(|fields| fields.remove("sceneItemTransform"))
                    .filter(|value| !value.is_null())
                    .unwrap_or_else(|| json!({}));
                drop(response_data);
                if js_json::normalize_with_limits(&mut transform, Limits::readback()).is_err() {
                    return unavailable_result(
                        message,
                        "Native transform numeric projection unavailable".into(),
                    );
                }
                let zero = |field: &str| {
                    transform
                        .get(field)
                        .and_then(Value::as_f64)
                        .is_some_and(|n| n == 0.0)
                };
                let warning =
                    (zero("sourceWidth") || zero("sourceHeight")).then(|| INVISIBLE.to_owned());
                // Exact Source order is available ONLY in this typed text. A
                // newly allocated callback body is outside its transport pool.
                result(message, transform, applied_json.as_str(), warning)
            }
            Err(reason) => unavailable_result(message, reason),
        }
    }
}
pub fn failure(message: &str) -> Value {
    json!({"content":[{"type":"text","text":format!("Error setting scene item transform: {message}")}],"isError":true})
}
fn unavailable_result(message: String, reason: String) -> Value {
    result(
        message,
        json!({}),
        "{}",
        Some(format!(
            "Could not read the applied transform back: {reason}"
        )),
    )
}
fn result(message: String, transform: Value, applied: &str, warning: Option<String>) -> Value {
    let mut text = format!("{message}\nApplied transform: {applied}");
    // json!(transform) serializes a borrow through to_value and would deep-copy
    // the selected subtree. Insert the owned Value directly instead.
    let mut structured = Map::new();
    structured.insert("message".into(), Value::String(message));
    structured.insert("sceneItemTransform".into(), transform);
    if let Some(warning) = warning {
        text.push_str("\nWarning: ");
        text.push_str(&warning);
        structured.insert(
            "warnings".into(),
            Value::Array(vec![Value::String(warning)]),
        );
    }
    let block = Value::Object(Map::from_iter([
        ("type".into(), Value::String("text".into())),
        ("text".into(), Value::String(text)),
    ]));
    Value::Object(Map::from_iter([
        ("content".into(), Value::Array(vec![block])),
        ("structuredContent".into(), Value::Object(structured)),
    ]))
}

pub type ReadFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<SceneItemTransformReadback, TransformReadbackError>> + Send + 'a,
    >,
>;
pub trait ReadAdapter {
    fn supports(&self) -> Option<bool>;
    fn read<'a>(&'a mut self, selectors: SceneItemSelectors, deadline: Instant) -> ReadFuture<'a>;
}
pub struct SessionReadback {
    handle: SessionHandle,
    capabilities: Option<Arc<HashSet<String>>>,
}
impl SessionReadback {
    pub fn new(handle: SessionHandle, capabilities: Option<Arc<HashSet<String>>>) -> Self {
        Self {
            handle,
            capabilities,
        }
    }
}
impl ReadAdapter for SessionReadback {
    fn supports(&self) -> Option<bool> {
        self.capabilities
            .as_ref()
            .map(|set| set.contains("GetSceneItemTransform"))
    }
    fn read<'a>(&'a mut self, selectors: SceneItemSelectors, deadline: Instant) -> ReadFuture<'a> {
        Box::pin(async move {
            let duration = deadline
                .checked_duration_since(Instant::now())
                .filter(|d| !d.is_zero())
                .ok_or(TransformReadbackError::Request(
                    RequestError::BeforeDispatch(BeforeDispatch::Deadline),
                ))?;
            self.handle
                .scene_item_transform_checked(self.handle.token(), selectors, duration, || Ok(()))
                .await
        })
    }
}
