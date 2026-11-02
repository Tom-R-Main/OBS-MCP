// SPDX-License-Identifier: GPL-2.0-only
//! Closed input mutations with Source-compatible, read-only follow-up evidence.
//! The enclosing retained job owns dispatch and durable receipt settlement.
//! This module cannot send a mutation or publish or promote a receipt.
use crate::{
    js_json::{self, Limits, NumericNormalizationError},
    observation::{ObservationError, ReadAdapter, ReadFuture, ReadRequest},
    preflight::renders_no_video,
    scene_audit::{ScreenshotLimit, screenshot_content},
    session::SessionHandle,
    stage_transport::Cancellation,
};
use serde_json::{Map, Value, json};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::time::{Instant, sleep, timeout_at};

/// The existing physical actor performs plain sequential reads. A successful
/// reply is not a frozen scene sample or evidence of an atomic postcondition.
/// The enclosing Generic job retains this adapter and its publication scope.
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
    fn supports(&self, request: ReadRequest) -> Option<bool> {
        self.capabilities
            .as_ref()
            .map(|set| set.contains(request.as_str()))
    }
    fn read<'a>(
        &'a mut self,
        request: ReadRequest,
        data: Value,
        deadline: Instant,
    ) -> ReadFuture<'a> {
        Box::pin(async move {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .filter(|duration| !duration.is_zero())
                .ok_or_else(unavailable)?;
            self.handle
                .request(self.handle.token(), request.as_str(), data, remaining)
                .await
                .map_err(|reason| {
                    if let Some(code) = reason.rejection_code() {
                        return ObservationError::rejected(
                            code,
                            "input_readback",
                            reason
                                .obs_failure_message(request.as_str())
                                .expect("typed rejection"),
                        );
                    }
                    let message = self
                        .handle
                        .authorized_disconnect_message(&reason)
                        .unwrap_or_else(|| {
                            "Input readback unavailable on the admitted OBS connection".into()
                        });
                    ObservationError::new("input_readback_unavailable", message)
                })
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    CreateInput,
    InputSettings,
}
impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Self::CreateInput => "obs-create-input",
            Self::InputSettings => "obs-set-input-settings",
        }
    }
    pub fn request_type(self) -> &'static str {
        match self {
            Self::CreateInput => "CreateInput",
            Self::InputSettings => "SetInputSettings",
        }
    }
    pub fn failure(self, message: &str) -> Value {
        let prefix = match self {
            Self::CreateInput => "Error creating input",
            Self::InputSettings => "Error setting input settings",
        };
        json!({"content":[{"type":"text","text":format!("{prefix}: {message}")}],"isError":true})
    }
}

/// Small metadata travels beside the single owned settings payload. It confers
/// no mutation, publication, restoration or observation authority.
#[derive(Clone, Copy, Debug)]
pub struct FollowUp {
    kind: Kind,
    screenshot: Option<bool>,
}
pub struct Request {
    data: Value,
    follow_up: FollowUp,
}
impl std::fmt::Debug for Request {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.debug_struct("InputMutationRequest")
            .field("kind", &self.follow_up.kind)
            .finish_non_exhaustive()
    }
}
fn kind(name: &str) -> Option<Kind> {
    Some(match name {
        "obs-create-input" => Kind::CreateInput,
        "obs-set-input-settings" => Kind::InputSettings,
        _ => return None,
    })
}
fn fields(kind: Kind) -> &'static [&'static str] {
    match kind {
        Kind::CreateInput => &[
            "canvasUuid",
            "sceneName",
            "inputName",
            "inputKind",
            "inputSettings",
            "sceneItemEnabled",
        ],
        Kind::InputSettings => &["inputName", "inputSettings", "overlay"],
    }
}
fn validate(kind: Kind, source: &Map<String, Value>) -> Option<FollowUp> {
    for key in if kind == Kind::CreateInput {
        &["sceneName", "inputName", "inputKind"][..]
    } else {
        &["inputName"][..]
    } {
        source.get(*key)?.as_str()?;
    }
    if kind == Kind::CreateInput
        && let Some(value) = source.get("canvasUuid")
    {
        value.as_str()?;
    }
    if let Some(value) = source.get("inputSettings") {
        value.as_object()?;
    } else if kind == Kind::InputSettings {
        return None;
    }
    let flag = if kind == Kind::CreateInput {
        "sceneItemEnabled"
    } else {
        "overlay"
    };
    if let Some(value) = source.get(flag) {
        value.as_bool()?;
    }
    let screenshot = match source.get("includeScreenshot") {
        None => None,
        Some(value) => Some(value.as_bool()?),
    };
    Some(FollowUp { kind, screenshot })
}
fn shaped(mut data: Map<String, Value>, follow_up: FollowUp) -> Option<Request> {
    // Source ZodRecord drops this own key at the record boundary. Nested unknown
    // values and OBS readback objects retain it; the numeric helper drops no keys.
    if let Some(settings) = data.get_mut("inputSettings").and_then(Value::as_object_mut) {
        settings.remove("__proto__");
    }
    let mut data = Value::Object(data);
    js_json::normalize_json_numbers(&mut data).ok()?;
    Some(Request { data, follow_up })
}
impl Request {
    pub fn parse(name: &str, args: &Value) -> Option<Self> {
        let kind = kind(name)?;
        let source = args.as_object()?;
        let follow_up = validate(kind, source)?;
        let data = fields(kind)
            .iter()
            .filter_map(|key| source.get(*key).map(|value| ((*key).into(), value.clone())))
            .collect();
        shaped(data, follow_up)
    }
    /// Consume already-owned admitted arguments. Settings move into the sole
    /// queued Value; discarded public extensions are not copied into the queue.
    pub fn parse_owned(name: &str, args: Value) -> Option<Self> {
        let kind = kind(name)?;
        let Value::Object(mut source) = args else {
            return None;
        };
        let follow_up = validate(kind, &source)?;
        let data = fields(kind)
            .iter()
            .filter_map(|key| source.remove(*key).map(|value| ((*key).into(), value)))
            .collect();
        shaped(data, follow_up)
    }
    pub fn kind(&self) -> Kind {
        self.follow_up.kind
    }
    pub fn data(&self) -> &Value {
        &self.data
    }
    pub fn arguments(&self) -> Value {
        self.follow_up.arguments(&self.data)
    }
    pub fn into_parts(self) -> (Value, FollowUp) {
        (self.data, self.follow_up)
    }
    /// Move the same opaque owner into Generic's normalized public arguments.
    pub fn into_public_parts(mut self) -> (Value, FollowUp) {
        if let Some(flag) = self.follow_up.screenshot {
            self.data["includeScreenshot"] = json!(flag);
        }
        (self.data, self.follow_up)
    }
}
impl FollowUp {
    pub fn kind(self) -> Kind {
        self.kind
    }
    pub fn arguments(self, data: &Value) -> Value {
        let mut args = data.clone();
        if let Some(screenshot) = self.screenshot {
            args["includeScreenshot"] = json!(screenshot);
        }
        args
    }
    /// The actual retained public-argument encoding, including the local
    /// screenshot flag, without cloning the opaque settings payload.
    pub fn retained_argument_bytes(
        self,
        data: &Value,
        limits: Limits,
    ) -> Result<usize, NumericNormalizationError> {
        let bytes = js_json::encoded_len(data, limits)?;
        let extra = self.screenshot.map_or(0, |flag| {
            r#","includeScreenshot":"#.len() + if flag { 4 } else { 5 }
        });
        bytes
            .checked_add(extra)
            .filter(|bytes| *bytes <= limits.max_encoded_bytes)
            .ok_or(NumericNormalizationError::EncodedBytes)
    }
    /// Called only after the enclosing owner durably records Accepted. The
    /// returned body is client evidence; it never changes that receipt phase.
    pub async fn finish(
        self,
        data: &Value,
        mutation_response: &Value,
        reader: &mut impl ReadAdapter,
        limit: Result<ScreenshotLimit, ObservationError>,
        cancellation: &Cancellation,
        deadline: Instant,
    ) -> Value {
        let name = data["inputName"].as_str().expect("parsed input name");
        let message = match self.kind {
            Kind::CreateInput => {
                let id = match js_string(mutation_response.get("sceneItemId"), 0) {
                    Ok(id) => id,
                    Err(()) => {
                        return self
                            .kind
                            .failure("Cannot convert object to primitive value");
                    }
                };
                format!(
                    "Successfully created input '{name}' of kind '{}' with ID {id}",
                    data["inputKind"].as_str().expect("parsed input kind")
                )
            }
            Kind::InputSettings => format!("Successfully updated settings for input: {name}"),
        };
        let mut scope = Scope {
            reader,
            cancellation,
            deadline,
        };
        let state = input_state(&mut scope, name).await;
        let mut result = input_result(message, state);
        if self.screenshot == Some(true) {
            let image = scope
                .read(
                    ReadRequest::SourceScreenshot,
                    json!({"sourceName":name,"imageFormat":"jpeg","imageWidth":960}),
                )
                .await
                .and_then(|response| limit.and_then(|limit| screenshot_content(&response, limit)));
            let content = result["content"]
                .as_array_mut()
                .expect("input result content");
            match image {
                Ok(image) => {
                    let payload = image["data"].as_str().expect("admitted image");
                    let padding = payload.bytes().rev().take_while(|b| *b == b'=').count();
                    let bytes = payload.len() / 4 * 3 - padding;
                    let mime = image["mimeType"].as_str().expect("admitted MIME");
                    content.push(json!({"type":"text","text":format!("OBS screenshot ({mime}, {bytes} bytes)")}));
                    content.push(image);
                }
                Err(reason) => {
                    content.push(
                        json!({"type":"text","text":format!("No screenshot: {}",reason.message)}),
                    );
                }
            }
        }
        result
    }
}
struct Scope<'a, R> {
    reader: &'a mut R,
    cancellation: &'a Cancellation,
    deadline: Instant,
}
impl<R: ReadAdapter> Scope<'_, R> {
    async fn read(&mut self, request: ReadRequest, data: Value) -> Result<Value, ObservationError> {
        if self.cancellation.is_cancelled() || Instant::now() >= self.deadline {
            return Err(unavailable());
        }
        if self.reader.supports(request) == Some(false) {
            return Err(ObservationError::new(
                "capability_unavailable",
                format!(
                    "OBS WebSocket does not advertise support for request '{}'",
                    request.as_str()
                ),
            ));
        }
        // An admitted physical read remains owned through its original deadline.
        // Dropping its reply future after dispatch would invalidate the shared
        // Session and discard unrelated production work. Cancellation prevents
        // subsequent reads; it does not abandon this in-flight reply owner.
        timeout_at(
            self.deadline,
            self.reader.read(request, data, self.deadline),
        )
        .await
        .unwrap_or_else(|_| Err(unavailable()))
    }
    async fn settle_delay(&self) -> bool {
        if self.cancellation.is_cancelled() || Instant::now() >= self.deadline {
            return false;
        }
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => false,
            result = timeout_at(self.deadline, sleep(Duration::from_millis(250))) => result.is_ok(),
        }
    }
}
fn unavailable() -> ObservationError {
    ObservationError::new(
        "input_readback_unavailable",
        "Input readback cancelled or original operation deadline expired",
    )
}
struct State {
    settings: Value,
    appearances: Vec<Value>,
    warnings: Vec<String>,
}
async fn appearances(
    scope: &mut Scope<'_, impl ReadAdapter>,
    name: &str,
) -> Result<Vec<Value>, ObservationError> {
    let program = scope
        .read(ReadRequest::CurrentProgramScene, Value::Null)
        .await?;
    let Some(scene) = program
        .get("currentProgramSceneName")
        .and_then(Value::as_str)
    else {
        return Ok(vec![]);
    };
    let list = scope
        .read(ReadRequest::SceneItemList, json!({"sceneName":scene}))
        .await?;
    let mut result=Value::Array(list.get("sceneItems").and_then(Value::as_array).into_iter().flatten()
        .filter(|item| item.is_object() && item.get("sourceName").and_then(Value::as_str)==Some(name) && item.get("sceneItemId").is_some_and(Value::is_number))
        .map(|item| {
            let transform=item.get("sceneItemTransform").filter(|value|value.is_object());
            let number=|key|transform.and_then(|value|value.get(key)).filter(|value|value.is_number()).cloned().unwrap_or(json!(0));
            json!({"sceneName":scene,"sceneItemId":item["sceneItemId"],"sceneItemEnabled":item.get("sceneItemEnabled")!=Some(&Value::Bool(false)),"sourceWidth":number("sourceWidth"),"sourceHeight":number("sourceHeight")})
        }).collect());
    js_json::normalize_with_limits(&mut result, Limits::readback())
        .map_err(|_| projection_unavailable())?;
    let Value::Array(result) = result else {
        unreachable!("constructed appearances array")
    };
    Ok(result)
}
fn blank(item: &Value) -> bool {
    item["sceneItemEnabled"] == true
        && ["sourceWidth", "sourceHeight"]
            .iter()
            .any(|key| item[*key].as_f64() == Some(0.0))
}
async fn input_state(scope: &mut Scope<'_, impl ReadAdapter>, name: &str) -> State {
    let mut state = State {
        settings: json!({}),
        appearances: vec![],
        warnings: vec![],
    };
    let mut kind = Value::Null;
    match scope
        .read(ReadRequest::InputSettings, json!({"inputName":name}))
        .await
    {
        Ok(mut response) => {
            if let Some(fields) = response.as_object_mut() {
                kind = fields.remove("inputKind").unwrap_or_default();
                if let Some(mut settings) = fields.remove("inputSettings").filter(Value::is_object)
                {
                    if js_json::normalize_with_limits(&mut settings, Limits::readback()).is_ok() {
                        state.settings = settings;
                    } else {
                        state.warnings.push(format!(
                            "Could not read the input back: {}",
                            projection_unavailable().message
                        ));
                    }
                }
            }
        }
        Err(reason) => state
            .warnings
            .push(format!("Could not read the input back: {}", reason.message)),
    }
    let no_video = renders_no_video(&kind, Some(&state.settings));
    for attempt in 0..4 {
        match appearances(scope, name).await {
            Ok(items) => state.appearances = items,
            Err(reason) => {
                state.warnings.push(format!(
                    "Could not read the program scene: {}",
                    reason.message
                ));
                break;
            }
        }
        if no_video || !state.appearances.iter().any(blank) || attempt == 3 {
            break;
        }
        if !scope.settle_delay().await {
            state.warnings.push(format!(
                "Could not read the program scene: {}",
                unavailable().message
            ));
            break;
        }
    }
    if !no_video && state.appearances.iter().any(blank) {
        state.warnings.push(format!("{name} renders at 0×0 in the program scene. A capture source usually needs a display, window, or application selected (macOS screen_capture application capture also needs display_uuid) and screen recording permission for OBS"));
    }
    state
}
fn input_result(message: String, state: State) -> Value {
    let sizes = state
        .appearances
        .iter()
        .map(|item| {
            format!(
                "{} item {}: {}×{}",
                item["sceneName"].as_str().unwrap(),
                crate::observation::js_string(Some(&item["sceneItemId"])),
                crate::observation::js_string(Some(&item["sourceWidth"])),
                crate::observation::js_string(Some(&item["sourceHeight"])),
            )
        })
        .collect::<Vec<_>>();
    let mut lines = vec![
        message.clone(),
        if sizes.is_empty() {
            "Not in the program scene".into()
        } else {
            format!("Size in program scene: {}", sizes.join("; "))
        },
    ];
    lines.extend(
        state
            .warnings
            .iter()
            .map(|warning| format!("Warning: {warning}")),
    );
    let text = lines.join("\n");
    let structured = Value::Object(Map::from_iter([
        ("message".into(), Value::String(message)),
        ("inputSettings".into(), state.settings),
        ("appearances".into(), Value::Array(state.appearances)),
        (
            "warnings".into(),
            Value::Array(state.warnings.into_iter().map(Value::String).collect()),
        ),
    ]));
    let block = Value::Object(Map::from_iter([
        ("type".into(), Value::String("text".into())),
        ("text".into(), Value::String(text)),
    ]));
    Value::Object(Map::from_iter([
        ("content".into(), Value::Array(vec![block])),
        ("structuredContent".into(), structured),
    ]))
}
fn projection_unavailable() -> ObservationError {
    ObservationError::new(
        "input_readback_unavailable",
        "Input readback finite numeric projection unavailable",
    )
}
fn js_string(value: Option<&Value>, depth: usize) -> Result<String, ()> {
    if depth > 128 {
        return Err(());
    }
    Ok(match value {
        None => "undefined".into(),
        Some(Value::Null) => "null".into(),
        Some(Value::Bool(value)) => value.to_string(),
        Some(Value::Number(_)) => crate::observation::js_string(value),
        Some(Value::String(value)) => value.clone(),
        Some(Value::Object(value)) => {
            if value.contains_key("toString") {
                return Err(());
            }
            "[object Object]".into()
        }
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                if value.is_null() {
                    Ok(String::new())
                } else {
                    js_string(Some(value), depth + 1)
                }
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(","),
    })
}

#[cfg(test)]
mod ownership_tests {
    use super::*;

    #[test]
    fn result_moves_large_readback_settings_appearances_and_warning_buffers() {
        let payload = "x".repeat(1024 * 1024);
        let payload_address = payload.as_ptr() as usize;
        let scene = "fixture-scene".repeat(100);
        let scene_address = scene.as_ptr() as usize;
        let warning = "fixture-warning".repeat(100);
        let warning_address = warning.as_ptr() as usize;
        let settings = Value::Object(Map::from_iter([("opaque".into(), Value::String(payload))]));
        let appearance = Value::Object(Map::from_iter([
            ("sceneName".into(), Value::String(scene)),
            ("sceneItemId".into(), json!(7)),
            ("sourceWidth".into(), json!(640)),
            ("sourceHeight".into(), json!(480)),
        ]));
        let result = input_result(
            "fixture-message".into(),
            State {
                settings,
                appearances: vec![appearance],
                warnings: vec![warning],
            },
        );
        let structured = &result["structuredContent"];
        let payload = structured["inputSettings"]["opaque"].as_str().unwrap();
        assert_eq!(payload.len(), 1024 * 1024);
        assert_eq!(
            payload.as_ptr() as usize,
            payload_address,
            "selected settings must move without allocating another payload buffer"
        );
        assert_eq!(
            structured["appearances"][0]["sceneName"]
                .as_str()
                .unwrap()
                .as_ptr() as usize,
            scene_address
        );
        assert_eq!(
            structured["warnings"][0].as_str().unwrap().as_ptr() as usize,
            warning_address
        );
    }
}
