// SPDX-License-Identifier: GPL-2.0-only
//! Structural audit over the same typed read adapter as scene planning. This
//! source-specific profile retains 512 reads and a UTF-16 evidence limit. Its
//! provider must use invalidates_event below, not the broader planning profile.
use crate::observation::{
    EvidenceState, ObservationError, ReadAdapter, ReadRequest, Target, identity,
    normalize_scene_item, safe_integer, signature,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    time::Duration,
};
use tokio::{
    sync::watch,
    time::{Instant, timeout_at},
};

pub const MAX_ITEMS: usize = 128;
pub const MAX_DEPTH: usize = 8;
pub const MAX_REQUESTS: usize = 512;
const EVIDENCE_UNITS: usize = 256 * 1024;
const REPORT_BYTES: usize = 512 * 1024;
const NUMERIC_FIELDS: &[&str] = &[
    "positionX",
    "positionY",
    "rotation",
    "scaleX",
    "scaleY",
    "sourceWidth",
    "sourceHeight",
    "cropLeft",
    "cropRight",
    "cropTop",
    "cropBottom",
    "alignment",
    "boundsAlignment",
    "boundsWidth",
    "boundsHeight",
];

fn error(message: impl Into<String>) -> ObservationError {
    ObservationError::new("evidence_unavailable", message)
}
pub fn invalidates_event(event: &str) -> bool {
    [
        "CurrentSceneCollection",
        "SceneCollection",
        "Canvas",
        "SceneCreated",
        "SceneRemoved",
        "SceneNameChanged",
        "SceneItem",
        "InputCreated",
        "InputRemoved",
        "InputNameChanged",
        "InputSettingsChanged",
        "VideoSettingsChanged",
    ]
    .iter()
    .any(|prefix| event.starts_with(prefix))
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Selector {
    source_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_uuid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    container_uuid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scene_item_id: Option<u64>,
}
pub struct AuditOptions {
    target: Target,
    required: Vec<Selector>,
    include_screenshot: bool,
}
impl AuditOptions {
    pub fn from_value(value: &Value) -> Result<Self, ObservationError> {
        let object = value
            .as_object()
            .ok_or_else(|| error("Invalid scene audit arguments"))?;
        let target = Target {
            scene_collection: identity(&value["sceneCollection"])?.to_owned(),
            scene_name: identity(&value["sceneName"])?.to_owned(),
            canvas_uuid: object
                .get("canvasUuid")
                .map(|value| identity(value).map(str::to_owned))
                .transpose()?,
        };
        let required = match object.get("requiredItems") {
            None => vec![],
            Some(value) => value
                .as_array()
                .filter(|items| items.len() <= 64)
                .ok_or_else(|| error("Invalid scene audit arguments"))?
                .iter()
                .map(|item| {
                    let object = item
                        .as_object()
                        .ok_or_else(|| error("Invalid scene audit arguments"))?;
                    Ok(Selector {
                        source_name: identity(&item["sourceName"])?.to_owned(),
                        source_uuid: object
                            .get("sourceUuid")
                            .map(|value| identity(value).map(str::to_owned))
                            .transpose()?,
                        container_uuid: object
                            .get("containerUuid")
                            .map(|value| identity(value).map(str::to_owned))
                            .transpose()?,
                        scene_item_id: object
                            .get("sceneItemId")
                            .map(|value| {
                                safe_integer(value)
                                    .filter(|value| *value >= 0.0)
                                    .map(|value| value as u64)
                                    .ok_or_else(|| error("Invalid scene audit arguments"))
                            })
                            .transpose()?,
                    })
                })
                .collect::<Result<Vec<_>, ObservationError>>()?,
        };
        let include_screenshot = object
            .get("includeScreenshot")
            .map(|value| {
                value
                    .as_bool()
                    .ok_or_else(|| error("Invalid scene audit arguments"))
            })
            .transpose()?
            .unwrap_or(false);
        Ok(Self {
            target,
            required,
            include_screenshot,
        })
    }
}

/// Scalar policy is supplied by the configuration owner; no environment or
/// user files are read here. An invalid limit matters only if an image is asked.
#[derive(Clone, Copy, Debug)]
pub struct ScreenshotLimit(usize);
impl Default for ScreenshotLimit {
    fn default() -> Self {
        Self(4 * 1024 * 1024)
    }
}
impl ScreenshotLimit {
    pub fn bytes(self) -> usize {
        self.0
    }
    pub fn parse(value: Option<&str>) -> Result<Self, ObservationError> {
        let Some(value) = value.filter(|value| !value.is_empty()) else {
            return Ok(Self::default());
        };
        if !value.bytes().all(|byte| byte.is_ascii_digit()) || value.starts_with('0') {
            return Err(error(
                "OBS_MCP_MAX_SCREENSHOT_BYTES must be a positive integer",
            ));
        }
        let bytes = value
            .parse::<usize>()
            .ok()
            .filter(|bytes| *bytes <= 6 * 1024 * 1024)
            .ok_or_else(|| error("OBS_MCP_MAX_SCREENSHOT_BYTES cannot exceed 6291456"))?;
        Ok(Self(bytes))
    }
}

pub struct AuditCompletion {
    pub completed_at: String,
    pub duration_ms: u64,
}
pub struct AuditSample<'a, R: ReadAdapter> {
    reader: &'a mut R,
    state: watch::Receiver<EvidenceState>,
    generation: u64,
    revision: u64,
    observed_at: String,
    deadline: Instant,
    requests: usize,
    invalidated: bool,
    closed: bool,
    requires_compatibility: bool,
}
impl<'a, R: ReadAdapter> AuditSample<'a, R> {
    pub fn new(
        reader: &'a mut R,
        state: watch::Receiver<EvidenceState>,
        generation: u64,
        observed_at: String,
        deadline: Instant,
    ) -> Self {
        let revision = state.borrow().revision;
        Self {
            reader,
            state,
            generation,
            revision,
            observed_at,
            deadline: deadline.min(Instant::now() + Duration::from_secs(12)),
            requests: 0,
            invalidated: false,
            closed: false,
            requires_compatibility: false,
        }
    }
    pub fn requests(&self) -> usize {
        self.requests
    }
    /// Peer rejection text and unsupported evidence projections belong to the
    /// compatibility owner until the native projection is separately qualified.
    pub fn requires_compatibility(&self) -> bool {
        self.requires_compatibility
    }
    pub fn close(&mut self) {
        self.closed = true;
    }
    fn refresh(&mut self) {
        let state = *self.state.borrow();
        if self.state.has_changed().is_err()
            || !state.connected
            || state.cancelled
            || state.generation != self.generation
            || state.revision != self.revision
        {
            self.invalidated = true;
        }
    }
    pub async fn read(
        &mut self,
        request: ReadRequest,
        data: Value,
    ) -> Result<Value, ObservationError> {
        self.refresh();
        if self.closed || self.invalidated {
            return Err(error("OBS state changed while sampling"));
        }
        let now = Instant::now();
        if now >= self.deadline || self.requests >= MAX_REQUESTS {
            return Err(error("Scene audit request or time limit reached"));
        }
        self.requests += 1;
        if self.reader.supports(request) == Some(false) {
            return Err(error(format!(
                "OBS WebSocket does not advertise support for request '{}'",
                request.as_str()
            )));
        }
        let deadline = self.deadline.min(now + Duration::from_secs(2));
        let work = timeout_at(deadline, self.reader.read(request, data, deadline));
        tokio::pin!(work);
        let value = loop {
            tokio::select! {biased;
                result=&mut work=>break result.map_err(|_|error(format!("Scene audit timed out reading {}",request.as_str())))?,
                changed=self.state.changed()=>{
                    if changed.is_err(){self.invalidated=true;return Err(error("OBS state changed while sampling"));}
                    let state=*self.state.borrow();
                    if !state.connected{self.invalidated=true;return Err(error("OBS state changed while sampling"));}
                    // Keep an admitted healthy read alive through event/cancel
                    // invalidation; reject its eventual evidence and all next reads.
                }
            }
        };
        let state = *self.state.borrow();
        if self.state.has_changed().is_err()
            || !state.connected
            || state.cancelled
            || state.generation != self.generation
            || state.revision != self.revision
        {
            self.invalidated = true;
        }
        let value = value.inspect_err(|error| {
            if error.request_code().is_some() {
                self.requires_compatibility = true;
            }
        })?;
        if self.invalidated || Instant::now() >= self.deadline {
            return Err(error(
                "OBS state changed or scene audit deadline elapsed while sampling",
            ));
        }
        if !value.is_object() {
            return Err(error(format!(
                "OBS returned incomplete {} state",
                request.as_str()
            )));
        }
        if request != ReadRequest::SourceScreenshot && json_utf16_units(&value)? > EVIDENCE_UNITS {
            return Err(error(format!(
                "OBS returned oversized {} evidence",
                request.as_str()
            )));
        }
        Ok(value)
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ItemIdentity {
    source_name: String,
    source_uuid: String,
    container_uuid: String,
    scene_item_id: u64,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Item {
    #[serde(flatten)]
    identity: ItemIdentity,
    path: Vec<ItemIdentity>,
    enabled: Option<bool>,
    effective_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transform: Option<Value>,
}
struct Container {
    uuid: String,
    group: bool,
    before: String,
}
struct Transform {
    item: usize,
    before: String,
}
struct Audit<'a, 'b, R: ReadAdapter> {
    sample: &'a mut AuditSample<'b, R>,
    options: &'a AuditOptions,
    checks: Vec<Value>,
    items: Vec<Item>,
    containers: Vec<Container>,
    transforms: Vec<Transform>,
    complete: bool,
    image: Option<Value>,
    scenes: HashMap<String, Value>,
    inputs: Vec<Value>,
}
impl<R: ReadAdapter> Audit<'_, '_, R> {
    fn add(
        &mut self,
        id: &str,
        status: &str,
        message: impl Into<String>,
        identity: Option<&ItemIdentity>,
        evidence: Option<Value>,
    ) {
        let message = message.into();
        let (message, supported) = truncate_message(&message);
        if !supported {
            self.complete = false;
            self.sample.requires_compatibility = true;
        }
        let mut check =
            json!({"id":id,"status":if supported{status}else{"unknown"},"message":message});
        if let Some(identity) = identity {
            check["identity"] = serde_json::to_value(identity).expect("audit identity");
        }
        if let Some(evidence) = evidence {
            check["evidence"] = evidence;
        }
        self.checks.push(check);
    }
    fn scoped(&self, uuid: &str) -> Value {
        let mut data = self.options.target.canvas();
        data["sceneUuid"] = json!(uuid);
        data
    }
    fn walk<'a>(
        &'a mut self,
        uuid: String,
        group: bool,
        path: Vec<ItemIdentity>,
        ancestors: HashSet<String>,
        ancestor_enabled: Option<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ObservationError>> + Send + 'a>> {
        Box::pin(async move {
            if path.len() > MAX_DEPTH || ancestors.contains(&uuid) {
                self.complete = false;
                self.add(
                    "membership",
                    "unknown",
                    "Nested scene cycle or depth limit reached",
                    None,
                    None,
                );
                return Ok(());
            }
            let request = if group {
                ReadRequest::GroupSceneItemList
            } else {
                ReadRequest::SceneItemList
            };
            let data = self.scoped(&uuid);
            let reply = self.sample.read(request, data).await?;
            let members = objects(&reply["sceneItems"])?
                .into_iter()
                .map(normalize_scene_item)
                .collect::<Vec<_>>();
            if members.len() > MAX_ITEMS || duplicate_item_ids(&members) {
                return Err(error(
                    "Scene item inventory is ambiguous or exceeds the item limit",
                ));
            }
            self.containers.push(Container {
                uuid: uuid.clone(),
                group,
                before: signature(&membership(&members))?,
            });
            for member in members {
                if self.items.len() >= MAX_ITEMS {
                    return Err(error("Scene audit item limit reached"));
                }
                if !text(&member["sourceName"])
                    || !text(&member["sourceUuid"])
                    || safe_integer(&member["sceneItemId"]).is_none_or(|value| value < 0.0)
                    || !member["isGroup"].is_boolean()
                {
                    return Err(error("Scene item identity or membership is incomplete"));
                }
                let identity = ItemIdentity {
                    source_name: member["sourceName"].as_str().unwrap().to_owned(),
                    source_uuid: member["sourceUuid"].as_str().unwrap().to_owned(),
                    container_uuid: uuid.clone(),
                    scene_item_id: safe_integer(&member["sceneItemId"]).unwrap() as u64,
                };
                let enabled = member["sceneItemEnabled"].as_bool();
                let effective_enabled = if enabled == Some(false) || ancestor_enabled == Some(false)
                {
                    Some(false)
                } else if enabled.is_none() || ancestor_enabled.is_none() {
                    None
                } else {
                    Some(true)
                };
                let mut item_path = path.clone();
                item_path.push(identity.clone());
                let index = self.items.len();
                self.items.push(Item {
                    identity: identity.clone(),
                    path: item_path.clone(),
                    enabled,
                    effective_enabled,
                    transform: None,
                });
                if enabled.is_none() {
                    self.add(
                        "item-enabled",
                        "unknown",
                        "OBS did not report item enabled state",
                        Some(&identity),
                        None,
                    );
                }
                let mut data = self.scoped(&uuid);
                data["sceneItemId"] = json!(identity.scene_item_id);
                let response = self
                    .sample
                    .read(ReadRequest::SceneItemTransform, data)
                    .await?;
                let nested = self.scenes.get(&identity.source_uuid).cloned();
                if let Some(transform) = bounded_transform(&response["sceneItemTransform"]) {
                    self.items[index].transform = Some(transform.clone());
                    self.transforms.push(Transform {
                        item: index,
                        before: signature(&transform)?,
                    });
                    if !valid_transform(&transform) {
                        self.add(
                            "transform",
                            "unknown",
                            "Item transform has missing, malformed, or nonfinite fields",
                            Some(&identity),
                            None,
                        );
                    } else {
                        self.add(
                            "transform",
                            "pass",
                            "Required transform fields are well formed",
                            Some(&identity),
                            None,
                        );
                        let inputs = self.options_input_matches(&identity);
                        if member["isGroup"] != true && nested.is_none() && inputs.len() != 1 {
                            self.add(
                                "source-identity",
                                "unknown",
                                "Item source could not be resolved uniquely in the input inventory",
                                Some(&identity),
                                None,
                            );
                        } else {
                            let audio_only = inputs.len() == 1
                                && audio_capture(inputs[0]["inputKind"].as_str().unwrap());
                            let unknown_media = inputs.len() == 1
                                && matches!(
                                    inputs[0]["inputKind"].as_str(),
                                    Some("ffmpeg_source" | "vlc_source")
                                )
                                && (number(&transform["sourceWidth"]) == 0.0
                                    || number(&transform["sourceHeight"]) == 0.0);
                            if unknown_media {
                                self.add("source-dimensions","unknown","Zero-size media source may be audio-only or lack video; filename settings do not establish media contents",Some(&identity),None);
                            } else {
                                let no_picture = number(&transform["sourceWidth"]) == 0.0
                                    || number(&transform["sourceHeight"]) == 0.0
                                    || number(&transform["cropLeft"])
                                        + number(&transform["cropRight"])
                                        >= number(&transform["sourceWidth"])
                                    || number(&transform["cropTop"])
                                        + number(&transform["cropBottom"])
                                        >= number(&transform["sourceHeight"])
                                    || number(&transform["scaleX"]) == 0.0
                                    || number(&transform["scaleY"]) == 0.0;
                                self.add("source-dimensions",if audio_only||!no_picture{"pass"}else{"fail"},if audio_only{"Source kind establishes audio-only capture"}else if no_picture{"Visual source has zero source size, zero scale, or is fully cropped"}else{"Visual source has nonzero dimensions after crop and scale"},Some(&identity),None);
                            }
                        }
                        if !path.is_empty()
                            || member["isGroup"] == true
                            || nested.is_some()
                            || number(&transform["rotation"]) != 0.0
                            || transform["boundsType"] != "OBS_BOUNDS_NONE"
                        {
                            self.add("geometry","unknown","Nested, rotated, or bounded geometry is outside this structural audit",Some(&identity),None);
                        }
                    }
                } else {
                    self.add(
                        "transform",
                        "unknown",
                        "OBS did not report an item transform",
                        Some(&identity),
                        None,
                    );
                }
                if member["isGroup"] == true || nested.is_some() {
                    if nested
                        .as_ref()
                        .is_some_and(|scene| scene["sceneName"] != identity.source_name)
                    {
                        self.complete = false;
                        self.add(
                            "membership",
                            "unknown",
                            "Nested scene name and UUID disagree",
                            Some(&identity),
                            None,
                        );
                    } else {
                        let mut next = ancestors.clone();
                        next.insert(uuid.clone());
                        self.walk(
                            identity.source_uuid.clone(),
                            member["isGroup"] == true,
                            item_path,
                            next,
                            effective_enabled,
                        )
                        .await?;
                    }
                }
            }
            Ok(())
        })
    }
    fn options_input_matches(&self, identity: &ItemIdentity) -> Vec<Value> {
        self.inputs
            .iter()
            .filter(|input| {
                input["inputUuid"] == identity.source_uuid
                    && input["inputName"] == identity.source_name
            })
            .cloned()
            .collect()
    }
}

/// Uses caller-owned completion metadata so this pure owner needs no wall clock
/// dependency. It always returns the structural report, including failure paths.
pub async fn audit<R: ReadAdapter>(
    sample: &mut AuditSample<'_, R>,
    options: &AuditOptions,
    screenshot_limit: Result<ScreenshotLimit, ObservationError>,
    completed: impl FnOnce() -> AuditCompletion,
) -> Value {
    let mut state = Audit {
        sample,
        options,
        checks: vec![],
        items: vec![],
        containers: vec![],
        transforms: vec![],
        complete: true,
        image: None,
        scenes: HashMap::new(),
        inputs: vec![],
    };
    let mut target = serde_json::to_value(&options.target).expect("audit target");
    let result = sample_report(&mut state, &mut target, screenshot_limit).await;
    if let Err(error) = result {
        if error.message == "Observation signature property collation is unavailable" {
            state.sample.requires_compatibility = true;
        }
        state.complete = false;
        state.add("observation", "unknown", error.message, None, None);
    }
    state.sample.refresh();
    state.sample.close();
    if state.sample.invalidated {
        for check in &mut state.checks {
            check["status"] = json!("unknown");
            check["message"] = json!(format!(
                "Invalidated observation: {}",
                check["message"].as_str().unwrap()
            ));
        }
        state.image = None;
        state.add(
            "consistency",
            "unknown",
            "OBS changed during the sample; all observed conclusions require a fresh audit",
            None,
            None,
        );
    }
    let status = if state.sample.invalidated || !state.complete {
        "unknown"
    } else if state.checks.iter().any(|check| check["status"] == "fail") {
        "fail"
    } else if state
        .checks
        .iter()
        .any(|check| check["status"] == "unknown")
    {
        "unknown"
    } else {
        "pass"
    };
    let completion = completed();
    let mut report = json!({"version":1,"scope":"structural","status":status,"target":target,"observedAt":state.sample.observed_at,"completedAt":completion.completed_at,"durationMs":completion.duration_ms,"requests":state.sample.requests,"limits":{"maxItems":128,"maxDepth":8,"maxRequests":512,"maxDurationMs":12000,"requestTimeoutMs":2000},"items":state.items,"checks":state.checks,"renderedContent":"unverified","limitations":["Structural results do not establish recording readiness or rendered content correctness.","No off-canvas, occlusion, legibility, capture-target, or composed-geometry verification is performed.","OBS reads are sequential observations, not an atomic snapshot; unobserved transient changes remain possible."]});
    if signature(&report).map_or(true, |text| text.len() > REPORT_BYTES) {
        report["status"] = json!("unknown");
        report["items"] = json!([]);
        report["checks"] = json!([{"id":"report-size","status":"unknown","message":"Scene audit evidence exceeded the 512 KiB report limit; item and check evidence was omitted"}]);
        state.image = None;
    }
    let mut content =
        vec![json!({"type":"text","text":serde_json::to_string(&report).expect("audit report")})];
    if let Some(image) = state.image {
        content.push(image);
    }
    json!({"content":content,"structuredContent":report})
}

async fn sample_report<R: ReadAdapter>(
    state: &mut Audit<'_, '_, R>,
    target: &mut Value,
    screenshot_limit: Result<ScreenshotLimit, ObservationError>,
) -> Result<(), ObservationError> {
    let collection = state
        .sample
        .read(ReadRequest::SceneCollectionList, Value::Null)
        .await?;
    if collection["currentSceneCollectionName"] != state.options.target.scene_collection {
        return Err(error(
            "The selected scene collection is not current or was not reported",
        ));
    }
    state.add(
        "collection",
        "pass",
        "Selected scene collection is current",
        None,
        None,
    );
    let reply = state
        .sample
        .read(ReadRequest::SceneList, state.options.target.canvas())
        .await?;
    let scenes = objects(&reply["scenes"])?;
    if scenes.len() > 256
        || scenes
            .iter()
            .any(|scene| !text(&scene["sceneName"]) || !text(&scene["sceneUuid"]))
        || scenes
            .iter()
            .map(|scene| scene["sceneUuid"].as_str())
            .collect::<HashSet<_>>()
            .len()
            != scenes.len()
    {
        return Err(error(
            "Scene inventory is incomplete, ambiguous, or exceeds the limit",
        ));
    }
    let selected = scenes
        .iter()
        .filter(|scene| scene["sceneName"] == state.options.target.scene_name)
        .collect::<Vec<_>>();
    if selected.len() != 1 {
        return Err(error("Selected scene is missing or ambiguous"));
    }
    let scene_uuid = selected[0]["sceneUuid"].as_str().unwrap().to_owned();
    target["sceneUuid"] = json!(scene_uuid);
    let reply = state
        .sample
        .read(ReadRequest::InputList, Value::Null)
        .await?;
    let inputs = objects(&reply["inputs"])?;
    if inputs.len() > 2048
        || inputs.iter().any(|input| {
            !text(&input["inputName"]) || !text(&input["inputUuid"]) || !text(&input["inputKind"])
        })
        || inputs
            .iter()
            .map(|input| input["inputUuid"].as_str())
            .collect::<HashSet<_>>()
            .len()
            != inputs.len()
    {
        return Err(error("Input inventory is incomplete or ambiguous"));
    }
    state.scenes = scenes
        .iter()
        .map(|scene| {
            (
                scene["sceneUuid"].as_str().unwrap().to_owned(),
                scene.clone(),
            )
        })
        .collect();
    state.inputs = inputs.clone();
    state
        .walk(
            scene_uuid.clone(),
            false,
            vec![],
            HashSet::new(),
            Some(true),
        )
        .await?;
    state.add(
        "membership",
        if state.complete { "pass" } else { "unknown" },
        if state.complete {
            format!(
                "Observed {} distinct scene item occurrences",
                state.items.len()
            )
        } else {
            "Scene membership observation is incomplete".to_owned()
        },
        None,
        None,
    );
    if state.items.is_empty() {
        state.add(
            "scene-content",
            "fail",
            "Selected scene has no scene items",
            None,
            None,
        );
    }
    for required in &state.options.required {
        let matches = state
            .items
            .iter()
            .filter(|item| {
                item.identity.source_name == required.source_name
                    && required
                        .source_uuid
                        .as_ref()
                        .is_none_or(|uuid| item.identity.source_uuid == *uuid)
                    && required
                        .container_uuid
                        .as_ref()
                        .is_none_or(|uuid| item.identity.container_uuid == *uuid)
                    && required
                        .scene_item_id
                        .is_none_or(|id| item.identity.scene_item_id == id)
            })
            .cloned()
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            state.add(
                "required-item",
                if matches.is_empty() && state.complete {
                    "fail"
                } else {
                    "unknown"
                },
                if matches.is_empty() {
                    format!("Required item {} was not found", required.source_name)
                } else {
                    format!(
                        "Required item {} matches multiple occurrences",
                        required.source_name
                    )
                },
                None,
                Some(serde_json::to_value(required).expect("selector")),
            );
        } else {
            let item = &matches[0];
            state.add(
                "required-item",
                match item.effective_enabled {
                    None => "unknown",
                    Some(true) => "pass",
                    Some(false) => "fail",
                },
                match item.effective_enabled {
                    None => "Required item or ancestor enabled state is unavailable",
                    Some(true) => "Required item and its ancestors are enabled",
                    Some(false) => "Required item or an ancestor is disabled",
                },
                Some(&item.identity),
                None,
            );
        }
    }
    if state.options.include_screenshot {
        let mut data = state.options.target.canvas();
        data["sourceUuid"] = json!(scene_uuid);
        data["imageFormat"] = json!("png");
        data["imageWidth"] = json!(960);
        let response = state.sample.read(ReadRequest::SourceScreenshot, data).await;
        let image = response
            .and_then(|response| screenshot_limit.and_then(|limit| screenshot(&response, limit)));
        match image {
            Ok(image) => {
                state.image = Some(image);
                state.add(
                    "screenshot",
                    "pass",
                    "Bounded scene screenshot included for separate visual inspection",
                    None,
                    None,
                );
            }
            Err(error) => state.add(
                "screenshot",
                "unknown",
                format!("Screenshot unavailable: {}", error.message),
                None,
                None,
            ),
        }
    }
    for index in 0..state.containers.len() {
        let container = &state.containers[index];
        let reply = state
            .sample
            .read(
                if container.group {
                    ReadRequest::GroupSceneItemList
                } else {
                    ReadRequest::SceneItemList
                },
                state.scoped(&container.uuid),
            )
            .await?;
        let members = objects(&reply["sceneItems"])?
            .into_iter()
            .map(normalize_scene_item)
            .collect::<Vec<_>>();
        if signature(&membership(&members))? != container.before {
            state.sample.invalidated = true;
        }
    }
    for transform in &state.transforms {
        let item = &state.items[transform.item];
        let mut data = state.scoped(&item.identity.container_uuid);
        data["sceneItemId"] = json!(item.identity.scene_item_id);
        let now = state
            .sample
            .read(ReadRequest::SceneItemTransform, data)
            .await?;
        let after = bounded_transform(&now["sceneItemTransform"])
            .map(|value| signature(&value))
            .transpose()?;
        if after.as_deref() != Some(&transform.before) {
            state.sample.invalidated = true;
        }
    }
    let reply = state
        .sample
        .read(ReadRequest::SceneList, state.options.target.canvas())
        .await?;
    // Preserve the source's short-circuit ordering: a differing scene list
    // skips the remaining final reads before invalidation is recorded.
    let changed = if signature(&json!(objects(&reply["scenes"])?))? != signature(&json!(scenes))? {
        true
    } else {
        let reply = state
            .sample
            .read(ReadRequest::InputList, Value::Null)
            .await?;
        if signature(&json!(objects(&reply["inputs"])?))? != signature(&json!(inputs))? {
            true
        } else {
            state
                .sample
                .read(ReadRequest::SceneCollectionList, Value::Null)
                .await?["currentSceneCollectionName"]
                != state.options.target.scene_collection
        }
    };
    if changed {
        state.sample.invalidated = true;
    }
    state.sample.refresh();
    if state.sample.invalidated {
        return Err(error("OBS state changed while sampling"));
    }
    state.add(
        "consistency",
        "pass",
        "Repeated observations agree; this is not an atomic snapshot",
        None,
        None,
    );
    Ok(())
}

fn objects(value: &Value) -> Result<Vec<Value>, ObservationError> {
    value
        .as_array()
        .filter(|values| values.iter().all(Value::is_object))
        .cloned()
        .ok_or_else(|| error("OBS returned an incomplete list"))
}
fn text(value: &Value) -> bool {
    value
        .as_str()
        .is_some_and(|value| (1..=256).contains(&value.encode_utf16().count()))
}
fn number(value: &Value) -> f64 {
    value.as_f64().unwrap_or(f64::NAN)
}
fn bounded_transform(value: &Value) -> Option<Value> {
    let object = value.as_object()?;
    let mut result = serde_json::Map::new();
    for field in NUMERIC_FIELDS.iter().copied().chain(["boundsType"]) {
        if let Some(value) = object.get(field).filter(|value| {
            value.as_f64().is_some_and(f64::is_finite) || field == "boundsType" && text(value)
        }) {
            result.insert(field.to_owned(), value.clone());
        }
    }
    Some(Value::Object(result))
}
fn valid_transform(value: &Value) -> bool {
    NUMERIC_FIELDS
        .iter()
        .all(|field| value[*field].as_f64().is_some_and(f64::is_finite))
        && [
            "sourceWidth",
            "sourceHeight",
            "boundsWidth",
            "boundsHeight",
            "cropLeft",
            "cropRight",
            "cropTop",
            "cropBottom",
        ]
        .iter()
        .all(|field| number(&value[*field]) >= 0.0)
        && ["cropLeft", "cropRight", "cropTop", "cropBottom"]
            .iter()
            .all(|field| number(&value[*field]).fract() == 0.0)
        && ["sourceWidth", "sourceHeight"].iter().all(|field| {
            number(&value[*field]).fract() == 0.0 && number(&value[*field]) <= u32::MAX as f64
        })
        && ["alignment", "boundsAlignment"]
            .iter()
            .all(|field| [0., 1., 2., 4., 5., 6., 8., 9., 10.].contains(&number(&value[*field])))
        && text(&value["boundsType"])
}
fn audio_capture(kind: &str) -> bool {
    kind == "sck_audio_capture"
        || kind.starts_with("jack_")
        || ["input_capture", "output_capture", "process_output_capture"]
            .iter()
            .any(|suffix| kind == *suffix || kind.ends_with(&format!("_{suffix}")))
}
fn membership(items: &[Value]) -> Value {
    json!(
        items
            .iter()
            .map(|item| {
                let mut projection = serde_json::Map::new();
                for field in [
                    "sourceName",
                    "sourceUuid",
                    "sceneItemId",
                    "sceneItemEnabled",
                    "isGroup",
                    "inputKind",
                ] {
                    if let Some(value) = item.get(field) {
                        projection.insert(field.to_owned(), value.clone());
                    }
                }
                Value::Object(projection)
            })
            .collect::<Vec<_>>()
    )
}
fn duplicate_item_ids(items: &[Value]) -> bool {
    items.iter().enumerate().any(|(index, item)| {
        items[..index].iter().any(|other| {
            match (item.get("sceneItemId"), other.get("sceneItemId")) {
                (None, None) => true,
                (Some(Value::Number(a)), Some(Value::Number(b))) => a.as_f64() == b.as_f64(),
                (Some(Value::Array(_) | Value::Object(_)), _)
                | (_, Some(Value::Array(_) | Value::Object(_))) => false,
                (Some(a), Some(b)) => a == b,
                _ => false,
            }
        })
    })
}
fn json_utf16_units(value: &Value) -> Result<usize, ObservationError> {
    Ok(match value {
        Value::Object(values) => {
            let mut size = 2;
            for (key, value) in values {
                size +=
                    signature(&json!(key))?.encode_utf16().count() + 1 + json_utf16_units(value)?;
            }
            size + values.len().saturating_sub(1)
        }
        Value::Array(values) => {
            let mut size = 2;
            for value in values {
                size += json_utf16_units(value)?;
            }
            size + values.len().saturating_sub(1)
        }
        _ => signature(value)?.encode_utf16().count(),
    })
}
fn truncate_message(value: &str) -> (String, bool) {
    let mut result = String::new();
    let mut units = 0;
    for character in value.chars() {
        let length = character.len_utf16();
        if units + length > 900 {
            return if units == 899 && length == 2 {
                (
                    "Scene audit diagnostic requires unsupported partial UTF-16 projection"
                        .to_owned(),
                    false,
                )
            } else {
                (result, true)
            };
        }
        result.push(character);
        units += length;
    }
    (result, true)
}
/// Reuse the exact source screenshot admission for resource content.
pub fn screenshot_content(
    response: &Value,
    limit: ScreenshotLimit,
) -> Result<Value, ObservationError> {
    screenshot(response, limit)
}
fn screenshot(response: &Value, limit: ScreenshotLimit) -> Result<Value, ObservationError> {
    let data = response
        .get("imageData")
        .and_then(Value::as_str)
        .ok_or_else(|| error("OBS returned a screenshot response without imageData"))?;
    let (header, payload) = data
        .split_once(',')
        .ok_or_else(|| error("OBS returned an invalid or unsupported screenshot data URL"))?;
    let header = header.to_ascii_lowercase();
    let mime = header
        .strip_prefix("data:")
        .and_then(|header| header.strip_suffix(";base64"))
        .filter(|mime| matches!(*mime, "image/png" | "image/jpeg" | "image/webp"))
        .ok_or_else(|| error("OBS returned an invalid or unsupported screenshot data URL"))?;
    let padding = payload
        .bytes()
        .rev()
        .take_while(|byte| *byte == b'=')
        .count();
    if padding > 2
        || !payload.as_bytes()[..payload.len() - padding]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'+' | b'/'))
    {
        return Err(error(
            "OBS returned an invalid or unsupported screenshot data URL",
        ));
    }
    if payload.is_empty() || !payload.len().is_multiple_of(4) {
        return Err(error("OBS returned invalid screenshot image data"));
    }
    let bytes = payload.len() / 4 * 3 - padding;
    if bytes > limit.0 {
        return Err(error(format!(
            "OBS screenshot is {bytes} bytes; limit is {} bytes",
            limit.0
        )));
    }
    Ok(json!({"type":"image","data":payload,"mimeType":mime}))
}
