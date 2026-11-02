// SPDX-License-Identifier: GPL-2.0-only
//! Bounded, read-only evidence ownership. The provider must advance revision
//! for invalidating events and publish disconnect/cancellation before reuse.
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{cmp::Ordering, collections::HashSet, future::Future, pin::Pin, time::Duration};
use tokio::{
    sync::watch,
    time::{Instant, timeout_at},
};

pub const MAX_REQUESTS: usize = 256;
pub const MAX_EVIDENCE_BYTES: usize = 256 * 1024;
pub const MAX_OBSERVATION: Duration = Duration::from_secs(12);
pub const MAX_READ: Duration = Duration::from_secs(2);
const SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

#[derive(Clone, PartialEq, Eq)]
pub struct ObservationError {
    pub reason: &'static str,
    pub message: String,
    request_code: Option<u64>,
}
impl std::fmt::Debug for ObservationError {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.debug_struct("ObservationError")
            .field("reason", &self.reason)
            .field("request_code", &self.request_code)
            .field("message_bytes", &self.message.len())
            .finish_non_exhaustive()
    }
}
impl ObservationError {
    pub fn new(reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            reason,
            message: message.into(),
            request_code: None,
        }
    }
    /// Retain an explicit OBS rejection separately from local capability or
    /// transport failures. A missing advertised method is not error code 604.
    pub fn rejected(code: u64, reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            reason,
            message: message.into(),
            request_code: Some(code),
        }
    }
    pub fn request_code(&self) -> Option<u64> {
        self.request_code
    }
    fn invalidated() -> Self {
        Self::new(
            "observation_invalidated",
            "OBS changed during observation; run obs-observe again",
        )
    }
    fn limit() -> Self {
        Self::new(
            "observation_limit",
            "Observation exceeded its 12 second budget",
        )
    }
    pub fn failure(&self) -> Value {
        json!({"reason":self.reason,"message":self.message,"nextCheck":"Run obs-observe and inspect the named target before retrying"})
    }
}
impl std::fmt::Display for ObservationError {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(code) = self.request_code {
            write!(output, "OBS request rejected with code {code}")
        } else {
            output.write_str(&self.message)
        }
    }
}
impl std::error::Error for ObservationError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadRequest {
    SceneCollectionList,
    SceneList,
    VideoSettings,
    InputList,
    InputKindList,
    SceneItemList,
    SceneItemTransform,
    GroupSceneItemList,
    SourceScreenshot,
    StudioModeEnabled,
    CurrentProgramScene,
    CurrentPreviewScene,
    RecordStatus,
    StreamStatus,
    ReplayBufferStatus,
    VirtualCamStatus,
    OutputList,
    OutputStatus,
    InputSettings,
    SourceFilterList,
    InputMute,
    InputVolume,
    InputAudioTracks,
    RecordDirectory,
    Stats,
    ProfileParameter,
    Version,
    HotkeyList,
    SourceActive,
    SceneItemId,
    SceneTransitionList,
    CurrentSceneTransition,
    PersistentData,
    ProfileList,
    StreamServiceSettings,
    SourceFilterKindList,
    SourceFilterDefaultSettings,
    SourceFilter,
    SpecialInputs,
    InputDefaultSettings,
    InputAudioBalance,
    InputAudioSyncOffset,
    InputAudioMonitorType,
    MediaInputStatus,
    LastReplayBufferReplay,
    OutputSettings,
    MonitorList,
    CanvasList,
    InputDeinterlaceFieldOrder,
    InputDeinterlaceMode,
    SceneItemEnabled,
    SceneItemIndex,
    SceneItemLocked,
    SceneItemBlendMode,
    SceneItemSource,
    GroupList,
    SceneSceneTransitionOverride,
    TransitionKindList,
    CurrentSceneTransitionCursor,
}
impl ReadRequest {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SceneCollectionList => "GetSceneCollectionList",
            Self::SceneList => "GetSceneList",
            Self::VideoSettings => "GetVideoSettings",
            Self::InputList => "GetInputList",
            Self::InputKindList => "GetInputKindList",
            Self::SceneItemList => "GetSceneItemList",
            Self::SceneItemTransform => "GetSceneItemTransform",
            Self::GroupSceneItemList => "GetGroupSceneItemList",
            Self::SourceScreenshot => "GetSourceScreenshot",
            Self::StudioModeEnabled => "GetStudioModeEnabled",
            Self::CurrentProgramScene => "GetCurrentProgramScene",
            Self::CurrentPreviewScene => "GetCurrentPreviewScene",
            Self::RecordStatus => "GetRecordStatus",
            Self::StreamStatus => "GetStreamStatus",
            Self::ReplayBufferStatus => "GetReplayBufferStatus",
            Self::VirtualCamStatus => "GetVirtualCamStatus",
            Self::OutputList => "GetOutputList",
            Self::OutputStatus => "GetOutputStatus",
            Self::InputSettings => "GetInputSettings",
            Self::SourceFilterList => "GetSourceFilterList",
            Self::InputMute => "GetInputMute",
            Self::InputVolume => "GetInputVolume",
            Self::InputAudioTracks => "GetInputAudioTracks",
            Self::RecordDirectory => "GetRecordDirectory",
            Self::Stats => "GetStats",
            Self::ProfileParameter => "GetProfileParameter",
            Self::Version => "GetVersion",
            Self::HotkeyList => "GetHotkeyList",
            Self::SourceActive => "GetSourceActive",
            Self::SceneItemId => "GetSceneItemId",
            Self::SceneTransitionList => "GetSceneTransitionList",
            Self::CurrentSceneTransition => "GetCurrentSceneTransition",
            Self::PersistentData => "GetPersistentData",
            Self::ProfileList => "GetProfileList",
            Self::StreamServiceSettings => "GetStreamServiceSettings",
            Self::SourceFilterKindList => "GetSourceFilterKindList",
            Self::SourceFilterDefaultSettings => "GetSourceFilterDefaultSettings",
            Self::SourceFilter => "GetSourceFilter",
            Self::SpecialInputs => "GetSpecialInputs",
            Self::InputDefaultSettings => "GetInputDefaultSettings",
            Self::InputAudioBalance => "GetInputAudioBalance",
            Self::InputAudioSyncOffset => "GetInputAudioSyncOffset",
            Self::InputAudioMonitorType => "GetInputAudioMonitorType",
            Self::MediaInputStatus => "GetMediaInputStatus",
            Self::LastReplayBufferReplay => "GetLastReplayBufferReplay",
            Self::OutputSettings => "GetOutputSettings",
            Self::MonitorList => "GetMonitorList",
            Self::CanvasList => "GetCanvasList",
            Self::InputDeinterlaceFieldOrder => "GetInputDeinterlaceFieldOrder",
            Self::InputDeinterlaceMode => "GetInputDeinterlaceMode",
            Self::SceneItemEnabled => "GetSceneItemEnabled",
            Self::SceneItemIndex => "GetSceneItemIndex",
            Self::SceneItemLocked => "GetSceneItemLocked",
            Self::SceneItemBlendMode => "GetSceneItemBlendMode",
            Self::SceneItemSource => "GetSceneItemSource",
            Self::GroupList => "GetGroupList",
            Self::SceneSceneTransitionOverride => "GetSceneSceneTransitionOverride",
            Self::TransitionKindList => "GetTransitionKindList",
            Self::CurrentSceneTransitionCursor => "GetCurrentSceneTransitionCursor",
        }
    }

    /// Closed read vocabulary. Property enumeration and effects stay excluded.
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "GetSceneCollectionList" => Self::SceneCollectionList,
            "GetSceneList" => Self::SceneList,
            "GetVideoSettings" => Self::VideoSettings,
            "GetInputList" => Self::InputList,
            "GetInputKindList" => Self::InputKindList,
            "GetSceneItemList" => Self::SceneItemList,
            "GetSceneItemTransform" => Self::SceneItemTransform,
            "GetGroupSceneItemList" => Self::GroupSceneItemList,
            "GetSourceScreenshot" => Self::SourceScreenshot,
            "GetStudioModeEnabled" => Self::StudioModeEnabled,
            "GetCurrentProgramScene" => Self::CurrentProgramScene,
            "GetCurrentPreviewScene" => Self::CurrentPreviewScene,
            "GetRecordStatus" => Self::RecordStatus,
            "GetStreamStatus" => Self::StreamStatus,
            "GetReplayBufferStatus" => Self::ReplayBufferStatus,
            "GetVirtualCamStatus" => Self::VirtualCamStatus,
            "GetOutputList" => Self::OutputList,
            "GetOutputStatus" => Self::OutputStatus,
            "GetInputSettings" => Self::InputSettings,
            "GetSourceFilterList" => Self::SourceFilterList,
            "GetInputMute" => Self::InputMute,
            "GetInputVolume" => Self::InputVolume,
            "GetInputAudioTracks" => Self::InputAudioTracks,
            "GetRecordDirectory" => Self::RecordDirectory,
            "GetStats" => Self::Stats,
            "GetProfileParameter" => Self::ProfileParameter,
            "GetVersion" => Self::Version,
            "GetHotkeyList" => Self::HotkeyList,
            "GetSourceActive" => Self::SourceActive,
            "GetSceneItemId" => Self::SceneItemId,
            "GetSceneTransitionList" => Self::SceneTransitionList,
            "GetCurrentSceneTransition" => Self::CurrentSceneTransition,
            "GetPersistentData" => Self::PersistentData,
            "GetProfileList" => Self::ProfileList,
            "GetStreamServiceSettings" => Self::StreamServiceSettings,
            "GetSourceFilterKindList" => Self::SourceFilterKindList,
            "GetSourceFilterDefaultSettings" => Self::SourceFilterDefaultSettings,
            "GetSourceFilter" => Self::SourceFilter,
            "GetSpecialInputs" => Self::SpecialInputs,
            "GetInputDefaultSettings" => Self::InputDefaultSettings,
            "GetInputAudioBalance" => Self::InputAudioBalance,
            "GetInputAudioSyncOffset" => Self::InputAudioSyncOffset,
            "GetInputAudioMonitorType" => Self::InputAudioMonitorType,
            "GetMediaInputStatus" => Self::MediaInputStatus,
            "GetLastReplayBufferReplay" => Self::LastReplayBufferReplay,
            "GetOutputSettings" => Self::OutputSettings,
            "GetMonitorList" => Self::MonitorList,
            "GetCanvasList" => Self::CanvasList,
            "GetInputDeinterlaceFieldOrder" => Self::InputDeinterlaceFieldOrder,
            "GetInputDeinterlaceMode" => Self::InputDeinterlaceMode,
            "GetSceneItemEnabled" => Self::SceneItemEnabled,
            "GetSceneItemIndex" => Self::SceneItemIndex,
            "GetSceneItemLocked" => Self::SceneItemLocked,
            "GetSceneItemBlendMode" => Self::SceneItemBlendMode,
            "GetSceneItemSource" => Self::SceneItemSource,
            "GetGroupList" => Self::GroupList,
            "GetSceneSceneTransitionOverride" => Self::SceneSceneTransitionOverride,
            "GetTransitionKindList" => Self::TransitionKindList,
            "GetCurrentSceneTransitionCursor" => Self::CurrentSceneTransitionCursor,
            _ => return None,
        })
    }
}

pub type ReadFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Value, ObservationError>> + Send + 'a>>;
pub trait ReadAdapter: Send {
    fn supports(&self, request: ReadRequest) -> Option<bool>;
    /// Dropping this future ends permission to deliver its reply. The transport
    /// owner must retire pending work; it must never replay a cancelled request.
    fn read<'a>(
        &'a mut self,
        request: ReadRequest,
        data: Value,
        deadline: Instant,
    ) -> ReadFuture<'a>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EvidenceState {
    pub generation: u64,
    pub revision: u64,
    pub connected: bool,
    pub cancelled: bool,
}
pub fn invalidates_event(event: &str) -> bool {
    // Telemetry is consumed by the take owner but changes no structural field.
    if event == "InputVolumeMeters" {
        return false;
    }
    [
        "CurrentSceneCollection",
        "SceneCollection",
        "Canvas",
        "Scene",
        "Input",
        "SourceFilter",
        "VideoSettings",
        "CurrentProgramScene",
        "CurrentPreviewScene",
        "StudioMode",
        "CurrentProfile",
        "Profile",
        "RecordState",
        "StreamState",
        "ReplayBufferState",
        "VirtualcamState",
    ]
    .iter()
    .any(|prefix| event.starts_with(prefix))
}

pub struct Observation<'a, R: ReadAdapter> {
    reader: &'a mut R,
    state: watch::Receiver<EvidenceState>,
    generation: u64,
    revision: u64,
    observed_at: String,
    deadline: Instant,
    requests: usize,
    closed: bool,
}
impl<'a, R: ReadAdapter> Observation<'a, R> {
    pub fn new(
        reader: &'a mut R,
        state: watch::Receiver<EvidenceState>,
        expected_generation: u64,
        observed_at: String,
        deadline: Instant,
    ) -> Result<Self, ObservationError> {
        let revision = state.borrow().revision;
        let result = Self {
            reader,
            state,
            generation: expected_generation,
            revision,
            observed_at,
            deadline: deadline.min(Instant::now() + MAX_OBSERVATION),
            requests: 0,
            closed: false,
        };
        result.check()?;
        Ok(result)
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    /// Frozen revision admitted by this sample, not a fresh state assertion.
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn observed_at(&self) -> &str {
        &self.observed_at
    }
    pub fn requests(&self) -> usize {
        self.requests
    }
    pub fn close(&mut self) {
        self.closed = true;
    }
    pub fn check(&self) -> Result<(), ObservationError> {
        check_state(
            *self.state.borrow(),
            self.generation,
            self.revision,
            self.deadline,
            self.closed,
        )?;
        if self.state.has_changed().is_err() {
            return Err(ObservationError::invalidated());
        }
        Ok(())
    }
    pub async fn read(
        &mut self,
        request: ReadRequest,
        data: Value,
    ) -> Result<Value, ObservationError> {
        self.check()?;
        self.requests = self
            .requests
            .checked_add(1)
            .ok_or_else(ObservationError::limit)?;
        if self.requests > MAX_REQUESTS {
            return Err(ObservationError::new(
                "observation_limit",
                "Observation exceeded its 256 request budget",
            ));
        }
        if self.reader.supports(request) == Some(false) {
            return Err(ObservationError::new(
                "capability_unavailable",
                format!(
                    "{} is unavailable; inspect obs-get-version",
                    request.as_str()
                ),
            ));
        }
        let now = Instant::now();
        let read_deadline = self.deadline.min(now + MAX_READ);
        let duration_ms = read_deadline.saturating_duration_since(now).as_millis();
        let work = timeout_at(
            read_deadline,
            self.reader.read(request, data, read_deadline),
        );
        tokio::pin!(work);
        let value = loop {
            tokio::select! {
                biased;
                result = &mut work => break match result {
                    Ok(result) => result,
                    Err(_) => Err(ObservationError::new("evidence_unavailable", format!("Request {} timed out after {duration_ms}ms", request.as_str()))),
                },
                changed = self.state.changed() => {
                    if changed.is_err() { return Err(ObservationError::invalidated()); }
                    if !self.state.borrow().connected { return Err(ObservationError::invalidated()); }
                    // Match the source: an audit event or cancellation fences
                    // the result but does not abort an admitted healthy RPC.
                    // Retain it until response/deadline; further reads fail.
                },
            }
        };
        // Even an already-ready reply loses to cancellation or invalidation.
        check_state(
            *self.state.borrow(),
            self.generation,
            self.revision,
            self.deadline,
            self.closed,
        )?;
        if self.state.has_changed().is_err() {
            return Err(ObservationError::invalidated());
        }
        let value = value?;
        if !value.is_object() {
            return Err(ObservationError::new(
                "evidence_unavailable",
                "OBS returned incomplete state",
            ));
        }
        if request != ReadRequest::SourceScreenshot && json_bytes(&value) > MAX_EVIDENCE_BYTES {
            return Err(ObservationError::new(
                "observation_limit",
                format!("Oversized {} evidence", request.as_str()),
            ));
        }
        Ok(value)
    }
}
fn check_state(
    state: EvidenceState,
    generation: u64,
    revision: u64,
    deadline: Instant,
    closed: bool,
) -> Result<(), ObservationError> {
    if state.cancelled {
        return Err(ObservationError::new(
            "cancelled",
            "Operation cancelled; inspect OBS before any further action",
        ));
    }
    if closed || !state.connected || state.generation != generation || state.revision != revision {
        return Err(ObservationError::invalidated());
    }
    if Instant::now() >= deadline {
        return Err(ObservationError::limit());
    }
    Ok(())
}

pub fn identity(value: &Value) -> Result<&str, ObservationError> {
    value
        .as_str()
        .filter(|value| (1..=256).contains(&value.encode_utf16().count()))
        .ok_or_else(|| {
            ObservationError::new("evidence_unavailable", "OBS did not report stable identity")
        })
}
pub fn list(value: &Value, maximum: usize) -> Result<Vec<Value>, ObservationError> {
    value
        .as_array()
        .filter(|items| items.len() <= maximum && items.iter().all(Value::is_object))
        .cloned()
        .ok_or_else(|| {
            ObservationError::new(
                "evidence_unavailable",
                "OBS returned an incomplete or oversized inventory",
            )
        })
}
pub fn safe_integer(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .filter(|value| value.is_finite() && value.fract() == 0.0 && value.abs() <= SAFE_INTEGER)
}
fn same_js_identity(a: Option<&Value>, b: Option<&Value>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(Value::Number(a)), Some(Value::Number(b))) => a.as_f64() == b.as_f64(),
        (Some(Value::Array(_) | Value::Object(_)), _)
        | (_, Some(Value::Array(_) | Value::Object(_))) => false,
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}
fn json_bytes(value: &Value) -> usize {
    fn string(value: &str) -> usize {
        2 + value
            .chars()
            .map(|character| match character {
                '"' | '\\' | '\u{8}' | '\u{c}' | '\n' | '\r' | '\t' => 2,
                '\u{0}'..='\u{1f}' => 6,
                other => other.len_utf8(),
            })
            .sum::<usize>()
    }
    match value {
        Value::Object(values) => {
            values.iter().fold(2usize, |sum, (key, value)| {
                sum.saturating_add(string(key))
                    .saturating_add(1)
                    .saturating_add(json_bytes(value))
            }) + values.len().saturating_sub(1)
        }
        Value::Array(values) => {
            values
                .iter()
                .fold(2usize, |sum, value| sum.saturating_add(json_bytes(value)))
                + values.len().saturating_sub(1)
        }
        Value::String(value) => string(value),
        Value::Number(value) => js_number(value.as_f64().expect("JSON number")).len(),
        Value::Bool(true) => 4,
        Value::Bool(false) => 5,
        Value::Null => 4,
    }
}
pub fn normalize_scene_item(mut item: Value) -> Value {
    if item["isGroup"].is_null()
        && item
            .as_object()
            .is_some_and(|item| item.contains_key("isGroup"))
        && item["sourceType"] == "OBS_SOURCE_TYPE_INPUT"
        && item["inputKind"]
            .as_str()
            .is_some_and(|kind| !kind.is_empty())
    {
        item["isGroup"] = json!(false);
    }
    item
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Target {
    pub scene_collection: String,
    pub scene_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canvas_uuid: Option<String>,
}
impl Target {
    pub fn validate(&self) -> Result<(), ObservationError> {
        identity(&json!(self.scene_collection))?;
        identity(&json!(self.scene_name))?;
        if let Some(canvas) = &self.canvas_uuid {
            identity(&json!(canvas))?;
        }
        Ok(())
    }
    pub fn canvas(&self) -> Value {
        self.canvas_uuid
            .as_ref()
            .map_or_else(|| json!({}), |canvas| json!({"canvasUuid":canvas}))
    }
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SceneIdentity {
    pub scene_name: String,
    pub scene_uuid: String,
}
#[derive(Clone, Debug, Serialize)]
pub struct SceneInventory {
    pub collection: String,
    pub scenes: Value,
    pub selected: SceneIdentity,
}
#[derive(Clone, Debug, Serialize)]
pub struct FlatScene {
    pub collection: String,
    pub scenes: Value,
    pub selected: SceneIdentity,
    pub video: Value,
    pub inputs: Vec<Value>,
    pub items: Vec<Value>,
}

pub async fn scene_inventory<R: ReadAdapter>(
    sample: &mut Observation<'_, R>,
    target: &Target,
) -> Result<SceneInventory, ObservationError> {
    target.validate()?;
    if target.canvas_uuid.is_some() {
        return Err(ObservationError::new(
            "capability_unavailable",
            "This workflow requires the default canvas: the pinned video-settings and transition contracts do not provide canvas-scoped operations",
        ));
    }
    let collection_reply = sample
        .read(ReadRequest::SceneCollectionList, Value::Null)
        .await?;
    let collection = identity(&collection_reply["currentSceneCollectionName"])?.to_owned();
    if collection != target.scene_collection {
        return Err(ObservationError::new(
            "target_changed",
            "Selected scene collection is not current",
        ));
    }
    let scenes = sample.read(ReadRequest::SceneList, target.canvas()).await?;
    let inventory = list(&scenes["scenes"], 256)?;
    let mut ids = HashSet::new();
    for scene in &inventory {
        identity(&scene["sceneName"])?;
        ids.insert(identity(&scene["sceneUuid"])?);
    }
    if ids.len() != inventory.len() {
        return Err(ObservationError::new(
            "target_changed",
            "Duplicate scene identities",
        ));
    }
    let selected = inventory
        .iter()
        .filter(|scene| scene["sceneName"] == target.scene_name)
        .collect::<Vec<_>>();
    if selected.len() != 1 {
        return Err(ObservationError::new(
            "target_changed",
            "Selected scene is missing or ambiguous",
        ));
    }
    let selected = SceneIdentity {
        scene_name: identity(&selected[0]["sceneName"])?.to_owned(),
        scene_uuid: identity(&selected[0]["sceneUuid"])?.to_owned(),
    };
    Ok(SceneInventory {
        collection,
        scenes,
        selected,
    })
}
pub async fn flat_scene<R: ReadAdapter>(
    sample: &mut Observation<'_, R>,
    target: &Target,
) -> Result<FlatScene, ObservationError> {
    let inventory = scene_inventory(sample, target).await?;
    let video = sample
        .read(ReadRequest::VideoSettings, target.canvas())
        .await?;
    let input_reply = sample.read(ReadRequest::InputList, Value::Null).await?;
    let inputs = list(&input_reply["inputs"], 2048)?;
    let item_reply = sample
        .read(
            ReadRequest::SceneItemList,
            json!({"sceneUuid":inventory.selected.scene_uuid}),
        )
        .await?;
    let items = list(&item_reply["sceneItems"], 128)?
        .into_iter()
        .map(normalize_scene_item)
        .collect::<Vec<_>>();
    if items.iter().enumerate().any(|(index, item)| {
        items[..index]
            .iter()
            .any(|other| same_js_identity(item.get("sceneItemId"), other.get("sceneItemId")))
    }) {
        return Err(ObservationError::new(
            "target_changed",
            "Duplicate scene item identities",
        ));
    }
    for item in &items {
        identity(&item["sourceName"])?;
        identity(&item["sourceUuid"])?;
        if safe_integer(&item["sceneItemId"]).is_none_or(|id| id < 0.0)
            || !item["isGroup"].is_boolean()
        {
            return Err(ObservationError::new(
                "evidence_unavailable",
                "Incomplete scene item identity",
            ));
        }
    }
    Ok(FlatScene {
        collection: inventory.collection,
        scenes: inventory.scenes,
        selected: inventory.selected,
        video,
        inputs,
        items,
    })
}

/// Canonical JS-compatible signatures for verified ASCII identifier keys.
/// Other key alphabets fail closed until their locale collation is qualified.
pub fn signature(value: &Value) -> Result<String, ObservationError> {
    let mut output = String::new();
    write_signature(value, &mut output)?;
    Ok(output)
}
pub fn digest(value: &Value) -> Result<String, ObservationError> {
    Ok(format!(
        "{:x}",
        Sha256::digest(signature(value)?.as_bytes())
    ))
}
fn collation(a: &str, b: &str) -> Ordering {
    a.to_ascii_lowercase()
        .bytes()
        .map(|byte| if byte == b'_' { 0 } else { byte })
        .cmp(
            b.to_ascii_lowercase()
                .bytes()
                .map(|byte| if byte == b'_' { 0 } else { byte }),
        )
        .then_with(|| {
            a.bytes()
                .zip(b.bytes())
                .find_map(|(a, b)| {
                    (a != b).then(|| a.is_ascii_uppercase().cmp(&b.is_ascii_uppercase()))
                })
                .unwrap_or(Ordering::Equal)
        })
}
fn array_index(key: &str) -> Option<u32> {
    let value = key.parse::<u32>().ok()?;
    (value != u32::MAX && value.to_string() == key).then_some(value)
}
fn write_signature(value: &Value, output: &mut String) -> Result<(), ObservationError> {
    match value {
        Value::Object(object) => {
            if object.keys().any(|key| {
                !key.bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            }) {
                return Err(ObservationError::new(
                    "evidence_unavailable",
                    "Observation signature property collation is unavailable",
                ));
            }
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort_by(|a, b| match (array_index(a), array_index(b)) {
                (Some(a), Some(b)) => a.cmp(&b),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => collation(a, b),
            });
            output.push('{');
            for (index, key) in keys.into_iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                output.push_str(&serde_json::to_string(key).expect("JSON key"));
                output.push(':');
                write_signature(&object[key], output)?;
            }
            output.push('}');
        }
        Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                write_signature(value, output)?;
            }
            output.push(']');
        }
        Value::Number(number) => output.push_str(&js_number(number.as_f64().ok_or_else(|| {
            ObservationError::new("evidence_unavailable", "Observation number is unavailable")
        })?)),
        _ => output.push_str(&serde_json::to_string(value).expect("JSON scalar")),
    }
    Ok(())
}
fn js_number(number: f64) -> String {
    if number == 0.0 {
        return "0".to_owned();
    }
    if number.fract() == 0.0 && number.abs() <= 9_007_199_254_740_991.0 {
        return number.to_string();
    }
    let sign = if number.is_sign_negative() { "-" } else { "" };
    // std's shortest representation can choose the odd decimal at an exact
    // shortest-decimal tie. Retain its minimal significant-digit count, then
    // recompute that precision with nearest/ties-even before the ECMA layout.
    let shortest = number.abs().to_string();
    let coefficient = shortest
        .split_once('e')
        .map_or(shortest.as_str(), |(n, _)| n);
    let significant = coefficient.replace('.', "").trim_matches('0').len();
    let source = format!("{:.*e}", significant.saturating_sub(1), number.abs());
    let (coefficient, exponent) =
        source
            .split_once('e')
            .map_or((source.as_str(), 0), |(coefficient, exponent)| {
                (
                    coefficient,
                    exponent.parse::<i32>().expect("float exponent"),
                )
            });
    let mut position = coefficient.find('.').unwrap_or(coefficient.len()) as i32 + exponent;
    let mut digits = coefficient.replace('.', "");
    while digits.starts_with('0') {
        digits.remove(0);
        position -= 1;
    }
    while digits.ends_with('0') {
        digits.pop();
    }
    let length = digits.len() as i32;
    if position > 0 && position <= 21 {
        if length <= position {
            format!("{sign}{digits}{}", "0".repeat((position - length) as usize))
        } else {
            format!(
                "{sign}{}.{}",
                &digits[..position as usize],
                &digits[position as usize..]
            )
        }
    } else if position > -6 && position <= 0 {
        format!("{sign}0.{}{digits}", "0".repeat((-position) as usize))
    } else {
        let exponent = position - 1;
        format!(
            "{sign}{}{}e{}{exponent}",
            &digits[..1],
            if digits.len() > 1 {
                format!(".{}", &digits[1..])
            } else {
                String::new()
            },
            if exponent >= 0 { "+" } else { "" }
        )
    }
}

pub(crate) fn js_string(value: Option<&Value>) -> String {
    match value {
        None => "undefined".to_owned(),
        Some(Value::Null) => "null".to_owned(),
        Some(Value::String(value)) => value.clone(),
        Some(Value::Number(value)) => js_number(value.as_f64().expect("JSON number")),
        Some(Value::Bool(value)) => value.to_string(),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                if value.is_null() {
                    String::new()
                } else {
                    js_string(Some(value))
                }
            })
            .collect::<Vec<_>>()
            .join(","),
        Some(Value::Object(_)) => "[object Object]".to_owned(),
    }
}
