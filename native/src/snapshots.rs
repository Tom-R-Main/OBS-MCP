// SPDX-License-Identifier: GPL-2.0-only
//! Legacy manual snapshots. This is not demo's field-owned recovery protocol:
//! a manual restore deliberately overwrites differing settings and matches
//! items by scene name/item ID, without UUID or replacement proof. Never use
//! its plan as automatic recovery authority. No mutation executor is provided.
use crate::observation::{ObservationError, ReadAdapter, ReadFuture, ReadRequest, js_string};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{
    collections::HashSet,
    fmt,
    fs::{self, File, Metadata, OpenOptions},
    future::Future,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    pin::Pin,
    time::{Duration, Instant},
};

pub const MAX_USER_SNAPSHOTS: usize = 20;
pub const MAX_AUTO_SNAPSHOTS: usize = 10;
const WRITABLE_TRANSFORM: &[&str] = &[
    "positionX",
    "positionY",
    "rotation",
    "scaleX",
    "scaleY",
    "alignment",
    "boundsType",
    "boundsAlignment",
    "boundsWidth",
    "boundsHeight",
    "cropLeft",
    "cropRight",
    "cropTop",
    "cropBottom",
    "cropToBounds",
];

#[derive(Clone, PartialEq, Eq)]
pub enum SnapshotError {
    Read(String),
    InvalidSnapshot,
    MissingRoot,
    UnsafePath,
    HistoryUnavailable,
    HistoryCorrupt,
    HistoryMissing,
    HistoryReplaced,
    LockBusy,
    LockLost,
    PublicationUncertain,
    Capacity,
    IdentityExhausted,
}
impl fmt::Display for SnapshotError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(if matches!(self, Self::Read(_)) {
            "Snapshot evidence unavailable"
        } else {
            self.tool_message()
        })
    }
}
impl SnapshotError {
    /// Peer details are available only through this explicit tool projection.
    pub fn tool_message(&self) -> &str {
        match self {
            Self::Read(message) => message,
            Self::InvalidSnapshot => "Snapshot does not contain supported legacy state",
            Self::MissingRoot => "Snapshot state root must be explicitly created before saving",
            Self::UnsafePath => "Snapshot storage path cannot be trusted",
            Self::HistoryUnavailable => "Snapshot history is unavailable",
            Self::HistoryCorrupt => {
                "Snapshot history is malformed; preserve it for explicit inspection"
            }
            Self::HistoryMissing => "Established snapshot history is missing",
            Self::HistoryReplaced => "Established snapshot history was replaced",
            Self::LockBusy => {
                "Snapshot history is locked; lock ownership must be resolved explicitly"
            }
            Self::LockLost => "Snapshot lock ownership changed",
            Self::PublicationUncertain => {
                "Snapshot publication may be visible but durability is unconfirmed"
            }
            Self::Capacity => "Snapshot history exceeds the configured byte capacity",
            Self::IdentityExhausted => "Snapshot staging identity is exhausted",
        }
    }
}
impl std::error::Error for SnapshotError {}
impl fmt::Debug for SnapshotError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(_) => output.write_str("SnapshotError::Read([REDACTED])"),
            other => fmt::Display::fmt(other, output),
        }
    }
}
impl From<ObservationError> for SnapshotError {
    fn from(error: ObservationError) -> Self {
        Self::Read(error.message)
    }
}
pub type SnapshotFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, SnapshotError>> + Send + 'a>>;

#[derive(Clone)]
pub struct ReadCall {
    pub request: ReadRequest,
    pub data: Value,
}
#[derive(Clone)]
pub struct BatchReadResult {
    pub ok: bool,
    pub code: Option<u64>,
    pub comment: Option<String>,
    pub response_data: Value,
}
impl fmt::Debug for ReadCall {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("ReadCall")
            .field("request", &self.request)
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for BatchReadResult {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("BatchReadResult")
            .field("ok", &self.ok)
            .field("code", &self.code)
            .finish_non_exhaustive()
    }
}
/// Batch execution is explicit. A production batch-capable adapter must retain
/// result order and request rejection codes, including unsupported audio604.
pub trait SnapshotReader: ReadAdapter {
    fn batch<'a>(&'a mut self, calls: Vec<ReadCall>) -> SnapshotFuture<'a, Vec<BatchReadResult>>;
}

/// Explicit current-adapter projection, not a claim of one OBS batch frame or
/// an atomic snapshot. The caller owns the complete monotonic deadline.
pub struct SequentialReads<'a, R: ReadAdapter> {
    reader: &'a mut R,
    deadline: tokio::time::Instant,
}
impl<'a, R: ReadAdapter> SequentialReads<'a, R> {
    pub fn new(reader: &'a mut R, deadline: tokio::time::Instant) -> Self {
        Self { reader, deadline }
    }
}
impl<R: ReadAdapter> ReadAdapter for SequentialReads<'_, R> {
    fn supports(&self, request: ReadRequest) -> Option<bool> {
        self.reader.supports(request)
    }
    fn read<'a>(
        &'a mut self,
        request: ReadRequest,
        data: Value,
        deadline: tokio::time::Instant,
    ) -> ReadFuture<'a> {
        if self.reader.supports(request) == Some(false) {
            return Box::pin(async move {
                Err(ObservationError::new(
                    "capability_unavailable",
                    format!(
                        "OBS WebSocket does not advertise support for request '{}'",
                        request.as_str()
                    ),
                ))
            });
        }
        self.reader.read(request, data, deadline.min(self.deadline))
    }
}
impl<R: ReadAdapter> SnapshotReader for SequentialReads<'_, R> {
    fn batch<'a>(&'a mut self, calls: Vec<ReadCall>) -> SnapshotFuture<'a, Vec<BatchReadResult>> {
        Box::pin(async move {
            if let Some(call) = calls
                .iter()
                .find(|call| self.reader.supports(call.request) == Some(false))
            {
                return Err(SnapshotError::Read(format!(
                    "OBS WebSocket does not advertise support for request '{}'",
                    call.request.as_str()
                )));
            }
            let mut results = Vec::with_capacity(calls.len());
            for call in calls {
                match self
                    .reader
                    .read(call.request, call.data, self.deadline)
                    .await
                {
                    Ok(response_data) => results.push(BatchReadResult {
                        ok: true,
                        code: Some(100),
                        comment: None,
                        response_data,
                    }),
                    Err(error) => {
                        if let Some(code) = error.request_code() {
                            results.push(BatchReadResult {
                                ok: false,
                                code: Some(code),
                                comment: Some(error.message),
                                response_data: json!({}),
                            });
                        } else {
                            return Err(error.into());
                        }
                    }
                }
            }
            Ok(results)
        })
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InputState {
    pub input_name: String,
    pub input_kind: String,
    pub input_settings: Map<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub muted: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_mul: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_tracks: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unavailable: Option<Map<String, Value>>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ItemState {
    pub scene_name: String,
    pub scene_item_id: f64,
    pub source_name: String,
    pub index: f64,
    pub enabled: bool,
    pub locked: bool,
    pub transform: Map<String, Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub obs_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scene_collection: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub taken_at: String,
    pub inputs: Vec<InputState>,
    pub items: Vec<ItemState>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}
impl fmt::Debug for Snapshot {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("Snapshot")
            .field("id", &self.id)
            .field("inputs", &self.inputs.len())
            .field("items", &self.items.len())
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for InputState {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.debug_struct("InputState").finish_non_exhaustive()
    }
}
impl fmt::Debug for ItemState {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("ItemState")
            .field("scene_item_id", &self.scene_item_id)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug)]
pub enum RequiredInputField {
    InputSettings,
    Muted,
    VolumeMul,
    AudioTracks,
}
impl RequiredInputField {
    fn name(self) -> &'static str {
        match self {
            Self::InputSettings => "inputSettings",
            Self::Muted => "muted",
            Self::VolumeMul => "volumeMul",
            Self::AudioTracks => "audioTracks",
        }
    }
}
#[derive(Default)]
pub struct CaptureOptions {
    pub scenes: Option<Vec<String>>,
    pub inputs: Option<Vec<String>>,
    pub label: Option<String>,
    pub auto: bool,
    pub scene_collection: Option<String>,
    pub required_input_fields: Vec<RequiredInputField>,
}
/// Supplied by the host's ID/clock/endpoint owner, not read from environment.
pub struct CaptureMetadata {
    pub id: String,
    pub obs_url: String,
    pub taken_at: String,
}
pub trait SnapshotWriter {
    fn save(&mut self, snapshot: &Snapshot) -> Result<(), SnapshotError>;
}

pub async fn read_items<R: SnapshotReader>(
    reader: &mut R,
    scenes: &[String],
) -> Result<Vec<ItemState>, SnapshotError> {
    let results = reader
        .batch(
            scenes
                .iter()
                .map(|scene| ReadCall {
                    request: ReadRequest::SceneItemList,
                    data: json!({"sceneName":scene}),
                })
                .collect(),
        )
        .await?;
    let mut items = Vec::new();
    for (index, scene) in scenes.iter().enumerate() {
        let result = results.get(index);
        if result.is_none_or(|result| !result.ok) {
            let detail = result
                .and_then(|result| result.comment.clone())
                .unwrap_or_else(|| {
                    format!(
                        "code {}",
                        result
                            .and_then(|result| result.code)
                            .map_or_else(|| "undefined".to_owned(), |code| code.to_string())
                    )
                });
            return Err(SnapshotError::Read(format!(
                "Cannot read scene {scene}: {detail}"
            )));
        }
        let response = &result.unwrap().response_data;
        for item in response
            .get("sceneItems")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|item| item.is_object())
        {
            let Some(scene_item_id) = item["sceneItemId"].as_f64() else {
                continue;
            };
            let index = js_numeric(item.get("sceneItemIndex").filter(|value| !value.is_null()));
            if !index.is_finite() {
                return Err(SnapshotError::InvalidSnapshot);
            }
            let transform = item["sceneItemTransform"]
                .as_object()
                .map(|transform| {
                    transform
                        .iter()
                        .filter(|(key, _)| WRITABLE_TRANSFORM.contains(&key.as_str()))
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect()
                })
                .unwrap_or_default();
            items.push(ItemState {
                scene_name: scene.clone(),
                scene_item_id,
                source_name: js_string(
                    item.get("sourceName")
                        .filter(|value| !value.is_null())
                        .or(Some(&Value::String(String::new()))),
                ),
                index,
                enabled: item["sceneItemEnabled"] != false,
                locked: item["sceneItemLocked"] == true,
                transform,
                extra: Map::new(),
            });
        }
    }
    Ok(items)
}

pub async fn read_inputs<R: SnapshotReader>(
    reader: &mut R,
    names: &[String],
    required: &[RequiredInputField],
) -> Result<Vec<InputState>, SnapshotError> {
    let results = reader
        .batch(
            names
                .iter()
                .flat_map(|name| {
                    [
                        ReadRequest::InputSettings,
                        ReadRequest::InputMute,
                        ReadRequest::InputVolume,
                        ReadRequest::InputAudioTracks,
                    ]
                    .map(|request| ReadCall {
                        request,
                        data: json!({"inputName":name}),
                    })
                })
                .collect(),
        )
        .await?;
    let mut saved_inputs = Vec::new();
    for (index, name) in names.iter().enumerate() {
        let settings = results.get(index * 4);
        let Some(settings) =
            settings.filter(|settings| settings.ok && settings.response_data.is_object())
        else {
            if !required.is_empty() {
                return Err(SnapshotError::Read(format!(
                    "Incomplete snapshot: cannot read settings for {name}"
                )));
            }
            continue;
        };
        let result = |offset: usize| results.get(index * 4 + offset);
        let data = |offset: usize| {
            result(offset)
                .filter(|result| result.ok && result.response_data.is_object())
                .map(|result| &result.response_data)
        };
        let mut saved = InputState {
            input_name: name.clone(),
            input_kind: js_string(
                settings
                    .response_data
                    .get("inputKind")
                    .filter(|value| !value.is_null())
                    .or(Some(&Value::String(String::new()))),
            ),
            input_settings: settings.response_data["inputSettings"]
                .as_object()
                .cloned()
                .unwrap_or_default(),
            muted: data(1).and_then(|data| data["inputMuted"].as_bool()),
            volume_mul: data(2).and_then(|data| data["inputVolumeMul"].as_f64()),
            audio_tracks: data(3)
                .and_then(|data| data["inputAudioTracks"].as_object())
                .cloned(),
            unavailable: None,
            extra: Map::new(),
        };
        let absent = [
            saved.muted.is_none(),
            saved.volume_mul.is_none(),
            saved.audio_tracks.is_none(),
        ];
        for ((field, offset), absent) in [("muted", 1), ("volumeMul", 2), ("audioTracks", 3)]
            .into_iter()
            .zip(absent)
        {
            if absent {
                saved.unavailable.get_or_insert_with(Map::new).insert(
                    field.to_owned(),
                    json!(
                        if result(offset).and_then(|result| result.code) == Some(604) {
                            "unsupported"
                        } else {
                            "failed"
                        }
                    ),
                );
            }
        }
        for field in required {
            let missing = match field {
                RequiredInputField::InputSettings => {
                    !settings.response_data["inputSettings"].is_object()
                }
                RequiredInputField::Muted => saved.muted.is_none(),
                RequiredInputField::VolumeMul => saved.volume_mul.is_none(),
                RequiredInputField::AudioTracks => saved.audio_tracks.is_none(),
            };
            if missing {
                let reason = saved
                    .unavailable
                    .as_ref()
                    .and_then(|unavailable| unavailable.get(field.name()))
                    .and_then(Value::as_str)
                    .unwrap_or("failed");
                return Err(SnapshotError::Read(format!(
                    "Incomplete snapshot: {name} {} unavailable ({reason})",
                    field.name()
                )));
            }
        }
        saved_inputs.push(saved);
    }
    Ok(saved_inputs)
}

pub async fn take_snapshot<R: SnapshotReader, W: SnapshotWriter>(
    reader: &mut R,
    writer: &mut W,
    options: CaptureOptions,
    metadata: CaptureMetadata,
    deadline: tokio::time::Instant,
) -> Result<Snapshot, SnapshotError> {
    let snapshot = capture_snapshot(reader, options, metadata, deadline).await?;
    writer.save(&snapshot)?;
    Ok(snapshot)
}

/// Read the legacy snapshot without publishing it. The service transfers its
/// sole file owner to a joined blocking job only after these reads settle.
pub async fn capture_snapshot<R: SnapshotReader>(
    reader: &mut R,
    options: CaptureOptions,
    metadata: CaptureMetadata,
    deadline: tokio::time::Instant,
) -> Result<Snapshot, SnapshotError> {
    capture_snapshot_with_metadata(reader, options, || metadata, deadline).await
}

/// Manual capture's ID/UTC factory runs after the scene/item/inventory reads
/// and before the input batch, matching the Source public callback. Callers
/// supplying already-owned demo metadata retain their existing semantics.
pub async fn capture_snapshot_with_metadata<R: SnapshotReader, F: FnOnce() -> CaptureMetadata>(
    reader: &mut R,
    mut options: CaptureOptions,
    metadata: F,
    deadline: tokio::time::Instant,
) -> Result<Snapshot, SnapshotError> {
    if options.scenes.is_none() && options.inputs.is_none() {
        let program = reader
            .read(ReadRequest::CurrentProgramScene, Value::Null, deadline)
            .await?;
        let name = program
            .get("currentProgramSceneName")
            .filter(|value| !value.is_null())
            .or_else(|| program.get("sceneName"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                SnapshotError::Read("OBS did not report the program scene".to_owned())
            })?;
        options.scenes = Some(vec![name.to_owned()]);
    }
    let items = read_items(reader, options.scenes.as_deref().unwrap_or(&[])).await?;
    let names = if let Some(names) = options.inputs {
        names
    } else {
        let response = reader
            .read(ReadRequest::InputList, Value::Null, deadline)
            .await?;
        let list = match response.get("inputs") {
            None | Some(Value::Null) => vec![],
            Some(Value::Array(values)) => values.iter().filter(|value| value.is_object()).collect(),
            _ => {
                return Err(SnapshotError::Read(
                    "OBS input inventory is not an array".to_owned(),
                ));
            }
        };
        let known = list
            .into_iter()
            .map(|input| js_string(input.get("inputName")))
            .collect::<HashSet<_>>();
        unique(items.iter().map(|item| item.source_name.clone()))
            .into_iter()
            .filter(|name| known.contains(name))
            .collect()
    };
    let metadata = metadata();
    let inputs = read_inputs(reader, &names, &options.required_input_fields).await?;
    let snapshot = Snapshot {
        id: metadata.id,
        obs_url: Some(metadata.obs_url),
        auto: options.auto.then_some(true),
        scene_collection: options.scene_collection.filter(|value| !value.is_empty()),
        label: options.label.filter(|value| !value.is_empty()),
        taken_at: metadata.taken_at,
        inputs,
        items,
        extra: Map::new(),
    };
    Ok(snapshot)
}

#[derive(Clone, PartialEq)]
pub enum Mutation {
    InputSettings {
        input_name: String,
        settings: Map<String, Value>,
    },
    InputMute {
        input_name: String,
        muted: bool,
    },
    InputVolume {
        input_name: String,
        volume: f64,
    },
    InputAudioTracks {
        input_name: String,
        tracks: Map<String, Value>,
    },
    ItemTransform {
        scene_name: String,
        item_id: f64,
        transform: Map<String, Value>,
    },
    ItemEnabled {
        scene_name: String,
        item_id: f64,
        enabled: bool,
    },
    ItemLocked {
        scene_name: String,
        item_id: f64,
        locked: bool,
    },
    ItemIndex {
        scene_name: String,
        item_id: f64,
        index: usize,
    },
}
impl fmt::Debug for Mutation {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        let request = match self {
            Self::InputSettings { .. } => "SetInputSettings",
            Self::InputMute { .. } => "SetInputMute",
            Self::InputVolume { .. } => "SetInputVolume",
            Self::InputAudioTracks { .. } => "SetInputAudioTracks",
            Self::ItemTransform { .. } => "SetSceneItemTransform",
            Self::ItemEnabled { .. } => "SetSceneItemEnabled",
            Self::ItemLocked { .. } => "SetSceneItemLocked",
            Self::ItemIndex { .. } => "SetSceneItemIndex",
        };
        output
            .debug_struct("Mutation")
            .field("request_type", &request)
            .finish_non_exhaustive()
    }
}
impl Mutation {
    pub fn request(&self) -> Value {
        match self {
            Self::InputSettings {
                input_name,
                settings,
            } => {
                json!({"requestType":"SetInputSettings","requestData":{"inputName":input_name,"inputSettings":settings,"overlay":false}})
            }
            Self::InputMute { input_name, muted } => {
                json!({"requestType":"SetInputMute","requestData":{"inputName":input_name,"inputMuted":muted}})
            }
            Self::InputVolume { input_name, volume } => {
                json!({"requestType":"SetInputVolume","requestData":{"inputName":input_name,"inputVolumeMul":volume}})
            }
            Self::InputAudioTracks { input_name, tracks } => {
                json!({"requestType":"SetInputAudioTracks","requestData":{"inputName":input_name,"inputAudioTracks":tracks}})
            }
            Self::ItemTransform {
                scene_name,
                item_id,
                transform,
            } => {
                json!({"requestType":"SetSceneItemTransform","requestData":{"sceneName":scene_name,"sceneItemId":item_id,"sceneItemTransform":transform}})
            }
            Self::ItemEnabled {
                scene_name,
                item_id,
                enabled,
            } => {
                json!({"requestType":"SetSceneItemEnabled","requestData":{"sceneName":scene_name,"sceneItemId":item_id,"sceneItemEnabled":enabled}})
            }
            Self::ItemLocked {
                scene_name,
                item_id,
                locked,
            } => {
                json!({"requestType":"SetSceneItemLocked","requestData":{"sceneName":scene_name,"sceneItemId":item_id,"sceneItemLocked":locked}})
            }
            Self::ItemIndex {
                scene_name,
                item_id,
                index,
            } => {
                json!({"requestType":"SetSceneItemIndex","requestData":{"sceneName":scene_name,"sceneItemId":item_id,"sceneItemIndex":index}})
            }
        }
    }
}
#[derive(Clone, PartialEq)]
pub struct RestoreChange {
    pub target: String,
    pub field: String,
    pub mutation: Mutation,
}
impl fmt::Debug for RestoreChange {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("RestoreChange")
            .field("mutation", &self.mutation)
            .finish_non_exhaustive()
    }
}
impl Serialize for RestoreChange {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        json!({"target":self.target,"field":self.field,"request":self.mutation.request()})
            .serialize(serializer)
    }
}
#[derive(Clone, PartialEq, Serialize)]
pub struct RestorePlan {
    pub changes: Vec<RestoreChange>,
    pub missing: Vec<String>,
}
impl fmt::Debug for RestorePlan {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("RestorePlan")
            .field("changes", &self.changes.len())
            .field("missing", &self.missing.len())
            .finish()
    }
}

/// Exact legacy manual comparison. UUID/kind/collection proof is absent from
/// this format; changing a source can still produce an overwrite plan.
pub async fn plan_restore<R: SnapshotReader>(
    reader: &mut R,
    snapshot: &Snapshot,
) -> Result<RestorePlan, SnapshotError> {
    let scenes = unique(snapshot.items.iter().map(|item| item.scene_name.clone()));
    let mut current_items = Vec::new();
    let mut readable = Vec::new();
    let mut missing = Vec::new();
    // Source runs these independent observations concurrently. The explicit
    // sequential projection preserves returned order, not atomicity/batching.
    for scene in scenes {
        match read_items(reader, std::slice::from_ref(&scene)).await {
            Ok(items) => {
                readable.push(scene);
                current_items.extend(items);
            }
            Err(_) => missing.push(format!(
                "scene {scene} no longer exists; its items were not restored"
            )),
        }
    }
    let names = snapshot
        .inputs
        .iter()
        .map(|input| input.input_name.clone())
        .collect::<Vec<_>>();
    let current_inputs = read_inputs(reader, &names, &[]).await?;
    let mut changes = Vec::new();
    for saved in &snapshot.inputs {
        let target = format!("input {}", saved.input_name);
        let Some(current) = current_inputs
            .iter()
            .find(|current| current.input_name == saved.input_name)
        else {
            missing.push(format!("{target} no longer exists"));
            continue;
        };
        if !same(
            &Value::Object(saved.input_settings.clone()),
            &Value::Object(current.input_settings.clone()),
        ) {
            changes.push(RestoreChange {
                target: target.clone(),
                field: "settings".to_owned(),
                mutation: Mutation::InputSettings {
                    input_name: saved.input_name.clone(),
                    settings: saved.input_settings.clone(),
                },
            });
        }
        if let Some(muted) = saved.muted
            && Some(muted) != current.muted
        {
            changes.push(RestoreChange {
                target: target.clone(),
                field: if muted { "mute" } else { "unmute" }.to_owned(),
                mutation: Mutation::InputMute {
                    input_name: saved.input_name.clone(),
                    muted,
                },
            });
        }
        if let Some(volume) = saved.volume_mul
            && current
                .volume_mul
                .is_none_or(|now| (volume - now).abs() >= 1e-6)
        {
            changes.push(RestoreChange {
                target: target.clone(),
                field: "volume".to_owned(),
                mutation: Mutation::InputVolume {
                    input_name: saved.input_name.clone(),
                    volume,
                },
            });
        }
        if let Some(tracks) = &saved.audio_tracks
            && current
                .audio_tracks
                .as_ref()
                .is_none_or(|now| !same(&json!(tracks), &json!(now)))
        {
            changes.push(RestoreChange {
                target,
                field: "audio tracks".to_owned(),
                mutation: Mutation::InputAudioTracks {
                    input_name: saved.input_name.clone(),
                    tracks: tracks.clone(),
                },
            });
        }
    }
    for saved in snapshot
        .items
        .iter()
        .filter(|item| readable.contains(&item.scene_name))
    {
        let target = format!(
            "{} › {} (#{})",
            saved.scene_name,
            saved.source_name,
            display_number(saved.scene_item_id)
        );
        let Some(current) = current_items.iter().find(|current| {
            current.scene_name == saved.scene_name && current.scene_item_id == saved.scene_item_id
        }) else {
            missing.push(format!("{target} was removed"));
            continue;
        };
        if !same_transform(&saved.transform, &current.transform) {
            changes.push(RestoreChange {
                target: target.clone(),
                field: "transform".to_owned(),
                mutation: Mutation::ItemTransform {
                    scene_name: saved.scene_name.clone(),
                    item_id: saved.scene_item_id,
                    transform: saved.transform.clone(),
                },
            });
        }
        if saved.enabled != current.enabled {
            changes.push(RestoreChange {
                target: target.clone(),
                field: if saved.enabled { "show" } else { "hide" }.to_owned(),
                mutation: Mutation::ItemEnabled {
                    scene_name: saved.scene_name.clone(),
                    item_id: saved.scene_item_id,
                    enabled: saved.enabled,
                },
            });
        }
        if saved.locked != current.locked {
            changes.push(RestoreChange {
                target,
                field: if saved.locked { "lock" } else { "unlock" }.to_owned(),
                mutation: Mutation::ItemLocked {
                    scene_name: saved.scene_name.clone(),
                    item_id: saved.scene_item_id,
                    locked: saved.locked,
                },
            });
        }
    }
    for scene in &readable {
        let mut now = current_items
            .iter()
            .filter(|item| item.scene_name == *scene)
            .collect::<Vec<_>>();
        now.sort_by(|a, b| {
            a.index
                .partial_cmp(&b.index)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let mut survivors = snapshot
            .items
            .iter()
            .filter(|saved| {
                saved.scene_name == *scene
                    && now
                        .iter()
                        .any(|current| current.scene_item_id == saved.scene_item_id)
            })
            .collect::<Vec<_>>();
        survivors.sort_by(|a, b| {
            a.index
                .partial_cmp(&b.index)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let mut next = 0;
        let target = now
            .iter()
            .map(|current| {
                if survivors
                    .iter()
                    .any(|saved| saved.scene_item_id == current.scene_item_id)
                {
                    let wanted = survivors[next];
                    next += 1;
                    *now.iter()
                        .find(|current| current.scene_item_id == wanted.scene_item_id)
                        .unwrap()
                } else {
                    *current
                }
            })
            .collect::<Vec<_>>();
        if target
            .iter()
            .zip(&now)
            .any(|(a, b)| a.scene_item_id != b.scene_item_id)
        {
            for (index, item) in target.into_iter().enumerate() {
                changes.push(RestoreChange {
                    target: format!(
                        "{} › {} (#{})",
                        scene,
                        item.source_name,
                        display_number(item.scene_item_id)
                    ),
                    field: format!("order (to {index})"),
                    mutation: Mutation::ItemIndex {
                        scene_name: scene.clone(),
                        item_id: item.scene_item_id,
                        index,
                    },
                });
            }
        }
    }
    for scene in readable {
        let saved = snapshot
            .items
            .iter()
            .filter(|item| item.scene_name == scene)
            .map(|item| item.scene_item_id)
            .collect::<Vec<_>>();
        for item in current_items
            .iter()
            .filter(|item| item.scene_name == scene && !saved.contains(&item.scene_item_id))
        {
            missing.push(format!(
                "{} › {} (#{}) was added since; it is left in place",
                scene,
                item.source_name,
                display_number(item.scene_item_id)
            ));
        }
    }
    Ok(RestorePlan { changes, missing })
}

pub fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(a), Value::Number(b)) => {
            (a.as_f64().unwrap() - b.as_f64().unwrap()).abs() < 1e-6
        }
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same(a, b))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(key, a)| b.get(key).is_some_and(|b| same(a, b)))
        }
        _ => a == b,
    }
}
pub fn same_transform(a: &Map<String, Value>, b: &Map<String, Value>) -> bool {
    a.keys()
        .chain(b.keys())
        .all(|key| match (a.get(key), b.get(key)) {
            (Some(a), Some(b)) => crate::scene_plan::same_transform_value(key, a, b),
            (None, None) => true,
            _ => false,
        })
}
fn unique(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = HashSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(value.clone()))
        .collect()
}
fn display_number(number: f64) -> String {
    crate::observation::signature(&json!(number)).unwrap_or_else(|_| "unavailable".to_owned())
}
fn js_numeric(value: Option<&Value>) -> f64 {
    match value {
        None | Some(Value::Null) => 0.0,
        Some(Value::Number(number)) => number.as_f64().unwrap(),
        Some(Value::Bool(value)) => f64::from(u8::from(*value)),
        Some(Value::String(value)) => {
            let value = value.trim_matches(|character: char| {
                matches!(character,'\u{9}'..='\u{d}'|'\u{20}'|'\u{a0}'|'\u{1680}'|'\u{2000}'..='\u{200a}'|'\u{2028}'|'\u{2029}'|'\u{202f}'|'\u{205f}'|'\u{3000}'|'\u{feff}')
            });
            if value.is_empty() {
                0.0
            } else if matches!(value, "Infinity" | "+Infinity" | "-Infinity") {
                if value.starts_with('-') {
                    f64::NEG_INFINITY
                } else {
                    f64::INFINITY
                }
            } else if let Some((radix, digits)) = [
                (16, "0x"),
                (16, "0X"),
                (8, "0o"),
                (8, "0O"),
                (2, "0b"),
                (2, "0B"),
            ]
            .into_iter()
            .find_map(|(radix, prefix)| value.strip_prefix(prefix).map(|digits| (radix, digits)))
            {
                if digits.is_empty() {
                    f64::NAN
                } else {
                    digits
                        .chars()
                        .try_fold(0.0, |value, digit| {
                            digit
                                .to_digit(radix)
                                .map(|digit| value * radix as f64 + digit as f64)
                        })
                        .unwrap_or(f64::NAN)
                }
            } else if value.bytes().all(|byte| {
                byte.is_ascii_digit() || matches!(byte, b'.' | b'+' | b'-' | b'e' | b'E')
            }) {
                value.parse::<f64>().unwrap_or(f64::NAN)
            } else {
                f64::NAN
            }
        }
        other => js_numeric(Some(&Value::String(js_string(other)))),
    }
}

pub fn prune(snapshots: Vec<Snapshot>) -> Vec<Snapshot> {
    let mut auto = snapshots
        .iter()
        .filter(|snapshot| snapshot.auto == Some(true))
        .count();
    let mut user = snapshots.len() - auto;
    snapshots
        .into_iter()
        .filter(|snapshot| {
            if snapshot.auto == Some(true) {
                let keep = auto <= MAX_AUTO_SNAPSHOTS;
                auto -= 1;
                keep
            } else {
                let keep = user <= MAX_USER_SNAPSHOTS;
                user -= 1;
                keep
            }
        })
        .collect()
}

/// One explicitly supplied snapshots.json file owner. File I/O is synchronous;
/// a host offloading it must retain the job until actual completion. Unknown
/// history and stale locks are refused, unlike the source's implicit repair.
pub struct SnapshotFile {
    root: PathBuf,
    path: PathBuf,
    root_identity: Metadata,
    known: Option<Metadata>,
    capacity: usize,
    sequence: u64,
    lock_wait: Duration,
}
impl SnapshotFile {
    pub fn new(root: impl AsRef<Path>, capacity: usize) -> Result<Self, SnapshotError> {
        if capacity == 0
            || u64::try_from(capacity)
                .ok()
                .and_then(|capacity| capacity.checked_add(1))
                .is_none()
        {
            return Err(SnapshotError::Capacity);
        }
        let root = root.as_ref().to_path_buf();
        validate_root(&root)?;
        let root_identity = fs::symlink_metadata(&root).map_err(|_| SnapshotError::MissingRoot)?;
        let path = root.join("snapshots.json");
        let known = match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                validate_file(&metadata)?;
                Some(metadata)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(_) => return Err(SnapshotError::HistoryUnavailable),
        };
        Ok(Self {
            root,
            path,
            root_identity,
            known,
            capacity,
            sequence: 0,
            lock_wait: Duration::from_secs(2),
        })
    }
    pub fn with_lock_wait(mut self, wait: Duration) -> Self {
        self.lock_wait = wait.min(Duration::from_secs(2));
        self
    }
    fn layout(&self) -> Result<(), SnapshotError> {
        validate_root(&self.root)?;
        let current = fs::symlink_metadata(&self.root).map_err(|_| SnapshotError::UnsafePath)?;
        if !same_file(&current, &self.root_identity) {
            return Err(SnapshotError::UnsafePath);
        }
        Ok(())
    }
    fn current_file(&self) -> Result<(), SnapshotError> {
        match (fs::symlink_metadata(&self.path), &self.known) {
            (Ok(current), Some(known)) => {
                validate_file(&current)?;
                if !same_file(&current, known)
                    || current.len() != known.len()
                    || current.modified().ok() != known.modified().ok()
                {
                    return Err(SnapshotError::HistoryReplaced);
                }
                Ok(())
            }
            (Err(error), None) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            (Err(error), Some(_)) if error.kind() == io::ErrorKind::NotFound => {
                Err(SnapshotError::HistoryMissing)
            }
            (Ok(_), None) => Err(SnapshotError::HistoryReplaced),
            _ => Err(SnapshotError::HistoryUnavailable),
        }
    }
    pub fn load(&mut self) -> Result<Vec<Snapshot>, SnapshotError> {
        self.layout()?;
        let metadata = match fs::symlink_metadata(&self.path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return if self.known.is_some() {
                    Err(SnapshotError::HistoryMissing)
                } else {
                    Ok(vec![])
                };
            }
            Err(_) => return Err(SnapshotError::HistoryUnavailable),
        };
        validate_file(&metadata)?;
        if self
            .known
            .as_ref()
            .is_some_and(|known| !same_file(known, &metadata))
        {
            return Err(SnapshotError::HistoryReplaced);
        }
        if metadata.len() > self.capacity as u64 {
            return Err(SnapshotError::Capacity);
        }
        let file = File::open(&self.path).map_err(|_| SnapshotError::HistoryUnavailable)?;
        let opened = file
            .metadata()
            .map_err(|_| SnapshotError::HistoryUnavailable)?;
        validate_file(&opened)?;
        if !same_file(&opened, &metadata) {
            return Err(SnapshotError::HistoryReplaced);
        }
        let mut bytes = Vec::new();
        file.take(self.capacity as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| SnapshotError::HistoryUnavailable)?;
        if bytes.len() > self.capacity {
            return Err(SnapshotError::Capacity);
        }
        let value: Value =
            serde_json::from_slice(&bytes).map_err(|_| SnapshotError::HistoryCorrupt)?;
        let values = value.as_array().ok_or(SnapshotError::HistoryCorrupt)?;
        for snapshot in values {
            for field in ["obsUrl", "auto", "sceneCollection", "label"] {
                if snapshot.get(field).is_some_and(Value::is_null) {
                    return Err(SnapshotError::HistoryCorrupt);
                }
            }
        }
        let snapshots: Vec<Snapshot> =
            serde_json::from_value(value).map_err(|_| SnapshotError::HistoryCorrupt)?;
        let after = fs::symlink_metadata(&self.path).map_err(|_| SnapshotError::HistoryMissing)?;
        validate_file(&after)?;
        if !same_file(&after, &opened)
            || after.len() != opened.len()
            || after.modified().ok() != opened.modified().ok()
        {
            return Err(SnapshotError::HistoryReplaced);
        }
        self.known = Some(after);
        Ok(snapshots)
    }
    pub fn for_endpoint(&mut self, endpoint: &str) -> Result<Vec<Snapshot>, SnapshotError> {
        Ok(self
            .load()?
            .into_iter()
            .filter(|snapshot| {
                snapshot
                    .obs_url
                    .as_deref()
                    .is_none_or(|url| url == endpoint)
            })
            .collect())
    }
}
impl SnapshotWriter for SnapshotFile {
    fn save(&mut self, snapshot: &Snapshot) -> Result<(), SnapshotError> {
        self.layout()?;
        let lock_path = self.root.join("snapshots.json.lock");
        let deadline = Instant::now() + self.lock_wait;
        let mut lock = loop {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(&lock_path) {
                Ok(file) => break OwnedFile::new(lock_path.clone(), file)?,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    if Instant::now() >= deadline {
                        return Err(SnapshotError::LockBusy);
                    }
                    std::thread::sleep(Duration::from_millis(25));
                }
                Err(_) => return Err(SnapshotError::HistoryUnavailable),
            }
        };
        let mut snapshots = self.load()?;
        snapshots.push(snapshot.clone());
        let snapshots = prune(snapshots);
        let mut bytes =
            serde_json::to_vec_pretty(&snapshots).map_err(|_| SnapshotError::InvalidSnapshot)?;
        bytes.push(b'\n');
        if bytes.len() > self.capacity {
            return Err(SnapshotError::Capacity);
        }
        if serde_json::from_slice::<Vec<Snapshot>>(&bytes).is_err() {
            return Err(SnapshotError::InvalidSnapshot);
        }
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(SnapshotError::IdentityExhausted)?;
        let temporary = self.root.join(format!(
            "snapshots.json.{}.{}.tmp",
            std::process::id(),
            self.sequence
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(&temporary)
            .map_err(|_| SnapshotError::HistoryUnavailable)?;
        let mut staged = OwnedFile::new(temporary, file)?;
        let file = staged
            .file
            .as_mut()
            .ok_or(SnapshotError::HistoryUnavailable)?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| SnapshotError::HistoryUnavailable)?;
        self.layout()?;
        lock.check()?;
        self.current_file()?;
        drop(staged.file.take());
        fs::rename(&staged.path, &self.path).map_err(|_| SnapshotError::HistoryUnavailable)?;
        staged.committed = true;
        self.known = Some(
            fs::symlink_metadata(&self.path).map_err(|_| SnapshotError::PublicationUncertain)?,
        );
        let directory = File::open(&self.root).map_err(|_| SnapshotError::PublicationUncertain)?;
        directory
            .sync_all()
            .map_err(|_| SnapshotError::PublicationUncertain)?;
        self.layout()
            .map_err(|_| SnapshotError::PublicationUncertain)?;
        self.current_file()
            .map_err(|_| SnapshotError::PublicationUncertain)?;
        lock.release()
            .map_err(|_| SnapshotError::PublicationUncertain)?;
        Ok(())
    }
}
struct OwnedFile {
    path: PathBuf,
    file: Option<File>,
    identity: Metadata,
    committed: bool,
}
impl OwnedFile {
    fn new(path: PathBuf, file: File) -> Result<Self, SnapshotError> {
        let identity = file
            .metadata()
            .map_err(|_| SnapshotError::HistoryUnavailable)?;
        Ok(Self {
            path,
            file: Some(file),
            identity,
            committed: false,
        })
    }
    fn check(&self) -> Result<(), SnapshotError> {
        if fs::symlink_metadata(&self.path)
            .is_ok_and(|metadata| same_file(&metadata, &self.identity))
        {
            Ok(())
        } else {
            Err(SnapshotError::LockLost)
        }
    }
    fn release(&mut self) -> Result<(), SnapshotError> {
        self.check()?;
        drop(self.file.take());
        fs::remove_file(&self.path).map_err(|_| SnapshotError::LockLost)?;
        self.committed = true;
        Ok(())
    }
}
impl Drop for OwnedFile {
    fn drop(&mut self) {
        drop(self.file.take());
        if !self.committed && self.check().is_ok() {
            let _ = fs::remove_file(&self.path);
        }
    }
}
fn validate_root(path: &Path) -> Result<(), SnapshotError> {
    if !path.is_absolute() {
        return Err(SnapshotError::UnsafePath);
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Prefix(_)
            | std::path::Component::RootDir
            | std::path::Component::Normal(_) => current.push(component.as_os_str()),
            _ => return Err(SnapshotError::UnsafePath),
        }
        let metadata = fs::symlink_metadata(&current).map_err(|_| SnapshotError::MissingRoot)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(SnapshotError::UnsafePath);
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = fs::symlink_metadata(path).map_err(|_| SnapshotError::MissingRoot)?;
        if metadata.permissions().mode() & 0o022 != 0 {
            return Err(SnapshotError::UnsafePath);
        }
    }
    Ok(())
}
fn validate_file(metadata: &Metadata) -> Result<(), SnapshotError> {
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(SnapshotError::UnsafePath);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 || metadata.mode() & 0o077 != 0 {
            return Err(SnapshotError::UnsafePath);
        }
    }
    Ok(())
}
fn same_file(a: &Metadata, b: &Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        a.dev() == b.dev() && a.ino() == b.ino()
    }
    #[cfg(not(unix))]
    {
        a.created().ok() == b.created().ok()
            && a.is_file() == b.is_file()
            && a.is_dir() == b.is_dir()
    }
}
