// SPDX-License-Identifier: GPL-2.0-only
//! Restricted preparation/recovery. Only attempted mute/track/FIT fields are
//! owned. Snapshot history and the private version1 recovery journal are distinct.
//! `applied` on a partial report retains source attempt semantics, not acceptance.
//! Native V2 journals persist intent, possible dispatch, and terminal delivery.
//! Possible dispatch is deliberately recorded before send; uncertain outcomes
//! never become automatic restoration authority by reading desired values.
use crate::{
    capture_backend,
    observation::{ObservationError, ReadAdapter, ReadFuture, ReadRequest, normalize_scene_item},
    preparation_journal::{Fence as JournalFence, Provenance, ProvenanceError, Step},
    snapshots::{
        self, CaptureMetadata, CaptureOptions, RequiredInputField, SnapshotReader, SnapshotWriter,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    fs::{self, File, Metadata, OpenOptions},
    future::Future,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::time::{Instant, timeout_at};

pub type DemoFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, DemoError>> + Send + 'a>>;

/// A deadline ends evidence admission, not ownership of admitted work. Concrete
/// adapters must independently bound cleanup. Drain the original future before
/// releasing the journal writer and discard every late outcome.
async fn retained_deadline<F: Future>(
    deadline: Instant,
    work: F,
) -> Result<F::Output, tokio::time::error::Elapsed> {
    tokio::pin!(work);
    match timeout_at(deadline, &mut work).await {
        Ok(result) => Ok(result),
        Err(elapsed) => {
            let _ = work.await;
            Err(elapsed)
        }
    }
}
#[derive(Clone, PartialEq, Eq)]
pub enum DemoError {
    Evidence(String),
    RecipeRevisionChanged,
    PeerRejected { code: u64, message: String },
    InvalidJournal,
    StorageUnavailable,
    StorageUncertain,
    InventoryLimit,
    UnsafePath,
    Missing,
    IdentityExhausted,
    LegacyJournalReadOnly,
    JournalTransition,
}
impl fmt::Display for DemoError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Evidence(_) | Self::RecipeRevisionChanged => {
                output.write_str("Demo evidence unavailable")
            }
            Self::PeerRejected { code, .. } => {
                write!(output, "Demo peer rejected request with code {code}")
            }
            _ => output.write_str(self.tool_message()),
        }
    }
}
impl DemoError {
    /// Explicit projection into the requesting client's tool response. Peer
    /// messages can contain private settings and must not enter diagnostics.
    pub fn tool_message(&self) -> &str {
        match self {
            Self::Evidence(message) | Self::PeerRejected { message, .. } => message,
            Self::RecipeRevisionChanged => {
                "OBS recipe evidence changed while reading state; inspect before retrying"
            }
            Self::InvalidJournal => "Invalid recovery journal; manual investigation required",
            Self::StorageUnavailable => "Recovery journal could not be persisted or inspected",
            Self::StorageUncertain => {
                "Recovery journal publication may be visible but durability is unconfirmed; manual investigation required"
            }
            Self::InventoryLimit => "Recovery inventory exceeds limit",
            Self::UnsafePath => "Recovery journal storage path cannot be trusted",
            Self::Missing => "Recovery journal is missing",
            Self::IdentityExhausted => "Recovery journal staging identity exhausted",
            Self::LegacyJournalReadOnly => {
                "Legacy recovery journals require conservative native import before writing"
            }
            Self::JournalTransition => "Recovery journal provenance transition is invalid",
        }
    }
}
impl fmt::Debug for DemoError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Evidence(_) | Self::PeerRejected { .. } => {
                output.write_str("DemoError::Evidence([REDACTED])")
            }
            other => fmt::Display::fmt(other, output),
        }
    }
}
impl std::error::Error for DemoError {}
impl From<ObservationError> for DemoError {
    fn from(error: ObservationError) -> Self {
        match error.request_code() {
            Some(code) => Self::PeerRejected {
                code,
                message: error.message,
            },
            None => Self::Evidence(error.message),
        }
    }
}
impl From<snapshots::SnapshotError> for DemoError {
    fn from(error: snapshots::SnapshotError) -> Self {
        Self::Evidence(error.tool_message().to_owned())
    }
}
impl From<ProvenanceError> for DemoError {
    fn from(_: ProvenanceError) -> Self {
        Self::JournalTransition
    }
}
fn evidence(message: impl Into<String>) -> DemoError {
    DemoError::Evidence(message.into())
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Selection {
    pub scene_collection: String,
    pub scene_name: String,
    pub input_name: String,
}
impl Selection {
    pub fn validate(&self) -> Result<(), DemoError> {
        if [&self.scene_collection, &self.scene_name, &self.input_name]
            .into_iter()
            .all(|value| !value.is_empty())
        {
            Ok(())
        } else {
            Err(evidence("OBS did not report stable object identity"))
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostClass {
    Loopback,
    Other,
    Unqualified,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EvidenceStamp {
    pub generation: u64,
    pub revision: u64,
    pub connected: bool,
    pub cancelled: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DispatchFence {
    pub generation: u64,
    pub revision: u64,
}
fn journal_fence(fence: DispatchFence) -> JournalFence {
    JournalFence {
        generation: fence.generation,
        evidence_revision: fence.revision,
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum FitKey {
    BoundsType,
    BoundsWidth,
    BoundsHeight,
    BoundsAlignment,
    Alignment,
    PositionX,
    PositionY,
    Rotation,
}
const FIT: [FitKey; 8] = [
    FitKey::BoundsType,
    FitKey::BoundsWidth,
    FitKey::BoundsHeight,
    FitKey::BoundsAlignment,
    FitKey::Alignment,
    FitKey::PositionX,
    FitKey::PositionY,
    FitKey::Rotation,
];
impl FitKey {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BoundsType => "boundsType",
            Self::BoundsWidth => "boundsWidth",
            Self::BoundsHeight => "boundsHeight",
            Self::BoundsAlignment => "boundsAlignment",
            Self::Alignment => "alignment",
            Self::PositionX => "positionX",
            Self::PositionY => "positionY",
            Self::Rotation => "rotation",
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "field", rename_all = "lowercase")]
pub enum Field {
    Muted {
        before: bool,
        after: bool,
        #[serde(flatten)]
        extra: Map<String, Value>,
    },
    Tracks {
        before: Map<String, Value>,
        after: Map<String, Value>,
        #[serde(flatten)]
        extra: Map<String, Value>,
    },
    Transform {
        key: FitKey,
        before: Value,
        after: Value,
        #[serde(flatten)]
        extra: Map<String, Value>,
    },
}
impl Field {
    fn before(&self) -> Value {
        match self {
            Self::Muted { before, .. } => json!(before),
            Self::Tracks { before, .. } => json!(before),
            Self::Transform { before, .. } => before.clone(),
        }
    }
    fn after(&self) -> Value {
        match self {
            Self::Muted { after, .. } => json!(after),
            Self::Tracks { after, .. } => json!(after),
            Self::Transform { after, .. } => after.clone(),
        }
    }
    fn valid(&self) -> bool {
        match self {
            Self::Muted { .. } => true,
            Self::Tracks { before, after, .. } => tracks(before) && tracks(after),
            Self::Transform {
                key, before, after, ..
            } => [before, after].into_iter().all(|value| {
                if *key == FitKey::BoundsType {
                    value
                        .as_str()
                        .is_some_and(|value| value.starts_with("OBS_BOUNDS_"))
                } else {
                    value.as_f64().is_some_and(f64::is_finite)
                }
            }),
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct State {
    #[serde(flatten)]
    selection: Selection,
    scene_uuid: String,
    input_uuid: String,
    scene_item_id: f64,
    settings: Map<String, Value>,
    muted: bool,
    tracks: Map<String, Value>,
    transform: Map<String, Value>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Journal {
    version: u8,
    operation_id: String,
    obs_url: String,
    snapshot_id: String,
    state: State,
    changes: Vec<Field>,
    attempted: usize,
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    recovered: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provenance: Option<Provenance>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}
impl Journal {
    pub fn import_unattempted(self) -> Result<Self, DemoError> {
        self.native()
    }
    pub fn record_intent(
        &mut self,
        index: usize,
        recovery: bool,
        fence: JournalFence,
    ) -> Result<(), DemoError> {
        let provenance = self
            .provenance
            .as_mut()
            .ok_or(DemoError::LegacyJournalReadOnly)?;
        if recovery {
            provenance.begin_recovery(index, fence)?
        } else {
            provenance.begin_preparation(index, fence)?
        }
        self.provenance_changed();
        Ok(())
    }
    pub fn record_step(
        &mut self,
        index: usize,
        recovery: bool,
        step: Step,
    ) -> Result<(), DemoError> {
        self.provenance
            .as_mut()
            .ok_or(DemoError::LegacyJournalReadOnly)?
            .advance(index, recovery, step)?;
        self.provenance_changed();
        Ok(())
    }
    pub fn record_observed_before(
        &mut self,
        index: usize,
        fence: JournalFence,
    ) -> Result<(), DemoError> {
        self.provenance
            .as_mut()
            .ok_or(DemoError::LegacyJournalReadOnly)?
            .observed_before(index, fence)?;
        Ok(())
    }
    pub fn complete_recovery(&mut self) -> Result<(), DemoError> {
        self.provenance
            .as_mut()
            .ok_or(DemoError::LegacyJournalReadOnly)?
            .complete()?;
        self.recovered = Some(json!(true));
        Ok(())
    }
    pub fn version(&self) -> u8 {
        self.version
    }
    pub fn provenance(&self) -> Option<&Provenance> {
        self.provenance.as_ref()
    }
    pub fn provenance_summary(&self) -> Value {
        match &self.provenance {
            Some(value) => {
                json!({"version":2,"sourceUncertain":value.uncertain(),"delivery":value.summary()})
            }
            None => {
                json!({"version":1,"sourceUncertain":self.attempted>0&&!self.is_recovered(),"legacyAttempted":self.attempted})
            }
        }
    }
    fn subject(&self) -> Value {
        json!({"operationId":self.operation_id,"snapshotId":self.snapshot_id,"state":self.state,"changes":self.changes,"extensions":self.extra})
    }
    fn native(mut self) -> Result<Self, DemoError> {
        if self.version == 1 {
            if self.attempted > 0 || self.is_recovered() {
                return Err(DemoError::LegacyJournalReadOnly);
            }
            self.version = 2;
            self.recovered = None;
            self.provenance = Some(Provenance::new(
                &self.obs_url,
                &self.subject(),
                self.changes.len(),
            ));
        }
        Ok(self)
    }
    fn provenance_changed(&mut self) {
        self.attempted = self.provenance.as_ref().unwrap().attempted();
    }
    fn uncertain(&self) -> bool {
        self.provenance.as_ref().map_or(
            self.attempted > 0 && !self.is_recovered(),
            Provenance::uncertain,
        )
    }
    fn fully_prepared(&self) -> bool {
        self.provenance
            .as_ref()
            .is_some_and(Provenance::all_preparation_accepted)
    }
    fn check_successor(&self, next: &Self) -> Result<(), DemoError> {
        if self.operation_id != next.operation_id
            || self.obs_url != next.obs_url
            || self.snapshot_id != next.snapshot_id
            || self.subject() != next.subject()
        {
            return Err(DemoError::JournalTransition);
        }
        if self.version == 1 {
            let imported = self.clone().native()?;
            if imported.to_value() != next.to_value() {
                return Err(DemoError::JournalTransition);
            }
            return Ok(());
        }
        if self.version != 2 || next.version != 2 {
            return Err(DemoError::JournalTransition);
        }
        self.provenance
            .as_ref()
            .ok_or(DemoError::InvalidJournal)?
            .successor(next.provenance.as_ref().ok_or(DemoError::InvalidJournal)?)?;
        Ok(())
    }
    fn is_recovered(&self) -> bool {
        self.recovered.as_ref().is_some_and(|value| match value {
            Value::Null => false,
            Value::Bool(value) => *value,
            Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
            Value::String(value) => !value.is_empty(),
            Value::Array(_) | Value::Object(_) => true,
        })
    }
    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }
    pub fn attempted(&self) -> usize {
        self.attempted
    }
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).expect("journal JSON")
    }
    pub fn from_value(mut value: Value) -> Result<Self, DemoError> {
        // JavaScript parses 1 and 1.0 to the same Number. Preserve the owning
        // validator's integer semantics without accepting strings or booleans.
        let fields = value.as_object_mut().ok_or(DemoError::InvalidJournal)?;
        let version = fields.get("version").and_then(Value::as_f64);
        if !matches!(version, Some(1.0 | 2.0)) {
            return Err(DemoError::InvalidJournal);
        }
        let length = fields
            .get("changes")
            .and_then(Value::as_array)
            .ok_or(DemoError::InvalidJournal)?
            .len();
        let attempted = fields
            .get("attempted")
            .and_then(Value::as_f64)
            .filter(|value| {
                value.is_finite()
                    && value.fract() == 0.0
                    && *value >= 0.0
                    && *value <= length as f64
            })
            .ok_or(DemoError::InvalidJournal)? as usize;
        fields.insert("version".into(), json!(version.unwrap() as u8));
        fields.insert("attempted".into(), json!(attempted));
        let journal: Self = serde_json::from_value(value).map_err(|_| DemoError::InvalidJournal)?;
        if !matches!(journal.version, 1 | 2)
            || !valid_id(&journal.operation_id)
            || journal.attempted > journal.changes.len()
            || journal.state.scene_item_id.fract() != 0.0
            || journal.changes.iter().any(|field| !field.valid())
        {
            return Err(DemoError::InvalidJournal);
        }
        if journal.version == 1 {
            if journal.provenance.is_some() {
                return Err(DemoError::InvalidJournal);
            }
        } else {
            let provenance = journal
                .provenance
                .as_ref()
                .ok_or(DemoError::InvalidJournal)?;
            provenance
                .validate(&journal.obs_url, &journal.subject(), journal.changes.len())
                .map_err(|_| DemoError::InvalidJournal)?;
            if journal.attempted != provenance.attempted()
                || journal
                    .recovered
                    .as_ref()
                    .is_some_and(|value| value != &json!(true))
                || provenance.completed != journal.is_recovered()
            {
                return Err(DemoError::InvalidJournal);
            }
        }
        Ok(journal)
    }
}
fn present_value<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}
impl fmt::Debug for Journal {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("Journal")
            .field("operation_id", &self.operation_id)
            .field("attempted", &self.attempted)
            .field("changes", &self.changes.len())
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for Field {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(match self {
            Self::Muted { .. } => "Field::Muted(..)",
            Self::Tracks { .. } => "Field::Tracks(..)",
            Self::Transform { .. } => "Field::Transform(..)",
        })
    }
}
pub trait JournalStore {
    /// A poison is sticky for this owner. `list`, `load`, and `persist` must
    /// refuse poisoned evidence. V2 durable provenance remains conservative on
    /// restart even when this local uncertainty quarantine is lost.
    fn poison(&mut self, operation_id: &str);
    fn list(&mut self) -> Result<Vec<Journal>, DemoError>;
    fn load(&mut self, operation_id: &str) -> Result<Journal, DemoError>;
    fn persist(&mut self, journal: &Journal) -> Result<(), DemoError>;
}
pub fn recovery_summary<J: JournalStore>(store: &mut J, endpoint: &str) -> Value {
    match store.list() {
        Ok(journals) => {
            json!({"status":"observed","operations":journals.into_iter().filter(|journal|!journal.is_recovered()&&journal.obs_url==endpoint).map(|journal|{let mut value=json!({"operationId":journal.operation_id,"sceneCollection":journal.state.selection.scene_collection,"sceneName":journal.state.selection.scene_name,"inputName":journal.state.selection.input_name,"attempted":journal.attempted});if journal.version==2{value["provenance"]=journal.provenance_summary();}value}).collect::<Vec<_>>()})
        }
        Err(DemoError::InventoryLimit) => {
            json!({"status":"unknown","reason":"Recovery inventory exceeds limit"})
        }
        Err(_) => {
            json!({"status":"unknown","reason":"Recovery journals could not be inspected; inspect obs-recover-demo"})
        }
    }
}
/// A fresh, bounded, metadata-only reader. This neither creates directories nor
/// lends writer methods. Concurrent publication can yield unknown; no atomic
/// cross-operation inventory or restoration permission is claimed.
pub fn inspect_recovery_summary(root: &Path, capacity: usize, endpoint: &str) -> Value {
    match JournalFile::new(root, capacity) {
        Ok(mut reader) => recovery_summary(&mut reader, endpoint),
        Err(_) => {
            json!({"status":"unknown","reason":"Recovery journals could not be inspected; inspect obs-recover-demo"})
        }
    }
}

/// Local provenance admission, before binding an adapter that may inspect OBS.
/// The live state cannot upgrade uncertain dispatch into restoration authority.
pub fn local_recovery_refusal(
    journal: &Journal,
    endpoint: &str,
) -> Result<Option<Value>, DemoError> {
    if journal.obs_url != endpoint {
        return Err(evidence("Journal belongs to a different OBS URL"));
    }
    if !journal.is_recovered() && journal.uncertain() {
        return Ok(Some(result(
            "unknown",
            json!({"operationId":journal.operation_id(),"applied":false,"issues":["Preparation or recovery dispatch is uncertain; inspect current state and provenance; automatic recovery is refused"],"provenance":journal.provenance_summary()}),
            true,
        )));
    }
    Ok(None)
}

#[derive(Clone)]
pub enum DemoMutation {
    Mute {
        input_name: String,
        muted: bool,
    },
    Tracks {
        input_name: String,
        tracks: Map<String, Value>,
    },
    Fit {
        scene_name: String,
        item_id: f64,
        key: FitKey,
        value: Value,
    },
}
impl fmt::Debug for DemoMutation {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(match self {
            Self::Mute { .. } => "DemoMutation::Mute(..)",
            Self::Tracks { .. } => "DemoMutation::Tracks(..)",
            Self::Fit { .. } => "DemoMutation::Fit(..)",
        })
    }
}
impl DemoMutation {
    pub fn request(&self) -> Value {
        match self {
            Self::Mute { input_name, muted } => {
                json!({"requestType":"SetInputMute","requestData":{"inputName":input_name,"inputMuted":muted}})
            }
            Self::Tracks { input_name, tracks } => {
                json!({"requestType":"SetInputAudioTracks","requestData":{"inputName":input_name,"inputAudioTracks":tracks}})
            }
            Self::Fit {
                scene_name,
                item_id,
                key,
                value,
            } => {
                json!({"requestType":"SetSceneItemTransform","requestData":{"sceneName":scene_name,"sceneItemId":item_id,"sceneItemTransform":{key.as_str():value}}})
            }
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    NotDispatched,
    Rejected,
    Unknown,
}
pub struct MutationFailure {
    pub delivery: Delivery,
    pub message: String,
}
impl fmt::Debug for MutationFailure {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("MutationFailure")
            .field("delivery", &self.delivery)
            .finish_non_exhaustive()
    }
}
pub type MutationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), MutationFailure>> + Send + 'a>>;
pub trait DemoAdapter: SnapshotReader {
    /// Reads/batches must settle their observed-event cursor before returning.
    /// The revision profile must cover this recipe's identity/settings/owned
    /// fields; sharing broader unrelated invalidations is a stricter projection.
    fn evidence(&self) -> EvidenceStamp;
    /// The concrete socket owner must compare generation AND recipe-evidence
    /// revision immediately before send, with no await between guard and send.
    /// It must not replay effects, and must settle its own event cursor before
    /// returning acceptance. Dropping a caller does not authorize detached work.
    fn mutate<'a>(
        &'a mut self,
        mutation: DemoMutation,
        fence: DispatchFence,
        deadline: Instant,
    ) -> MutationFuture<'a>;
}
/// Snapshot helpers retain their batch API; this owner supplies the operation's
/// monotonic bound. Deadline expiry retains and drains admitted work. Production
/// adapters must bound cleanup, retire ambiguity, and never detach or replay it.
struct BoundedReads<'a, A> {
    adapter: &'a mut A,
    deadline: Instant,
    generation: u64,
}
impl<A: DemoAdapter> BoundedReads<'_, A> {
    fn check(&self) -> Result<(), ObservationError> {
        let state = self.adapter.evidence();
        if state.connected && !state.cancelled && state.generation == self.generation {
            Ok(())
        } else {
            Err(ObservationError::new(
                "demo_invalidated",
                "OBS connection or recipe evidence changed; inspect state before retrying",
            ))
        }
    }
}
impl<A: DemoAdapter> ReadAdapter for BoundedReads<'_, A> {
    fn supports(&self, request: ReadRequest) -> Option<bool> {
        self.adapter.supports(request)
    }
    fn read<'a>(
        &'a mut self,
        request: ReadRequest,
        data: Value,
        deadline: Instant,
    ) -> ReadFuture<'a> {
        Box::pin(async move {
            self.check()?;
            let result = retained_deadline(
                deadline.min(self.deadline),
                self.adapter
                    .read(request, data, deadline.min(self.deadline)),
            )
            .await
            .map_err(|_| {
                ObservationError::new(
                    "demo_deadline",
                    "Restricted demo deadline elapsed; inspect state before retrying",
                )
            })??;
            self.check()?;
            Ok(result)
        })
    }
}
impl<A: DemoAdapter> SnapshotReader for BoundedReads<'_, A> {
    fn batch<'a>(
        &'a mut self,
        calls: Vec<snapshots::ReadCall>,
    ) -> snapshots::SnapshotFuture<'a, Vec<snapshots::BatchReadResult>> {
        Box::pin(async move {
            self.check()?;
            let result = retained_deadline(self.deadline, self.adapter.batch(calls))
                .await
                .map_err(|_| {
                    snapshots::SnapshotError::Read(
                        "Restricted demo deadline elapsed; inspect state before retrying".into(),
                    )
                })??;
            self.check()?;
            Ok(result)
        })
    }
}
#[derive(Clone, Default)]
pub struct DemoAdmission(Arc<AtomicBool>);
pub struct DemoPermit(Arc<AtomicBool>);
impl DemoAdmission {
    pub fn try_enter(&self) -> Option<DemoPermit> {
        self.0
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| DemoPermit(self.0.clone()))
    }
}
impl Drop for DemoPermit {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
pub struct DemoContext {
    pub endpoint: String,
    pub host: HostClass,
    pub generation: u64,
    pub deadline: Instant,
    pub admission: DemoAdmission,
}
pub struct PrepareMetadata {
    pub operation_id: String,
    pub snapshot: CaptureMetadata,
}
pub struct Demo<'a, A: DemoAdapter, S: SnapshotWriter, J: JournalStore> {
    adapter: &'a mut A,
    snapshots: &'a mut S,
    journals: &'a mut J,
    context: DemoContext,
}
impl<'a, A: DemoAdapter, S: SnapshotWriter, J: JournalStore> Demo<'a, A, S, J> {
    pub fn new(
        adapter: &'a mut A,
        snapshots: &'a mut S,
        journals: &'a mut J,
        context: DemoContext,
    ) -> Self {
        Self {
            adapter,
            snapshots,
            journals,
            context,
        }
    }
    fn check(&self) -> Result<EvidenceStamp, DemoError> {
        let stamp = self.adapter.evidence();
        if !stamp.connected || stamp.cancelled || stamp.generation != self.context.generation {
            return Err(evidence(
                "OBS connection or recipe evidence changed; inspect state before retrying",
            ));
        }
        if Instant::now() >= self.context.deadline {
            return Err(evidence(
                "Restricted demo deadline elapsed; inspect state before retrying",
            ));
        }
        Ok(stamp)
    }
    fn persist(&mut self, journal: &Journal) -> Result<(), DemoError> {
        let result = self.journals.persist(journal);
        if matches!(result, Err(DemoError::StorageUncertain)) {
            self.journals.poison(&journal.operation_id);
        }
        result
    }
    async fn request(
        &mut self,
        request: ReadRequest,
        data: Value,
    ) -> Result<Map<String, Value>, DemoError> {
        self.check()?;
        let reply = retained_deadline(
            self.context.deadline,
            self.adapter.read(request, data, self.context.deadline),
        )
        .await
        .map_err(|_| {
            evidence("Restricted demo deadline elapsed; inspect state before retrying")
        })??;
        self.check()?;
        reply
            .as_object()
            .cloned()
            .ok_or_else(|| evidence("OBS returned incomplete state"))
    }
    async fn read_state(
        &mut self,
        selection: &Selection,
    ) -> Result<(State, Map<String, Value>, DispatchFence), DemoError> {
        match self.context.host {
            HostClass::Loopback => {}
            HostClass::Other => return Err(evidence("V1 requires local OBS on loopback")),
            HostClass::Unqualified => {
                return Err(evidence("OBS URL host classification is unavailable"));
            }
        }
        let initial = self.check()?;
        if self
            .request(ReadRequest::SceneCollectionList, Value::Null)
            .await?
            .get("currentSceneCollectionName")
            != Some(&json!(selection.scene_collection))
        {
            return Err(evidence("Selected scene collection is not current"));
        }
        for request in [
            ReadRequest::StreamStatus,
            ReadRequest::RecordStatus,
            ReadRequest::VirtualCamStatus,
            ReadRequest::ReplayBufferStatus,
        ] {
            let status = match self.request(request, Value::Null).await {
                Ok(status) => status,
                Err(DemoError::PeerRejected { code: 604, .. })
                    if matches!(
                        request,
                        ReadRequest::VirtualCamStatus | ReadRequest::ReplayBufferStatus
                    ) =>
                {
                    self.check()?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if status.get("outputActive") != Some(&json!(false))
                || status.get("outputReconnecting") == Some(&json!(true))
            {
                return Err(evidence(format!(
                    "Preparation and recovery require inactive outputs: {} was active or unavailable",
                    request.as_str()
                )));
            }
        }
        let outputs = self.request(ReadRequest::OutputList, Value::Null).await?;
        for output in list(outputs.get("outputs"))? {
            let name = string(output.get("outputName"))?;
            let status = self
                .request(ReadRequest::OutputStatus, json!({"outputName":name}))
                .await?;
            if status.get("outputActive") != Some(&json!(false))
                || status.get("outputReconnecting") != Some(&json!(false))
            {
                return Err(evidence(format!(
                    "Preparation and recovery require inactive outputs: {name} was active or unavailable"
                )));
            }
        }
        let scene_reply = self.request(ReadRequest::SceneList, Value::Null).await?;
        if scene_reply.get("currentProgramSceneName") != Some(&json!(selection.scene_name)) {
            return Err(evidence(
                "Selected scene must already be the current program scene",
            ));
        }
        let scenes = list(scene_reply.get("scenes"))?;
        let selected_scenes = scenes
            .iter()
            .filter(|scene| scene.get("sceneName") == Some(&json!(selection.scene_name)))
            .collect::<Vec<_>>();
        if selected_scenes.len() != 1 {
            return Err(evidence("Scene selection is ambiguous or missing"));
        }
        let scene_uuid = string(selected_scenes[0].get("sceneUuid"))?.to_owned();
        let input_reply = self.request(ReadRequest::InputList, Value::Null).await?;
        let inputs = list(input_reply.get("inputs"))?;
        let selected_inputs = inputs
            .iter()
            .filter(|input| input.get("inputName") == Some(&json!(selection.input_name)))
            .collect::<Vec<_>>();
        if selected_inputs.len() != 1
            || !selected_inputs[0]
                .get("inputKind")
                .and_then(Value::as_str)
                .is_some_and(capture_backend::window_capture)
        {
            return Err(evidence(
                "Select one existing macOS, XComposite or PipeWire window capture input",
            ));
        }
        let kind = selected_inputs[0]
            .get("inputKind")
            .and_then(Value::as_str)
            .unwrap()
            .to_owned();
        let video_only = capture_backend::video_only(&kind);
        let input_uuid = string(selected_inputs[0].get("inputUuid"))?.to_owned();
        let scene_names = scenes
            .iter()
            .map(|scene| string(scene.get("sceneName")).map(str::to_owned))
            .collect::<Result<HashSet<_>, _>>()?;
        let mut selected = None;
        let mut occurrences = 0;
        for scene in scenes {
            let name = scene.get("sceneName").cloned().unwrap_or(Value::Null);
            let members = self
                .request(ReadRequest::SceneItemList, json!({"sceneName":name}))
                .await?;
            for item in list(members.get("sceneItems"))? {
                let item = normalize_scene_item(Value::Object(item));
                let item = item.as_object().unwrap();
                if !item.get("isGroup").is_some_and(Value::is_boolean)
                    || !item.get("sourceName").is_some_and(Value::is_string)
                {
                    return Err(evidence(
                        "Incomplete scene membership; source exclusivity unavailable",
                    ));
                }
                if item["isGroup"] == true
                    || scene_names.contains(item["sourceName"].as_str().unwrap())
                {
                    return Err(evidence(
                        "V1 requires a collection without nested scenes or groups to prove source exclusivity",
                    ));
                }
                if item.get("sourceName") == Some(&json!(selection.input_name))
                    || item.get("sourceUuid") == Some(&json!(input_uuid))
                {
                    occurrences += 1;
                    if name == selection.scene_name {
                        selected = Some(item.clone());
                    }
                }
            }
        }
        let Some(selected) = selected.filter(|_| occurrences == 1) else {
            return Err(evidence(
                "Capture must appear exactly once and may not be shared with another scene",
            ));
        };
        let item_id = selected
            .get("sceneItemId")
            .and_then(Value::as_f64)
            .filter(|value| value.fract() == 0.0);
        if selected.get("sourceUuid") != Some(&json!(input_uuid))
            || selected.get("sourceName") != Some(&json!(selection.input_name))
            || item_id.is_none()
        {
            return Err(evidence("Scene item/source identity is incomplete"));
        }
        if selected.get("sceneItemEnabled") != Some(&json!(true)) {
            return Err(evidence("Capture must already be enabled"));
        }
        let transform = selected
            .get("sceneItemTransform")
            .and_then(Value::as_object)
            .cloned()
            .ok_or_else(|| evidence("OBS returned incomplete state"))?;
        for key in FIT {
            if !transform.contains_key(key.as_str())
                || key != FitKey::BoundsType
                    && !transform[key.as_str()].as_f64().is_some_and(f64::is_finite)
            {
                return Err(evidence(format!(
                    "Incomplete snapshot: transform {} unavailable",
                    key.as_str()
                )));
            }
        }
        if !transform["boundsType"]
            .as_str()
            .is_some_and(|value| value.starts_with("OBS_BOUNDS_"))
        {
            return Err(evidence(
                "Incomplete snapshot: transform boundsType unavailable",
            ));
        }
        if ["sourceWidth", "sourceHeight"].iter().any(|key| {
            !transform
                .get(*key)
                .and_then(Value::as_f64)
                .is_some_and(|value| value > 0.0)
        }) {
            return Err(evidence("Capture has no reported source dimensions"));
        }
        let required = if video_only {
            vec![RequiredInputField::InputSettings]
        } else {
            vec![
                RequiredInputField::InputSettings,
                RequiredInputField::Muted,
                RequiredInputField::AudioTracks,
            ]
        };
        let mut input = snapshots::read_inputs(
            &mut BoundedReads {
                adapter: self.adapter,
                deadline: self.context.deadline,
                generation: self.context.generation,
            },
            std::slice::from_ref(&selection.input_name),
            &required,
        )
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| evidence("OBS returned incomplete state"))?;
        if input.input_kind != kind
            || !capture_backend::configured_window(&input.input_kind, &input.input_settings)
        {
            return Err(evidence(
                "Select a preconfigured window capture; V1 never retargets it",
            ));
        }
        if video_only {
            input.muted = Some(true);
            input.audio_tracks = Some(silent_tracks());
        }
        let tracks = input
            .audio_tracks
            .filter(tracks)
            .ok_or_else(|| evidence("Incomplete snapshot: all six audio tracks are required"))?;
        let video = self
            .request(ReadRequest::VideoSettings, Value::Null)
            .await?;
        if ["baseWidth", "baseHeight"].iter().any(|key| {
            !video
                .get(*key)
                .and_then(Value::as_f64)
                .is_some_and(|value| value > 0.0)
        }) {
            return Err(evidence("Canvas dimensions unavailable"));
        }
        if self
            .request(ReadRequest::SceneCollectionList, Value::Null)
            .await?
            .get("currentSceneCollectionName")
            != Some(&json!(selection.scene_collection))
        {
            return Err(evidence("Collection changed while reading state"));
        }
        let final_stamp = self.check()?;
        if final_stamp.revision != initial.revision {
            return Err(DemoError::RecipeRevisionChanged);
        }
        let desired=json!({"boundsType":"OBS_BOUNDS_SCALE_INNER","boundsWidth":video["baseWidth"],"boundsHeight":video["baseHeight"],"boundsAlignment":0,"alignment":5,"positionX":0,"positionY":0,"rotation":0}).as_object().unwrap().clone();
        Ok((
            State {
                selection: selection.clone(),
                scene_uuid,
                input_uuid,
                scene_item_id: item_id.unwrap(),
                settings: input.input_settings,
                muted: input.muted.unwrap_or(true),
                tracks,
                transform,
                extra: Map::new(),
            },
            desired,
            DispatchFence {
                generation: final_stamp.generation,
                revision: final_stamp.revision,
            },
        ))
    }
    /// Only a newly persisted acceptance permits one fresh read sample after a
    /// late notification. This never retries an effect or relaxes its expected
    /// identity/field comparisons. Consume the permission on the first sample,
    /// whether that sample succeeds or reports another failure.
    async fn read_after_accepted(
        &mut self,
        selection: &Selection,
        expected: &State,
        accepted: &mut bool,
    ) -> Result<(State, Map<String, Value>, DispatchFence), DemoError> {
        let may_resample = std::mem::take(accepted);
        let sample = self.read_state(selection).await;
        if may_resample && matches!(sample, Err(DemoError::RecipeRevisionChanged)) {
            self.check()?;
            let fresh = self.read_state(selection).await?;
            if !same_identity(expected, &fresh.0) || !unchanged(&fresh.0, expected) {
                return Err(evidence(
                    "State conflict while resampling accepted mutation; writes stopped",
                ));
            }
            Ok(fresh)
        } else {
            sample
        }
    }

    async fn write(
        &mut self,
        state: &State,
        field: &Field,
        after: bool,
        fence: DispatchFence,
    ) -> Result<(), MutationFailure> {
        let value = if after { field.after() } else { field.before() };
        let mutation = match field {
            Field::Muted { .. } => DemoMutation::Mute {
                input_name: state.selection.input_name.clone(),
                muted: value.as_bool().unwrap(),
            },
            Field::Tracks { .. } => DemoMutation::Tracks {
                input_name: state.selection.input_name.clone(),
                tracks: value.as_object().unwrap().clone(),
            },
            Field::Transform { key, .. } => DemoMutation::Fit {
                scene_name: state.selection.scene_name.clone(),
                item_id: state.scene_item_id,
                key: *key,
                value,
            },
        };
        retained_deadline(
            self.context.deadline,
            self.adapter.mutate(mutation, fence, self.context.deadline),
        )
        .await
        .unwrap_or_else(|_| {
            Err(MutationFailure {
                delivery: Delivery::Unknown,
                message: "Restricted demo deadline elapsed; inspect state before retrying".into(),
            })
        })
    }
    pub async fn prepare(
        &mut self,
        selection: Selection,
        apply: bool,
        metadata: PrepareMetadata,
    ) -> Value {
        let Some(_permit) = self.context.admission.try_enter() else {
            return result(
                "conflict",
                json!({"applied":false,"issues":["Another restricted demo operation is running"]}),
                true,
            );
        };
        let mut journal = None;
        let mut attempted = false;
        let output = self
            .prepare_inner(selection, apply, metadata, &mut journal, &mut attempted)
            .await;
        match output {
            Ok(value) => value,
            Err(error) => {
                let mut details = json!({"applied":attempted,"issues":[error.tool_message()],"recovery":if journal.is_some(){"Inspect obs-recover-demo; no blind retry"}else{"Nothing changed"}});
                if let Some(journal) = journal {
                    details["operationId"] = json!(journal.operation_id);
                    details["snapshotId"] = json!(journal.snapshot_id);
                }
                result(if attempted { "partial" } else { "failed" }, details, true)
            }
        }
    }
    async fn prepare_inner(
        &mut self,
        selection: Selection,
        apply: bool,
        metadata: PrepareMetadata,
        journal: &mut Option<Journal>,
        attempted: &mut bool,
    ) -> Result<Value, DemoError> {
        selection.validate()?;
        let (state, desired, _) = self.read_state(&selection).await?;
        let pending = self.journals.list()?.into_iter().find(|journal| {
            !journal.is_recovered()
                && journal.obs_url == self.context.endpoint
                && journal.state.selection.scene_collection == selection.scene_collection
                && journal.state.selection.input_name == selection.input_name
        });
        if let Some(existing) = pending {
            if existing.fully_prepared()
                && same_identity(&state, &existing.state)
                && matches(&state, &existing.changes, true)
                && prepared(&state, &desired)
            {
                return Ok(result(
                    "already-prepared",
                    json!({"operationId":existing.operation_id,"snapshotId":existing.snapshot_id,"applied":false}),
                    false,
                ));
            }
            return Ok(result(
                "conflict",
                json!({"operationId":existing.operation_id,"applied":false,"issues":["An unresolved preparation exists; inspect/recover it before preparing again"]}),
                true,
            ));
        }
        let changes = changes(&state, &desired);
        if !apply || changes.is_empty() {
            return Ok(result(
                if changes.is_empty() {
                    "already-prepared"
                } else {
                    "planned"
                },
                json!({"applied":false,"changes":changes}),
                false,
            ));
        }
        if !valid_id(&metadata.operation_id) {
            return Err(evidence("Invalid operationId"));
        }
        if metadata.snapshot.obs_url != self.context.endpoint {
            return Err(evidence("Snapshot metadata belongs to a different OBS URL"));
        }
        let settings = self
            .request(
                ReadRequest::InputSettings,
                json!({"inputName":selection.input_name}),
            )
            .await?;
        let video_only = settings
            .get("inputKind")
            .and_then(Value::as_str)
            .is_some_and(capture_backend::video_only);
        let options = CaptureOptions {
            scenes: Some(vec![selection.scene_name.clone()]),
            inputs: Some(vec![selection.input_name.clone()]),
            label: Some("before restricted demo".to_owned()),
            auto: true,
            scene_collection: Some(selection.scene_collection.clone()),
            required_input_fields: if video_only {
                vec![RequiredInputField::InputSettings]
            } else {
                vec![
                    RequiredInputField::InputSettings,
                    RequiredInputField::Muted,
                    RequiredInputField::AudioTracks,
                ]
            },
        };
        let snapshot = snapshots::take_snapshot(
            &mut BoundedReads {
                adapter: self.adapter,
                deadline: self.context.deadline,
                generation: self.context.generation,
            },
            self.snapshots,
            options,
            metadata.snapshot,
            self.context.deadline,
        )
        .await?;
        let input = snapshot.inputs.first();
        let item = snapshot.items.iter().find(|item| {
            item.scene_item_id == state.scene_item_id && item.source_name == selection.input_name
        });
        if input.is_none()
            || item.is_none()
            || !snapshots::same(
                &json!(input.unwrap().input_settings),
                &json!(state.settings),
            )
            || !video_only
                && (input.unwrap().muted != Some(state.muted)
                    || input.unwrap().audio_tracks.as_ref().is_none_or(|tracks| {
                        !snapshots::same(&json!(tracks), &json!(state.tracks))
                    }))
            || FIT.iter().any(|key| {
                !same_value(
                    *key,
                    item.unwrap().transform.get(key.as_str()),
                    state.transform.get(key.as_str()),
                )
            })
        {
            return Err(evidence("State changed during snapshot; nothing changed"));
        }
        *journal = Some(Journal {
            version: 2,
            operation_id: metadata.operation_id,
            obs_url: self.context.endpoint.clone(),
            snapshot_id: snapshot.id,
            state: state.clone(),
            changes: changes.clone(),
            attempted: 0,
            recovered: None,
            provenance: None,
            extra: Map::new(),
        });
        let saved = journal.as_mut().unwrap();
        saved.provenance = Some(Provenance::new(
            &saved.obs_url,
            &saved.subject(),
            saved.changes.len(),
        ));
        self.persist(journal.as_ref().unwrap())?;
        let mut expected = state.clone();
        let mut accepted = false;
        for (index, change) in changes.iter().enumerate() {
            let (now, _, fence) = self
                .read_after_accepted(&selection, &expected, &mut accepted)
                .await?;
            if !same_identity(&state, &now) || !unchanged(&now, &expected) {
                return Err(evidence(
                    "State conflict during preparation; automatic writes stopped",
                ));
            }
            let saved = journal.as_mut().unwrap();
            saved
                .provenance
                .as_mut()
                .unwrap()
                .begin_preparation(index, journal_fence(fence))?;
            saved.provenance_changed();
            self.persist(saved)?;
            saved
                .provenance
                .as_mut()
                .unwrap()
                .advance(index, false, Step::PossibleDispatch)?;
            saved.provenance_changed();
            self.persist(saved)?;
            *attempted = true;
            match self.write(&state, change, true, fence).await {
                Ok(()) => {
                    saved
                        .provenance
                        .as_mut()
                        .unwrap()
                        .advance(index, false, Step::Accepted)?;
                    saved.provenance_changed();
                    self.persist(saved)?;
                    advance(&mut expected, change, true);
                    accepted = true;
                }
                Err(error) => {
                    let terminal = match error.delivery {
                        Delivery::NotDispatched => Step::NotDispatched,
                        Delivery::Rejected => Step::Rejected,
                        Delivery::Unknown => Step::Unknown,
                    };
                    saved
                        .provenance
                        .as_mut()
                        .unwrap()
                        .advance(index, false, terminal)?;
                    saved.provenance_changed();
                    self.persist(saved)?;
                    return Err(evidence(error.message));
                }
            }
        }
        let (after, _, _) = self
            .read_after_accepted(&selection, &expected, &mut accepted)
            .await?;
        if !same_identity(&state, &after)
            || !matches(&after, &changes, true)
            || !prepared(&after, &desired)
        {
            return Err(evidence("Preparation postconditions did not match"));
        }
        let saved = journal.as_ref().unwrap();
        Ok(result(
            "prepared",
            json!({"operationId":saved.operation_id,"snapshotId":saved.snapshot_id,"applied":true,"changes":changes}),
            false,
        ))
    }
    pub async fn recover(&mut self, operation_id: &str, apply: bool) -> Value {
        let Some(_permit) = self.context.admission.try_enter() else {
            return result(
                "conflict",
                json!({"applied":false,"issues":["Another restricted demo operation is running"]}),
                true,
            );
        };
        let mut attempted = false;
        match self
            .recover_inner(operation_id, apply, &mut attempted)
            .await
        {
            Ok(value) => value,
            Err(error) => result(
                if attempted { "partial" } else { "failed" },
                json!({"operationId":operation_id,"applied":attempted,"issues":[error.tool_message()],"recovery":"Inspect current state before retrying"}),
                true,
            ),
        }
    }
    async fn recover_inner(
        &mut self,
        operation_id: &str,
        apply: bool,
        attempted: &mut bool,
    ) -> Result<Value, DemoError> {
        let mut journal = self.journals.load(operation_id)?;
        if let Some(refusal) = local_recovery_refusal(&journal, &self.context.endpoint)? {
            return Ok(refusal);
        }
        let (state, _, initial_fence) = self.read_state(&journal.state.selection).await?;
        if !same_identity(&journal.state, &state) {
            return Ok(result(
                "conflict",
                json!({"operationId":operation_id,"applied":false,"issues":["Collection/object identity or capture settings changed"]}),
                true,
            ));
        }
        if journal.is_recovered() {
            if !matches(&state, &journal.changes, false) {
                return Ok(result(
                    "conflict",
                    json!({"operationId":operation_id,"applied":false,"issues":["State changed after recovery; journal will not be reapplied"]}),
                    true,
                ));
            }
            return Ok(result(
                "already-recovered",
                json!({"operationId":operation_id,"applied":false}),
                false,
            ));
        }
        let conflicts = journal
            .changes
            .iter()
            .enumerate()
            .filter(|(index, change)| {
                let before = equal(change, &value(&state, change), &change.before());
                let field = journal
                    .provenance
                    .as_ref()
                    .map(|provenance| &provenance.fields[*index]);
                if field.is_some_and(|field| field.restored()) {
                    !before
                } else {
                    !before
                        && (!field.is_some_and(|field| field.owns_restore())
                            || !equal(change, &value(&state, change), &change.after()))
                }
            })
            .map(|(_, change)| change.clone())
            .collect::<Vec<_>>();
        if !conflicts.is_empty() {
            return Ok(result(
                "conflict",
                json!({"operationId":operation_id,"applied":false,"conflicts":conflicts}),
                true,
            ));
        }
        let remaining = journal
            .changes
            .iter()
            .enumerate()
            .filter(|(index, change)| {
                journal.provenance.as_ref().is_some_and(|provenance| {
                    provenance.fields[*index].owns_restore()
                        && !provenance.fields[*index].restored()
                }) && !equal(change, &value(&state, change), &change.before())
            })
            .map(|(_, change)| change.clone())
            .collect::<Vec<_>>();
        if !apply {
            return Ok(result(
                "planned",
                json!({"operationId":operation_id,"applied":false,"changes":remaining}),
                false,
            ));
        }
        if journal.version == 1 {
            journal = journal.native()?;
            self.persist(&journal)?;
        }
        let mut expected = state.clone();
        let mut accepted = false;
        for index in 0..journal.changes.len() {
            if journal.provenance.as_ref().unwrap().fields[index].restored() {
                continue;
            }
            let change = journal.changes[index].clone();
            if journal.provenance.as_ref().unwrap().fields[index].recovery_status()
                == Some(Step::Intent)
            {
                journal
                    .provenance
                    .as_mut()
                    .unwrap()
                    .advance(index, true, Step::NotDispatched)?;
                self.persist(&journal)?;
            }
            if equal(&change, &value(&state, &change), &change.before()) {
                journal
                    .provenance
                    .as_mut()
                    .unwrap()
                    .observed_before(index, journal_fence(initial_fence))?;
                self.persist(&journal)?;
                continue;
            }
            let (now, _, fence) = self
                .read_after_accepted(&journal.state.selection, &expected, &mut accepted)
                .await?;
            if !same_identity(&journal.state, &now)
                || !journal
                    .changes
                    .iter()
                    .all(|change| equal(change, &value(&now, change), &value(&expected, change)))
            {
                return Err(evidence("State conflict during recovery; writes stopped"));
            }
            journal
                .provenance
                .as_mut()
                .unwrap()
                .begin_recovery(index, journal_fence(fence))?;
            self.persist(&journal)?;
            journal
                .provenance
                .as_mut()
                .unwrap()
                .advance(index, true, Step::PossibleDispatch)?;
            self.persist(&journal)?;
            *attempted = true;
            match self.write(&journal.state, &change, false, fence).await {
                Ok(()) => {
                    journal
                        .provenance
                        .as_mut()
                        .unwrap()
                        .advance(index, true, Step::Accepted)?;
                    self.persist(&journal)?;
                    advance(&mut expected, &change, false);
                    accepted = true;
                }
                Err(error) => {
                    let terminal = match error.delivery {
                        Delivery::NotDispatched => Step::NotDispatched,
                        Delivery::Rejected => Step::Rejected,
                        Delivery::Unknown => Step::Unknown,
                    };
                    journal
                        .provenance
                        .as_mut()
                        .unwrap()
                        .advance(index, true, terminal)?;
                    self.persist(&journal)?;
                    return Err(evidence(error.message));
                }
            }
        }
        let (after, _, _) = self
            .read_after_accepted(&journal.state.selection, &expected, &mut accepted)
            .await?;
        if !same_identity(&journal.state, &after) || !matches(&after, &journal.changes, false) {
            return Err(evidence("Recovery postconditions did not match"));
        }
        journal.provenance.as_mut().unwrap().complete()?;
        journal.recovered = Some(json!(true));
        self.persist(&journal)?;
        Ok(result(
            "recovered",
            json!({"operationId":operation_id,"applied":!remaining.is_empty(),"changes":remaining}),
            false,
        ))
    }
}

fn result(status: &str, details: Value, is_error: bool) -> Value {
    let mut report = json!({"status":status});
    report
        .as_object_mut()
        .unwrap()
        .extend(details.as_object().unwrap().clone());
    let mut result = json!({"content":[{"type":"text","text":serde_json::to_string(&report).expect("demo report")}],"structuredContent":report});
    if is_error {
        result["isError"] = json!(true);
    }
    result
}
fn list(value: Option<&Value>) -> Result<Vec<Map<String, Value>>, DemoError> {
    value
        .and_then(Value::as_array)
        .filter(|values| values.iter().all(Value::is_object))
        .map(|values| {
            values
                .iter()
                .map(|value| value.as_object().unwrap().clone())
                .collect()
        })
        .ok_or_else(|| evidence("OBS returned an incomplete list"))
}
fn string(value: Option<&Value>) -> Result<&str, DemoError> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| evidence("OBS did not report stable object identity"))
}
fn tracks(value: &Map<String, Value>) -> bool {
    value.len() == 6
        && (1..=6).all(|index| value.get(&index.to_string()).is_some_and(Value::is_boolean))
}
fn silent_tracks() -> Map<String, Value> {
    (1..=6)
        .map(|index| (index.to_string(), json!(false)))
        .collect()
}
fn same_value(key: FitKey, a: Option<&Value>, b: Option<&Value>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => crate::scene_plan::same_transform_value(key.as_str(), a, b),
        (None, None) => true,
        _ => false,
    }
}
fn same_identity(a: &State, b: &State) -> bool {
    a.scene_uuid == b.scene_uuid
        && a.input_uuid == b.input_uuid
        && a.scene_item_id == b.scene_item_id
        && snapshots::same(&json!(a.settings), &json!(b.settings))
}
fn prepared(state: &State, desired: &Map<String, Value>) -> bool {
    state.muted
        && state.tracks.values().all(|value| value == false)
        && FIT.iter().all(|key| {
            same_value(
                *key,
                state.transform.get(key.as_str()),
                desired.get(key.as_str()),
            )
        })
}
fn unchanged(now: &State, expected: &State) -> bool {
    now.muted == expected.muted
        && snapshots::same(&json!(now.tracks), &json!(expected.tracks))
        && FIT.iter().all(|key| {
            same_value(
                *key,
                now.transform.get(key.as_str()),
                expected.transform.get(key.as_str()),
            )
        })
}
fn value(state: &State, field: &Field) -> Value {
    match field {
        Field::Muted { .. } => json!(state.muted),
        Field::Tracks { .. } => json!(state.tracks),
        Field::Transform { key, .. } => state
            .transform
            .get(key.as_str())
            .cloned()
            .unwrap_or(Value::Null),
    }
}
fn equal(field: &Field, a: &Value, b: &Value) -> bool {
    match field {
        Field::Transform { key, .. } => same_value(*key, Some(a), Some(b)),
        _ => snapshots::same(a, b),
    }
}
fn matches(state: &State, fields: &[Field], after: bool) -> bool {
    fields.iter().all(|field| {
        equal(
            field,
            &value(state, field),
            &if after { field.after() } else { field.before() },
        )
    })
}
fn changes(state: &State, desired: &Map<String, Value>) -> Vec<Field> {
    let mut fields = vec![
        Field::Muted {
            before: state.muted,
            after: true,
            extra: Map::new(),
        },
        Field::Tracks {
            before: state.tracks.clone(),
            after: silent_tracks(),
            extra: Map::new(),
        },
    ];
    fields.extend(FIT.into_iter().map(|key| Field::Transform {
        key,
        before: state.transform[key.as_str()].clone(),
        after: desired[key.as_str()].clone(),
        extra: Map::new(),
    }));
    fields
        .into_iter()
        .filter(|field| !equal(field, &field.before(), &field.after()))
        .collect()
}
fn advance(state: &mut State, field: &Field, after: bool) {
    let value = if after { field.after() } else { field.before() };
    match field {
        Field::Muted { .. } => state.muted = value.as_bool().unwrap(),
        Field::Tracks { .. } => state.tracks = value.as_object().unwrap().clone(),
        Field::Transform { key, .. } => {
            state.transform.insert(key.as_str().to_owned(), value);
        }
    }
}
fn valid_id(value: &str) -> bool {
    if matches!(
        value,
        "00000000-0000-0000-0000-000000000000" | "ffffffff-ffff-ffff-ffff-ffffffffffff"
    ) {
        return true;
    }
    let bytes = value.as_bytes();
    bytes.len() == 36
        && bytes.iter().enumerate().all(|(index, byte)| {
            if [8, 13, 18, 23].contains(&index) {
                *byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
        && (b'1'..=b'8').contains(&bytes[14])
        && matches!(bytes[19], b'8' | b'9' | b'a' | b'b' | b'A' | b'B')
}

/// Explicit source-owned journal directory under one approved existing root.
/// Unknown/corrupt evidence is never repaired or interpreted as no pending work.
/// One service owns this root. Static symlink/hardlink and observed replacement
/// checks do not provide a sandbox against concurrent same-user ancestor swaps.
pub struct JournalFile {
    root: PathBuf,
    directory: PathBuf,
    root_identity: Metadata,
    directory_identity: Option<Metadata>,
    capacity: usize,
    sequence: u64,
    poisoned: HashSet<String>,
    known_ids: HashSet<String>,
    known_files: HashMap<String, Metadata>,
    observer: Option<PublicationObserver>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublicationPoint {
    StageSynced,
    Renamed,
    DirectorySynced,
}
#[derive(Clone, Copy, Debug)]
pub struct PublicationMetadata<'a> {
    pub operation_id: &'a str,
    pub revision: u64,
}
pub type PublicationObserver = Box<dyn FnMut(PublicationPoint, PublicationMetadata<'_>) + Send>;
impl JournalFile {
    /// Trusted synchronous diagnostics/fault observer. No callbacks are installed
    /// by default; observers must not perform external actions or detached work.
    pub fn set_publication_observer(&mut self, observer: Option<PublicationObserver>) {
        self.observer = observer;
    }
    fn observe(&mut self, point: PublicationPoint, journal: &Journal) {
        if let Some(observer) = &mut self.observer {
            observer(
                point,
                PublicationMetadata {
                    operation_id: &journal.operation_id,
                    revision: journal.provenance.as_ref().unwrap().revision,
                },
            );
        }
    }
    pub fn new(root: impl AsRef<Path>, capacity: usize) -> Result<Self, DemoError> {
        let root = root.as_ref().to_path_buf();
        validate_root(&root)?;
        if capacity == 0
            || u64::try_from(capacity)
                .ok()
                .and_then(|capacity| capacity.checked_add(1))
                .is_none()
        {
            return Err(DemoError::StorageUnavailable);
        }
        let root_identity =
            fs::symlink_metadata(&root).map_err(|_| DemoError::StorageUnavailable)?;
        let mut owner = Self {
            directory: root.join("demo-recovery"),
            root,
            root_identity,
            directory_identity: None,
            capacity,
            sequence: 0,
            poisoned: HashSet::new(),
            known_ids: HashSet::new(),
            known_files: HashMap::new(),
            observer: None,
        };
        owner.layout()?;
        Ok(owner)
    }
    fn layout(&mut self) -> Result<(), DemoError> {
        validate_root(&self.root)?;
        if !same_file(
            &self.root_identity,
            &fs::symlink_metadata(&self.root).map_err(|_| DemoError::UnsafePath)?,
        ) {
            return Err(DemoError::UnsafePath);
        }
        match fs::symlink_metadata(&self.directory) {
            Ok(metadata) => {
                if !metadata.is_dir() || metadata.file_type().is_symlink() {
                    return Err(DemoError::UnsafePath);
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    if metadata.mode() & 0o077 != 0 {
                        return Err(DemoError::UnsafePath);
                    }
                }
                if self
                    .directory_identity
                    .as_ref()
                    .is_some_and(|known| !same_file(known, &metadata))
                {
                    return Err(DemoError::UnsafePath);
                }
                self.directory_identity = Some(metadata);
            }
            Err(error)
                if error.kind() == io::ErrorKind::NotFound && self.directory_identity.is_none() => {
            }
            Err(_) => return Err(DemoError::UnsafePath),
        }
        Ok(())
    }
}
impl JournalStore for JournalFile {
    fn poison(&mut self, operation_id: &str) {
        self.poisoned.insert(operation_id.to_owned());
    }
    fn list(&mut self) -> Result<Vec<Journal>, DemoError> {
        if !self.poisoned.is_empty() {
            return Err(DemoError::StorageUncertain);
        }
        self.layout()?;
        let entries = match fs::read_dir(&self.directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
            Err(_) => return Err(DemoError::StorageUnavailable),
        };
        let mut names = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|_| DemoError::StorageUnavailable)?;
            names.push(
                entry
                    .file_name()
                    .into_string()
                    .map_err(|_| DemoError::InvalidJournal)?,
            );
            if names.len() > 1024 {
                return Err(DemoError::InventoryLimit);
            }
        }
        names.sort();
        self.layout()?;
        if self
            .known_ids
            .iter()
            .any(|id| !names.contains(&format!("{id}.json")))
        {
            return Err(DemoError::Missing);
        }
        names
            .into_iter()
            .filter(|name| name.ends_with(".json"))
            .map(|name| self.load(name.strip_suffix(".json").unwrap()))
            .collect()
    }
    fn load(&mut self, id: &str) -> Result<Journal, DemoError> {
        if self.poisoned.contains(id) {
            return Err(DemoError::StorageUncertain);
        }
        if !valid_id(id) {
            return Err(evidence("Invalid operationId"));
        }
        self.layout()?;
        let path = self.directory.join(format!("{id}.json"));
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                DemoError::Missing
            } else {
                DemoError::StorageUnavailable
            }
        })?;
        validate_file(&metadata)?;
        if metadata.len() > self.capacity as u64 {
            return Err(DemoError::InvalidJournal);
        }
        let mut file = File::open(&path).map_err(|_| DemoError::StorageUnavailable)?;
        if !same_file(
            &metadata,
            &file.metadata().map_err(|_| DemoError::StorageUnavailable)?,
        ) {
            return Err(DemoError::UnsafePath);
        }
        let mut bytes = Vec::new();
        Read::by_ref(&mut file)
            .take(self.capacity as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| DemoError::StorageUnavailable)?;
        if bytes.len() > self.capacity {
            return Err(DemoError::InvalidJournal);
        }
        let after = file.metadata().map_err(|_| DemoError::StorageUnavailable)?;
        let named = fs::symlink_metadata(&path).map_err(|_| DemoError::UnsafePath)?;
        validate_file(&after)?;
        validate_file(&named)?;
        if !same_version(&metadata, &after) || !same_version(&after, &named) {
            return Err(DemoError::UnsafePath);
        }
        self.layout()?;
        let journal = Journal::from_value(
            serde_json::from_slice(&bytes).map_err(|_| DemoError::InvalidJournal)?,
        )?;
        if journal.operation_id != id {
            return Err(DemoError::InvalidJournal);
        }
        if self
            .known_files
            .get(id)
            .is_some_and(|known| !same_version(known, &named))
        {
            return Err(DemoError::UnsafePath);
        }
        self.known_ids.insert(id.to_owned());
        self.known_files.insert(id.to_owned(), named);
        Ok(journal)
    }
    fn persist(&mut self, journal: &Journal) -> Result<(), DemoError> {
        if self.poisoned.contains(&journal.operation_id) {
            return Err(DemoError::StorageUncertain);
        }
        let journal = Journal::from_value(journal.to_value())?;
        if journal.version != 2 {
            return Err(DemoError::LegacyJournalReadOnly);
        }
        self.layout()?;
        if !self.directory.exists() {
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            builder
                .create(&self.directory)
                .map_err(|_| DemoError::StorageUnavailable)?;
        }
        self.layout()?;
        let bytes = serde_json::to_vec(&journal).map_err(|_| DemoError::InvalidJournal)?;
        if bytes.len() > self.capacity {
            return Err(DemoError::InvalidJournal);
        }
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(DemoError::IdentityExhausted)?;
        let path = self
            .directory
            .join(format!("{}.json", journal.operation_id));
        let previous = match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                validate_file(&metadata)?;
                let previous = self.load(&journal.operation_id)?;
                previous.check_successor(&journal)?;
                Some(metadata)
            }
            Err(error)
                if error.kind() == io::ErrorKind::NotFound
                    && !self.known_ids.contains(&journal.operation_id) =>
            {
                let provenance = journal
                    .provenance
                    .as_ref()
                    .ok_or(DemoError::InvalidJournal)?;
                if provenance.revision != 0
                    || provenance.completed
                    || provenance
                        .fields
                        .iter()
                        .any(|field| field.preparation.is_some() || !field.recovery.is_empty())
                {
                    return Err(DemoError::JournalTransition);
                }
                None
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(DemoError::Missing);
            }
            Err(_) => return Err(DemoError::StorageUnavailable),
        };
        let temporary = self.directory.join(format!(
            "{}.json.{}.{}.tmp",
            journal.operation_id,
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
        let mut file = options
            .open(&temporary)
            .map_err(|_| DemoError::StorageUnavailable)?;
        let identity = file.metadata().map_err(|_| DemoError::StorageUnavailable)?;
        let mut stage = Staging {
            path: temporary,
            identity,
            committed: false,
        };
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| DemoError::StorageUnavailable)?;
        self.observe(PublicationPoint::StageSynced, &journal);
        drop(file);
        self.layout()?;
        match (previous.as_ref(), fs::symlink_metadata(&path)) {
            (Some(previous), Ok(current)) if same_version(previous, &current) => {}
            (None, Err(error)) if error.kind() == io::ErrorKind::NotFound => {}
            _ => return Err(DemoError::UnsafePath),
        }
        let stage_metadata =
            fs::symlink_metadata(&stage.path).map_err(|_| DemoError::UnsafePath)?;
        validate_file(&stage_metadata)?;
        if !same_file(&stage.identity, &stage_metadata) {
            return Err(DemoError::UnsafePath);
        }
        fs::rename(&stage.path, &path).map_err(|_| DemoError::StorageUnavailable)?;
        stage.committed = true;
        self.observe(PublicationPoint::Renamed, &journal);
        let publication = (|| {
            for (directory, identity) in [
                (&self.directory, self.directory_identity.as_ref().unwrap()),
                (&self.root, &self.root_identity),
            ] {
                let file = File::open(directory).map_err(|_| DemoError::StorageUncertain)?;
                if !same_file(
                    identity,
                    &file.metadata().map_err(|_| DemoError::StorageUncertain)?,
                ) {
                    return Err(DemoError::StorageUncertain);
                }
                file.sync_all().map_err(|_| DemoError::StorageUncertain)?;
            }
            self.layout().map_err(|_| DemoError::StorageUncertain)?;
            let published = fs::symlink_metadata(&path).map_err(|_| DemoError::StorageUncertain)?;
            if !same_file(&stage.identity, &published) {
                return Err(DemoError::StorageUncertain);
            }
            self.observe(PublicationPoint::DirectorySynced, &journal);
            Ok(published)
        })();
        match publication {
            Err(error) => {
                self.poison(&journal.operation_id);
                Err(error)
            }
            Ok(metadata) => {
                self.known_ids.insert(journal.operation_id.clone());
                self.known_files
                    .insert(journal.operation_id.clone(), metadata);
                Ok(())
            }
        }
    }
}
struct Staging {
    path: PathBuf,
    identity: Metadata,
    committed: bool,
}
impl Drop for Staging {
    fn drop(&mut self) {
        if !self.committed
            && fs::symlink_metadata(&self.path)
                .is_ok_and(|metadata| same_file(&self.identity, &metadata))
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}
fn validate_root(path: &Path) -> Result<(), DemoError> {
    if !path.is_absolute() {
        return Err(DemoError::UnsafePath);
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Prefix(_)
            | std::path::Component::RootDir
            | std::path::Component::Normal(_) => current.push(component.as_os_str()),
            _ => return Err(DemoError::UnsafePath),
        }
        let metadata = fs::symlink_metadata(&current).map_err(|_| DemoError::StorageUnavailable)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(DemoError::UnsafePath);
        }
    }
    Ok(())
}
fn validate_file(metadata: &Metadata) -> Result<(), DemoError> {
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(DemoError::UnsafePath);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 || metadata.mode() & 0o077 != 0 {
            return Err(DemoError::UnsafePath);
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
            && a.is_dir() == b.is_dir()
            && a.is_file() == b.is_file()
    }
}
fn same_version(a: &Metadata, b: &Metadata) -> bool {
    same_file(a, b) && a.len() == b.len() && a.modified().ok() == b.modified().ok()
}
