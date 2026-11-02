// SPDX-License-Identifier: GPL-2.0-only
//! Stage/take workflow ownership. This module never sends raw OBS mutations.
//! The service lends its single receipt writer and retains the workflow future;
//! dropping a future after dispatch leaves durable uncertainty, not permission
//! to replay. Stateful MCP routes are not registered by this library.

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    time::Duration,
};
use tokio::{sync::watch, time::Instant};

use crate::{
    clock::from_epoch_duration,
    control::Disposition,
    custody::Clock,
    observation::{
        EvidenceState, Observation, ObservationError, ReadAdapter, ReadRequest, SceneIdentity,
        Target, flat_scene, identity, list, normalize_scene_item, safe_integer,
    },
    operation_store::{
        OperationKind, OperationReceipt, OperationStore, OperationUpdate, Phase, PrepareDetails,
        StoreError,
    },
};

pub const STAGE_TTL_MS: u64 = 10 * 60_000;
const VERIFICATION_MS: u64 = 5_000;
type Fingerprint = [u8; 32];
// Owner-local codec: fingerprints never enter reports, receipts or persistent
// tokens, and cannot be compared with a different process/connection owner.
// Unicode property keys use deterministic scalar-value order, not localeCompare;
// all numbers use the source runtime's finite IEEE-754 equality, including -0.
const FINGERPRINT_CODEC: &[u8] = b"obs-mcp-stage-private-json-v1\0";

fn normalize_private(value: &mut Value) -> Result<(), ObservationError> {
    let invalid =
        || ObservationError::new("evidence_unavailable", "Non-finite numeric stage evidence");
    match value {
        Value::Number(number) => {
            let finite = number
                .as_f64()
                .filter(|number| number.is_finite())
                .ok_or_else(invalid)?;
            *number = serde_json::Number::from_f64(if finite == 0.0 { 0.0 } else { finite })
                .ok_or_else(invalid)?;
        }
        Value::Array(values) => {
            for value in values {
                normalize_private(value)?;
            }
        }
        Value::Object(object) => {
            let mut entries: Vec<_> = std::mem::take(object).into_iter().collect();
            entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            for (key, mut value) in entries {
                normalize_private(&mut value)?;
                object.insert(key, value);
            }
        }
        _ => {}
    }
    Ok(())
}

struct HashWriter(Sha256);
impl std::io::Write for HashWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn private_fingerprint(mut value: Value) -> Result<Fingerprint, ObservationError> {
    normalize_private(&mut value)?;
    let mut output = HashWriter(Sha256::new());
    output.0.update(FINGERPRINT_CODEC);
    serde_json::to_writer(&mut output, &value).map_err(|_| {
        ObservationError::new(
            "evidence_unavailable",
            "Stage evidence could not be encoded",
        )
    })?;
    Ok(output.0.finalize().into())
}

type FutureResult<'a, T, E = ObservationError> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Synchronization must drain already-received session events before returning
/// evidence for a fresh sample. It does not freeze future human changes.
/// Admitted reads must settle their own bounded transport cleanup. A local
/// workflow deadline discards late evidence, not ownership of the read future.
pub trait StageReader: ReadAdapter {
    fn endpoint_hash(&self) -> &str;
    fn connection_generation(&self) -> Option<u64>;
    fn cancelled(&self) -> bool;
    fn synchronize<'a>(
        &'a mut self,
        deadline: Instant,
    ) -> FutureResult<'a, watch::Receiver<EvidenceState>>;
}

pub enum StageAdmission<'a> {
    Stage(&'a Target),
    Take { stage_id: &'a str },
}

pub enum AdmissionError {
    Policy(Disposition),
    Invalid(ObservationError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mutation {
    SetPreview { scene_uuid: String },
    Transition,
}

#[derive(Debug)]
pub enum MutationError {
    NotDispatched(ObservationError),
    Policy(Disposition),
    Rejected {
        code: u64,
    },
    RejectedDetailed {
        code: u64,
        comment: crate::session::RejectionComment,
        request_type: &'static str,
    },
    Unknown(ObservationError),
}

/// An adapter must evaluate policy initially and again at the actual socket
/// dispatch, bind the expected generation to its opaque session token, and
/// never retry an effect. There is no raw request-string method here.
/// The adapter owns an independent request/cleanup bound and must settle the
/// admitted future without detached work. A workflow timeout retains and drains
/// that exact future, discarding late outcomes before lending its writer again.
pub trait StageMutator: Send {
    fn set_receipt_guard(&mut self, _fence: crate::receipt_broker::DispatchFence, _id: String) {}
    fn endpoint_hash(&self) -> &str;
    fn connection_generation(&self) -> Option<u64>;
    fn admit(&mut self, call: StageAdmission<'_>) -> Result<(), AdmissionError>;
    fn mutate<'a>(
        &'a mut self,
        mutation: Mutation,
        expected_generation: u64,
        expected_revision: u64,
        deadline: Instant,
    ) -> FutureResult<'a, (), MutationError>;
}

pub struct Context<'a, R: StageReader, M: StageMutator> {
    pub reader: &'a mut R,
    pub mutator: &'a mut M,
    pub store: &'a mut OperationStore,
    pub clock: Clock,
    pub deadline: Instant,
    pub screenshot_max_bytes: usize,
}

#[derive(Clone)]
struct Subject {
    scene_uuid: String,
    fingerprint: Fingerprint,
    sources: Vec<String>,
}

/// Constructed only by bounded dependency inspection, not external JSON.
#[derive(Clone)]
pub struct Inspection {
    intended: Subject,
    program: Subject,
    preview: SceneIdentity,
    generation: u64,
    revision: u64,
}
impl Inspection {
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn intended_scene_uuid(&self) -> &str {
        &self.intended.scene_uuid
    }
    pub fn program_scene_uuid(&self) -> &str {
        &self.program.scene_uuid
    }
    pub fn preview(&self) -> &SceneIdentity {
        &self.preview
    }
}

struct Dependencies {
    scene_ids: HashSet<String>,
    inputs: Vec<Value>,
    visited: HashSet<String>,
    sources: Vec<String>,
    parts: Vec<Value>,
    count: usize,
}
impl Dependencies {
    fn walk<'a, 'reader, R: ReadAdapter>(
        &'a mut self,
        sample: &'a mut Observation<'reader, R>,
        uuid: String,
        group: bool,
        depth: usize,
    ) -> FutureResult<'a, ()> {
        Box::pin(async move {
            if depth > 8 || !self.visited.insert(uuid.clone()) {
                return Err(ObservationError::new(
                    "evidence_unavailable",
                    "Nested scene cycle, reuse or depth limit prevents staging",
                ));
            }
            let reply = sample
                .read(
                    if group {
                        ReadRequest::GroupSceneItemList
                    } else {
                        ReadRequest::SceneItemList
                    },
                    json!({"sceneUuid":uuid}),
                )
                .await?;
            let items: Vec<_> = list(&reply["sceneItems"], 128)?
                .into_iter()
                .map(normalize_scene_item)
                .collect();
            let mut ids = HashSet::new();
            for item in &items {
                let id = safe_integer(&item["sceneItemId"]).ok_or_else(|| {
                    ObservationError::new("evidence_unavailable", "Incomplete occurrence state")
                })? as i64;
                if !ids.insert(id) {
                    return Err(ObservationError::new(
                        "evidence_unavailable",
                        "Duplicate occurrence identity",
                    ));
                }
            }
            self.parts.push(json!({"uuid":uuid,"items":items}));
            self.parts.push(
                sample
                    .read(ReadRequest::SourceFilterList, json!({"sourceUuid":uuid}))
                    .await?,
            );
            for item in &items {
                self.count += 1;
                if self.count > 128 {
                    return Err(ObservationError::new(
                        "observation_limit",
                        "Staging dependency limit reached",
                    ));
                }
                let source_uuid = identity(&item["sourceUuid"])?.to_owned();
                identity(&item["sourceName"])?;
                if !item["sceneItemEnabled"].is_boolean() || !item["isGroup"].is_boolean() {
                    return Err(ObservationError::new(
                        "evidence_unavailable",
                        "Incomplete occurrence state",
                    ));
                }
                self.parts.push(
                    sample
                        .read(
                            ReadRequest::SceneItemTransform,
                            json!({"sceneUuid":uuid,"sceneItemId":item["sceneItemId"]}),
                        )
                        .await?,
                );
                if item["isGroup"] == true || self.scene_ids.contains(&source_uuid) {
                    self.walk(sample, source_uuid, item["isGroup"] == true, depth + 1)
                        .await?;
                } else if !self.sources.contains(&source_uuid) {
                    let inputs: Vec<_> = self
                        .inputs
                        .iter()
                        .filter(|input| {
                            input["inputUuid"] == source_uuid
                                && input["inputName"] == item["sourceName"]
                        })
                        .collect();
                    if inputs.len() != 1 {
                        return Err(ObservationError::new(
                            "target_changed",
                            "Source inventory and scene identity disagree",
                        ));
                    }
                    self.sources.push(source_uuid.clone());
                    self.parts.push(inputs[0].clone());
                    self.parts.push(
                        sample
                            .read(ReadRequest::InputSettings, json!({"inputUuid":source_uuid}))
                            .await?,
                    );
                    self.parts.push(
                        sample
                            .read(
                                ReadRequest::SourceFilterList,
                                json!({"sourceUuid":source_uuid}),
                            )
                            .await?,
                    );
                    for request in [
                        ReadRequest::InputMute,
                        ReadRequest::InputVolume,
                        ReadRequest::InputAudioTracks,
                    ] {
                        match sample.read(request, json!({"inputUuid":source_uuid})).await {
                            Ok(state) => self.parts.push(json!({"type":request.as_str(),"sourceUuid":source_uuid,"state":state})),
                            Err(error) => {
                                sample.check()?;
                                if error.request_code() != Some(604) { return Err(error); }
                                self.parts.push(json!({"type":request.as_str(),"sourceUuid":source_uuid,"capability":"unsupported"}));
                            }
                        }
                    }
                }
            }
            Ok(())
        })
    }
}

async fn subject<R: ReadAdapter>(
    sample: &mut Observation<'_, R>,
    target: &Target,
) -> Result<Subject, ObservationError> {
    let root = flat_scene(sample, target).await?;
    let mut dependencies = Dependencies {
        scene_ids: list(&root.scenes["scenes"], 256)?
            .iter()
            .map(|scene| identity(&scene["sceneUuid"]).map(str::to_owned))
            .collect::<Result<_, _>>()?,
        inputs: root.inputs,
        visited: HashSet::new(),
        sources: vec![],
        parts: vec![],
        count: 0,
    };
    dependencies
        .walk(sample, root.selected.scene_uuid.clone(), false, 0)
        .await?;
    let scene_uuid = root.selected.scene_uuid.clone();
    // Move retained evidence into the digest document instead of serializing a
    // borrowed Vec<Value> into a second deep copy of every settings payload.
    let mut document = serde_json::Map::new();
    document.insert("collection".to_owned(), Value::String(root.collection));
    document.insert(
        "scene".to_owned(),
        serde_json::to_value(root.selected).map_err(|_| {
            ObservationError::new("evidence_unavailable", "Incomplete scene identity")
        })?,
    );
    document.insert("video".to_owned(), root.video);
    document.insert("parts".to_owned(), Value::Array(dependencies.parts));
    Ok(Subject {
        scene_uuid,
        fingerprint: private_fingerprint(Value::Object(document))?,
        sources: dependencies.sources,
    })
}

pub async fn inspect<R: ReadAdapter>(
    sample: &mut Observation<'_, R>,
    target: &Target,
) -> Result<Inspection, ObservationError> {
    if sample
        .read(ReadRequest::StudioModeEnabled, Value::Null)
        .await?["studioModeEnabled"]
        != true
    {
        return Err(ObservationError::new(
            "capability_unavailable",
            "Enable Studio Mode explicitly before staging; this workflow never toggles it",
        ));
    }
    let before = flat_scene(sample, target).await?;
    let program_name = identity(&before.scenes["currentProgramSceneName"])?.to_owned();
    let preview = sample
        .read(ReadRequest::CurrentPreviewScene, target.canvas())
        .await?;
    let preview_name = identity(&preview["currentPreviewSceneName"])?.to_owned();
    let scenes = list(&before.scenes["scenes"], 256)?;
    let program: Vec<_> = scenes
        .iter()
        .filter(|scene| {
            scene["sceneName"] == program_name
                && before
                    .scenes
                    .get("currentProgramSceneUuid")
                    .is_none_or(|uuid| scene["sceneUuid"] == *uuid)
        })
        .collect();
    let preview_scenes: Vec<_> = scenes
        .iter()
        .filter(|scene| {
            scene["sceneName"] == preview_name
                && preview
                    .get("currentPreviewSceneUuid")
                    .is_none_or(|uuid| scene["sceneUuid"] == *uuid)
        })
        .collect();
    if program.len() != 1 || preview_scenes.len() != 1 {
        return Err(ObservationError::new(
            "evidence_unavailable",
            "Program/preview identities are missing or ambiguous",
        ));
    }
    let intended = subject(sample, target).await?;
    let program = subject(
        sample,
        &Target {
            scene_name: program_name,
            ..target.clone()
        },
    )
    .await?;
    let after = flat_scene(sample, target).await?;
    if private_fingerprint(
        serde_json::to_value(&before).map_err(|_| {
            ObservationError::new("evidence_unavailable", "Incomplete scene evidence")
        })?,
    )? != private_fingerprint(
        serde_json::to_value(&after).map_err(|_| {
            ObservationError::new("evidence_unavailable", "Incomplete scene evidence")
        })?,
    )? || private_fingerprint(preview)?
        != private_fingerprint(
            sample
                .read(ReadRequest::CurrentPreviewScene, target.canvas())
                .await?,
        )?
    {
        return Err(ObservationError::new(
            "observation_invalidated",
            "Program or preview changed during inspection",
        ));
    }
    if sample
        .read(ReadRequest::StudioModeEnabled, Value::Null)
        .await?["studioModeEnabled"]
        != true
    {
        return Err(ObservationError::new(
            "observation_invalidated",
            "Studio Mode changed during inspection",
        ));
    }
    Ok(Inspection {
        intended,
        program,
        preview: SceneIdentity {
            scene_name: preview_name,
            scene_uuid: identity(&preview_scenes[0]["sceneUuid"])?.to_owned(),
        },
        generation: sample.generation(),
        revision: sample.revision(),
    })
}

#[derive(Clone)]
struct Staged {
    target: Target,
    generation: u64,
    expires_at: u64,
    subject: Fingerprint,
    program: Fingerprint,
    scene_uuid: String,
}

#[derive(Default)]
pub struct StageMachine {
    tokens: HashMap<String, Staged>,
    endpoint_hash: Option<String>,
}

pub struct Image {
    mime_type: String,
    data: String,
    size_bytes: usize,
}
impl Image {
    pub fn mime_type(&self) -> &str {
        &self.mime_type
    }
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }
}

pub struct Success {
    pub report: Value,
    pub image: Option<Image>,
}
impl Success {
    pub fn tool_result(&self) -> Value {
        let mut content = vec![json!({"type":"text","text":self.report.to_string()})];
        if let Some(image) = &self.image {
            content.push(json!({"type":"image","mimeType":image.mime_type,"data":image.data}));
        }
        json!({"content":content,"structuredContent":self.report})
    }
}

pub struct Failure {
    pub report: Value,
    pub policy: Option<Disposition>,
    pub receipt_error: Option<StoreError>,
}
impl Failure {
    pub fn tool_result(&self) -> Value {
        if let Some(policy) = &self.policy
            && let Some(mut result) = policy.tool_result()
        {
            // Initial wrapper refusals preserve the source projection. A new
            // native queue can refuse after consuming a stage and persisting
            // intent; retain both that custody and the original policy reason.
            if matches!(policy, Disposition::Refused { .. })
                && self.report.get("operationId").is_some()
            {
                let mut report = self.report.clone();
                report["policyRefusal"] = result.clone();
                let consumed = report.get("intendedSceneObservedOnProgram").is_some();
                report["stageTokenConsumed"] = json!(consumed);
                report["nextCheck"] = json!(if consumed {
                    "This stageId was consumed. Read obs-observe, stage again and inspect the new preview before requesting another transition; do not replay this operationId"
                } else {
                    "No new stageId was published. Read obs-observe and stage again; do not replay this operationId"
                });
                result["structuredContent"] = report;
            }
            return result;
        }
        json!({"content":[{"type":"text","text":self.report.to_string()}],"structuredContent":self.report,"isError":true})
    }
}

enum Fault {
    Evidence(ObservationError),
    Receipt(StoreError),
    Policy(Disposition),
}
impl From<ObservationError> for Fault {
    fn from(error: ObservationError) -> Self {
        Self::Evidence(error)
    }
}
impl From<StoreError> for Fault {
    fn from(error: StoreError) -> Self {
        Self::Receipt(error)
    }
}
impl From<AdmissionError> for Fault {
    fn from(error: AdmissionError) -> Self {
        match error {
            AdmissionError::Policy(policy) => Self::Policy(policy),
            AdmissionError::Invalid(error) => Self::Evidence(error),
        }
    }
}

#[derive(Default)]
struct Tracking {
    receipt: Option<OperationReceipt>,
    dispatched: bool,
    accepted: Option<bool>,
}

impl<'a, R: StageReader, M: StageMutator> Context<'a, R, M> {
    fn validate(&self) -> Result<(), Fault> {
        if self.store.endpoint_hash() != self.reader.endpoint_hash()
            || self.store.endpoint_hash() != self.mutator.endpoint_hash()
        {
            return Err(Fault::Receipt(StoreError::EndpointMismatch));
        }
        if !(1..=6 * 1024 * 1024).contains(&self.screenshot_max_bytes) {
            return Err(ObservationError::new(
                "evidence_unavailable",
                "Invalid screenshot byte limit",
            )
            .into());
        }
        Ok(())
    }
    fn check_deadline(&self) -> Result<(), ObservationError> {
        if Instant::now() >= self.deadline {
            return Err(ObservationError::new(
                "observation_limit",
                "Stage operation deadline expired",
            ));
        }
        Ok(())
    }
    fn connected(&self, generation: u64) -> bool {
        self.reader.connection_generation() == Some(generation)
            && self.mutator.connection_generation() == Some(generation)
    }
    fn check_cancelled(&self) -> Result<(), ObservationError> {
        if self.reader.cancelled() {
            return Err(ObservationError::new(
                "cancelled",
                "Operation cancelled; inspect OBS before any further action",
            ));
        }
        Ok(())
    }
    async fn read_object(
        &mut self,
        request: ReadRequest,
        data: Value,
        deadline: Instant,
    ) -> Result<Value, ObservationError> {
        let mut work = self.reader.read(request, data, deadline);
        let reply = match tokio::time::timeout_at(deadline, &mut work).await {
            Ok(result) => result?,
            Err(_) => {
                // Cleanup is part of this admitted read's ownership. A late
                // reply cannot become evidence or authorize another read.
                let _ = work.await;
                return Err(ObservationError::new(
                    "evidence_unavailable",
                    format!("Request {} timed out", request.as_str()),
                ));
            }
        };
        if !reply.is_object() {
            return Err(ObservationError::new(
                "evidence_unavailable",
                "OBS returned incomplete state",
            ));
        }
        Ok(reply)
    }
    async fn inspect(&mut self, target: &Target) -> Result<Inspection, ObservationError> {
        let state = self.reader.synchronize(self.deadline).await?;
        let generation = state.borrow().generation;
        let mut sample = Observation::new(
            self.reader,
            state,
            generation,
            (self.clock)().utc,
            self.deadline,
        )?;
        let result = inspect(&mut sample, target).await;
        sample.close();
        result
    }
    fn prepare(
        &mut self,
        tracking: &mut Tracking,
        operation_id: &str,
        stage_id: &str,
        kind: OperationKind,
        target: &Target,
        inspection: &Inspection,
    ) -> Result<(), Fault> {
        tracking.receipt = Some(self.store.prepare(
            operation_id,
            kind,
            inspection.generation,
            &(self.clock)().utc,
            PrepareDetails {
                scene_collection: Some(&target.scene_collection),
                scene_uuid: Some(&inspection.intended.scene_uuid),
                stage_id: Some(stage_id),
                take_id: None,
            },
        )?);
        Ok(())
    }
    fn update(&mut self, tracking: &mut Tracking, update: OperationUpdate) -> Result<(), Fault> {
        let receipt = tracking.receipt.as_ref().ok_or(StoreError::Missing)?;
        tracking.receipt = Some(self.store.update(receipt, update, &(self.clock)().utc)?);
        Ok(())
    }
    async fn mutate(
        &mut self,
        tracking: &mut Tracking,
        mutation: Mutation,
        generation: u64,
        revision: u64,
    ) -> Result<(), Fault> {
        self.check_deadline()?;
        let mut update = OperationUpdate::new(Phase::Dispatching);
        update.request_dispatched = Some(None);
        self.update(tracking, update)?;
        tracking.dispatched = true;
        if let Some(fence) = self.store.dispatch_fence() {
            self.mutator.set_receipt_guard(
                fence,
                tracking.receipt.as_ref().unwrap().operation_id().into(),
            );
        }
        let outcome = {
            let mut work = self
                .mutator
                .mutate(mutation, generation, revision, self.deadline);
            match tokio::time::timeout_at(self.deadline, &mut work).await {
                Ok(result) => result,
                Err(_) => {
                    // The borrowed receipt writer cannot be returned while this
                    // admitted operation still has cleanup duties. Drain, never
                    // respawn, and do not promote a late success to Accepted.
                    let _ = work.await;
                    Err(MutationError::Unknown(ObservationError::new(
                        "observation_limit",
                        "Stage operation deadline expired after mutation admission",
                    )))
                }
            }
        };
        match outcome {
            Ok(()) => {
                tracking.accepted = Some(true);
                let mut update = OperationUpdate::new(Phase::Accepted);
                update.request_dispatched = Some(Some(true));
                update.request_accepted = Some(Some(true));
                self.update(tracking, update)
            }
            Err(MutationError::NotDispatched(error)) => {
                tracking.dispatched = false;
                Err(error.into())
            }
            Err(MutationError::Policy(policy)) => {
                tracking.dispatched = false;
                Err(Fault::Policy(policy))
            }
            Err(MutationError::Rejected { code }) => {
                tracking.accepted = Some(false);
                Err(ObservationError::rejected(
                    code,
                    if code == 604 {
                        "capability_unavailable"
                    } else {
                        "evidence_unavailable"
                    },
                    format!("OBS stage mutation failed with code {code}"),
                )
                .into())
            }
            Err(MutationError::RejectedDetailed {
                code,
                comment,
                request_type,
            }) => {
                tracking.accepted = Some(false);
                let message = crate::session::RequestError::RejectedDetailed { code, comment }
                    .obs_failure_message(request_type)
                    .expect("typed rejection has a code");
                Err(ObservationError::rejected(
                    code,
                    if code == 604 {
                        "capability_unavailable"
                    } else {
                        "evidence_unavailable"
                    },
                    message,
                )
                .into())
            }
            Err(MutationError::Unknown(error)) => Err(error.into()),
        }
    }
    fn fail(&mut self, tracking: &mut Tracking, fault: Fault, take: bool) -> Failure {
        if tracking.receipt.is_some() {
            let mut update = OperationUpdate::new(if tracking.dispatched {
                Phase::Unknown
            } else {
                Phase::NotDispatched
            });
            update.request_accepted = Some(tracking.accepted);
            if !tracking.dispatched {
                update.request_dispatched = Some(Some(false));
            }
            if tracking.accepted == Some(false) {
                update.request_dispatched = Some(Some(true));
            }
            let _ = self.update(tracking, update);
        }
        let (reason, message, policy, receipt_error) = match fault {
            Fault::Evidence(error) => (error.reason, error.message, None, None),
            Fault::Receipt(error) => ("receipt_unavailable", "Operation receipt could not be persisted or inspected; inspect obs-observe before any further production change".to_owned(), None, Some(error)),
            Fault::Policy(disposition) => ("policy_refused", "Control policy refused this operation".to_owned(), Some(disposition), None),
        };
        let mut report = json!({"status":if tracking.dispatched {"unknown"} else {"blocked"},"requestDispatched":tracking.dispatched,"requestAccepted":tracking.accepted,"reason":reason,"message":message,"nextCheck":if take {if tracking.dispatched {"Read obs-observe and inspect program before any further transition; do not replay this stageId"} else {"Stage and inspect again"}} else {"Read obs-observe and inspect preview before staging again"}});
        if let Some(receipt) = &tracking.receipt {
            report["operationId"] = json!(receipt.operation_id());
        }
        if take {
            report["intendedSceneObservedOnProgram"] = Value::Null;
            report["renderedComposition"] = json!("unverified");
        }
        Failure {
            report,
            policy,
            receipt_error,
        }
    }
}

impl StageMachine {
    pub fn token_count(&self) -> usize {
        self.tokens.len()
    }
    /// The mutable borrow serializes stage/take ownership. The service should
    /// return operation_in_progress instead of waiting behind another workflow.
    pub async fn stage<R: StageReader, M: StageMutator>(
        &mut self,
        context: &mut Context<'_, R, M>,
        target: Target,
        stage_id: &str,
        operation_id: &str,
    ) -> Result<Success, Failure> {
        let mut tracking = Tracking::default();
        let result: Result<Success, Fault> = async {
            target.validate()?; context.validate()?;
            context.mutator.admit(StageAdmission::Stage(&target)).map_err(Fault::from)?;
            if self.endpoint_hash.as_deref().is_some_and(|endpoint| endpoint != context.reader.endpoint_hash()) { return Err(ObservationError::new("stage_unavailable", "Stage workflow belongs to another OBS domain").into()); }
            // Establish the connection before invalidating preceding stage IDs.
            context.reader.synchronize(context.deadline).await?;
            self.endpoint_hash = Some(context.reader.endpoint_hash().to_owned());
            self.tokens.clear();
            let before = context.inspect(&target).await?;
            context.check_cancelled()?;
            context.check_deadline()?;
            context.prepare(&mut tracking, operation_id, stage_id, OperationKind::StageScene, &target, &before)?;
            if before.preview.scene_uuid != before.intended.scene_uuid { context.mutate(&mut tracking, Mutation::SetPreview { scene_uuid: before.intended.scene_uuid.clone() }, before.generation, before.revision).await?; }
            let screenshot = context.read_object(ReadRequest::SourceScreenshot, json!({"sourceUuid":before.intended.scene_uuid,"imageFormat":"png","imageWidth":960}), context.deadline.min(Instant::now() + Duration::from_secs(10))).await?;
            let image = screenshot_image(&screenshot, context.screenshot_max_bytes)?;
            drop(screenshot);
            let after = context.inspect(&target).await?;
            if before.generation != after.generation || before.intended.fingerprint != after.intended.fingerprint || before.program.fingerprint != after.program.fingerprint || after.preview.scene_uuid != before.intended.scene_uuid { return Err(ObservationError::new("target_changed", "Subject or program changed while staging; inspect current preview before staging again").into()); }
            let shared: Vec<_> = after.intended.sources.iter().filter(|source| after.program.sources.contains(source)).cloned().collect();
            let expires_at = (context.clock)().epoch_ms.checked_add(STAGE_TTL_MS).ok_or_else(|| ObservationError::new("observation_limit", "Stage expiry exceeds supported clock range"))?;
            let expires_utc = from_epoch_duration(Duration::from_millis(expires_at)).map_err(|_| ObservationError::new("observation_limit", "Stage expiry exceeds supported UTC range"))?.utc;
            let entry = Staged { target: target.clone(), generation: after.generation, expires_at, subject: after.intended.fingerprint, program: after.program.fingerprint, scene_uuid: after.intended.scene_uuid.clone() };
            let mut update = OperationUpdate::new(Phase::Observed); update.postcondition_observed = Some(true); context.update(&mut tracking, update)?;
            self.tokens.insert(stage_id.to_owned(), entry);
            let mut target_report = serde_json::to_value(&target).map_err(|_| ObservationError::new("evidence_unavailable", "Invalid target"))?; target_report["sceneUuid"] = json!(after.intended.scene_uuid);
            Ok(Success { report: json!({"status":"staged","operationId":operation_id,"stageId":stage_id,"expiresAt":expires_utc,"target":target_report,"generation":after.generation,"preview":after.preview,"programSceneUuid":after.program.scene_uuid,"sharedSources":shared,"requestDispatched":tracking.dispatched,"requestAccepted":tracking.accepted,"intendedSceneObservedInPreview":true,"renderedComposition":"unverified","nextCheck":"Inspect the preview image, then call obs-take-scene with this stageId","limitations":["Changing a shared input, filter or audio setting can also affect program.","Live pixels can change without a structural edit; a screenshot is an observation, not a content lock."]}), image: Some(image) })
        }.await;
        result.map_err(|fault| context.fail(&mut tracking, fault, false))
    }

    pub async fn take<R: StageReader, M: StageMutator>(
        &mut self,
        context: &mut Context<'_, R, M>,
        stage_id: &str,
        operation_id: &str,
    ) -> Result<Success, Failure> {
        let mut tracking = Tracking::default();
        let result: Result<Success, Fault> = async {
            context.validate()?;
            context.mutator.admit(StageAdmission::Take { stage_id }).map_err(Fault::from)?;
            if self.endpoint_hash.as_deref().is_some_and(|endpoint| endpoint != context.reader.endpoint_hash()) { return Err(ObservationError::new("stage_unavailable", "Stage expired, was already consumed, or belongs to another server; stage again").into()); }
            let now = (context.clock)().epoch_ms;
            self.tokens.retain(|_, entry| entry.expires_at > now);
            // Consume before the first asynchronous inspection, even on failure.
            let entry = self.tokens.remove(stage_id).ok_or_else(|| ObservationError::new("stage_unavailable", "Stage expired, was already consumed, or belongs to another server; stage again"))?;
            if !context.connected(entry.generation) { return Err(ObservationError::new("observation_invalidated", "Connection changed since staging").into()); }
            let before = context.inspect(&entry.target).await?;
            if entry.expires_at <= (context.clock)().epoch_ms || before.generation != entry.generation || before.intended.fingerprint != entry.subject || before.program.fingerprint != entry.program || before.preview.scene_uuid != entry.scene_uuid { return Err(ObservationError::new("target_changed", "Program, preview or staged dependencies changed; stage and inspect again").into()); }
            context.check_deadline()?;
            context.check_cancelled()?;
            context.prepare(&mut tracking, operation_id, stage_id, OperationKind::TakeScene, &entry.target, &before)?;
            context.mutate(&mut tracking, Mutation::Transition, entry.generation, before.revision).await?;
            let verification_end = (context.clock)().epoch_ms.checked_add(VERIFICATION_MS).ok_or_else(|| ObservationError::new("verification_timeout", "Transition verification clock unavailable"))?;
            let verification_deadline = Instant::now() + Duration::from_millis(VERIFICATION_MS);
            loop {
                context.check_cancelled()?;
                context.check_deadline()?;
                if (context.clock)().epoch_ms >= verification_end || Instant::now() >= verification_deadline { return Err(ObservationError::new("verification_timeout", "Intended scene was not observed on program within five seconds").into()); }
                if !context.connected(entry.generation) { return Err(ObservationError::new("observation_invalidated", "Connection changed after transition dispatch").into()); }
                let deadline = context.deadline.min(Instant::now() + Duration::from_secs(1));
                let collection = context.read_object(ReadRequest::SceneCollectionList, Value::Null, deadline).await?;
                let program = context.read_object(ReadRequest::CurrentProgramScene, entry.target.canvas(), context.deadline.min(Instant::now() + Duration::from_secs(1))).await?;
                if collection["currentSceneCollectionName"] != entry.target.scene_collection { return Err(ObservationError::new("target_changed", "Collection changed after transition dispatch").into()); }
                if program["currentProgramSceneUuid"] == entry.scene_uuid && program["currentProgramSceneName"] == entry.target.scene_name {
                    let collection_after = context.read_object(ReadRequest::SceneCollectionList, Value::Null, context.deadline.min(Instant::now() + Duration::from_secs(1))).await?;
                    if collection_after["currentSceneCollectionName"] != entry.target.scene_collection { return Err(ObservationError::new("target_changed", "Collection changed during program verification").into()); }
                    if !context.connected(entry.generation) { return Err(ObservationError::new("observation_invalidated", "Connection changed during verification").into()); }
                    let mut update = OperationUpdate::new(Phase::Observed); update.postcondition_observed = Some(true); context.update(&mut tracking, update)?;
                    let mut target = serde_json::to_value(&entry.target).map_err(|_| ObservationError::new("evidence_unavailable", "Invalid target"))?; target["sceneUuid"] = json!(entry.scene_uuid);
                    return Ok(Success { report: json!({"status":"verified","operationId":operation_id,"requestDispatched":true,"requestAccepted":true,"intendedSceneObservedOnProgram":true,"target":target,"verifiedAt":(context.clock)().utc,"renderedComposition":"unverified","nextCheck":"Inspect the program screenshot to assess the rendered composition"}), image: None });
                }
                tokio::time::sleep_until(context.deadline.min(Instant::now() + Duration::from_millis(100))).await;
            }
        }.await;
        result.map_err(|fault| context.fail(&mut tracking, fault, true))
    }
}

fn screenshot_image(response: &Value, maximum: usize) -> Result<Image, ObservationError> {
    let invalid = || {
        ObservationError::new(
            "evidence_unavailable",
            "OBS returned invalid screenshot image data",
        )
    };
    let source = response
        .get("imageData")
        .and_then(Value::as_str)
        .ok_or_else(invalid)?;
    let (header, data) = source.split_once(',').ok_or_else(invalid)?;
    let header = header.to_ascii_lowercase();
    let mime = header
        .strip_prefix("data:")
        .and_then(|header| header.strip_suffix(";base64"))
        .filter(|mime| ["image/png", "image/jpeg", "image/webp"].contains(mime))
        .ok_or_else(invalid)?;
    if data.is_empty() || data.len() % 4 != 0 {
        return Err(invalid());
    }
    let padding = data.len() - data.trim_end_matches('=').len();
    if padding > 2
        || !data[..data.len() - padding]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'+' || byte == b'/')
    {
        return Err(invalid());
    }
    let size = data.len() / 4 * 3 - padding;
    if size > maximum {
        return Err(ObservationError::new(
            "evidence_unavailable",
            format!("OBS screenshot is {size} bytes; limit is {maximum} bytes"),
        ));
    }
    Ok(Image {
        mime_type: mime.to_owned(),
        data: data.to_owned(),
        size_bytes: size,
    })
}
