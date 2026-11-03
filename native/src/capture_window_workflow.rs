// SPDX-License-Identifier: GPL-2.0-only
//! Service-private retained Capture composition. Closed domain descriptions are
//! not authority: every send rechecks the original policy, deadline, Session,
//! observation revision and sole receipt scope. Admitted I/O is always joined.
use crate::{
    capture_window::{
        self as domain, Candidate, Discovery, Effect, Failure, InputIdentity, Kind, Readings,
        Request, Status,
    },
    control::{Disposition, GateRequest},
    custody::{Clock, SharedControl},
    observation::ReadRequest,
    operation_store::{
        CaptureCounts, CaptureInitial, CapturePhase, CapturePlan, CaptureReceipt, CaptureStep,
        CaptureUpdate, OperationStore,
    },
    session::{BatchOptions, BatchRequest, Execution as BatchExecution, RequestError},
    snapshot_tools,
    snapshots::{CaptureMetadata, Snapshot},
};
use serde_json::{Value, json};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::time::Instant;

pub(crate) fn policy(
    request: &Request,
    caller: &str,
    control: &SharedControl,
    clock: &Clock,
) -> Result<Disposition, ()> {
    Ok(control.lock().map_err(|_| ())?.evaluate(
        GateRequest {
            tool_name: "obs-capture-window",
            arguments: &request.arguments(),
            read_only_hint: false,
            caller,
            confirmation: Default::default(),
        },
        clock().epoch_ms,
    ))
}
fn fail(message: impl Into<String>) -> Failure {
    Failure::thrown(message)
}
fn changed() -> Failure {
    Failure::direct(
        "Capture source or selection changed during discovery; rerun with a stable window. No capture changed",
    )
}
#[derive(Clone)]
pub(crate) struct Ready {
    original: Discovery,
    expected: Discovery,
    pub snapshot: Option<String>,
    target_settings: Option<Value>,
}
impl Ready {
    fn effect_plan(&self) -> CapturePlan {
        CapturePlan {
            initial: if self.original.existing().is_none() {
                CaptureInitial::Create
            } else if self.original.kind().portal() {
                CaptureInitial::None
            } else {
                CaptureInitial::Retarget
            },
            audio: self.original.request().silent() && !self.original.kind().video_only(),
            fit: self.original.request().fit(),
        }
    }
    pub(crate) fn has_effects(&self) -> bool {
        self.effect_plan().has_effects()
    }
}
pub(crate) enum Planned {
    Finished(Value),
    Ready(Box<Ready>),
    Save(Box<Ready>, Box<Snapshot>),
}
pub(crate) struct Context {
    pub reads: snapshot_tools::Context,
    pub control: SharedControl,
    pub clock: Clock,
    pub caller: String,
    pub request: Request,
    pub operation_id: String,
    pub screenshot_limit:
        Result<crate::scene_audit::ScreenshotLimit, crate::observation::ObservationError>,
}
pub(crate) struct Execution {
    pub store: Option<OperationStore>,
    pub result: Value,
    pub broken: bool,
}

/// One bounded semantic sample uses a single revision. A later sample may
/// accept our acknowledged changes only after explicit expected-state updates.
struct Reader {
    context: snapshot_tools::Context,
    stamp: Option<(u64, u64)>,
    requests: usize,
    bytes: usize,
    revision_lost: AtomicBool,
}
impl Reader {
    fn new(context: snapshot_tools::Context) -> Result<Self, Failure> {
        if context.handle.token() != context.reader.token()
            || context.fence.token() != context.handle.token()
            || !context.fence.full_profile()
            || context.endpoint.is_empty()
            || context.reader.endpoint_hash() != context.handle.endpoint_hash()
            || context.fence.endpoint_hash() != context.handle.endpoint_hash()
            || context.handle.event_subscriptions() & crate::service::REQUIRED_SUBSCRIPTIONS
                != crate::service::REQUIRED_SUBSCRIPTIONS
        {
            return Err(fail("Capture Session evidence is unavailable"));
        }
        Ok(Self {
            context,
            stamp: None,
            requests: 0,
            bytes: 0,
            revision_lost: AtomicBool::new(false),
        })
    }
    fn check(&self) -> Result<(), Failure> {
        let state = self.context.reader.evidence();
        if self.context.cancellation.is_cancelled() || Instant::now() >= self.context.deadline {
            return Err(fail(
                "Capture Window cancelled or original deadline expired",
            ));
        }
        if self.context.handle.invalidation().is_some()
            || !state.connected
            || state.cancelled
            || state.generation != self.context.handle.token().generation()
        {
            return Err(changed());
        }
        if let Some((generation, revision)) = self.stamp
            && !self.context.fence.ready_at_revision(generation, revision)
        {
            // Only an otherwise healthy revision change can retire a read
            // sample. Identity, value, cancellation and transport failures
            // remain distinct refusals and never acquire resampling rights.
            if state.generation == generation
                && state.revision != revision
                && self
                    .context
                    .fence
                    .ready_at_revision(state.generation, state.revision)
            {
                self.revision_lost.store(true, Ordering::Release);
            }
            return Err(changed());
        }
        Ok(())
    }
    async fn begin(&mut self) -> Result<(), Failure> {
        self.stamp = None;
        self.revision_lost.store(false, Ordering::Release);
        self.check()?;
        self.context
            .reader
            .synchronize(self.context.deadline)
            .await
            .map_err(|_| changed())?;
        let e = self.context.reader.evidence();
        self.stamp = Some((e.generation, e.revision));
        self.check()
    }
    async fn read(&mut self, request: ReadRequest, data: Value) -> Result<Value, Failure> {
        self.fixed(request.as_str(), data).await
    }
    async fn properties(&mut self, id: &InputIdentity, expected: &Value) -> Result<Value, Failure> {
        // This is the only non-ReadRequest read admitted by this closed owner. It can
        // never enumerate unsafe Mac application/display string lists.
        let settings = self
            .read(ReadRequest::InputSettings, id.projection())
            .await?;
        domain::inventory_guard(id.kind(), domain::settings(id.kind(), &settings)?)?;
        if settings != *expected {
            return Err(changed());
        }
        self.fixed("GetInputPropertiesListPropertyItems",json!({"inputName":id.name(),"inputUuid":id.uuid(),"propertyName":if id.kind()==Kind::ScreenCapture{"window"}else{"capture_window"}})).await
    }
    async fn fixed(&mut self, name: &'static str, data: Value) -> Result<Value, Failure> {
        self.check()?;
        if self.requests >= 512 {
            return Err(fail("Capture read count exceeds native limits"));
        }
        self.requests += 1;
        if self
            .context
            .capabilities
            .as_ref()
            .is_some_and(|c| !c.contains(name))
        {
            return Err(fail(format!(
                "OBS WebSocket does not advertise support for request '{name}'"
            )));
        }
        let (generation, revision) = self.stamp.ok_or_else(changed)?;
        let fence = self.context.fence.clone();
        let cancel = self.context.cancellation.clone();
        let deadline = self.context.deadline;
        let result = self
            .context
            .handle
            .request_checked(
                self.context.handle.token(),
                name,
                data,
                deadline.saturating_duration_since(Instant::now()),
                move || {
                    if cancel.is_cancelled()
                        || Instant::now() >= deadline
                        || !fence.ready_at_revision(generation, revision)
                    {
                        Err("Capture read evidence expired".into())
                    } else {
                        Ok(())
                    }
                },
            )
            .await;
        // Even cancellation cannot drop an admitted observer. Synchronization and
        // result checks happen only after that same observer has settled.
        self.context
            .reader
            .synchronize(self.context.deadline)
            .await
            .map_err(|_| changed())?;
        let observation = self.check();
        // A real RPC refusal, capacity error or transport failure cannot be
        // hidden by a concurrent revision change. Only a successful read or
        // this reader's pre-send fence may retire a read sample for resampling.
        if !matches!(
            &result,
            Ok(_)
                | Err(RequestError::BeforeDispatch(
                    crate::session::BeforeDispatch::GuardRefused(_)
                ))
        ) {
            self.revision_lost.store(false, Ordering::Release);
        }
        observation?;
        let value = result.map_err(|e| {
            fail(
                e.obs_failure_message(name)
                    .unwrap_or_else(|| "Capture read outcome is unavailable".into()),
            )
        })?;
        domain::bounded(&[&value])?;
        self.bytes = self
            .bytes
            .checked_add(
                crate::js_json::encoded_len(
                    &value,
                    crate::js_json::Limits {
                        max_depth: domain::MAX_DEPTH,
                        max_encoded_bytes: domain::MAX_BYTES,
                    },
                )
                .map_err(|_| fail("Capture read exceeds native limits"))?,
            )
            .filter(|n| *n <= domain::MAX_BYTES)
            .ok_or_else(|| fail("Capture cumulative read bytes exceed native limits"))?;
        Ok(value)
    }
    async fn collect(
        &mut self,
        request: &Request,
        platform: Option<&str>,
        expected: Option<&Discovery>,
    ) -> Result<Discovery, Failure> {
        self.begin().await?;
        // Reject replacement identity before inspecting its settings or native list.
        let inputs = self.read(ReadRequest::InputList, json!({})).await?;
        if let Some(old) = expected {
            old.compare_identity(&inputs)?;
        }
        let scenes = self.read(ReadRequest::SceneList, json!({})).await?;
        let scene = domain::scene(request, &scenes)?;
        let advertised = if platform == Some("macos") {
            vec![]
        } else {
            let kinds = self.read(ReadRequest::InputKindList, json!({})).await?;
            kinds["inputKinds"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        };
        let rows = domain::inputs(&inputs)?;
        let existing = domain::found(rows, request.input_name());
        let kind = domain::resolve_kind(request, platform, &advertised, existing)?;
        let existing = existing.map(InputIdentity::parse).transpose()?;
        let membership = if let Some(id) = &existing {
            let v = self
                .read(ReadRequest::SceneItemList, scene.projection())
                .await?;
            domain::membership(id, &v)?;
            v
        } else {
            json!({"sceneItems":[]})
        };
        let identities = if let Some(old) = expected {
            vec![old.inventory().clone()]
        } else if let Some(id) = &existing {
            vec![id.clone()]
        } else {
            rows.iter()
                .filter(|r| r["inputKind"] == kind.name())
                .filter_map(|r| InputIdentity::parse(r).ok())
                .collect::<Vec<_>>()
        };
        let mut candidates = Vec::new();
        for id in identities {
            let read = self.read(ReadRequest::InputSettings, id.projection()).await;
            let settings = match read {
                Ok(value) => value,
                Err(error) => {
                    self.check()?;
                    if existing.is_some() {
                        return Err(error);
                    }
                    continue;
                }
            };
            if let Err(error) =
                domain::settings(kind, &settings).and_then(|s| domain::inventory_guard(kind, s))
            {
                if existing.is_some() {
                    return Err(error);
                }
                continue;
            }
            let properties = if kind.portal() {
                json!({})
            } else {
                self.properties(&id, &settings).await?
            };
            candidates.push((id, settings, properties));
            break;
        }
        let borrowed = candidates
            .iter()
            .map(|(id, settings, properties)| Candidate {
                input_uuid: id.uuid(),
                settings,
                properties,
            })
            .collect::<Vec<_>>();
        let readings = Readings {
            scenes: &scenes,
            inputs: &inputs,
            membership: &membership,
            candidates: &borrowed,
        };
        if let Some(old) = expected {
            old.compare(readings)?;
            Ok(old.clone())
        } else {
            domain::discover(request.clone(), platform, &advertised, readings)
        }
    }
}

pub(crate) async fn plan<F: FnOnce() -> CaptureMetadata + Send>(
    reads: snapshot_tools::Context,
    request: &Request,
    platform: Option<&str>,
    metadata: F,
) -> Planned {
    let result = async {
        if let Some(error) = request
            .selector_refusal()
            .or_else(|| request.platform_refusal(platform))
        {
            return Err(error);
        }
        let mut reader = Reader::new(reads.clone())?;
        let original = reader.collect(request, platform, None).await?;
        reader.collect(request, platform, Some(&original)).await?;
        if !request.apply() {
            return Ok(Planned::Finished(original.plan_result()?));
        }
        let mut ready = Box::new(Ready {
            expected: original.clone(),
            original,
            snapshot: None,
            target_settings: None,
        });
        if ready.original.existing().is_some() {
            // The entire snapshot capture is inside the discovery stamp. ABA events
            // invalidate it even when names/settings later look identical.
            let snapshot = snapshot_tools::capture_window(reads, request, metadata)
                .await
                .map_err(|e| fail(e.tool_message()))?;
            reader.check()?;
            reader
                .collect(request, platform, Some(&ready.original))
                .await?;
            ready.snapshot = Some(snapshot.id.clone());
            return Ok(Planned::Save(ready, Box::new(snapshot)));
        }
        Ok(Planned::Ready(ready))
    }
    .await;
    result.unwrap_or_else(|e: Failure| Planned::Finished(e.result()))
}
fn evidence(
    mut result: Value,
    ready: &Ready,
    facts: &[(CaptureStep, CaptureCounts)],
    uncertain: bool,
) -> Value {
    if result["isError"] == true && !facts.is_empty() {
        // Discovery's pre-effect refusals say no capture changed. Reusing that
        // assertion after a known ACK would be false even with honest metadata.
        if let Some(rows) = result["content"].as_array_mut() {
            for row in rows {
                if let Some(text) = row["text"].as_str() {
                    let text=text.replace("No capture changed","Earlier effects were acknowledged; no automatic rollback was attempted").replace("no capture changed","earlier effects were acknowledged; no automatic rollback was attempted");
                    row["text"] = json!(text);
                }
            }
        }
    }
    if !result["structuredContent"].is_object() {
        result["structuredContent"] = json!({});
    }
    let facts = json!({"snapshotId":ready.snapshot,"knownSteps":facts.iter().map(|(step,c)|json!({"stage":step,"replyAcknowledged":true,"successful":c.successful,"failed":c.failed,"skipped":c.skipped})).collect::<Vec<_>>(),"receiptUncertain":uncertain,"postconditionObserved":false,"renderedTarget":"unverified"});
    result["structuredContent"]["captureEvidence"] = facts.clone();
    match domain::report(result) {
        Ok(value) => value,
        Err(error) => {
            // The fixed metadata is independently bounded (three tiny stage counts and
            // the validated internal snapshot ID). Oversized peer diagnostics/images
            // must not erase known effects or the persisted snapshot.
            let mut fallback = error.result();
            fallback["structuredContent"] = json!({"captureEvidence":facts});
            domain::report(fallback).unwrap_or_else(|error| error.result())
        }
    }
}
pub(crate) fn refusal(ready: &Ready, message: &str) -> Value {
    evidence(fail(message).result(), ready, &[], false)
}

/// A separately admitted image is never an arbitrary Value/template bypass.
/// Only screenshot_content can construct these private fields; final assembly
/// accepts text-only bounded reports plus at most this one checked image.
struct Picture {
    data: String,
    mime: String,
    bytes: usize,
    limit: crate::scene_audit::ScreenshotLimit,
}
impl Picture {
    fn checked(
        response: Value,
        limit: crate::scene_audit::ScreenshotLimit,
    ) -> Result<Self, Failure> {
        let image = crate::scene_audit::screenshot_content(&response, limit)
            .map_err(|e| fail(e.message))?;
        let Value::Object(mut image) = image else {
            return Err(fail("Capture screenshot admission unavailable"));
        };
        let Some(Value::String(data)) = image.remove("data") else {
            return Err(fail("Capture screenshot admission unavailable"));
        };
        let Some(Value::String(mime)) = image.remove("mimeType") else {
            return Err(fail("Capture screenshot admission unavailable"));
        };
        let padding = data.bytes().rev().take_while(|b| *b == b'=').count();
        let bytes = data.len() / 4 * 3 - padding;
        Ok(Self {
            data,
            mime,
            bytes,
            limit,
        })
    }
}
struct Output {
    report: Value,
    picture: Option<Picture>,
}
fn assemble(mut report: Value, picture: Option<Picture>) -> Result<Value, Failure> {
    // The ordinary report cap remains independent of encoded image payload bytes.
    // Reject all other content kinds/schema additions before the single image is
    // attached; no caller can substitute arbitrary frames or extra images.
    report = domain::report(report)?;
    let fields = report
        .as_object()
        .ok_or_else(|| fail("Capture report admission unavailable"))?;
    if fields
        .keys()
        .any(|k| !matches!(k.as_str(), "content" | "structuredContent" | "isError"))
        || report["content"].as_array().is_none_or(|rows| {
            rows.iter().any(|row| {
                row.as_object().is_none_or(|r| {
                    r.len() != 2
                        || r.get("type").and_then(Value::as_str) != Some("text")
                        || !r.get("text").is_some_and(Value::is_string)
                })
            })
        })
    {
        return Err(fail("Capture report admission unavailable"));
    }
    if let Some(picture) = picture {
        let maximum = domain::MAX_REPORT_BYTES
            .checked_add(
                picture
                    .limit
                    .bytes()
                    .div_ceil(3)
                    .checked_mul(4)
                    .ok_or_else(|| fail("Capture image envelope exceeds native limits"))?,
            )
            .and_then(|n| n.checked_add(128))
            .ok_or_else(|| fail("Capture image envelope exceeds native limits"))?;
        report["content"]
            .as_array_mut()
            .unwrap()
            .push(json!({"type":"image","data":picture.data,"mimeType":picture.mime}));
        crate::js_json::encoded_len(
            &report,
            crate::js_json::Limits {
                max_depth: domain::MAX_DEPTH,
                max_encoded_bytes: maximum,
            },
        )
        .map_err(|_| fail("Capture image envelope exceeds native limits"))?;
    }
    Ok(report)
}

struct Owner {
    store: Option<OperationStore>,
    receipt: Option<CaptureReceipt>,
    facts: Vec<(CaptureStep, CaptureCounts)>,
    poisoned: bool,
}
#[derive(Clone, Copy)]
enum SampleMode {
    Strict,
    AfterAcknowledgedEffect,
}
impl Owner {
    fn sample_mode(&self) -> SampleMode {
        if self.facts.is_empty() {
            SampleMode::Strict
        } else {
            SampleMode::AfterAcknowledgedEffect
        }
    }
    fn update(&mut self, update: CaptureUpdate, context: &Context) -> Result<(), Failure> {
        let old = self
            .receipt
            .as_ref()
            .ok_or_else(|| fail("Capture receipt unavailable"))?;
        match self
            .store
            .as_mut()
            .ok_or_else(|| fail("Capture effect writer unavailable"))?
            .update_capture(old, update, &(context.clock)().utc)
        {
            Ok(r) => {
                self.receipt = Some(r);
                Ok(())
            }
            Err(_) => {
                self.poisoned = true;
                if let Some(store) = &self.store {
                    store.read_view().mark_uncertain();
                }
                Err(fail(
                    "Capture receipt could not be published; inspect obs-observe before another change",
                ))
            }
        }
    }
    async fn dispatch(
        &mut self,
        step: CaptureStep,
        effects: Vec<Effect>,
        reader: &Reader,
        context: &Context,
    ) -> Result<Value, Failure> {
        reader.check()?;
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| fail("Capture effect writer unavailable"))?;
        let fence = store
            .dispatch_fence()
            .ok_or_else(|| fail("Capture receipt custody unavailable"))?;
        if fence.is_capture_window() != Ok(true)
            || store.endpoint_hash() != context.reads.handle.endpoint_hash()
        {
            return Err(fail("Capture receipt binding unavailable"));
        }
        if policy(
            &context.request,
            &context.caller,
            &context.control,
            &context.clock,
        )
        .map_err(|_| fail("Capture policy unavailable"))?
            != Disposition::Allow
        {
            return Err(fail("Capture policy refused continuation"));
        }
        let expected_count = match step {
            CaptureStep::Audio => 2,
            _ => 1,
        };
        if effects.len() != expected_count
            || self.receipt.as_ref().and_then(|r| r.next_step()) != Some(step)
        {
            return Err(fail("Capture stage does not match closed plan"));
        }
        let projections = effects
            .into_iter()
            .map(|e| e.projection())
            .collect::<Vec<_>>();
        let methods = projections
            .iter()
            .map(|p| p["requestType"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        if context
            .reads
            .capabilities
            .as_ref()
            .is_some_and(|c| methods.iter().any(|m| !c.contains(m)))
        {
            return Err(fail("Capture effect capability unavailable"));
        }
        self.update(CaptureUpdate::Dispatch(step), context)?;
        let cancellation = context.reads.cancellation.clone();
        let deadline = context.reads.deadline;
        let control = context.control.clone();
        let clock = context.clock.clone();
        let request = context.request.clone();
        let caller = context.caller.clone();
        let id = context.operation_id.clone();
        let observation = context.reads.fence.clone();
        let (generation, revision) = reader.stamp.ok_or_else(changed)?;
        let guard = move || {
            if cancellation.is_cancelled()
                || Instant::now() >= deadline
                || !observation.ready_at_revision(generation, revision)
            {
                return Err(
                    "Capture evidence/cancellation/deadline changed before dispatch".into(),
                );
            }
            if !fence.permits(&id).unwrap_or(false) {
                return Err("Capture receipt custody changed before dispatch".into());
            }
            if policy(&request, &caller, &control, &clock) != Ok(Disposition::Allow) {
                return Err("Capture policy changed before dispatch".into());
            }
            Ok(())
        };
        // Single initial/fit keep Source's single ACK semantics. Only the two fixed
        // audio setters form a halt-on-failure batch. No arbitrary method admission.
        let (response, counts, update, error) = if step == CaptureStep::Audio {
            let requests = projections
                .into_iter()
                .map(|mut p| BatchRequest {
                    request_type: p["requestType"].as_str().unwrap().into(),
                    request_data: p.as_object_mut().unwrap().remove("requestData"),
                })
                .collect();
            match context
                .reads
                .handle
                .batch_checked(
                    context.reads.handle.token(),
                    requests,
                    BatchOptions {
                        execution: BatchExecution::SerialRealtime,
                        halt_on_failure: true,
                        timeout: Some(deadline.saturating_duration_since(Instant::now())),
                    },
                    guard,
                )
                .await
            {
                Ok(rows) => {
                    let c = CaptureCounts {
                        successful: rows.iter().filter(|r| r.ok).count() as u8,
                        failed: rows.iter().filter(|r| !r.ok).count() as u8,
                        skipped: 2 - rows.len() as u8,
                    };
                    let statuses = rows
                        .iter()
                        .map(|r| Status {
                            request_type: r.request_type.clone(),
                            ok: r.ok,
                            code: r.code as i64,
                            comment: r.comment.clone(),
                        })
                        .collect::<Vec<_>>();
                    (
                        json!({}),
                        Some(c),
                        CaptureUpdate::Acknowledged(c),
                        domain::validate_silence(&statuses).err(),
                    )
                }
                Err(e) => {
                    let update = if matches!(e, RequestError::BeforeDispatch(_)) {
                        CaptureUpdate::NotDispatched
                    } else {
                        CaptureUpdate::Unknown
                    };
                    (
                        Value::Null,
                        None,
                        update,
                        Some(fail(
                            "Capture audio outcome unavailable; no continuation or retry was made",
                        )),
                    )
                }
            }
        } else {
            let mut projection = projections.into_iter().next().unwrap();
            let name = projection["requestType"].as_str().unwrap().to_owned();
            let data = projection
                .as_object_mut()
                .unwrap()
                .remove("requestData")
                .unwrap();
            match context
                .reads
                .handle
                .request_checked(
                    context.reads.handle.token(),
                    &name,
                    data,
                    deadline.saturating_duration_since(Instant::now()),
                    guard,
                )
                .await
            {
                Ok(value) => {
                    let c = CaptureCounts {
                        successful: 1,
                        failed: 0,
                        skipped: 0,
                    };
                    (value, Some(c), CaptureUpdate::Acknowledged(c), None)
                }
                Err(e) if e.rejection_code().is_some() => {
                    let c = CaptureCounts {
                        successful: 0,
                        failed: 1,
                        skipped: 0,
                    };
                    (
                        Value::Null,
                        Some(c),
                        CaptureUpdate::Acknowledged(c),
                        Some(fail(e.obs_failure_message(&name).unwrap())),
                    )
                }
                Err(e) => {
                    let update = if matches!(e, RequestError::BeforeDispatch(_)) {
                        CaptureUpdate::NotDispatched
                    } else {
                        CaptureUpdate::Unknown
                    };
                    (
                        Value::Null,
                        None,
                        update,
                        Some(fail(
                            "Capture effect outcome unavailable; no continuation or retry was made",
                        )),
                    )
                }
            }
        };
        // No cancellation branch may run between admission and this exact observer's
        // settlement. ACK facts remain reportable even if durable publication fails.
        if update == CaptureUpdate::Unknown {
            self.poisoned = true;
        }
        if let Some(c) = counts {
            self.facts.push((step, c));
        }
        let published = self.update(update, context);
        if published.is_err()
            && let Some(c) = counts
        {
            return Err(fail(format!(
                "Capture {step:?} reply acknowledged: {} successful, {} failed, {} skipped; receipt publication uncertain",
                c.successful, c.failed, c.skipped
            )));
        }
        published?;
        if let Some(error) = error {
            return Err(error);
        }
        Ok(response)
    }
}

pub(crate) async fn apply(
    store: Option<OperationStore>,
    mut ready: Box<Ready>,
    context: Context,
    platform: Option<String>,
) -> Execution {
    let mut owner = Owner {
        store,
        receipt: None,
        facts: Vec::new(),
        poisoned: false,
    };
    let result = run(&mut owner, &mut ready, &context, platform.as_deref()).await;
    if owner.receipt.as_ref().is_some_and(|r| {
        matches!(
            r.phase(),
            CapturePhase::Prepared | CapturePhase::Continuation
        )
    }) {
        let update = if result.is_ok() && owner.receipt.as_ref().unwrap().next_step().is_none() {
            CaptureUpdate::Close
        } else {
            CaptureUpdate::Abort
        };
        if owner.update(update, &context).is_err() {
            owner.poisoned = true;
        }
    }
    let uncertain = owner.poisoned;
    let output = match result {
        Ok(output) => output,
        Err(error) => Output {
            report: error.result(),
            picture: None,
        },
    };
    let report = evidence(output.report, &ready, &owner.facts, uncertain);
    let mut result = assemble(report, output.picture)
        .unwrap_or_else(|error| evidence(error.result(), &ready, &owner.facts, uncertain));
    if uncertain {
        result["isError"] = json!(true);
    }
    Execution {
        store: owner.store,
        result,
        broken: uncertain || context.reads.handle.invalidation().is_some(),
    }
}
async fn run(
    owner: &mut Owner,
    ready: &mut Ready,
    context: &Context,
    platform: Option<&str>,
) -> Result<Output, Failure> {
    let mut reader = Reader::new(context.reads.clone())?;
    // Includes a fresh post-publication check and does not legitimize replacement
    // identities by reading their properties first.
    fresh(
        &mut reader,
        ready,
        &context.request,
        platform,
        SampleMode::Strict,
    )
    .await?;
    let plan = ready.effect_plan();
    if plan.has_effects() {
        let store = owner
            .store
            .as_mut()
            .ok_or_else(|| fail("Capture effect writer unavailable"))?;
        let fence = store
            .dispatch_fence()
            .ok_or_else(|| fail("Capture custody unavailable"))?;
        if fence.is_capture_window() != Ok(true) || fence.admission().map_or(true, |a| a.blocking) {
            return Err(fail(
                "Unresolved receipt prevents Capture Window; inspect obs-observe",
            ));
        }
        owner.receipt = Some(
            match store.prepare_capture(
                &context.operation_id,
                context.reads.handle.token().generation(),
                plan,
                &(context.clock)().utc,
            ) {
                Ok(receipt) => receipt,
                Err(_) => {
                    owner.poisoned = true;
                    store.read_view().mark_uncertain();
                    return Err(fail("Capture receipt could not be prepared"));
                }
            },
        );
    }
    let target = if let Some(effect) = ready.original.initial_intent() {
        let response = owner
            .dispatch(CaptureStep::Initial, vec![effect], &reader, context)
            .await?;
        let target = match ready.original.existing() {
            Some(id) => id.clone(),
            None => ready.original.created_target(&response)?,
        };
        // Our own write changes the observation revision. Rebind only after its ACK,
        // then prove exact target/scene/member and expected overlay before more writes.
        reader.begin().await?;
        let inputs = reader.read(ReadRequest::InputList, json!({})).await?;
        if domain::found(domain::inputs(&inputs)?, target.name())
            .map(InputIdentity::parse)
            .transpose()?
            .as_ref()
            != Some(&target)
        {
            return Err(changed());
        }
        let settings = reader
            .read(ReadRequest::InputSettings, target.projection())
            .await?;
        let map = domain::settings(target.kind(), &settings)?;
        let overlay = ready.original.selected_overlay();
        if overlay
            .as_object()
            .unwrap()
            .iter()
            .any(|(k, v)| map.get(k) != Some(v))
        {
            return Err(changed());
        }
        if ready.original.existing().is_some() {
            let mut expected = ready.original.settings_value().clone();
            for (k, v) in overlay.as_object().unwrap() {
                expected[k] = v.clone();
            }
            if settings["inputSettings"] != expected {
                return Err(changed());
            }
        }
        let scenes = reader.read(ReadRequest::SceneList, json!({})).await?;
        if domain::scene(&context.request, &scenes)? != *ready.original.scene() {
            return Err(changed());
        }
        let membership = reader
            .read(
                ReadRequest::SceneItemList,
                ready.original.scene().projection(),
            )
            .await?;
        let members = domain::membership(&target, &membership)?;
        if ready.original.existing().is_some() && members != ready.original.membership() {
            return Err(changed());
        }
        ready.target_settings = Some(settings["inputSettings"].clone());
        ready.expected = ready.original.after_initial(
            target.clone(),
            settings["inputSettings"].clone(),
            members,
        );
        target
    } else {
        ready.original.existing().ok_or_else(changed)?.clone()
    };
    fresh(
        &mut reader,
        ready,
        &context.request,
        platform,
        owner.sample_mode(),
    )
    .await?;
    if plan.audio {
        owner
            .dispatch(
                CaptureStep::Audio,
                ready.original.audio_intents(&target)?,
                &reader,
                context,
            )
            .await?;
        fresh(
            &mut reader,
            ready,
            &context.request,
            platform,
            owner.sample_mode(),
        )
        .await?;
    }
    let mut fitted = None;
    if plan.fit {
        let video = reader.read(ReadRequest::VideoSettings, json!({})).await?;
        let item=reader.read(ReadRequest::SceneItemId,json!({"sceneName":ready.original.scene().name(),"sceneUuid":ready.original.scene().uuid(),"sourceName":target.name(),"sourceUuid":target.uuid()})).await?;
        // Fit's name-based ID read never authorizes a transform without exact current
        // UUID membership and scene evidence from the same stable semantic sample.
        let scenes = reader.read(ReadRequest::SceneList, json!({})).await?;
        let members = reader
            .read(
                ReadRequest::SceneItemList,
                ready.original.scene().projection(),
            )
            .await?;
        let effect = ready
            .original
            .fit_intent(&target, &scenes, &members, &video, &item)?
            .ok_or_else(changed)?;
        owner
            .dispatch(CaptureStep::Fit, vec![effect], &reader, context)
            .await?;
        fitted = Some(domain::Canvas::parse(&video)?);
    }
    // Final readback may be unavailable; the receipt retains prior ACKs and never
    // claims a rendered target. No replacement is inspected by settings/screenshot.
    fresh(
        &mut reader,
        ready,
        &context.request,
        platform,
        owner.sample_mode(),
    )
    .await?;
    let state = readback(
        &mut reader,
        ready,
        &target,
        context,
        platform,
        owner.sample_mode(),
    )
    .await?;
    let silence = if plan.audio {
        vec![
            Status {
                request_type: "SetInputMute".into(),
                ok: true,
                code: 100,
                comment: None,
            },
            Status {
                request_type: "SetInputAudioTracks".into(),
                ok: true,
                code: 100,
                comment: None,
            },
        ]
    } else {
        vec![]
    };
    let mut result = ready
        .original
        .result(ready.snapshot.as_deref(), &state, &silence, fitted)?;
    let mut picture = None;
    if context.request.include_screenshot() {
        let sample_mode = owner.sample_mode();
        let screenshot=async {
            fresh(&mut reader,ready,&context.request,platform,sample_mode).await?;
            let response=reader.read(ReadRequest::SourceScreenshot,json!({"sourceName":target.name(),"sourceUuid":target.uuid(),"imageFormat":"jpeg","imageWidth":960})).await?;
            let limit=context.screenshot_limit.clone().map_err(|e|fail(e.message))?;
            Picture::checked(response,limit)
        }.await;
        match screenshot {
            Ok(image) => {
                result["content"].as_array_mut().unwrap().push(json!({"type":"text","text":format!("OBS screenshot ({}, {} bytes)",image.mime,image.bytes)}));
                picture = Some(image);
            }
            Err(error) => result["content"]
                .as_array_mut()
                .unwrap()
                .push(json!({"type":"text","text":format!("No screenshot: {}",error.message())})),
        }
    }
    Ok(Output {
        report: domain::report(result)?,
        picture,
    })
}

async fn fresh(
    reader: &mut Reader,
    ready: &Ready,
    request: &Request,
    platform: Option<&str>,
    mode: SampleMode,
) -> Result<(), Failure> {
    // Late events from our own acknowledged effect may invalidate one sample.
    // Retire only that read sample: rerun the complete expected-state proof,
    // retain cumulative read/byte budgets and the original deadline, and never
    // redispatch a mutation. A stable mismatching value still refuses at once.
    for _ in 0..3 {
        let result = fresh_sample(reader, ready, request, platform).await;
        if result.is_ok()
            || matches!(mode, SampleMode::Strict)
            || !reader.revision_lost.load(Ordering::Acquire)
        {
            return result;
        }
    }
    fresh_sample(reader, ready, request, platform).await
}
async fn fresh_sample(
    reader: &mut Reader,
    ready: &Ready,
    request: &Request,
    platform: Option<&str>,
) -> Result<(), Failure> {
    reader
        .collect(request, platform, Some(&ready.expected))
        .await?;
    if let (Some(target), Some(expected)) = (ready.expected.existing(), &ready.target_settings) {
        let actual = reader
            .read(ReadRequest::InputSettings, target.projection())
            .await?;
        domain::settings(target.kind(), &actual)?;
        if actual["inputSettings"] != *expected {
            return Err(changed());
        }
    }
    Ok(())
}
async fn readback(
    reader: &mut Reader,
    ready: &Ready,
    target: &InputIdentity,
    context: &Context,
    platform: Option<&str>,
    mode: SampleMode,
) -> Result<Value, Failure> {
    let mut settings = Value::Null;
    let mut appearances = vec![];
    let mut warnings = vec![];
    for attempt in 0..4 {
        // Every settle attempt checks target identity before program/source reads.
        fresh(reader, ready, &context.request, platform, mode).await?;
        settings = reader
            .read(ReadRequest::InputSettings, target.projection())
            .await?;
        domain::settings(target.kind(), &settings)?;
        let program = reader
            .read(ReadRequest::CurrentProgramScene, json!({}))
            .await?;
        appearances.clear();
        if let (Some(name), Some(uuid)) = (
            program["currentProgramSceneName"].as_str(),
            program["currentProgramSceneUuid"].as_str(),
        ) {
            let list = reader
                .read(
                    ReadRequest::SceneItemList,
                    json!({"sceneName":name,"sceneUuid":uuid}),
                )
                .await?;
            for row in list["sceneItems"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|r| r["sourceName"] == target.name() && r["sourceUuid"] == target.uuid())
            {
                appearances.push(json!({"sceneName":name,"sceneItemId":row["sceneItemId"],"sceneItemEnabled":row["sceneItemEnabled"]!=false,"sourceWidth":row["sceneItemTransform"]["sourceWidth"].as_f64().unwrap_or(0.0),"sourceHeight":row["sceneItemTransform"]["sourceHeight"].as_f64().unwrap_or(0.0)}));
            }
        }
        let blank = appearances.iter().any(|r| {
            r["sceneItemEnabled"] == true && (r["sourceWidth"] == 0 || r["sourceHeight"] == 0)
        });
        if !blank {
            break;
        }
        if attempt < 3 {
            tokio::select! {_=reader.context.cancellation.cancelled()=>return Err(fail("Capture Window cancelled during size settling")),_=tokio::time::sleep_until(reader.context.deadline.min(Instant::now()+Duration::from_millis(250)))=>{}}
            reader.begin().await?;
        } else {
            warnings.push(format!("{} renders at 0×0 in the program scene. A capture source usually needs a display, window, or application selected (macOS screen_capture application capture also needs display_uuid) and screen recording permission for OBS",target.name()));
        }
    }
    Ok(
        json!({"inputSettings":settings["inputSettings"],"appearances":appearances,"warnings":warnings}),
    )
}

#[cfg(test)]
mod assembly_tests {
    use super::*;
    fn picture() -> Picture {
        Picture::checked(
            json!({"imageData":"data:image/jpeg;base64,YQ=="}),
            crate::scene_audit::ScreenshotLimit::default(),
        )
        .unwrap()
    }
    #[test]
    fn image_lane_rejects_arbitrary_report_content_and_second_image_before_assembly() {
        for value in [
            json!({"content":[{"type":"image","data":"YQ==","mimeType":"image/jpeg"}]}),
            json!({"content":[{"type":"text","text":"safe","extra":true}]}),
            json!({"content":[{"type":"text","text":"safe"}],"frame":{"method":"arbitrary"}}),
        ] {
            assert!(assemble(value, Some(picture())).is_err());
        }
        let output=assemble(json!({"content":[{"type":"text","text":"safe"}],"structuredContent":{"postconditionObserved":false}}),Some(picture())).unwrap();
        assert_eq!(
            output["content"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|r| r["type"] == "image")
                .count(),
            1
        );
    }
    #[test]
    fn separate_image_lane_cannot_expand_escaped_diagnostic_budget_or_payload_preference() {
        assert!(
            assemble(
                json!({"content":[{"type":"text","text":"\u{0001}".repeat(200_000)}]}),
                Some(picture())
            )
            .is_err()
        );
        assert!(
            Picture::checked(
                json!({"imageData":"data:image/jpeg;base64,YWFh"}),
                crate::scene_audit::ScreenshotLimit::parse(Some("1")).unwrap()
            )
            .is_err()
        );
    }
}
