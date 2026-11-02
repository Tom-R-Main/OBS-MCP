// SPDX-License-Identifier: GPL-2.0-only
//! One retained, closed two-stage effect owner. It owns no socket, task, file,
//! recording ticket or public route. Service must retain/finish the same owner;
//! Drop is emergency revocation, not an awaited shutdown or automatic abort.
use crate::{
    apply_scene::{Effect, Request as SceneRequest},
    control::{Disposition, GateRequest},
    custody::{Clock, SharedControl},
    operation_store::{
        ApplyCounts, ApplyPhase, ApplyReceipt, ApplyUpdate, OperationStore, StoreError,
    },
    session::{
        BatchOptions, BatchRequest, BatchResult, BeforeDispatch, ConnectionToken, Execution,
        RequestError, SessionHandle,
    },
    stage_transport::Cancellation,
};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;
#[derive(Clone)]
pub struct Capabilities {
    connection: ConnectionToken,
    requests: HashSet<String>,
}
impl Capabilities {
    pub fn from_version(session: &SessionHandle, version: &Value) -> Option<Self> {
        Some(Self {
            connection: session.token(),
            requests: version
                .get("availableRequests")?
                .as_array()?
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
        })
    }
}
pub struct Start {
    pub operation_id: String,
    pub request: SceneRequest,
    pub caller: String,
    pub creations: Vec<Effect>,
    pub capabilities: Option<Capabilities>,
    pub cancellation: Cancellation,
    pub admitted_at: Instant,
    pub timeout: Duration,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Creations,
    Changes,
}
#[derive(Debug)]
pub enum BeginError {
    Busy,
    Binding,
    Empty,
    InvalidEffect,
    InvalidRequest,
    Deadline,
    Cancelled,
    Unsupported(&'static str),
    Policy(Disposition),
    PolicyUnavailable,
    Receipt(StoreError),
    Uncertain,
    WrongPhase,
}
#[derive(Debug)]
pub enum FinishError {
    NoPending,
}
pub struct Outcome {
    pub stage: Stage,
    pub transport: Result<Vec<BatchResult>, RequestError>,
    pub receipt: ApplyReceipt,
    pub receipt_error: Option<StoreError>,
    pub policy_refusal: Option<Disposition>,
    /// Independent ACK facts survive a failure to publish the terminal receipt.
    /// None for BeforeDispatch or Unknown; no ACK/counts are fabricated.
    pub acknowledged: Option<ApplyCounts>,
    pub not_attempted: Option<u64>,
}
impl fmt::Debug for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApplySceneOutcome")
            .field("stage", &self.stage)
            .field("phase", &self.receipt.phase())
            .field("receipt_error", &self.receipt_error)
            .finish_non_exhaustive()
    }
}
struct Context {
    operation_id: String,
    request: SceneRequest,
    arguments: Value,
    caller: String,
    capabilities: Option<Capabilities>,
    cancellation: Cancellation,
    deadline: Instant,
}
impl Context {
    fn policy(&self, control: &SharedControl, clock: &Clock) -> Result<Disposition, ()> {
        Ok(control.lock().map_err(|_| ())?.evaluate(
            GateRequest {
                tool_name: "obs-apply-scene",
                arguments: &self.arguments,
                read_only_hint: false,
                caller: &self.caller,
                confirmation: Default::default(),
            },
            clock().epoch_ms,
        ))
    }
}
type Observer = Pin<Box<dyn Future<Output = Result<Vec<BatchResult>, RequestError>> + Send>>;
struct Pending {
    stage: Stage,
    count: u64,
    observer: Observer,
    refusal: Arc<Mutex<Option<Disposition>>>,
}
struct Workflow {
    context: Arc<Context>,
    receipt: ApplyReceipt,
}
pub struct ApplySceneOwner {
    store: OperationStore,
    session: SessionHandle,
    control: SharedControl,
    clock: Clock,
    workflow: Option<Workflow>,
    pending: Option<Pending>,
    poisoned: bool,
}
impl fmt::Debug for ApplySceneOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApplySceneOwner")
            .field("pending", &self.pending.is_some())
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}
fn creation(effect: &Effect) -> bool {
    matches!(
        effect,
        Effect::CreateScene { .. } | Effect::CreateInput { .. } | Effect::CreateItem { .. }
    )
}
fn exact(a: &Value, b: &Value) -> bool {
    let mut a = a.clone();
    let mut b = b.clone();
    crate::js_json::normalize_json_numbers(&mut a).is_ok()
        && crate::js_json::normalize_json_numbers(&mut b).is_ok()
        && a == b
}
/// Subjects/values remain tied to the original typed Source request; observed
/// item IDs and canvas dimensions still require the composing Service's reads.
fn valid(effect: &Effect, request: &SceneRequest, args: &Value) -> bool {
    let scene = request.scene_name();
    let source = |name: &str| {
        args["sources"]
            .as_array()
            .and_then(|rows| rows.iter().find(|row| row["name"].as_str() == Some(name)))
    };
    let item = |name: &str, id: f64| name == scene && id.is_finite();
    let named = |name: &str| source(name).is_some();
    let enabled = |name: &str, wanted: bool| {
        source(name)
            .is_some_and(|s| s.get("visible").and_then(Value::as_bool).unwrap_or(true) == wanted)
    };
    match effect {
        Effect::CreateScene{scene_name}=>scene_name==scene,
        Effect::CreateInput{scene_name,input_name,kind,settings,enabled:wanted}=>scene_name==scene&&source(input_name).is_some_and(|s|!kind.is_empty()&&s["kind"].as_str()==Some(kind.as_str())&&exact(settings,s.get("settings").unwrap_or(&json!({}))))&&enabled(input_name,*wanted),
        Effect::CreateItem{scene_name,input_name,enabled:wanted}=>scene_name==scene&&named(input_name)&&enabled(input_name,*wanted),
        Effect::SettingsOverlay{input_name,settings}=>settings.is_object()&&source(input_name).and_then(|s|s.get("settings")).is_some_and(|wanted|exact(settings,wanted)),
        Effect::Mute{input_name,muted}=>source(input_name).is_some_and(|s|s["muted"].as_bool()==Some(*muted)),
        Effect::VolumeDb{input_name,db}=>db.is_finite()&&source(input_name).is_some_and(|s|s["volumeDb"].as_f64()==Some(*db)),
        Effect::AudioTracks{input_name,tracks}=>source(input_name).and_then(|s|s.get("audioTracks")).and_then(Value::as_array).is_some_and(|wanted|{
            let expected=(1u8..=6).map(|track|(track.to_string(),json!(wanted.iter().any(|n|n.as_u64()==Some(track as u64))))).collect::<serde_json::Map<_,_>>();*tracks==Value::Object(expected)
        }),
        Effect::Enabled{scene_name,id,enabled}=>item(scene_name,*id)&&request.sources().iter().any(|s|source(s.name()).is_some_and(|s|s["visible"].as_bool()==Some(*enabled))),
        Effect::Locked{scene_name,id,locked}=>item(scene_name,*id)&&request.sources().iter().any(|s|source(s.name()).is_some_and(|s|s["locked"].as_bool()==Some(*locked))),
        Effect::Transform{scene_name,id,transform}=>item(scene_name,*id)&&transform.as_object().is_some_and(|t|request.sources().iter().any(|s|{
            let spec=source(s.name()).expect("typed source");let mut wanted=if spec["fit"].as_str()==Some("canvas") {
                let Some(width)=t.get("boundsWidth").filter(|v|v.as_f64().is_some_and(f64::is_finite))else{return false;};let Some(height)=t.get("boundsHeight").filter(|v|v.as_f64().is_some_and(f64::is_finite))else{return false;};
                json!({"boundsType":"OBS_BOUNDS_SCALE_INNER","boundsWidth":width,"boundsHeight":height,"boundsAlignment":0,"alignment":5,"positionX":0,"positionY":0})
            }else{json!({})};
            if let Some(explicit)=spec["transform"].as_object(){for (k,v) in explicit {wanted[k]=v.clone();}}
            !t.is_empty()&&exact(transform,&wanted)
        })),
        Effect::RemoveItem{scene_name,id}=>item(scene_name,*id)&&args["removeOthers"].as_bool()==Some(true),
        Effect::Index{scene_name,id,..}=>item(scene_name,*id)&&args["order"].as_bool()==Some(true),
    }
}
impl ApplySceneOwner {
    pub fn new(
        store: OperationStore,
        session: SessionHandle,
        control: SharedControl,
        clock: Clock,
    ) -> Result<Self, BeginError> {
        if session.invalidation().is_some()
            || store.endpoint_hash() != session.endpoint_hash()
            || store
                .dispatch_fence()
                .is_none_or(|f| f.is_apply_scene() != Ok(true))
        {
            return Err(BeginError::Binding);
        }
        Ok(Self {
            store,
            session,
            control,
            clock,
            workflow: None,
            pending: None,
            poisoned: false,
        })
    }
    pub fn has_pending(&self) -> bool {
        self.pending.is_some()
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }
    pub fn receipt(&self) -> Option<&ApplyReceipt> {
        self.workflow.as_ref().map(|w| &w.receipt)
    }
    fn poison(&mut self, error: StoreError) -> BeginError {
        self.poisoned = true;
        self.store.read_view().mark_uncertain();
        BeginError::Receipt(error)
    }
    fn check(&self, context: &Context, effects: &[Effect], stage: Stage) -> Result<(), BeginError> {
        if self.poisoned {
            return Err(BeginError::Uncertain);
        }
        if self.session.invalidation().is_some()
            || self
                .store
                .dispatch_fence()
                .is_none_or(|f| f.is_apply_scene() != Ok(true))
        {
            return Err(BeginError::Binding);
        }
        if Instant::now() >= context.deadline {
            return Err(BeginError::Deadline);
        }
        match context.policy(&self.control, &self.clock) {
            Ok(Disposition::Allow) => {}
            Ok(no) => return Err(BeginError::Policy(no)),
            Err(()) => return Err(BeginError::PolicyUnavailable),
        }
        if context.cancellation.is_cancelled() {
            return Err(BeginError::Cancelled);
        }
        if effects.iter().any(|e| {
            creation(e) != (stage == Stage::Creations)
                || !valid(e, &context.request, &context.arguments)
        }) {
            return Err(BeginError::InvalidEffect);
        }
        if let Some(caps) = &context.capabilities {
            if caps.connection != self.session.token() {
                return Err(BeginError::Binding);
            }
            for effect in effects {
                if !caps.requests.contains(effect.request_type()) {
                    return Err(BeginError::Unsupported(effect.request_type()));
                }
            }
        }
        Ok(())
    }
    pub fn begin(&mut self, start: Start) -> Result<(), BeginError> {
        if self.pending.is_some() || self.workflow.is_some() {
            return Err(BeginError::Busy);
        }
        if !start.request.apply() {
            return Err(BeginError::InvalidRequest);
        }
        let names = start
            .request
            .sources()
            .iter()
            .map(|s| s.name())
            .collect::<HashSet<_>>();
        if names.len() != start.request.sources().len()
            || names.contains(start.request.scene_name())
        {
            return Err(BeginError::InvalidRequest);
        }
        let deadline = start
            .admitted_at
            .checked_add(start.timeout)
            .filter(|_| !start.timeout.is_zero() && start.admitted_at <= Instant::now())
            .ok_or(BeginError::Deadline)?;
        let context = Arc::new(Context {
            operation_id: start.operation_id,
            arguments: start.request.arguments(),
            request: start.request,
            caller: start.caller,
            capabilities: start.capabilities,
            cancellation: start.cancellation,
            deadline,
        });
        self.check(&context, &start.creations, Stage::Creations)?;
        if self
            .store
            .dispatch_fence()
            .ok_or(BeginError::Binding)?
            .admission()
            .map_or(true, |a| a.blocking)
        {
            return Err(BeginError::Uncertain);
        }
        if start.creations.len() > context.request.sources().len() + 1 {
            return Err(BeginError::InvalidEffect);
        }
        let mut unique = HashSet::new();
        let mut scene_seen = false;
        for effect in &start.creations {
            match effect {
                Effect::CreateScene { .. } => {
                    if scene_seen {
                        return Err(BeginError::InvalidEffect);
                    }
                    scene_seen = true;
                }
                Effect::CreateInput { input_name, .. } | Effect::CreateItem { input_name, .. } => {
                    if !unique.insert(input_name) {
                        return Err(BeginError::InvalidEffect);
                    }
                }
                _ => return Err(BeginError::InvalidEffect),
            }
        }
        let count = u64::try_from(start.creations.len()).map_err(|_| BeginError::InvalidEffect)?;
        let receipt = match self.store.prepare_apply(
            &context.operation_id,
            self.session.token().generation(),
            count,
            &(self.clock)().utc,
        ) {
            Ok(r) => r,
            Err(e) => return Err(self.poison(e)),
        };
        self.workflow = Some(Workflow { context, receipt });
        if start.creations.is_empty() {
            self.publish(ApplyUpdate::Ready)?;
            Ok(())
        } else {
            self.dispatch(start.creations, Stage::Creations)
        }
    }
    fn publish(&mut self, update: ApplyUpdate) -> Result<(), BeginError> {
        let receipt = self.receipt().ok_or(BeginError::WrongPhase)?.clone();
        match self
            .store
            .update_apply(&receipt, update, &(self.clock)().utc)
        {
            Ok(r) => {
                self.workflow.as_mut().unwrap().receipt = r;
                Ok(())
            }
            Err(e) => Err(self.poison(e)),
        }
    }
    /// Original request/caller/budget/cancellation remain owned from begin;
    /// callers cannot replace them between stages or grant another deadline.
    pub fn begin_changes(&mut self, effects: Vec<Effect>) -> Result<(), BeginError> {
        if self.pending.is_some() {
            return Err(BeginError::Busy);
        }
        if self
            .receipt()
            .is_none_or(|r| r.phase() != ApplyPhase::Continuation)
        {
            return Err(BeginError::WrongPhase);
        }
        if effects.is_empty() {
            return Err(BeginError::Empty);
        }
        let context = self.workflow.as_ref().unwrap().context.clone();
        self.check(&context, &effects, Stage::Changes)?;
        self.dispatch(effects, Stage::Changes)
    }
    fn dispatch(&mut self, effects: Vec<Effect>, stage: Stage) -> Result<(), BeginError> {
        let count = u64::try_from(effects.len()).map_err(|_| BeginError::InvalidEffect)?;
        self.publish(if stage == Stage::Creations {
            ApplyUpdate::DispatchCreations
        } else {
            ApplyUpdate::DispatchChanges(count)
        })?;
        let context = self.workflow.as_ref().unwrap().context.clone();
        let session = self.session.clone();
        let expected = session.token();
        let control = self.control.clone();
        let clock = self.clock.clone();
        let fence = self.store.dispatch_fence().ok_or(BeginError::Binding)?;
        let refusal = Arc::new(Mutex::new(None));
        let observed = refusal.clone();
        let observer = Box::pin(async move {
            let methods = effects.iter().map(Effect::request_type).collect::<Vec<_>>();
            let requests = effects
                .into_iter()
                .map(|effect| {
                    let mut body = effect.request();
                    BatchRequest {
                        request_type: effect.request_type().into(),
                        request_data: body.as_object_mut().and_then(|b| b.remove("requestData")),
                    }
                })
                .collect();
            let remaining = context
                .deadline
                .checked_duration_since(Instant::now())
                .filter(|v| !v.is_zero())
                .ok_or(RequestError::BeforeDispatch(BeforeDispatch::Deadline))?;
            session.batch_checked(expected,requests,BatchOptions{execution:Execution::SerialRealtime,halt_on_failure:stage==Stage::Creations,timeout:Some(remaining)},move||{
                if !context.request.apply(){return Err("Apply flag unavailable".into());}
                match context.policy(&control,&clock){Ok(Disposition::Allow)=>{},Ok(no)=>{if let Ok(mut saved)=observed.lock(){*saved=Some(no);}return Err("Apply Scene control policy changed before dispatch".into());},Err(())=>return Err("Apply Scene control policy unavailable".into())}
                if context.cancellation.is_cancelled()||Instant::now()>=context.deadline{return Err("Apply Scene cancelled or original deadline expired before dispatch".into());}
                if context.capabilities.as_ref().is_some_and(|caps|caps.connection!=expected||methods.iter().any(|method|!caps.requests.contains(*method))){return Err("Apply Scene capabilities changed before dispatch".into());}
                if !fence.permits(&context.operation_id).unwrap_or(false){return Err("Apply Scene receipt custody changed before dispatch".into());}Ok(())
            }).await
        });
        self.pending = Some(Pending {
            stage,
            count,
            observer,
            refusal,
        });
        Ok(())
    }
    pub async fn finish(&mut self) -> Result<Outcome, FinishError> {
        let pending = self.pending.as_mut().ok_or(FinishError::NoPending)?;
        let transport = pending.observer.as_mut().await;
        let pending = self.pending.take().expect("retained observer settled");
        let acknowledged = transport.as_ref().ok().map(|rows| {
            let successful = rows.iter().filter(|r| r.ok).count() as u64;
            ApplyCounts {
                successful,
                failed: rows.len() as u64 - successful,
                skipped: pending.count - rows.len() as u64,
            }
        });
        let not_attempted =
            matches!(transport, Err(RequestError::BeforeDispatch(_))).then_some(pending.count);
        let update = match (pending.stage, acknowledged, not_attempted) {
            (Stage::Creations, Some(c), _) => ApplyUpdate::CreationsAcknowledged(c),
            (Stage::Changes, Some(c), _) => ApplyUpdate::ChangesAcknowledged(c),
            (Stage::Creations, None, Some(_)) => ApplyUpdate::CreationsNotDispatched,
            (Stage::Changes, None, Some(_)) => ApplyUpdate::ChangesNotDispatched,
            (Stage::Creations, None, None) => ApplyUpdate::CreationsUnknown,
            (Stage::Changes, None, None) => ApplyUpdate::ChangesUnknown,
        };
        let receipt_error = self.publish(update).err().map(|error| match error {
            BeginError::Receipt(e) => e,
            _ => unreachable!("known workflow"),
        });
        let policy_refusal = pending.refusal.lock().ok().and_then(|mut v| v.take());
        Ok(Outcome {
            stage: pending.stage,
            transport,
            receipt: self.receipt().unwrap().clone(),
            receipt_error,
            policy_refusal,
            acknowledged,
            not_attempted,
        })
    }
    pub fn close(&mut self) -> Result<ApplyReceipt, BeginError> {
        self.end(ApplyUpdate::Close)
    }
    pub fn abort(&mut self) -> Result<ApplyReceipt, BeginError> {
        self.end(ApplyUpdate::Abort)
    }
    fn end(&mut self, update: ApplyUpdate) -> Result<ApplyReceipt, BeginError> {
        if self.pending.is_some() {
            return Err(BeginError::Busy);
        }
        if self.poisoned {
            return Err(BeginError::Uncertain);
        }
        let phase = self.receipt().ok_or(BeginError::WrongPhase)?.phase();
        if !matches!(
            (phase, update),
            (ApplyPhase::Continuation, ApplyUpdate::Close)
                | (
                    ApplyPhase::Prepared | ApplyPhase::Continuation,
                    ApplyUpdate::Abort
                )
        ) {
            return Err(BeginError::WrongPhase);
        }
        self.publish(update)?;
        Ok(self.receipt().unwrap().clone())
    }
    /// Refusal preserves the owner and exact observer/continuation custody.
    pub fn into_store(self) -> Result<OperationStore, Box<Self>> {
        if self.pending.is_some()
            || !self.poisoned
                && self.receipt().is_some_and(|r| {
                    matches!(
                        r.phase(),
                        ApplyPhase::Prepared
                            | ApplyPhase::DispatchingCreations
                            | ApplyPhase::Continuation
                            | ApplyPhase::DispatchingChanges
                    )
                })
        {
            Err(Box::new(self))
        } else {
            Ok(self.store)
        }
    }
}
