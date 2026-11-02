// SPDX-License-Identifier: GPL-2.0-only
//! Concrete stage adapters borrow one existing physical session's observer.
//! They own no socket, event consumer, reconnect loop or receipt writer. The
//! request-local relay is joined separately from the externally owned bridge
//! and session. Only StageMachine with its single writer may drive mutations.

use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::{sync::watch, task::JoinHandle, time::Instant};

use crate::{
    control::{
        CallerContext, ConfirmationAnswer, ConfirmationContext, Disposition, GateRequest,
        caller_name, client_can_elicit,
    },
    custody::{Clock, SharedControl},
    observation::{
        EvidenceState, ObservationError, ReadAdapter, ReadFuture, ReadRequest, Target, identity,
    },
    session::{ALL_EVENTS, BeforeDispatch, RequestError, SessionHandle},
    session_observation::{ObservationBridge, ObservationFence, SessionReader},
    stage::{AdmissionError, Mutation, MutationError, StageAdmission, StageMutator, StageReader},
};

type FutureResult<'a, T, E = ObservationError> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'a>>;

/// Stage fingerprints scene-item transforms. OBS excludes their high-volume
/// events from ALL_EVENTS, so a default subscription cannot fence queued writes.
pub const REQUIRED_STAGE_EVENTS: u64 = ALL_EVENTS | (1 << 19);

struct CancelState {
    cancelled: AtomicBool,
    notify: watch::Sender<bool>,
}
/// A monotonic request-scoped signal. Dropping a frontend does not implicitly
/// cancel service-owned work; its owner explicitly calls cancel when required.
#[derive(Clone)]
pub struct Cancellation(Arc<CancelState>);
impl Cancellation {
    pub fn new() -> Self {
        let (notify, _) = watch::channel(false);
        Self(Arc::new(CancelState {
            cancelled: AtomicBool::new(false),
            notify,
        }))
    }
    pub fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::Release);
        self.0.notify.send_replace(true);
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }
    /// Wait for the monotonic signal without spawning a watcher task. This is
    /// permission to stop a cancellable local wait, not a cleanup receipt for
    /// admitted OBS or process work.
    pub async fn cancelled(&self) {
        let mut signal = self.0.notify.subscribe();
        loop {
            if *signal.borrow_and_update() || self.is_cancelled() {
                return;
            }
            if signal.changed().await.is_err() {
                return;
            }
        }
    }
}
impl Default for Cancellation {
    fn default() -> Self {
        Self::new()
    }
}

enum CallKind {
    Stage(Target),
    Take(String),
}
/// Validated public arguments and an intrinsic fixed tool registration. There
/// is no caller-supplied tool name, readOnlyHint or OBS request type.
pub struct BoundCall {
    kind: CallKind,
    arguments: Value,
}
impl BoundCall {
    pub fn stage(target: Target) -> Result<Self, ObservationError> {
        target.validate()?;
        if target.canvas_uuid.is_some() {
            return Err(ObservationError::new(
                "capability_unavailable",
                "This workflow requires the default canvas: the pinned video-settings and transition contracts do not provide canvas-scoped operations",
            ));
        }
        let arguments = serde_json::to_value(&target).map_err(|_| invalid_binding())?;
        Ok(Self {
            kind: CallKind::Stage(target),
            arguments,
        })
    }
    pub fn take(stage_id: &str) -> Result<Self, ObservationError> {
        if !valid_uuid(stage_id) {
            return Err(invalid_binding());
        }
        Ok(Self {
            kind: CallKind::Take(stage_id.to_owned()),
            arguments: json!({"stageId":stage_id}),
        })
    }
    fn tool_name(&self) -> &'static str {
        match self.kind {
            CallKind::Stage(_) => "obs-stage-scene",
            CallKind::Take(_) => "obs-take-scene",
        }
    }
    fn matches(&self, call: StageAdmission<'_>) -> bool {
        match (&self.kind, call) {
            (CallKind::Stage(bound), StageAdmission::Stage(actual)) => {
                bound.scene_collection == actual.scene_collection
                    && bound.scene_name == actual.scene_name
                    && bound.canvas_uuid == actual.canvas_uuid
            }
            (CallKind::Take(bound), StageAdmission::Take { stage_id }) => bound == stage_id,
            _ => false,
        }
    }
    fn permits(&self, mutation: &Mutation) -> bool {
        matches!(
            (&self.kind, mutation),
            (CallKind::Stage(_), Mutation::SetPreview { .. })
                | (CallKind::Take(_), Mutation::Transition)
        )
    }
}

/// Identity/capability interpretation is resolved once from the same precedence
/// as the established server. Only the typed confirmation answer is retained;
/// unrelated response content and auth metadata are not copied into this owner.
pub struct CallerBinding {
    name: String,
    confirm_enabled: bool,
    can_elicit: bool,
    answer: Option<Value>,
    explicit_confirm: Option<bool>,
}
impl CallerBinding {
    pub fn resolve(
        context: CallerContext<'_>,
        legacy_capabilities: Option<&Value>,
        confirm_enabled: bool,
        input_responses: Option<&Value>,
        explicit_confirm: Option<bool>,
    ) -> Self {
        let can_elicit = client_can_elicit(context.envelope, legacy_capabilities);
        let name = caller_name(context);
        let answer = match ConfirmationAnswer::from_responses(input_responses) {
            ConfirmationAnswer::Missing => None,
            ConfirmationAnswer::Accepted(confirm) => {
                Some(json!({"confirm":{"action":"accept","content":{"confirm":confirm}}}))
            }
            ConfirmationAnswer::OtherResponse => Some(json!({"confirm":{"action":"decline"}})),
        };
        Self {
            name,
            confirm_enabled,
            can_elicit,
            answer,
            explicit_confirm,
        }
    }
    fn evaluate(
        &self,
        control: &mut crate::control::ControlDomain,
        call: &BoundCall,
        now_ms: u64,
    ) -> Disposition {
        control.evaluate(
            GateRequest {
                tool_name: call.tool_name(),
                arguments: &call.arguments,
                read_only_hint: false,
                caller: &self.name,
                confirmation: ConfirmationContext {
                    enabled: self.confirm_enabled,
                    can_elicit: self.can_elicit,
                    input_responses: self.answer.as_ref(),
                },
            },
            now_ms,
        )
    }
}

pub struct TransportContext<'a> {
    pub handle: SessionHandle,
    pub bridge: &'a ObservationBridge,
    pub control: SharedControl,
    pub clock: Clock,
    pub cancellation: Cancellation,
}

struct Scope {
    active: AtomicBool,
    cancellation: Cancellation,
    state: watch::Sender<EvidenceState>,
    generation: u64,
}
impl Scope {
    // Multiple publishers cannot roll back a newer revision/cancellation or
    // restore permission after disconnection. No lock crosses an await.
    fn publish(&self, source: EvidenceState, alive: bool) {
        self.state.send_modify(|current| {
            current.revision = current.revision.max(source.revision);
            current.connected &= source.connected
                && alive
                && self.active.load(Ordering::Acquire)
                && source.generation == self.generation;
            current.cancelled |= source.cancelled || self.cancellation.is_cancelled();
        });
    }
}

pub struct StageTransport<'a> {
    pub reader: TransportReader<'a>,
    pub mutator: TransportMutator,
}
impl<'a> StageTransport<'a> {
    pub fn bind(
        context: TransportContext<'a>,
        mut call: BoundCall,
        caller: CallerBinding,
    ) -> Result<Self, ObservationError> {
        let fence = context.bridge.fence();
        let source = context.bridge.evidence();
        let initial = *source.borrow();
        if !fence.full_profile()
            || context.handle.event_subscriptions() & REQUIRED_STAGE_EVENTS != REQUIRED_STAGE_EVENTS
        {
            return Err(ObservationError::new(
                "capability_unavailable",
                "Stage transport requires the full source invalidation profile and default plus scene-item-transform event subscriptions",
            ));
        }
        if fence.token() != context.handle.token()
            || fence.endpoint_hash() != context.handle.endpoint_hash()
            || context.handle.invalidation().is_some()
            || !initial.connected
            || initial.generation != context.handle.token().generation()
        {
            return Err(invalidated());
        }
        if caller.confirm_enabled
            && let Some(confirm) = caller.explicit_confirm
        {
            call.arguments["confirm"] = json!(confirm);
        }
        let mut local = initial;
        local.cancelled |= context.cancellation.is_cancelled();
        let (state, _) = watch::channel(local);
        let scope = Arc::new(Scope {
            active: AtomicBool::new(true),
            cancellation: context.cancellation,
            state,
            generation: initial.generation,
        });
        let mut global = source.clone();
        let pump_scope = scope.clone();
        let mut cancellation = pump_scope.cancellation.0.notify.subscribe();
        let relay = tokio::spawn(async move {
            loop {
                let source = *global.borrow_and_update();
                let alive = global.has_changed().is_ok();
                pump_scope.publish(source, alive);
                if !alive
                    || !source.connected
                    || source.cancelled
                    || pump_scope.cancellation.is_cancelled()
                    || !pump_scope.active.load(Ordering::Acquire)
                {
                    return;
                }
                tokio::select! { result = global.changed() => { if result.is_err() { pump_scope.publish(source,false); return; } }, _ = cancellation.changed() => {} }
            }
        });
        let reader = TransportReader {
            bridge: context.bridge,
            handle: context.handle.clone(),
            inner: context.bridge.reader(),
            global: source,
            scope: scope.clone(),
            relay: Some(relay),
        };
        let mutator = TransportMutator {
            handle: context.handle,
            fence,
            scope,
            control: context.control,
            clock: context.clock,
            call: Arc::new(call),
            caller: Arc::new(caller),
            receipt_guard: None,
            attempted: false,
            admitted: false,
            used: false,
        };
        Ok(Self { reader, mutator })
    }
    /// Joins only the request-local relay. The service separately joins the
    /// external ObservationBridge and ObsSession after all scopes are retired.
    pub async fn shutdown(self) -> Result<(), ObservationError> {
        self.reader.shutdown().await
    }
}

pub struct TransportReader<'a> {
    bridge: &'a ObservationBridge,
    handle: SessionHandle,
    inner: SessionReader,
    global: watch::Receiver<EvidenceState>,
    scope: Arc<Scope>,
    relay: Option<JoinHandle<()>>,
}
impl TransportReader<'_> {
    fn refresh(&self) {
        let source = *self.global.borrow();
        let alive = self.global.has_changed().is_ok() && self.handle.invalidation().is_none();
        self.scope.publish(source, alive);
    }
    pub async fn shutdown(mut self) -> Result<(), ObservationError> {
        self.scope.active.store(false, Ordering::Release);
        self.refresh();
        if let Some(task) = self.relay.take() {
            task.abort();
            match task.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(_) => return Err(invalidated()),
            }
        }
        Ok(())
    }
}
impl Drop for TransportReader<'_> {
    fn drop(&mut self) {
        self.scope.active.store(false, Ordering::Release);
        self.refresh();
        if let Some(task) = self.relay.take() {
            task.abort();
        }
    }
}
impl ReadAdapter for TransportReader<'_> {
    fn supports(&self, request: ReadRequest) -> Option<bool> {
        self.inner.supports(request)
    }
    fn read<'a>(
        &'a mut self,
        request: ReadRequest,
        data: Value,
        deadline: Instant,
    ) -> ReadFuture<'a> {
        Box::pin(async move {
            self.refresh();
            if self.cancelled() {
                return Err(cancelled());
            }
            if !self.scope.state.borrow().connected {
                return Err(invalidated());
            }
            // Cancellation/events fence delivery without dropping this healthy
            // admitted read. SessionReader retains its actual actor request.
            let reply = self.inner.read(request, data, deadline).await;
            // Rejected replies also need the publication barrier. In particular
            // code604 must not be tolerated using evidence from before an event
            // already received ahead of that rejection.
            let synchronized = self.bridge.synchronize(deadline).await;
            self.refresh();
            if self.cancelled() {
                return Err(cancelled());
            }
            synchronized?;
            reply
        })
    }
}
impl StageReader for TransportReader<'_> {
    fn endpoint_hash(&self) -> &str {
        self.handle.endpoint_hash()
    }
    fn connection_generation(&self) -> Option<u64> {
        self.refresh();
        self.scope
            .state
            .borrow()
            .connected
            .then(|| self.handle.token().generation())
    }
    fn cancelled(&self) -> bool {
        self.refresh();
        self.scope.state.borrow().cancelled
    }
    fn synchronize<'a>(
        &'a mut self,
        deadline: Instant,
    ) -> FutureResult<'a, watch::Receiver<EvidenceState>> {
        Box::pin(async move {
            self.bridge.synchronize(deadline).await?;
            self.refresh();
            Ok(self.scope.state.subscribe())
        })
    }
}

enum GuardRefusal {
    Policy(Disposition),
    Evidence,
    Cancelled,
    Unavailable,
    Deadline,
}
pub struct TransportMutator {
    handle: SessionHandle,
    fence: ObservationFence,
    scope: Arc<Scope>,
    control: SharedControl,
    clock: Clock,
    call: Arc<BoundCall>,
    caller: Arc<CallerBinding>,
    receipt_guard: Option<(crate::receipt_broker::DispatchFence, String)>,
    attempted: bool,
    admitted: bool,
    used: bool,
}
impl StageMutator for TransportMutator {
    fn set_receipt_guard(&mut self, fence: crate::receipt_broker::DispatchFence, id: String) {
        self.receipt_guard = Some((fence, id));
    }
    fn endpoint_hash(&self) -> &str {
        self.handle.endpoint_hash()
    }
    fn connection_generation(&self) -> Option<u64> {
        (self.scope.active.load(Ordering::Acquire) && self.handle.invalidation().is_none())
            .then(|| self.handle.token().generation())
    }
    fn admit(&mut self, admission: StageAdmission<'_>) -> Result<(), AdmissionError> {
        if self.attempted
            || !self.call.matches(admission)
            || !self.scope.active.load(Ordering::Acquire)
        {
            return Err(AdmissionError::Invalid(invalid_binding()));
        }
        self.attempted = true;
        let mut control = self
            .control
            .lock()
            .map_err(|_| AdmissionError::Invalid(unavailable()))?;
        let disposition = self
            .caller
            .evaluate(&mut control, &self.call, (self.clock)().epoch_ms);
        if disposition != Disposition::Allow {
            return Err(AdmissionError::Policy(disposition));
        }
        self.admitted = true;
        Ok(())
    }
    fn mutate<'a>(
        &'a mut self,
        mutation: Mutation,
        expected_generation: u64,
        expected_revision: u64,
        deadline: Instant,
    ) -> FutureResult<'a, (), MutationError> {
        let valid = self.admitted && !self.used && self.call.permits(&mutation);
        self.used = true;
        Box::pin(async move {
            if !valid {
                return Err(MutationError::NotDispatched(invalid_binding()));
            }
            if expected_generation != self.handle.token().generation() {
                return Err(MutationError::NotDispatched(invalidated()));
            }
            let (request, data) = match mutation {
                Mutation::SetPreview { scene_uuid } => {
                    identity(&json!(scene_uuid)).map_err(MutationError::NotDispatched)?;
                    ("SetCurrentPreviewScene", json!({"sceneUuid":scene_uuid}))
                }
                Mutation::Transition => ("TriggerStudioModeTransition", json!({})),
            };
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .filter(|duration| !duration.is_zero())
                .ok_or_else(|| MutationError::NotDispatched(expired()))?;
            let (control, clock, scope, fence, caller, call) = (
                self.control.clone(),
                self.clock.clone(),
                self.scope.clone(),
                self.fence.clone(),
                self.caller.clone(),
                self.call.clone(),
            );
            let receipt_guard = self.receipt_guard.take();
            let refusal = Arc::new(Mutex::new(None));
            let observed = refusal.clone();
            let result = self
                .handle
                .request_checked(self.handle.token(), request, data, remaining, move || {
                    let reason = match control.lock() {
                        Err(_) => Some(GuardRefusal::Unavailable),
                        Ok(mut control) => {
                            let decision = caller.evaluate(&mut control, &call, clock().epoch_ms);
                            if decision == Disposition::Allow {
                                None
                            } else {
                                Some(GuardRefusal::Policy(decision))
                            }
                        }
                    }
                    .or_else(|| {
                        if receipt_guard
                            .as_ref()
                            .is_some_and(|(fence, id)| !fence.permits(id).unwrap_or(false))
                        {
                            Some(GuardRefusal::Unavailable)
                        } else {
                            None
                        }
                    })
                    .or_else(|| {
                        if scope.cancellation.is_cancelled() {
                            Some(GuardRefusal::Cancelled)
                        } else if Instant::now() >= deadline {
                            Some(GuardRefusal::Deadline)
                        } else if !scope.active.load(Ordering::Acquire)
                            || !fence.ready_at_revision(expected_generation, expected_revision)
                        {
                            Some(GuardRefusal::Evidence)
                        } else {
                            None
                        }
                    });
                    if let Some(reason) = reason {
                        if let Ok(mut target) = observed.lock() {
                            *target = Some(reason);
                        }
                        Err("Stage dispatch guard refused; inspect the typed outcome".to_owned())
                    } else {
                        Ok(())
                    }
                })
                .await;
            match result {
                Ok(_) => Ok(()),
                Err(RequestError::Rejected { code }) => Err(MutationError::Rejected { code }),
                Err(RequestError::RejectedDetailed { code, comment }) => {
                    Err(MutationError::RejectedDetailed {
                        code,
                        comment,
                        request_type: request,
                    })
                }
                Err(RequestError::Unknown(_)) => {
                    Err(MutationError::Unknown(ObservationError::new(
                        "observation_invalidated",
                        "Stage mutation reply is unavailable; inspect OBS before another production change",
                    )))
                }
                Err(RequestError::BeforeDispatch(reason)) => {
                    let observed = refusal.lock().ok().and_then(|mut value| value.take());
                    Err(match observed {
                        Some(GuardRefusal::Policy(policy)) => MutationError::Policy(policy),
                        Some(GuardRefusal::Cancelled) => MutationError::NotDispatched(cancelled()),
                        Some(GuardRefusal::Unavailable) => {
                            MutationError::NotDispatched(unavailable())
                        }
                        Some(GuardRefusal::Evidence) => MutationError::NotDispatched(invalidated()),
                        Some(GuardRefusal::Deadline) => MutationError::NotDispatched(expired()),
                        None => MutationError::NotDispatched(match reason {
                            BeforeDispatch::Deadline => expired(),
                            BeforeDispatch::Cancelled => cancelled(),
                            BeforeDispatch::Capacity | BeforeDispatch::ByteCapacity => {
                                ObservationError::new(
                                    "operation_in_progress",
                                    "Stage transport request capacity is unavailable",
                                )
                            }
                            _ => invalidated(),
                        }),
                    })
                }
            }
        })
    }
}

fn invalid_binding() -> ObservationError {
    ObservationError::new(
        "evidence_unavailable",
        "Stage transport arguments do not match the validated workflow binding",
    )
}
fn invalidated() -> ObservationError {
    ObservationError::new(
        "observation_invalidated",
        "Stage evidence or physical connection changed before dispatch",
    )
}
fn cancelled() -> ObservationError {
    ObservationError::new(
        "cancelled",
        "Operation cancelled; inspect OBS before any further action",
    )
}
fn unavailable() -> ObservationError {
    ObservationError::new("evidence_unavailable", "Control policy is unavailable")
}
fn expired() -> ObservationError {
    ObservationError::new(
        "observation_limit",
        "Stage mutation deadline expired before dispatch",
    )
}
fn valid_uuid(value: &str) -> bool {
    if value == "00000000-0000-0000-0000-000000000000"
        || value == "ffffffff-ffff-ffff-ffff-ffffffffffff"
    {
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
