// SPDX-License-Identifier: GPL-2.0-only
//! Isolated take supervisor, not an MCP registration or a second OBS service.
//!
//! The service arbiter lends its sole journal writer and an exact connection.
//! All jobs live in this supervisor's JoinSet; cancelling a frontend wait does
//! not drop/replay a possibly dispatched effect. shutdown() must be retained
//! and awaited before taking the writer back. Emergency Drop aborts owned jobs
//! and is NOT evidence that OBS stopped. A process hard kill cannot stop OBS.
//!
//! Concrete event subscription, high-volume meter/PPM workers, writable-host
//! preflight, file inspection and log publication adapters remain required.
//! Injected adapters must use the existing session's checked queue, enforce
//! the supplied final guard, never reconnect/replay, and own no journal writer.
use crate::{
    control::{ClaimOutcome, ConfirmationContext, Disposition, GateRequest, TakeLock},
    custody::{Clock, SharedControl},
    observation::ReadRequest,
    operation_store::{
        OperationKind, OperationReceipt, OperationStore, OperationUpdate, Phase, PrepareDetails,
        StoreError,
    },
    output_start::{self, ObservedEvent, OutputFuture, OutputIo, Subscription},
    preflight::{
        DEMO_REQUIRED_CHECKS, Options as PreflightOptions, Report as PreflightReport, Status,
    },
    record_clip,
    record_impact::{
        AttemptId, ImpactChanges, ImpactStatus, RecordImpactOwner, RecordImpactView, Revocation,
        TakeImpactTicket,
    },
    session::{ALL_EVENTS, ConnectionToken, SessionHandle},
    stage_transport::Cancellation,
    take_monitor::{
        MonitorOptions, Phase as MonitorPhase, SampleKind, SampleObservation, Summary,
        TakeMonitorCore, WarningKind,
    },
};
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore, broadcast, mpsc, oneshot, watch},
    task::{Id, JoinError, JoinHandle, JoinSet},
    time::{Instant, sleep, sleep_until, timeout, timeout_at},
};

pub type IoFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
pub type DispatchGuard = Box<dyn FnOnce() -> Result<(), String> + Send + 'static>;
#[derive(Clone)]
pub enum Effect {
    StartRecord,
    StopRecord,
    Chapter(String),
}
impl fmt::Debug for Effect {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(match self {
            Self::StartRecord => "StartRecord",
            Self::StopRecord => "StopRecord",
            Self::Chapter(_) => "Chapter { .. }",
        })
    }
}
impl Effect {
    pub fn request_type(&self) -> &'static str {
        match self {
            Self::StartRecord => "StartRecord",
            Self::StopRecord => "StopRecord",
            Self::Chapter(_) => "CreateRecordChapter",
        }
    }
    pub fn request_data(&self) -> Value {
        match self {
            Self::Chapter(name) => json!({"chapterName":name}),
            _ => Value::Null,
        }
    }
}
#[derive(Clone)]
pub enum DispatchError {
    BeforeDispatch(String),
    Rejected { code: u64, message: String },
    Unknown(String),
}
impl DispatchError {
    pub fn message(&self) -> &str {
        match self {
            Self::BeforeDispatch(message)
            | Self::Unknown(message)
            | Self::Rejected { message, .. } => message,
        }
    }
}
impl fmt::Debug for DispatchError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BeforeDispatch(_) => out.write_str("BeforeDispatch { .. }"),
            Self::Rejected { code, .. } => out
                .debug_struct("Rejected")
                .field("code", code)
                .finish_non_exhaustive(),
            Self::Unknown(_) => out.write_str("Unknown { .. }"),
        }
    }
}
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClipCheck {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_seconds: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_streams: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_streams: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_volume_db: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped: Option<String>,
}
pub struct MonitorSetup {
    pub record_tracks: Vec<u8>,
    pub input_tracks: HashMap<String, Map<String, Value>>,
    pub tracks_error: Option<String>,
    pub program: Value,
    pub stats: Option<Value>,
    pub supports_ppm: bool,
}
/// Read/dispatch workers borrow this loan's existing connection. They must not
/// hide extra sockets, retry loops, store writers or detached tasks. Every
/// returned future is owned and joined by the supervisor.
pub trait TakeIo: Send + Sync + 'static {
    fn preflight(&self, options: PreflightOptions) -> IoFuture<PreflightReport>;
    fn read(&self, request: ReadRequest, duration: Duration) -> IoFuture<Result<Value, String>>;
    /// Fresh status for the owned Stop workflow. Concrete single-session
    /// transports may reserve this exact read without prioritizing other reads.
    fn stop_status(&self, duration: Duration) -> IoFuture<Result<Value, String>> {
        self.read(ReadRequest::RecordStatus, duration)
    }

    fn dispatch(
        &self,
        expected: ConnectionToken,
        effect: Effect,
        duration: Duration,
        guard: DispatchGuard,
    ) -> IoFuture<Result<Value, DispatchError>>;
    fn monitor_setup(&self) -> IoFuture<Result<MonitorSetup, String>>;
    fn picture(&self, program: String) -> IoFuture<Result<Vec<u8>, String>>;
    fn inspect(&self, path: String, measure_loudness: bool) -> IoFuture<ClipCheck>;
    fn write_log(&self, path: String, summary: Summary) -> IoFuture<Option<String>>;
    /// Release this take's subscriptions and join its observation workers.
    /// Idempotent; a later monitor_setup may open a fresh per-take scope.
    /// The adapter owns independently bounded cancellation/cleanup. Completing
    /// with Err does not establish worker exit or permit observer-scope reuse.
    fn join_observers(&self) -> IoFuture<Result<(), String>>;
}
pub enum Event {
    /// Source-owned frame; application releases its original transport permits.
    Observed(crate::service_events::ObservedFrame),
    Obs {
        connection: ConnectionToken,
        event_type: String,
        data: Value,
    },
    Disconnected {
        connection: ConnectionToken,
    },
    /// Queue-delivery barrier only. A concrete adapter must first fence its
    /// producer against SessionHandle::event_cursor(); enqueuing this alone is
    /// not proof that the transport had no earlier unpublished OBS event.
    Barrier {
        connection: ConnectionToken,
        done: oneshot::Sender<()>,
    },
}
#[derive(Clone, Default)]
pub struct Policy {
    pub caller: String,
    pub confirmation_enabled: bool,
    pub can_elicit: bool,
    pub input_responses: Option<Value>,
}
#[derive(Clone, Debug)]
pub struct StartOptions {
    pub expect_silent: bool,
    pub min_free_disk_mb: f64,
    pub lock: bool,
    pub chapter: Option<String>,
    pub max_duration_seconds: Option<u16>,
    pub require_verified_preflight: bool,
}
impl Default for StartOptions {
    fn default() -> Self {
        Self {
            expect_silent: false,
            min_free_disk_mb: 1024.0,
            lock: true,
            chapter: None,
            max_duration_seconds: None,
            require_verified_preflight: false,
        }
    }
}
#[derive(Clone)]
pub struct Limits {
    pub rpc: Duration,
    pub setup: Duration,
    pub stop: Duration,
    pub inspection: Duration,
    pub file: Duration,
    pub start_confirmation: Duration,
    pub stats_interval: Duration,
    pub picture_interval: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            rpc: Duration::from_secs(10),
            setup: Duration::from_secs(10),
            stop: Duration::from_secs(10),
            inspection: Duration::from_secs(375),
            file: Duration::from_secs(10),
            start_confirmation: Duration::from_secs(5),
            stats_interval: Duration::from_secs(2),
            picture_interval: Duration::from_secs(1),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnerError {
    Binding,
    IncompleteEvents,
    InvalidOptions,
    Capacity,
    Closed,
    WorkerFailed,
}
pub struct Loan {
    pub session: SessionHandle,
    pub store: OperationStore,
    pub control: SharedControl,
    pub clock: Clock,
    /// One source shared by take IDs and receipt IDs. It must return unique,
    /// valid UUIDs. Durable store collision checks remain authoritative.
    pub identities: Box<dyn FnMut() -> String + Send>,
    pub io: Arc<dyn TakeIo>,
    pub events: mpsc::Receiver<Event>,
}
/// Rejected admission returns the exact loan, including its sole writer.
pub struct SpawnFailure {
    pub reason: OwnerError,
    pub loan: Box<Loan>,
}
impl fmt::Debug for SpawnFailure {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("SpawnFailure")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}
/// No native Reidentify/meter-subscription adapter is qualified yet. Require
/// the complete source event mask and initial InputVolumeMeters intent.
pub const REQUIRED_SUBSCRIPTIONS: u64 = ALL_EVENTS | (1 << 16);
#[derive(Clone, Debug)]
pub struct ShutdownReceipt {
    pub joined: bool,
    pub stop_confirmed: bool,
    pub result: Value,
    pub observers_joined: bool,
}
struct Joined {
    store: Option<OperationStore>,
    receipt: ShutdownReceipt,
}
pub struct TakeSupervisor {
    handle: TakeHandle,
    shutdown: watch::Sender<bool>,
    task: Option<JoinHandle<Joined>>,
    joined: Option<Joined>,
    // Keep private admission through every cancellation-safe shutdown await.
    _private_impact_owner: Option<RecordImpactOwner>,
}
#[derive(Clone)]
pub struct TakeHandle {
    sender: mpsc::Sender<Command>,
    slots: Arc<Semaphore>,
    shared: Arc<Shared>,
    summary: watch::Receiver<Value>,
}
struct Reply {
    sender: oneshot::Sender<Value>,
    _slot: OwnedSemaphorePermit,
}
/// Startup readiness transfers the existing supervisor back to Service while
/// its exact-Take clip job continues under the same JoinSet and writer loan.
pub(crate) struct ClipAdmission {
    ready: oneshot::Receiver<()>,
    result: oneshot::Receiver<Value>,
}
pub(crate) struct ClipResult(oneshot::Receiver<Value>);
impl ClipAdmission {
    pub(crate) async fn ready(self) -> Result<ClipResult, OwnerError> {
        self.ready.await.map_err(|_| OwnerError::WorkerFailed)?;
        Ok(ClipResult(self.result))
    }
}
impl ClipResult {
    pub(crate) async fn finish(self) -> Result<Value, OwnerError> {
        self.0.await.map_err(|_| OwnerError::WorkerFailed)
    }
}
enum Command {
    Start(
        StartOptions,
        Policy,
        Option<(TakeImpactTicket, Instant)>,
        Reply,
    ),
    Stop(Option<f64>, Policy, Reply),
    Mark(String, Policy, Reply),
    Status(Reply),
    Clip(
        record_clip::Request,
        Policy,
        Cancellation,
        Option<(TakeImpactTicket, Instant)>,
        oneshot::Sender<()>,
        Reply,
    ),
}
#[derive(Default, Clone)]
struct Continuity {
    started: bool,
    stopped: bool,
    lost: Option<String>,
    path: Option<String>,
}
struct StartReservation {
    cancellation: Option<Cancellation>,
    impact: Arc<TakeImpactTicket>,
    impact_deadline: Option<Instant>,
    receipt: Option<OperationReceipt>,
    receipt_unavailable: bool,
    continuity: Arc<Mutex<Continuity>>,
}
struct Take {
    impact: Arc<TakeImpactTicket>,
    blocked_impact: Option<AttemptId>,
    id: String,
    options: StartOptions,
    policy: Policy,
    local: bool,
    monitor: TakeMonitorCore,
    continuity: Arc<Mutex<Continuity>>,
    start_operation: Option<OperationReceipt>,
    stop_operation: Option<OperationReceipt>,
    receipt_unavailable: bool,
    stop_uncertain: bool,
    stop_running: bool,
    stop_result: Option<Value>,
    last_stop: Option<Value>,
    chapter_notes: Vec<String>,
    lease_holder: Option<String>,
    deadline: Option<Instant>,
    deadline_fired: bool,
    setup_pending: bool,
    monitor_stop_running: bool,
    monitor_summary: Option<Summary>,
    next_stats: Option<Instant>,
    next_picture: Option<Instant>,
    program: Option<String>,
}
struct Inner {
    store: Option<OperationStore>,
    starting: Option<StartReservation>,
    take: Option<Arc<Mutex<Take>>>,
    completed: Option<Value>,
}
struct Shared {
    impact: RecordImpactView,
    inner: Mutex<Inner>,
    io: Arc<dyn TakeIo>,
    session: SessionHandle,
    control: SharedControl,
    clock: Clock,
    identities: Mutex<Box<dyn FnMut() -> String + Send>>,
    limits: Limits,
    changed: Notify,
    events: broadcast::Sender<(String, Instant, Value)>,
    closing: AtomicBool,
    closed: AtomicBool,
    clip_shutdown: Cancellation,
}
fn error(text: impl Into<String>) -> Value {
    json!({"content":[{"type":"text","text":text.into()}],"isError":true})
}
fn no_take() -> Value {
    error("No take is recording; start one with obs-take-start")
}
fn now(shared: &Shared) -> i64 {
    (shared.clock)().epoch_ms.min(i64::MAX as u64) as i64
}
fn current(shared: &Shared) -> Option<Arc<Mutex<Take>>> {
    shared.inner.lock().unwrap().take.clone()
}
fn notify(shared: &Shared) {
    shared.changed.notify_waiters();
}
const IMPACT_LOST: &str =
    "External recording-impact operation invalidated owned recording authority";
fn apply_impact(ticket: &TakeImpactTicket, continuity: &Arc<Mutex<Continuity>>) -> ImpactStatus {
    let status = ticket.check();
    if let ImpactStatus::Revoked(reason) = status {
        let mut state = continuity.lock().unwrap();
        if state.lost.is_none() {
            state.lost=Some(match reason {
            Revocation::ConnectionClosed=>"OBS disconnected; recording ownership cannot be established after reconnect",
            Revocation::OwnerGone=>"Recording-impact owner is unavailable",
            Revocation::Poisoned|Revocation::IdentityExhausted=>"Recording-impact authority is unavailable",
            _=>IMPACT_LOST,
        }.into());
        }
    }
    status
}
fn take_impact(take: &Take) -> ImpactStatus {
    apply_impact(&take.impact, &take.continuity)
}
async fn wait_impact(
    shared: &Shared,
    ticket: &TakeImpactTicket,
    continuity: &Arc<Mutex<Continuity>>,
    deadline: Instant,
) -> bool {
    let Ok(mut changes) = shared.impact.changes() else {
        apply_impact(ticket, continuity);
        return false;
    };
    loop {
        match apply_impact(ticket, continuity) {
            ImpactStatus::Ready => return true,
            ImpactStatus::Revoked(_) => return false,
            ImpactStatus::Suspended(_) => {
                if changes.changed_until(deadline).await.is_err() {
                    apply_impact(ticket, continuity);
                    return false;
                }
            }
        }
    }
}
async fn wait_take_impact(shared: &Shared, take: &Arc<Mutex<Take>>, deadline: Instant) -> bool {
    let (ticket, continuity) = {
        let mut data = take.lock().unwrap();
        let status = take_impact(&data);
        if let ImpactStatus::Suspended(id) = status {
            data.blocked_impact = Some(id);
        }
        (data.impact.clone(), data.continuity.clone())
    };
    let ready = wait_impact(shared, &ticket, &continuity, deadline).await;
    if ready {
        take.lock().unwrap().blocked_impact = None;
    }
    ready
}
fn suspended_stop(take: &Arc<Mutex<Take>>, definitely_unsent: bool) -> Value {
    let data = take.lock().unwrap();
    let reason = if definitely_unsent {
        "Owned Stop was not dispatched; external recording impact suspended its authority"
    } else {
        "Owned Stop is suspended by an external recording-impact operation"
    };
    json!({"content":[{"type":"text","text":format!("Take {}: {reason}. Recording stop is unconfirmed; check OBS",data.id)}],"isError":true,"structuredContent":{"takeId":data.id,"stopConfirmed":false,"ownership":"suspended","reason":reason,"recordingImpact":"suspended"}})
}
async fn impact_stop_refusal(shared: &Arc<Shared>, take: &Arc<Mutex<Take>>) -> Value {
    let status = {
        let data = take.lock().unwrap();
        take_impact(&data)
    };
    if matches!(status, ImpactStatus::Suspended(_) | ImpactStatus::Ready) {
        return suspended_stop(take, false);
    }
    take.lock().unwrap().deadline = None;
    // Revocation cannot authorize Stop or establish a file. A bounded fresh
    // read may still retire an inactive lost Take, preserving the pre-existing
    // lifetime/lease cleanup path without restoring recording authority.
    let inactive = record_status(shared, shared.limits.rpc).await == Some(false);
    if let Err(reason) = monitor_stop(shared, take).await {
        return uncertain(shared, take, reason);
    }
    let reason = take
        .lock()
        .unwrap()
        .continuity
        .lock()
        .unwrap()
        .lost
        .clone()
        .unwrap_or_else(|| IMPACT_LOST.into());
    if inactive {
        {
            let mut data = take.lock().unwrap();
            release_lease(shared, &mut data);
        }
        clear_current(shared, take);
        return uncertain(
            shared,
            take,
            format!("{reason}; OBS is now inactive, but the owned file cannot be established"),
        );
    }
    uncertain(shared, take, reason)
}
// The current exact Stop reservation only, after typed BeforeDispatch. A
// visible/failed publication never grants permission. Retain the old file;
// continuation prepares a fresh UUID after this successful full-tuple update.
fn retire_unsent_stop(
    shared: &Shared,
    take: &Arc<Mutex<Take>>,
) -> Result<OperationReceipt, StoreError> {
    let mut data = take.lock().unwrap();
    let receipt = data.stop_operation.as_ref().ok_or(StoreError::Conflict)?;
    let value = receipt.to_value();
    if receipt.phase() != Phase::Dispatching
        || value["kind"] != "record-stop"
        || value["takeId"] != data.id
        || !value["requestDispatched"].is_null()
        || !value["requestAccepted"].is_null()
        || value["postconditionObserved"] != false
    {
        return Err(StoreError::Conflict);
    }
    let mut patch = OperationUpdate::new(Phase::NotDispatched);
    patch.request_dispatched = Some(Some(false));
    patch.request_accepted = Some(None);
    patch.postcondition_observed = Some(false);
    let retired = update(shared, receipt, patch)?;
    data.stop_uncertain = false;
    data.stop_operation = None;
    Ok(retired)
}
fn read_snapshot(shared: &Shared) -> Value {
    let Some(take) = current(shared) else {
        return Value::Null;
    };
    let take = take.lock().unwrap();
    let impact = take_impact(&take);
    let continuity = take.continuity.lock().unwrap();
    json!({"takeId":take.id,"locked":take.options.lock,"maxDurationSeconds":take.options.max_duration_seconds,"ownership":if continuity.lost.is_some(){"unavailable"}else if matches!(impact,ImpactStatus::Suspended(_)){"suspended"}else{"continuous"},"take":take.monitor.summary(now(shared))})
}
impl TakeSupervisor {
    /// Standalone compatibility owner. External recording-impacting effects
    /// must share a Service-lifetime owner via spawn_with_record_impact instead.
    pub fn spawn(loan: Loan, limits: Limits) -> Result<Self, SpawnFailure> {
        let owner = match RecordImpactOwner::bind(&loan.session) {
            Ok(owner) => owner,
            Err(_) => {
                return Err(SpawnFailure {
                    reason: OwnerError::Binding,
                    loan: Box::new(loan),
                });
            }
        };
        let view = owner.view();
        Self::spawn_inner(loan, limits, view, Some(owner))
    }
    /// The caller must retain the matching owner through normal Take cleanup
    /// and admitted manual settlement; borrowed views never extend admission.
    pub fn spawn_with_record_impact(
        loan: Loan,
        limits: Limits,
        impact: RecordImpactView,
    ) -> Result<Self, SpawnFailure> {
        Self::spawn_inner(loan, limits, impact, None)
    }
    fn spawn_inner(
        loan: Loan,
        limits: Limits,
        impact: RecordImpactView,
        private_owner: Option<RecordImpactOwner>,
    ) -> Result<Self, SpawnFailure> {
        let changes = if impact.token() == loan.session.token() {
            impact.changes().ok()
        } else {
            None
        };
        let Some(changes) = changes else {
            return Err(SpawnFailure {
                reason: OwnerError::Binding,
                loan: Box::new(loan),
            });
        };
        if loan.store.endpoint_hash() != loan.session.endpoint_hash() {
            return Err(SpawnFailure {
                reason: OwnerError::Binding,
                loan: Box::new(loan),
            });
        }
        if loan.session.event_subscriptions() & REQUIRED_SUBSCRIPTIONS != REQUIRED_SUBSCRIPTIONS {
            return Err(SpawnFailure {
                reason: OwnerError::IncompleteEvents,
                loan: Box::new(loan),
            });
        }
        if [
            &limits.rpc,
            &limits.setup,
            &limits.stop,
            &limits.inspection,
            &limits.file,
            &limits.start_confirmation,
            &limits.stats_interval,
            &limits.picture_interval,
        ]
        .into_iter()
        .any(|duration| duration.is_zero() || Instant::now().checked_add(*duration).is_none())
        {
            return Err(SpawnFailure {
                reason: OwnerError::InvalidOptions,
                loan: Box::new(loan),
            });
        }
        let (events, _) = broadcast::channel(256);
        let shared = Arc::new(Shared {
            impact,
            inner: Mutex::new(Inner {
                store: Some(loan.store),
                starting: None,
                take: None,
                completed: None,
            }),
            io: loan.io,
            session: loan.session,
            control: loan.control,
            clock: loan.clock,
            identities: Mutex::new(loan.identities),
            limits,
            changed: Notify::new(),
            events,
            closing: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            clip_shutdown: Cancellation::new(),
        });
        let (sender, commands) = mpsc::channel(32);
        let (shutdown, stopping) = watch::channel(false);
        let (summaries, summary) = watch::channel(Value::Null);
        let handle = TakeHandle {
            sender,
            slots: Arc::new(Semaphore::new(32)),
            shared: shared.clone(),
            summary,
        };
        let task = tokio::spawn(actor(
            shared,
            commands,
            loan.events,
            stopping,
            summaries,
            changes,
        ));
        Ok(Self {
            handle,
            shutdown,
            task: Some(task),
            joined: None,
            _private_impact_owner: private_owner,
        })
    }
    /// A loan cannot be reused while startup or a live/uncertain take remains.
    pub fn has_take_or_start(&self) -> bool {
        let inner = self.handle.shared.inner.lock().unwrap();
        inner.starting.is_some() || inner.take.is_some()
    }
    /// Preserve idempotent stop results when the combined service reclaims the writer.
    pub fn completed_result(&self) -> Option<Value> {
        self.handle.shared.inner.lock().unwrap().completed.clone()
    }
    pub fn handle(&self) -> TakeHandle {
        self.handle.clone()
    }
    /// Cancellation-safe frontend await: retain this owner and resume the same
    /// JoinHandle. No known-stop receipt is returned on emergency cancellation.
    pub async fn shutdown(&mut self) -> Result<ShutdownReceipt, OwnerError> {
        self.shutdown.send_replace(true);
        if self.joined.is_none() {
            let joined = self.task.as_mut().ok_or(OwnerError::Closed)?.await;
            self.task.take();
            let result = joined.map_err(|_| {
                self.handle.shared.closed.store(true, Ordering::Release);
                OwnerError::WorkerFailed
            })?;
            self.joined = Some(result);
        }
        Ok(self.joined.as_ref().unwrap().receipt.clone())
    }
    pub fn take_store(&mut self) -> Option<OperationStore> {
        self.joined
            .as_mut()
            .filter(|joined| joined.receipt.observers_joined)
            .and_then(|joined| joined.store.take())
    }
}
impl Drop for TakeSupervisor {
    fn drop(&mut self) {
        self.handle.shared.closed.store(true, Ordering::Release);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
impl TakeHandle {
    fn admission(&self) -> Result<(Reply, oneshot::Receiver<Value>), OwnerError> {
        if self.shared.closing.load(Ordering::Acquire) || self.shared.closed.load(Ordering::Acquire)
        {
            return Err(OwnerError::Closed);
        }
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| OwnerError::Capacity)?;
        let (sender, receiver) = oneshot::channel();
        Ok((
            Reply {
                sender,
                _slot: slot,
            },
            receiver,
        ))
    }
    async fn send(
        &self,
        command: Command,
        receiver: oneshot::Receiver<Value>,
    ) -> Result<Value, OwnerError> {
        self.sender
            .try_send(command)
            .map_err(|_| OwnerError::Capacity)?;
        receiver.await.map_err(|_| OwnerError::Closed)
    }
    pub async fn start(&self, options: StartOptions, policy: Policy) -> Result<Value, OwnerError> {
        let (reply, receiver) = self.admission()?;
        self.send(Command::Start(options, policy, None, reply), receiver)
            .await
    }
    /// Service-admitted start transfers its original epoch and wait bound.
    /// The exact registry is checked again by reserve_start; no fresh ticket
    /// may replace a supplied admission after an external recording impact.
    pub(crate) async fn start_with_impact_ticket(
        &self,
        options: StartOptions,
        policy: Policy,
        ticket: TakeImpactTicket,
        deadline: Instant,
    ) -> Result<Value, OwnerError> {
        let (reply, receiver) = self.admission()?;
        self.send(
            Command::Start(options, policy, Some((ticket, deadline)), reply),
            receiver,
        )
        .await
    }
    /// An owned clip captures its newly created Take, not a later current Take.
    /// Dropping this frontend future does not cancel the admitted operation.
    pub async fn record_clip(
        &self,
        request: record_clip::Request,
        policy: Policy,
        cancel: Cancellation,
    ) -> Result<Value, OwnerError> {
        self.enqueue_clip(request, policy, cancel, None)?
            .ready()
            .await?
            .finish()
            .await
    }
    pub(crate) fn enqueue_clip(
        &self,
        request: record_clip::Request,
        policy: Policy,
        cancel: Cancellation,
        admitted: Option<(TakeImpactTicket, Instant)>,
    ) -> Result<ClipAdmission, OwnerError> {
        let (reply, result) = self.admission()?;
        let (ready, receiver) = oneshot::channel();
        self.sender
            .try_send(Command::Clip(
                request, policy, cancel, admitted, ready, reply,
            ))
            .map_err(|_| OwnerError::Capacity)?;
        Ok(ClipAdmission {
            ready: receiver,
            result,
        })
    }
    pub async fn stop(
        &self,
        requested_seconds: Option<f64>,
        policy: Policy,
    ) -> Result<Value, OwnerError> {
        let (reply, receiver) = self.admission()?;
        self.send(Command::Stop(requested_seconds, policy, reply), receiver)
            .await
    }
    pub async fn mark(&self, name: String, policy: Policy) -> Result<Value, OwnerError> {
        let (reply, receiver) = self.admission()?;
        self.send(Command::Mark(name, policy, reply), receiver)
            .await
    }
    pub async fn status(&self) -> Result<Value, OwnerError> {
        let (reply, receiver) = self.admission()?;
        self.send(Command::Status(reply), receiver).await
    }
    pub fn snapshot(&self) -> Value {
        if self.shared.closed.load(Ordering::Acquire) {
            json!({"status":"unknown","reason":"take_supervisor_closed"})
        } else {
            read_snapshot(&self.shared)
        }
    }
    /// Source-shaped metadata used by production observation; monitor samples,
    /// paths and settings do not escape this read-only projection.
    pub fn production_metadata(&self) -> Result<Value, OwnerError> {
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(OwnerError::Closed);
        }
        let take = self.shared.inner.lock().unwrap().take.clone();
        let Some(take) = take else {
            return Ok(Value::Null);
        };
        let take = take.lock().unwrap();
        let summary = take.monitor.summary(now(&self.shared));
        if !summary.running {
            return Ok(Value::Null);
        }
        let impact = take_impact(&take);
        let lost = take.continuity.lock().unwrap().lost.clone().or_else(|| {
            matches!(impact, ImpactStatus::Suspended(_))
                .then(|| "External recording impact is pending".into())
        });
        Ok(
            json!({"id":take.id,"running":summary.running,"startedAt":summary.started_at,"expectSilent":summary.expect_silent,"locked":take.options.lock,"maxDurationSeconds":take.options.max_duration_seconds,"ownershipLost":lost}),
        )
    }
    /// Source resource projection at read time. No OBS request or publication
    /// right is created; stopped/private retired monitors are not active takes.
    pub fn resource_summary(&self) -> Result<Value, OwnerError> {
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(OwnerError::Closed);
        }
        let take = self.shared.inner.lock().unwrap().take.clone();
        let Some(take) = take else {
            return Ok(json!({"running":false}));
        };
        let summary = take.lock().unwrap().monitor.summary(now(&self.shared));
        Ok(if summary.running {
            serde_json::to_value(summary).expect("bounded take summary")
        } else {
            json!({"running":false})
        })
    }
    pub fn summaries(&self) -> watch::Receiver<Value> {
        self.summary.clone()
    }
}
fn reply(reply: Reply, value: Value) {
    let _ = reply.sender.send(value);
}
async fn bounded<T>(duration: Duration, mut future: IoFuture<T>) -> Option<T> {
    match timeout(duration, &mut future).await {
        Ok(result) => Some(result),
        Err(_) => {
            // The supervisor still owns this exact job. A timeout changes its
            // public classification, never ends the backend's cleanup duty.
            // Drain the retained future; discard late success, never replay.
            let _ = future.await;
            None
        }
    }
}
fn take_id(shared: &Shared) -> String {
    (shared.identities.lock().unwrap())()
        .chars()
        .take(8)
        .collect()
}
fn prepare(shared: &Shared, kind: OperationKind, id: &str) -> Result<OperationReceipt, StoreError> {
    let operation = (shared.identities.lock().unwrap())();
    let clock = (shared.clock)();
    shared
        .inner
        .lock()
        .unwrap()
        .store
        .as_mut()
        .expect("writer loan")
        .prepare(
            &operation,
            kind,
            shared.session.token().generation(),
            &clock.utc,
            PrepareDetails {
                take_id: Some(id),
                ..PrepareDetails::default()
            },
        )
}
fn update(
    shared: &Shared,
    receipt: &OperationReceipt,
    update: OperationUpdate,
) -> Result<OperationReceipt, StoreError> {
    shared
        .inner
        .lock()
        .unwrap()
        .store
        .as_mut()
        .expect("writer loan")
        .update(receipt, update, &(shared.clock)().utc)
}
fn dispatching(
    shared: &Shared,
    receipt: &OperationReceipt,
) -> Result<OperationReceipt, StoreError> {
    let mut patch = OperationUpdate::new(Phase::Dispatching);
    patch.request_dispatched = Some(None);
    update(shared, receipt, patch)
}
fn accepted(shared: &Shared, receipt: &OperationReceipt) -> Result<OperationReceipt, StoreError> {
    let mut patch = OperationUpdate::new(Phase::Accepted);
    patch.request_dispatched = Some(Some(true));
    patch.request_accepted = Some(Some(true));
    update(shared, receipt, patch)
}
fn guard(
    shared: Arc<Shared>,
    take: Option<Arc<Mutex<Take>>>,
    effect: Effect,
    policy: Policy,
) -> DispatchGuard {
    Box::new(move || {
        if shared.closed.load(Ordering::Acquire)
            || (matches!(effect, Effect::StartRecord) && shared.closing.load(Ordering::Acquire))
        {
            return Err("Take supervisor is closing".into());
        }
        if matches!(effect, Effect::StartRecord)
            && shared
                .inner
                .lock()
                .map_err(|_| "Take startup unavailable")?
                .starting
                .as_ref()
                .and_then(|start| start.cancellation.as_ref())
                .is_some_and(Cancellation::is_cancelled)
        {
            return Err("Record Clip cancelled before dispatch".into());
        }
        if let Some(take) = take {
            let active = current(&shared).is_some_and(|active| Arc::ptr_eq(&active, &take));
            if !active {
                return Err("Take ownership is no longer current".into());
            }
            if matches!(effect, Effect::StopRecord) {
                let continuity = take.lock().unwrap().continuity.clone();
                let continuity = continuity.lock().unwrap();
                if continuity.lost.is_some() || continuity.stopped {
                    return Err("Recording ownership changed before dispatch".into());
                }
            }
        }
        let (tool, arguments) = match &effect {
            Effect::StartRecord => ("obs-take-start", json!({})),
            Effect::StopRecord => ("obs-take-stop", json!({})),
            Effect::Chapter(name) => ("obs-take-mark", json!({"name":name})),
        };
        let disposition = shared
            .control
            .lock()
            .map_err(|_| "Control policy unavailable")?
            .evaluate(
                GateRequest {
                    tool_name: tool,
                    arguments: &arguments,
                    read_only_hint: false,
                    caller: &policy.caller,
                    confirmation: ConfirmationContext {
                        enabled: policy.confirmation_enabled,
                        can_elicit: policy.can_elicit,
                        input_responses: policy.input_responses.as_ref(),
                    },
                },
                (shared.clock)().epoch_ms,
            );
        if disposition == Disposition::Allow {
            let binding = if let Some(take) = current(&shared) {
                let take = take.lock().unwrap();
                Some((take.impact.clone(), take.continuity.clone()))
            } else {
                let inner = shared.inner.lock().unwrap();
                inner
                    .starting
                    .as_ref()
                    .map(|start| (start.impact.clone(), start.continuity.clone()))
            };
            if let Some((ticket, continuity)) = binding {
                let status = apply_impact(&ticket, &continuity);
                if let ImpactStatus::Suspended(id) = status
                    && matches!(effect, Effect::StopRecord)
                    && let Some(take) = current(&shared)
                {
                    take.lock().unwrap().blocked_impact = Some(id);
                }
                if !matches!(status, ImpactStatus::Ready) {
                    return Err("Recording impact blocks owned dispatch".into());
                }
            }
            Ok(())
        } else {
            Err(disposition
                .tool_result()
                .and_then(|result| result["content"][0]["text"].as_str().map(str::to_owned))
                .unwrap_or_else(|| "Take dispatch refused by policy".into()))
        }
    })
}
async fn dispatch(
    shared: Arc<Shared>,
    take: Option<Arc<Mutex<Take>>>,
    effect: Effect,
    policy: Policy,
    duration: Duration,
) -> Result<Value, DispatchError> {
    let name = effect.request_type();
    let final_guard = guard(shared.clone(), take, effect.clone(), policy);
    bounded(
        duration,
        shared
            .io
            .dispatch(shared.session.token(), effect, duration, final_guard),
    )
    .await
    .unwrap_or_else(|| {
        Err(DispatchError::Unknown(format!(
            "Request {name} timed out after {}ms",
            duration.as_millis()
        )))
    })
}
struct Watch {
    receiver: broadcast::Receiver<(String, Instant, Value)>,
    event: &'static str,
}
impl Subscription for Watch {
    fn next(&mut self) -> Pin<Box<dyn Future<Output = Option<ObservedEvent>> + Send + '_>> {
        Box::pin(async move {
            loop {
                match self.receiver.recv().await {
                    Ok((name, at, data)) if name == self.event => {
                        return Some(ObservedEvent { at, data });
                    }
                    Ok(_) => {}
                    Err(_) => return None,
                }
            }
        })
    }
}
fn subscribe(shared: &Shared, event: &'static str) -> Watch {
    Watch {
        receiver: shared.events.subscribe(),
        event,
    }
}
struct StartIo {
    shared: Arc<Shared>,
    policy: Policy,
}
impl OutputIo for StartIo {
    fn subscribe(&self, event: &'static str) -> Box<dyn Subscription> {
        Box::new(subscribe(&self.shared, event))
    }
    fn request(&self, request: &'static str) -> OutputFuture<Result<Value, String>> {
        let shared = self.shared.clone();
        let policy = self.policy.clone();
        Box::pin(async move {
            if request == "StartRecord" {
                dispatch(
                    shared.clone(),
                    None,
                    Effect::StartRecord,
                    policy,
                    shared.limits.rpc,
                )
                .await
                .map_err(|error| error.message().into())
            } else if request == "GetRecordStatus" {
                bounded(
                    shared.limits.rpc,
                    shared.io.read(ReadRequest::RecordStatus, shared.limits.rpc),
                )
                .await
                .unwrap_or_else(|| Err("Recording status is unavailable".into()))
            } else {
                Err("Unsupported take output request".into())
            }
        })
    }
}
fn set_current(shared: &Shared, take: Arc<Mutex<Take>>) {
    let mut inner = shared.inner.lock().unwrap();
    let data = take.lock().unwrap();
    shared.control.lock().unwrap().set_take_lock(Some(TakeLock {
        id: data.id.clone(),
        locked: data.options.lock,
    }));
    drop(data);
    inner.take = Some(take);
    notify(shared);
}
fn clear_current(shared: &Shared, take: &Arc<Mutex<Take>>) {
    let mut inner = shared.inner.lock().unwrap();
    if inner
        .take
        .as_ref()
        .is_some_and(|active| Arc::ptr_eq(active, take))
    {
        shared.control.lock().unwrap().set_take_lock(None);
        inner.take = None;
    }
}
fn release_lease(shared: &Shared, take: &mut Take) {
    if let Some(holder) = take.lease_holder.take() {
        shared
            .control
            .lock()
            .unwrap()
            .release(&holder, (shared.clock)().epoch_ms);
    }
}
async fn start_take(shared: Arc<Shared>, options: StartOptions, policy: Policy) -> Value {
    let result = match start_reserved_core(shared.clone(), options, policy).await {
        Ok(take) => start_report(&shared, &take),
        Err(result) => result,
    };
    shared.inner.lock().unwrap().starting = None;
    notify(&shared);
    result
}
fn reserve_start(
    shared: &Shared,
    options: &StartOptions,
    admitted: Option<(TakeImpactTicket, Instant)>,
) -> Option<Value> {
    // Never hold Inner while acquiring an existing take's mutex: receipt
    // publication uses the opposite (take -> writer) order synchronously.
    if let Some(take) = current(shared) {
        return Some(error(format!(
            "Take {} is already recording; stop it with obs-take-stop first",
            take.lock().unwrap().id
        )));
    }
    let mut inner = shared.inner.lock().unwrap();
    if inner.starting.is_some() {
        return Some(error(
            "Another take is being started; wait for it, then stop it with obs-take-stop",
        ));
    }
    if !options.min_free_disk_mb.is_finite()
        || options.min_free_disk_mb < 0.0
        || options.min_free_disk_mb.fract() != 0.0
        || options
            .max_duration_seconds
            .is_some_and(|seconds| seconds == 0 || seconds > 3600)
        || options.chapter.as_ref().is_some_and(String::is_empty)
    {
        return Some(error("Start Take failed: Invalid take options"));
    }
    let (impact, impact_deadline) = if let Some((ticket, deadline)) = admitted {
        if !shared.impact.owns_ticket(&ticket) || matches!(ticket.check(), ImpactStatus::Revoked(_))
        {
            return Some(error(
                "Start Take admission was invalidated by an external recording-impact operation",
            ));
        }
        (Arc::new(ticket), Some(deadline))
    } else {
        match shared.impact.reserve_take() {
            Ok(ticket) => (Arc::new(ticket), None),
            Err(_) => {
                return Some(error(
                    "Start Take is blocked by an external recording-impact operation",
                ));
            }
        }
    };
    inner.completed = None;
    inner.starting = Some(StartReservation {
        cancellation: None,
        impact,
        impact_deadline,
        receipt: None,
        receipt_unavailable: false,
        continuity: Arc::new(Mutex::new(Continuity::default())),
    });
    None
}
async fn start_reserved_core(
    shared: Arc<Shared>,
    options: StartOptions,
    policy: Policy,
) -> Result<Arc<Mutex<Take>>, Value> {
    let (impact, continuity, impact_deadline) = {
        let inner = shared.inner.lock().unwrap();
        let start = inner.starting.as_ref().expect("reserved start");
        (
            start.impact.clone(),
            start.continuity.clone(),
            start
                .impact_deadline
                .unwrap_or_else(|| Instant::now() + shared.limits.setup),
        )
    };
    let cancelled = || {
        shared
            .inner
            .lock()
            .unwrap()
            .starting
            .as_ref()
            .and_then(|start| start.cancellation.as_ref())
            .is_some_and(|cancel| cancel.is_cancelled() || shared.clip_shutdown.is_cancelled())
    };
    if cancelled() {
        return Err(error("Record Clip cancelled before recording started"));
    }
    let preflight = bounded(
        shared.limits.setup,
        shared.io.preflight(PreflightOptions {
            min_free_disk_mb: options.min_free_disk_mb,
            expect_silent: options.expect_silent,
            require_verified: options.require_verified_preflight,
        }),
    )
    .await;
    let Some(preflight) = preflight else {
        return Err(error("Start Take failed: Preflight timed out"));
    };
    if options.require_verified_preflight {
        let missing = DEMO_REQUIRED_CHECKS
            .iter()
            .filter(|id| {
                !preflight
                    .checks
                    .iter()
                    .any(|check| check.id == **id && check.status == Status::Pass)
            })
            .copied()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(error(format!(
                "Not recording; required preflight checks are not verified: {}",
                missing.join(", ")
            )));
        }
    }
    if !preflight.ready {
        return Err(error(format!(
            "Not recording; preflight failed:\n{}",
            preflight
                .checks
                .iter()
                .filter(|check| check.status == Status::Fail)
                .map(|check| format!("- {}: {}", check.id, check.message))
                .collect::<Vec<_>>()
                .join("\n")
        )));
    }
    if !wait_impact(&shared, &impact, &continuity, impact_deadline).await {
        return Err(error(
            "Start Take is blocked or revoked by an external recording-impact operation",
        ));
    }
    if cancelled() {
        return Err(error("Record Clip cancelled before recording started"));
    }
    // Preserve read-only preflight refusal precedence. Inventory remains an
    // authority gate BEFORE any Start intent, even when a fresh impact ticket
    // is Ready; a new ticket cannot clear unresolved durable history.
    let admission = {
        let inner = shared.inner.lock().unwrap();
        inner
            .store
            .as_ref()
            .expect("writer loan")
            .read_view()
            .admission()
    };
    match admission {
        Ok(admission) if !admission.blocking => {}
        Ok(_) => {
            return Err(error(
                "Start Take failed: Operation receipts are unresolved or unavailable; inspect obs-observe before starting another take",
            ));
        }
        Err(_) => {
            return Err(error(
                "Start Take failed: Operation receipt could not be persisted or inspected; inspect obs-observe before any further production change",
            ));
        }
    }
    let id = take_id(&shared);
    // Preserve the original post-preflight event-lineage boundary. The SAME
    // impact ticket crosses this replacement; prior output notifications do
    // not become events for this Start, and a prior veto cannot be erased.
    let continuity = Arc::new(Mutex::new(Continuity::default()));
    let receipt = match prepare(&shared, OperationKind::RecordStart, &id)
        .and_then(|receipt| dispatching(&shared, &receipt))
    {
        Ok(receipt) => receipt,
        Err(_) => {
            return Err(error(
                "Start Take failed: Operation receipt could not be persisted or inspected; inspect obs-observe before any further production change",
            ));
        }
    };
    let cancellation = shared
        .inner
        .lock()
        .unwrap()
        .starting
        .as_ref()
        .and_then(|start| start.cancellation.clone());
    shared.inner.lock().unwrap().starting = Some(StartReservation {
        cancellation,
        impact_deadline: Some(impact_deadline),
        impact: impact.clone(),
        receipt: Some(receipt),
        receipt_unavailable: false,
        continuity: continuity.clone(),
    });
    let accepted_shared = shared.clone();
    let start_io = StartIo {
        shared: shared.clone(),
        policy: policy.clone(),
    };
    let started = output_start::start_output_and_confirm(
        &start_io,
        output_start::Definition {
            timeout: shared.limits.start_confirmation,
            ..output_start::RECORD_OUTPUT
        },
        move || {
            let receipt = accepted_shared
                .inner
                .lock()
                .unwrap()
                .starting
                .as_ref()
                .and_then(|start| start.receipt.clone());
            if let Some(receipt) = receipt {
                let result = accepted(&accepted_shared, &receipt);
                if let Ok(receipt) = result {
                    if let Some(start) = accepted_shared.inner.lock().unwrap().starting.as_mut() {
                        start.receipt = Some(receipt);
                    }
                } else if let Some(start) = accepted_shared.inner.lock().unwrap().starting.as_mut()
                {
                    start.receipt_unavailable = true;
                }
            }
            Ok(())
        },
    )
    .await;
    let mut start_operation = shared
        .inner
        .lock()
        .unwrap()
        .starting
        .as_ref()
        .and_then(|start| start.receipt.clone());
    if started["isError"] == true {
        if let Some(receipt) = &start_operation
            && let Ok(receipt) = update(&shared, receipt, OperationUpdate::new(Phase::Unknown))
        {
            start_operation = Some(receipt);
        }
        let mut result = started;
        result["structuredContent"] = json!({"operationId":start_operation.as_ref().map(OperationReceipt::operation_id),"outcome":"unknown","nextCheck":"Inspect recording status and obs-observe before starting another take"});
        return Err(result);
    }
    let impact_ready = wait_impact(&shared, &impact, &continuity, impact_deadline).await;
    {
        let mut continuity = continuity.lock().unwrap();
        if !continuity.started {
            continuity.lost =
                Some("Start was confirmed only by status; recording identity is uncertain".into());
        }
    }
    let snapshot = (shared.clock)();
    let monitor = TakeMonitorCore::new(
        snapshot.epoch_ms.min(i64::MAX as u64) as i64,
        snapshot.utc,
        MonitorOptions {
            expect_silent: options.expect_silent,
            ..MonitorOptions::default()
        },
    );
    let mut receipt_unavailable = shared
        .inner
        .lock()
        .unwrap()
        .starting
        .as_ref()
        .is_some_and(|start| start.receipt_unavailable);
    if let Some(receipt) = &start_operation {
        let continuous = impact_ready
            && matches!(apply_impact(&impact, &continuity), ImpactStatus::Ready)
            && continuity.lock().unwrap().lost.is_none();
        let mut patch = OperationUpdate::new(if continuous {
            Phase::Observed
        } else {
            Phase::Unknown
        });
        patch.postcondition_observed = Some(continuous);
        match update(&shared, receipt, patch) {
            Ok(receipt) => start_operation = Some(receipt),
            Err(_) => receipt_unavailable = true,
        }
    }
    let deadline = options
        .max_duration_seconds
        .map(|seconds| Instant::now() + Duration::from_secs(u64::from(seconds)));
    let take = Arc::new(Mutex::new(Take {
        impact,
        blocked_impact: None,
        id: id.clone(),
        options: options.clone(),
        policy: policy.clone(),
        local: preflight
            .checks
            .iter()
            .any(|check| check.id == "record-directory" && check.status == Status::Pass),
        monitor,
        continuity,
        start_operation,
        stop_operation: None,
        receipt_unavailable,
        stop_uncertain: false,
        stop_running: false,
        stop_result: None,
        last_stop: None,
        chapter_notes: vec![],
        lease_holder: None,
        deadline,
        deadline_fired: false,
        setup_pending: true,
        monitor_stop_running: false,
        monitor_summary: None,
        next_stats: None,
        next_picture: None,
        program: None,
    }));
    set_current(&shared, take.clone());
    let mut setup = if impact_ready {
        bounded(shared.limits.setup, shared.io.monitor_setup())
            .await
            .unwrap_or_else(|| Err("Take monitor setup timed out".into()))
    } else {
        Err("External recording impact blocked monitor initialization".into())
    };
    let (ticket, continuity) = {
        let data = take.lock().unwrap();
        (data.impact.clone(), data.continuity.clone())
    };
    if !wait_impact(&shared, &ticket, &continuity, impact_deadline).await {
        setup = Err("External recording impact invalidated monitor initialization".into());
    }
    {
        let mut data = take.lock().unwrap();
        data.setup_pending = false;
        if let Ok(setup) = setup.as_ref() {
            data.monitor
                .load_tracks(setup.record_tracks.clone(), setup.input_tracks.clone());
            if let Some(error) = &setup.tracks_error {
                data.monitor.tracks_unknown(now(&shared), error);
            }
            data.monitor.load_program_scene(&setup.program);
            data.program = setup
                .program
                .get("currentProgramSceneName")
                .filter(|value| !value.is_null())
                .or_else(|| setup.program.get("sceneName"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            if let Some(stats) = &setup.stats {
                data.monitor.observe_stats(now(&shared), stats);
            }
            if data
                .monitor
                .complete_setup(now(&shared), setup.supports_ppm)
            {
                data.next_stats = Some(Instant::now() + shared.limits.stats_interval);
                if setup.supports_ppm {
                    data.next_picture = Some(Instant::now() + shared.limits.picture_interval);
                }
            }
        }
    }
    notify(&shared);
    let interrupted = {
        let data = take.lock().unwrap();
        data.stop_running
            || data.stop_result.is_some()
            || data.monitor.phase() != MonitorPhase::Running
    };
    let clip_cancelled = cancelled();
    if setup.is_err() || interrupted || clip_cancelled {
        let setup_failed = setup.is_err();
        let stopped = stop_take(shared.clone(), take.clone(), None, internal_policy(&policy)).await;
        let mut result = stopped.clone();
        result["isError"] = json!(true);
        let prefix = if let Err(message) = setup {
            format!(
                "Take monitor initialization failed: {message}\n{}",
                stopped["content"]
                    .as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .filter_map(|item| item["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n")
            )
        } else if clip_cancelled {
            "Record Clip cancelled during monitor initialization; inspect the stop result".into()
        } else {
            "Take deadline elapsed during monitor initialization; inspect the stop result".into()
        };
        let mut content = vec![json!({"type":"text","text":prefix})];
        if interrupted && !setup_failed {
            content.extend(stopped["content"].as_array().cloned().unwrap_or_default());
        }
        result["content"] = json!(content);
        return Err(result);
    }
    if let Some(name) = options.chapter.as_ref().filter(|name| !name.is_empty()) {
        let _ = mark_take(shared.clone(), take.clone(), name.clone(), policy.clone()).await;
    }
    Ok(take)
}
fn start_report(shared: &Shared, take: &Mutex<Take>) -> Value {
    let mut data = take.lock().unwrap();
    let caller = data.policy.caller.clone();
    let claimed = shared.control.lock().unwrap().claim(
        &caller,
        240,
        &format!("recording take {}", data.id),
        (shared.clock)().epoch_ms,
    );
    let claim_note=match claimed {
        Ok(ClaimOutcome::Claimed(_))=>{data.lease_holder=Some(caller.clone());String::new()},
        Ok(ClaimOutcome::HeldBy(lease))=>format!(". {} already has control of OBS",lease.holder),
        _=>". Other clients sharing this server can still change OBS: this client did not identify itself (over HTTP, add ?client=<name> to the URL)".into(),
    };
    let summary = data.monitor.summary(now(shared));
    let impact = take_impact(&data);
    let lost = data.continuity.lock().unwrap().lost.clone();
    let text=format!("Take {} is recording{}. Watching audio{}; stop it with obs-take-stop{}{}{}",data.id,if data.options.lock {" (profile and output changes locked)"} else {""},if summary.picture_checks=="on" {", frames, and picture"}else{" and frames"},data.options.max_duration_seconds.map_or_else(String::new,|seconds|format!(". Server stop deadline: {seconds}s (requires this process and continuous OBS connection)")),lost.as_ref().map_or_else(String::new,|reason|format!(". Ownership unavailable: {reason}; automatic stop is disabled")),claim_note);
    let mut structured = json!({"takeId":data.id,"receiptStatus":if data.receipt_unavailable {json!("unknown")}else{data.start_operation.as_ref().map(|receipt|receipt.to_value()["phase"].clone()).unwrap_or(Value::Null)},"ownership":if lost.is_some(){"unavailable"}else{"continuous"},"locked":data.options.lock,"pictureChecks":summary.picture_checks,"chapterNotes":data.chapter_notes});
    if matches!(impact, ImpactStatus::Suspended(_)) {
        structured["ownership"] = json!("suspended");
        structured["recordingImpact"] = json!("suspended");
    }
    if let Some(receipt) = &data.start_operation {
        structured["operationId"] = json!(receipt.operation_id());
    }
    if let Some(seconds) = data.options.max_duration_seconds {
        structured["maxDurationSeconds"] = json!(seconds);
    }
    json!({"content":[{"type":"text","text":text}],"structuredContent":structured})
}
async fn clip_wait(shared: &Shared, cancel: &Cancellation, deadline: Instant) -> bool {
    if cancel.is_cancelled() || shared.clip_shutdown.is_cancelled() {
        return false;
    }
    tokio::select! {
        _=sleep_until(deadline)=>!cancel.is_cancelled()&&!shared.clip_shutdown.is_cancelled(),
        _=cancel.cancelled()=>false,
        _=shared.clip_shutdown.cancelled()=>false,
    }
}
async fn run_clip(
    shared: Arc<Shared>,
    request: record_clip::Request,
    policy: Policy,
    cancel: Cancellation,
    ready: oneshot::Sender<()>,
    slot: Reply,
) {
    let started =
        start_reserved_core(shared.clone(), request.start_options(), policy.clone()).await;
    shared.inner.lock().unwrap().starting = None;
    notify(&shared);
    let take = match started {
        Ok(take) => take,
        Err(result) => {
            let _ = ready.send(());
            reply(slot, result);
            return;
        }
    };
    // Source starts this clock after core start and monitor setup, not at ACK.
    let origin = Instant::now();
    let _ = ready.send(());
    let (seconds, chapters) = request.into_schedule();
    for chapter in chapters {
        if !clip_wait(
            &shared,
            &cancel,
            origin + Duration::from_secs_f64(chapter.at_seconds),
        )
        .await
        {
            break;
        }
        let _ = mark_take(
            shared.clone(),
            take.clone(),
            chapter.name,
            internal_policy(&policy),
        )
        .await;
    }
    let _ = clip_wait(&shared, &cancel, origin + Duration::from_secs_f64(seconds)).await;
    // Cancellation wakes local waits, never drops an admitted OBS/media future.
    // The exact captured Take retains its stop/impact/continuity fences.
    let mut result = stop_take(
        shared.clone(),
        take.clone(),
        Some(seconds),
        internal_policy(&policy),
    )
    .await;
    // Source's cached stop result retains its chapterNotes array by reference.
    // Preserve later rejection notes in this clip reply without modifying the
    // historical cached Stop, its file, or any replacement Take's monitor.
    if let Some(fields) = result
        .get_mut("structuredContent")
        .and_then(Value::as_object_mut)
        && fields.contains_key("chapterNotes")
    {
        fields.insert(
            "chapterNotes".into(),
            json!(take.lock().unwrap().chapter_notes),
        );
    }
    reply(slot, result);
    notify(&shared);
}
fn internal_policy(policy: &Policy) -> Policy {
    Policy {
        confirmation_enabled: false,
        ..policy.clone()
    }
}

async fn monitor_stop(shared: &Arc<Shared>, take: &Arc<Mutex<Take>>) -> Result<Summary, String> {
    loop {
        let notified = shared.changed.notified();
        let begin = {
            let mut data = take.lock().unwrap();
            if let Some(summary) = &data.monitor_summary {
                return Ok(summary.clone());
            }
            if data.monitor_stop_running {
                false
            } else {
                data.monitor_stop_running = true;
                data.monitor.begin_stop(now(shared));
                data.next_stats = None;
                data.next_picture = None;
                true
            }
        };
        if !begin {
            notified.await;
            continue;
        }
        if let Some(Ok(stats)) = bounded(
            shared.limits.rpc,
            shared.io.read(ReadRequest::Stats, shared.limits.rpc),
        )
        .await
        {
            take.lock()
                .unwrap()
                .monitor
                .observe_stats(now(shared), &stats);
        }
        loop {
            let notified = shared.changed.notified();
            let ready = {
                let data = take.lock().unwrap();
                !data.setup_pending && !data.monitor.samples_pending()
            };
            if ready {
                break;
            }
            notified.await;
        }
        // A future ending is insufficient if its adapter owns an observation
        // worker. Await the explicit per-take observer join before re-admission,
        // summary publication, file verification, or releasing the take lock.
        if let Err(error) = shared.io.join_observers().await {
            take.lock().unwrap().monitor_stop_running = false;
            notify(shared);
            return Err(format!("Take monitor cleanup is unavailable ({error})"));
        }
        let mut data = take.lock().unwrap();
        data.monitor
            .finish_cleanup()
            .expect("joined monitor samples");
        let summary = data.monitor.summary(now(shared));
        data.monitor_summary = Some(summary.clone());
        notify(shared);
        return Ok(summary);
    }
}
fn uncertain(shared: &Shared, take: &Arc<Mutex<Take>>, reason: String) -> Value {
    let mut data = take.lock().unwrap();
    // An Observed RecordStop is a historical stopped-output fact, not file
    // verification or authority over a later recording. Later impact/file
    // refusal must not rewrite that known past observation as Unknown.
    if let Some(receipt) = &data.stop_operation
        && receipt.phase() != Phase::Observed
        && let Ok(receipt) = update(shared, receipt, OperationUpdate::new(Phase::Unknown))
    {
        data.stop_operation = Some(receipt);
    }
    let impact = take_impact(&data);
    let lost = data.continuity.lock().unwrap().lost.is_some();
    let mut content = json!({"takeId":data.id,"stopConfirmed":false,"ownership":if lost{"unavailable"}else if matches!(impact,ImpactStatus::Suspended(_)){"suspended"}else{"continuous"},"reason":reason});
    let historical = data
        .stop_operation
        .as_ref()
        .is_some_and(|receipt| receipt.phase() == Phase::Observed);
    if let Some(receipt) = &data.stop_operation {
        content["operationId"] = json!(receipt.operation_id());
    }
    let conclusion = if historical {
        content["receiptStatus"] = json!("observed");
        content["stopObservation"] = json!("historical");
        "The prior owned stop was observed, but current recording/file completion is unconfirmed; check OBS. No automatic retry was made"
    } else {
        "Recording stop is unconfirmed; check OBS. No automatic retry was made"
    };
    json!({"content":[{"type":"text","text":format!("Take {}: {reason}. {conclusion}",data.id)}],"isError":true,"structuredContent":content})
}
async fn stop_take(
    shared: Arc<Shared>,
    take: Arc<Mutex<Take>>,
    requested: Option<f64>,
    policy: Policy,
) -> Value {
    let mut waiting = false;
    loop {
        let notified = shared.changed.notified();
        let begin = {
            let mut data = take.lock().unwrap();
            if let Some(result) = &data.stop_result {
                return result.clone();
            }
            if waiting && !data.stop_running {
                return data.last_stop.clone().expect("joined shared stop attempt");
            }
            if data.stop_running {
                false
            } else {
                data.stop_running = true;
                true
            }
        };
        if !begin {
            waiting = true;
            notified.await;
            continue;
        }
        let result = stop_once(shared.clone(), take.clone(), requested, policy).await;
        let completed = !current(&shared).is_some_and(|active| Arc::ptr_eq(&active, &take));
        {
            let mut data = take.lock().unwrap();
            data.stop_running = false;
            data.last_stop = Some(result.clone());
            if completed {
                data.stop_result = Some(result.clone());
            }
        }
        if completed {
            shared.inner.lock().unwrap().completed = Some(result.clone());
        }
        notify(&shared);
        return result;
    }
}
async fn record_status(shared: &Shared, duration: Duration) -> Option<bool> {
    bounded(duration, shared.io.stop_status(duration))
        .await?
        .ok()?
        .get("outputActive")?
        .as_bool()
}
async fn stop_once(
    shared: Arc<Shared>,
    take: Arc<Mutex<Take>>,
    requested: Option<f64>,
    policy: Policy,
) -> Value {
    let impact_deadline = Instant::now() + shared.limits.stop;
    let active = loop {
        if !wait_take_impact(&shared, &take, impact_deadline).await {
            return impact_stop_refusal(&shared, &take).await;
        }
        let Some(active) = record_status(&shared, shared.limits.rpc).await else {
            return uncertain(&shared, &take, "Recording status is unavailable".into());
        };
        let status = {
            let data = take.lock().unwrap();
            take_impact(&data)
        };
        match status {
            ImpactStatus::Ready => break active,
            ImpactStatus::Suspended(_) => continue,
            ImpactStatus::Revoked(_) => return impact_stop_refusal(&shared, &take).await,
        }
    };
    let continuity = take.lock().unwrap().continuity.clone();
    let observed = continuity.lock().unwrap().clone();
    if !active && let Some(lost) = observed.lost.as_ref() {
        take.lock().unwrap().deadline = None;
        if let Err(reason) = monitor_stop(&shared, &take).await {
            return uncertain(&shared, &take, reason);
        }
        {
            let mut data = take.lock().unwrap();
            release_lease(&shared, &mut data);
        }
        clear_current(&shared, &take);
        return uncertain(
            &shared,
            &take,
            format!(
                "{}; OBS is now inactive, but the owned file cannot be established",
                lost
            ),
        );
    }
    if !active && !observed.stopped {
        return uncertain(
            &shared,
            &take,
            "Inactive status has no matching stopped event or completed file".into(),
        );
    }
    if active && (observed.lost.is_some() || observed.stopped) {
        take.lock().unwrap().deadline = None;
        if let Err(reason) = monitor_stop(&shared, &take).await {
            return uncertain(&shared, &take, reason);
        }
        return uncertain(&shared,&take,observed.lost.unwrap_or_else(||"The owned recording stopped; the current recording may belong to another operator".into()));
    }
    let mut output_path = observed.path;
    if active {
        if take.lock().unwrap().stop_uncertain {
            return uncertain(&shared,&take,"A prior stop dispatch is unresolved; only observation can reconcile it, and no stop request was replayed".into());
        }
        let id = take.lock().unwrap().id.clone();
        let existing = take.lock().unwrap().stop_operation.clone();
        let receipt = existing.map_or_else(|| prepare(&shared, OperationKind::RecordStop, &id), Ok);
        let receipt = match receipt.and_then(|receipt| dispatching(&shared, &receipt)) {
            Ok(receipt) => receipt,
            Err(_) => {
                return uncertain(
                    &shared,
                    &take,
                    "Stop intent could not be persisted; no stop request was dispatched".into(),
                );
            }
        };
        take.lock().unwrap().stop_operation = Some(receipt);
        let expectation = take.lock().unwrap().monitor.begin_stop_expectation();
        let deadline = Instant::now() + shared.limits.stop;
        let mut stopped = subscribe(&shared, "RecordStateChanged");
        take.lock().unwrap().stop_uncertain = true;
        let result = dispatch(
            shared.clone(),
            Some(take.clone()),
            Effect::StopRecord,
            policy,
            shared.limits.stop,
        )
        .await;
        match result {
            Ok(_) => {
                let receipt = take.lock().unwrap().stop_operation.clone().unwrap();
                match accepted(&shared,&receipt) {Ok(receipt)=>take.lock().unwrap().stop_operation=Some(receipt),Err(_)=>return uncertain(&shared,&take,"Stop request outcome is uncertain (Operation receipt could not be persisted or inspected; inspect obs-observe before any further production change)".into())}
            }
            Err(error) => {
                if matches!(error, DispatchError::BeforeDispatch(_)) {
                    let retired = match retire_unsent_stop(&shared, &take) {
                        Ok(receipt) => receipt,
                        Err(_) => {
                            return uncertain(
                                &shared,
                                &take,
                                "Unsent Stop receipt could not be safely retired".into(),
                            );
                        }
                    };
                    take.lock()
                        .unwrap()
                        .monitor
                        .rollback_stop_expectation(expectation);
                    let (status, blocked) = {
                        let data = take.lock().unwrap();
                        (take_impact(&data), data.blocked_impact.is_some())
                    };
                    if matches!(status, ImpactStatus::Revoked(_)) {
                        return impact_stop_refusal(&shared, &take).await;
                    }
                    if blocked {
                        let mut result = suspended_stop(&take, true);
                        result["structuredContent"]["operationId"] = json!(retired.operation_id());
                        result["structuredContent"]["receiptStatus"] = json!("not-dispatched");
                        return result;
                    }
                    let data = take.lock().unwrap();
                    let reason = format!("Stop request was not dispatched ({})", error.message());
                    let lost = data.continuity.lock().unwrap().lost.is_some();
                    return json!({"content":[{"type":"text","text":format!("Take {}: {reason}",data.id)}],"isError":true,"structuredContent":{"takeId":data.id,"stopConfirmed":false,"ownership":if lost{"unavailable"}else{"continuous"},"reason":reason,"receiptStatus":"not-dispatched","operationId":retired.operation_id()}});
                }
                if let DispatchError::Rejected { .. } = error {
                    let mut data = take.lock().unwrap();
                    data.stop_uncertain = false;
                    if let Some(receipt) = &data.stop_operation {
                        let mut patch = OperationUpdate::new(Phase::Unknown);
                        patch.request_dispatched = Some(Some(true));
                        patch.request_accepted = Some(Some(false));
                        if let Ok(receipt) = update(&shared, receipt, patch) {
                            data.stop_operation = Some(receipt);
                        }
                    }
                }
                return uncertain(
                    &shared,
                    &take,
                    format!("Stop request outcome is uncertain ({})", error.message()),
                );
            }
        }
        let stopped = loop {
            let event = match timeout_at(deadline, stopped.next()).await {
                Ok(Some(event)) => event,
                _ => break None,
            };
            if event.at > deadline {
                break None;
            }
            if event.data.get("outputState").and_then(Value::as_str)
                == Some("OBS_WEBSOCKET_OUTPUT_STOPPED")
            {
                break Some(event.data);
            }
        };
        if let Some(stopped) = &stopped
            && let Some(path) = stopped.get("outputPath").and_then(Value::as_str)
        {
            output_path = Some(path.into());
        }
        let mut inactive = false;
        while stopped.is_some()
            && continuity.lock().unwrap().lost.is_none()
            && Instant::now() < deadline
        {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let after = record_status(&shared, remaining.max(Duration::from_millis(1))).await;
            if continuity.lock().unwrap().lost.is_some() || Instant::now() >= deadline {
                break;
            }
            match after {
                Some(false) => {
                    inactive = true;
                    break;
                }
                Some(true) => {
                    sleep(
                        Duration::from_millis(200)
                            .min(deadline.saturating_duration_since(Instant::now())),
                    )
                    .await
                }
                None => break,
            }
        }
        if !wait_take_impact(&shared, &take, impact_deadline).await {
            return impact_stop_refusal(&shared, &take).await;
        }
        let lost = continuity.lock().unwrap().lost.clone();
        if let Some(reason) = lost {
            return uncertain(&shared, &take, reason);
        }
        if !inactive {
            return uncertain(
                &shared,
                &take,
                "OBS did not confirm a completed stop and inactive status".into(),
            );
        }
    }
    if !wait_take_impact(&shared, &take, impact_deadline).await {
        return impact_stop_refusal(&shared, &take).await;
    }
    take.lock().unwrap().deadline = None;
    let summary = match monitor_stop(&shared, &take).await {
        Ok(summary) => summary,
        Err(reason) => return uncertain(&shared, &take, reason),
    };
    if !wait_take_impact(&shared, &take, impact_deadline).await {
        return impact_stop_refusal(&shared, &take).await;
    }
    {
        let mut data = take.lock().unwrap();
        release_lease(&shared, &mut data);
        if output_path.is_none() {
            output_path = data.monitor.output_path().map(str::to_owned);
        }
    }
    let Some(path) = output_path.filter(|path| !path.is_empty()) else {
        clear_current(&shared, &take);
        return error(format!(
            "Take {} stopped, but OBS did not report its file",
            take.lock().unwrap().id
        ));
    };
    let id = take.lock().unwrap().id.clone();
    let existing = take.lock().unwrap().stop_operation.clone();
    let receipt = existing.map_or_else(|| prepare(&shared, OperationKind::RecordStop, &id), Ok);
    // V1 RecordStop Observed establishes the stopped-output postcondition,
    // not verified media or current authority. Publish this historical fact
    // before independently retained inspection, so other healthy scopes need
    // not wait for media work. Later impact still vetoes the file/Take result.
    let mut patch = OperationUpdate::new(Phase::Observed);
    patch.postcondition_observed = Some(true);
    let receipt = receipt.and_then(|receipt| update(&shared, &receipt, patch));
    let receipt_status = if let Ok(receipt) = receipt {
        take.lock().unwrap().stop_operation = Some(receipt);
        "observed"
    } else {
        "unknown"
    };
    let (local, silent) = {
        let data = take.lock().unwrap();
        (data.local, data.options.expect_silent)
    };
    let inspection = if local {
        bounded(
            shared.limits.inspection,
            shared.io.inspect(path.clone(), silent),
        )
        .await
        .unwrap_or_else(|| ClipCheck {
            skipped: Some("ffprobe failed: Recording inspection timed out".into()),
            ..ClipCheck::default()
        })
    } else {
        ClipCheck {
            skipped: Some("OBS is remote, so the file was not inspected".into()),
            ..ClipCheck::default()
        }
    };
    if !wait_take_impact(&shared, &take, impact_deadline).await {
        return impact_stop_refusal(&shared, &take).await;
    }
    let log = if local {
        bounded(
            shared.limits.file,
            shared.io.write_log(path.clone(), summary.clone()),
        )
        .await
        .flatten()
    } else {
        None
    };
    if !wait_take_impact(&shared, &take, impact_deadline).await {
        return impact_stop_refusal(&shared, &take).await;
    }
    let mut result = {
        let data = take.lock().unwrap();
        take_result(&data, &summary, &path, &inspection, log, requested)
    };
    result["structuredContent"]["receiptStatus"] = json!(receipt_status);
    result["structuredContent"]["stopConfirmed"] = json!(true);
    if let Some(receipt) = &take.lock().unwrap().stop_operation {
        result["structuredContent"]["operationId"] = json!(receipt.operation_id());
    }
    clear_current(&shared, &take);
    result
}
async fn mark_take(
    shared: Arc<Shared>,
    take: Arc<Mutex<Take>>,
    name: String,
    policy: Policy,
) -> Value {
    let (id, at) = {
        let mut data = take.lock().unwrap();
        let at = data.monitor.now(now(&shared));
        data.monitor.mark(now(&shared), name.clone());
        (data.id.clone(), at)
    };
    let note = dispatch(
        shared.clone(),
        Some(take.clone()),
        Effect::Chapter(name.clone()),
        policy,
        shared.limits.rpc,
    )
    .await
    .err()
    .map(|error| format!("Chapter \"{name}\" not added: {}", error.message()));
    if let Some(note) = &note {
        take.lock().unwrap().chapter_notes.push(note.clone());
    }
    let mut structured = json!({"takeId":id,"atSeconds":at,"name":name});
    if let Some(note) = &note {
        structured["note"] = json!(note);
    }
    notify(&shared);
    json!({"content":[{"type":"text","text":note.unwrap_or_else(||format!("Marked \"{name}\" at {}s",num(at)))}],"structuredContent":structured})
}

fn describe(summary: &Summary) -> Vec<String> {
    let mut lines = vec![
        summary
            .audio
            .iter()
            .find(|level| level.recorded)
            .filter(|level| level.peak_db > summary.silence_threshold_db)
            .map_or_else(
                || {
                    format!(
                        "Monitor: no recorded input rose above {} dB",
                        num(summary.silence_threshold_db)
                    )
                },
                |level| {
                    format!(
                        "Monitor: loudest recorded input {} at {} dB",
                        level.input_name,
                        num(level.peak_db)
                    )
                },
            ),
    ];
    if summary.frames.render_skipped + summary.frames.output_skipped > 0.0 {
        lines.push(format!(
            "Monitor: {} frame(s) missed rendering, {} skipped encoding",
            num(summary.frames.render_skipped),
            num(summary.frames.output_skipped)
        ));
    }
    if summary.picture_checks == "on" {
        let still = summary
            .stills
            .iter()
            .map(|span| span.end_seconds - span.start_seconds)
            .sum::<f64>();
        let still_note = if summary.stills.is_empty() {
            "no qualified unchanged stretch reported".to_owned()
        } else {
            format!("{}s unchanged", num((still * 100.0 + 0.5).floor() / 100.0))
        };
        lines.push(format!(
            "Monitor: {} picture sample(s), {} black stretch(es), {still_note}",
            summary.frames_sampled,
            summary.blanks.len()
        ));
    }
    lines.extend(summary.warnings.iter().map(|warning| {
        format!(
            "Warning at {}s: {}",
            num(warning.at_seconds),
            warning.message
        )
    }));
    lines
}
fn take_result(
    take: &Take,
    summary: &Summary,
    path: &str,
    inspection: &ClipCheck,
    log: Option<String>,
    requested: Option<f64>,
) -> Value {
    let mut problems = summary
        .warnings
        .iter()
        .filter(|warning| {
            warning.kind == WarningKind::Blank
                || warning.kind == WarningKind::Output
                || (warning.kind == WarningKind::Audio && take.options.expect_silent)
        })
        .map(|warning| format!("At {}s: {}", num(warning.at_seconds), warning.message))
        .collect::<Vec<_>>();
    let expected = requested.unwrap_or(summary.duration_seconds);
    if let Some(duration) = inspection
        .duration_seconds
        .filter(|duration| *duration < expected - 2.0)
    {
        problems.push(if let Some(requested) = requested {
            format!(
                "The file is {}s, shorter than the requested {}s",
                fixed1(duration),
                num(requested)
            )
        } else {
            format!(
                "The file is {}s, but the take ran {}s",
                fixed1(duration),
                num(summary.duration_seconds)
            )
        });
    }
    if let Some(volume) = inspection
        .max_volume_db
        .filter(|volume| take.options.expect_silent && *volume > -60.0)
    {
        problems.push(format!(
            "Expected silence, but the audio peaks at {} dB",
            num(volume)
        ));
    }
    if inspection.video_streams == Some(0) {
        problems.push("The recording has no video stream".into());
    }
    let skipped = inspection
        .skipped
        .as_ref()
        .filter(|reason| !reason.is_empty());
    let status = if !problems.is_empty() {
        "fail"
    } else if skipped.is_some() {
        "unavailable"
    } else {
        "pass"
    };
    let mut verification = json!({"status":status,"scope":"File duration, video presence, expected silence, and observed monitor warnings; capture identity and framing require separate review"});
    if let Some(reason) = skipped {
        verification["reason"] = json!(reason);
    }
    let mut lines = vec![
        format!("Recorded {}s to {path}", num(expected)),
        if let Some(reason) = skipped {
            format!("Not verified: {reason}")
        } else {
            format!(
                "File checks {status}: {}s, {} audio stream(s){}",
                inspection
                    .duration_seconds
                    .map_or_else(|| "undefined".into(), fixed1),
                inspection
                    .audio_streams
                    .map_or_else(|| "undefined".into(), |n| n.to_string()),
                inspection
                    .max_volume_db
                    .map_or_else(String::new, |volume| format!(", peak {} dB", num(volume)))
            )
        },
    ];
    lines.extend(
        describe(summary)
            .into_iter()
            .filter(|line| !line.starts_with("Warning")),
    );
    if !summary.chapters.is_empty() {
        lines.push(format!(
            "Chapters: {}",
            summary
                .chapters
                .iter()
                .map(|chapter| format!("{}s {}", num(chapter.at_seconds), chapter.name))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(log) = log.as_ref().filter(|log| !log.is_empty()) {
        lines.push(format!("Take log: {log}"));
    }
    lines.extend(take.chapter_notes.clone());
    lines.extend(problems.iter().map(|problem| format!("Problem: {problem}")));
    let mut structured = serde_json::to_value(inspection).unwrap();
    let object = structured.as_object_mut().unwrap();
    object.extend(json!({"takeId":take.id,"verification":verification,"outputPath":path,"chapterNotes":take.chapter_notes,"problems":problems,"takeLog":log,"take":summary}).as_object().unwrap().clone());
    let mut result =
        json!({"content":[{"type":"text","text":lines.join("\n")}],"structuredContent":structured});
    if !problems.is_empty() {
        result["isError"] = json!(true);
    }
    result
}
async fn status(shared: Arc<Shared>, take: Arc<Mutex<Take>>) -> Value {
    let (id, max, summary) = {
        let data = take.lock().unwrap();
        (
            data.id.clone(),
            data.options.max_duration_seconds,
            data.monitor.summary(now(&shared)),
        )
    };
    let record = bounded(
        shared.limits.rpc,
        shared.io.read(ReadRequest::RecordStatus, shared.limits.rpc),
    )
    .await
    .and_then(Result::ok)
    .unwrap_or_else(|| json!({}));
    let (lost, impact) = {
        let data = take.lock().unwrap();
        let impact = take_impact(&data);
        let lost = data.continuity.lock().unwrap().lost.clone();
        (lost, impact)
    };
    let mut first = format!("Take {id}: {}s", num(summary.duration_seconds));
    if let Some(bytes) = record.get("outputBytes").and_then(Value::as_f64) {
        first.push_str(&format!(", {} MB", fixed1(bytes / 1_048_576.0)));
    }
    if record.get("outputPaused") == Some(&Value::Bool(true)) {
        first.push_str(", paused");
    }
    let mut lines = vec![first];
    lines.extend(describe(&summary));
    let mut structured = json!({"takeId":id,"ownership":if lost.is_some(){"unavailable"}else{"continuous"},"record":record,"take":summary});
    if matches!(impact, ImpactStatus::Suspended(_)) {
        structured["ownership"] = json!("suspended");
        structured["recordingImpact"] = json!("suspended");
    }
    if let Some(max) = max {
        structured["maxDurationSeconds"] = json!(max);
    }
    if let Some(reason) = lost {
        structured["ownershipReason"] = json!(reason);
    }
    json!({"content":[{"type":"text","text":lines.join("\n")}],"structuredContent":structured})
}
fn num(n: f64) -> String {
    if n.is_nan() {
        "NaN".into()
    } else if n == f64::NEG_INFINITY {
        "-Infinity".into()
    } else if n == f64::INFINITY {
        "Infinity".into()
    } else if n == 0.0 {
        "0".into()
    } else {
        n.to_string()
    }
}
fn fixed1(n: f64) -> String {
    if !n.is_finite() || n.abs() >= 1e21 {
        return num(n);
    }
    let value = n.abs();
    let bits = value.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32;
    let mantissa =
        u128::from((bits & ((1_u64 << 52) - 1)) | if exponent == 0 { 0 } else { 1_u64 << 52 }) * 10;
    let shift = if exponent == 0 {
        -1074
    } else {
        exponent - 1023 - 52
    };
    let tenths = if shift >= 0 {
        mantissa << shift
    } else if -shift >= 128 {
        0
    } else {
        let divisor = 1_u128 << (-shift);
        mantissa / divisor + u128::from(mantissa % divisor >= divisor / 2)
    };
    format!(
        "{}{}.{}",
        if n < 0.0 { "-" } else { "" },
        tenths / 10,
        tenths % 10
    )
}

fn observe(shared: &Shared, event: Event) {
    if let Event::Observed(frame) = event {
        frame.apply(|connection, event_type, data| {
            observe(
                shared,
                Event::Obs {
                    connection,
                    event_type,
                    data,
                },
            );
        });
        return;
    }
    if let Event::Barrier { connection, done } = event {
        if connection == shared.session.token() {
            let _ = done.send(());
        }
        return;
    }
    let (connection, name, data) = match event {
        Event::Obs {
            connection,
            event_type,
            data,
        } => (connection, event_type, data),
        Event::Disconnected { connection } => (connection, "disconnected".into(), Value::Null),
        Event::Barrier { .. } | Event::Observed(_) => {
            unreachable!("owned frame and barrier handled above")
        }
    };
    let (take, starting) = {
        let inner = shared.inner.lock().unwrap();
        (
            inner.take.clone(),
            inner
                .starting
                .as_ref()
                .map(|start| start.continuity.clone()),
        )
    };
    let continuity = take
        .as_ref()
        .map(|take| take.lock().unwrap().continuity.clone())
        .or(starting);
    if let Some(continuity) = continuity {
        let mut state = continuity.lock().unwrap();
        if connection != shared.session.token() || name == "disconnected" {
            state.lost = Some(
                "OBS disconnected; recording ownership cannot be established after reconnect"
                    .into(),
            );
        } else if name == "RecordStateChanged" {
            match data.get("outputState").and_then(Value::as_str) {
                Some("OBS_WEBSOCKET_OUTPUT_STARTED") => {
                    if state.started || state.stopped {
                        state.lost = Some("A different recording may have started".into());
                    }
                    state.started = true;
                }
                Some("OBS_WEBSOCKET_OUTPUT_STOPPED") => state.stopped = true,
                _ => {}
            }
            if state.lost.is_none()
                && let Some(path) = data
                    .get("outputPath")
                    .and_then(Value::as_str)
                    .filter(|path| !path.is_empty())
            {
                state.path = Some(path.into());
            }
        }
    }
    if let Some(take) = take {
        let mut take = take.lock().unwrap();
        if name == "disconnected" || connection != shared.session.token() {
            take.monitor.on_disconnect(now(shared));
            if let Some(receipt) = &take.start_operation
                && let Ok(receipt) = update(shared, receipt, OperationUpdate::new(Phase::Unknown))
            {
                take.start_operation = Some(receipt);
            }
        } else {
            take.monitor.on_event(now(shared), &name, &data);
            if name == "CurrentProgramSceneChanged"
                && let Some(scene) = data.get("sceneName").and_then(Value::as_str)
            {
                take.program = Some(scene.into());
            }
        }
    }
    if connection == shared.session.token() {
        let _ = shared.events.send((name, Instant::now(), data));
    }
    notify(shared);
}
fn eligible_deadline(deadline: Option<Instant>, fired: bool, job_active: bool) -> Option<Instant> {
    deadline.filter(|_| !fired && !job_active)
}
fn reserve_deadline_job(
    deadline: Option<Instant>,
    fired: &mut bool,
    job_active: bool,
    now: Instant,
) -> bool {
    let due = eligible_deadline(deadline, *fired, job_active).is_some_and(|at| at <= now);
    if due {
        *fired = true;
    }
    due
}
fn schedule(
    shared: &Arc<Shared>,
    jobs: &mut JoinSet<()>,
    deadline_job_active: bool,
) -> Option<Instant> {
    let take = current(shared)?;
    let mut data = take.lock().unwrap();
    let mut next = vec![];
    if let Some(deadline) =
        eligible_deadline(data.deadline, data.deadline_fired, deadline_job_active)
    {
        next.push(deadline);
    }
    for (kind, at, interval) in [
        (
            SampleKind::Stats,
            data.next_stats,
            shared.limits.stats_interval,
        ),
        (
            SampleKind::Picture,
            data.next_picture,
            shared.limits.picture_interval,
        ),
    ] {
        if let Some(at) = at {
            if at <= Instant::now() {
                match kind {
                    SampleKind::Stats => data.next_stats = Some(Instant::now() + interval),
                    SampleKind::Picture => data.next_picture = Some(Instant::now() + interval),
                }
                if let Some(ticket) = data.monitor.begin_sample(kind) {
                    let shared = shared.clone();
                    let take = take.clone();
                    let program = data.program.clone();
                    jobs.spawn(async move {
                        match kind {
                            SampleKind::Stats => {
                                let result = bounded(
                                    shared.limits.rpc,
                                    shared.io.read(ReadRequest::Stats, shared.limits.rpc),
                                )
                                .await
                                .and_then(Result::ok);
                                let mut data = take.lock().unwrap();
                                if let Some(stats) = result {
                                    data.monitor.finish_sample(
                                        ticket,
                                        now(&shared),
                                        SampleObservation::Stats(&stats),
                                    );
                                } else {
                                    data.monitor.finish_sample(
                                        ticket,
                                        now(&shared),
                                        SampleObservation::Failed,
                                    );
                                }
                            }
                            SampleKind::Picture => {
                                let result = if let Some(program) = program {
                                    bounded(shared.limits.rpc, shared.io.picture(program))
                                        .await
                                        .and_then(Result::ok)
                                } else {
                                    None
                                };
                                let mut data = take.lock().unwrap();
                                if let Some(luma) = result {
                                    data.monitor.finish_sample(
                                        ticket,
                                        now(&shared),
                                        SampleObservation::Picture(&luma),
                                    );
                                } else {
                                    data.monitor.finish_sample(
                                        ticket,
                                        now(&shared),
                                        SampleObservation::Failed,
                                    );
                                }
                            }
                        }
                        notify(&shared);
                    });
                }
                next.push(Instant::now() + interval);
            } else {
                next.push(at);
            }
        }
    }
    next.into_iter().min()
}
fn refresh_impacts(shared: &Arc<Shared>, deadline_job_active: bool) {
    let starting = {
        let inner = shared.inner.lock().unwrap();
        inner
            .starting
            .as_ref()
            .map(|start| (start.impact.clone(), start.continuity.clone()))
    };
    if let Some((ticket, continuity)) = starting {
        apply_impact(&ticket, &continuity);
    }
    if let Some(take) = current(shared) {
        let mut data = take.lock().unwrap();
        match take_impact(&data) {
            ImpactStatus::Ready => {
                if data.blocked_impact.is_some()
                    && !deadline_job_active
                    && !data.stop_running
                    && !data.stop_uncertain
                {
                    data.blocked_impact = None;
                    // The ticket watermark proves every intervening cleared
                    // attempt was exact NoDispatch, even if signals coalesced.
                    // Preserve the original absolute deadline; never extend it.
                    if data.deadline.is_some() {
                        data.deadline_fired = false;
                    }
                }
            }
            ImpactStatus::Suspended(id) => {
                if data.blocked_impact.is_some() {
                    data.blocked_impact = Some(id);
                }
            }
            ImpactStatus::Revoked(_) => {
                data.blocked_impact = None;
                // An unfired original deadline still owns one read-only
                // reconciliation. Its revoked ticket cannot dispatch Stop or
                // promote a file; inactive status may retire the old lease.
                // A previously fired suspension is not rearmed on revocation.
            }
        }
    }
}
fn job_completed(
    shared: &Arc<Shared>,
    result: Result<(Id, ()), JoinError>,
    deadline_job: &mut Option<(Id, Arc<Mutex<Take>>)>,
) {
    let id = match &result {
        Ok((id, ())) => *id,
        Err(error) => error.id(),
    };
    if deadline_job
        .as_ref()
        .is_some_and(|(active, _)| *active == id)
    {
        *deadline_job = None;
    }
    if result.is_err() {
        observe(
            shared,
            Event::Disconnected {
                connection: shared.session.token(),
            },
        );
    }
    notify(shared);
}
async fn deadline_stop(shared: Arc<Shared>, take: Arc<Mutex<Take>>) {
    let was_running = take.lock().unwrap().stop_running;
    let policy = internal_policy(&take.lock().unwrap().policy);
    stop_take(shared.clone(), take.clone(), None, policy.clone()).await;
    let blocked = take.lock().unwrap().blocked_impact.is_some();
    if was_running && !blocked && current(&shared).is_some_and(|active| Arc::ptr_eq(&active, &take))
    {
        stop_take(shared, take, None, policy).await;
    }
}
async fn actor(
    shared: Arc<Shared>,
    mut commands: mpsc::Receiver<Command>,
    mut events: mpsc::Receiver<Event>,
    mut stopping: watch::Receiver<bool>,
    summary: watch::Sender<Value>,
    mut impact_changes: ImpactChanges,
) -> Joined {
    let mut jobs = JoinSet::new();
    let mut deadline_job: Option<(Id, Arc<Mutex<Take>>)> = None;
    let mut impact_open = true;
    let mut events_open = true;
    loop {
        // Completed handles must not accumulate behind a continuously ready
        // event stream; live frontend work has32 admissions, with one worker
        // per monitor sample kind and a single deadline reconciliation job.
        while let Some(result) = jobs.try_join_next_with_id() {
            job_completed(&shared, result, &mut deadline_job);
        }
        refresh_impacts(&shared, deadline_job.is_some());
        if *stopping.borrow() {
            break;
        }
        if let Some(take) = current(&shared) {
            let due = {
                let mut data = take.lock().unwrap();
                let deadline = data.deadline;
                reserve_deadline_job(
                    deadline,
                    &mut data.deadline_fired,
                    deadline_job.is_some(),
                    Instant::now(),
                )
            };
            if due {
                let worker = jobs.spawn(deadline_stop(shared.clone(), take.clone()));
                deadline_job = Some((worker.id(), take));
            }
        }
        let next = schedule(&shared, &mut jobs, deadline_job.is_some());
        summary.send_replace(read_snapshot(&shared));
        tokio::select! {
            _=stopping.changed()=>break,
            event=events.recv(),if events_open=>match event {Some(event)=>observe(&shared,event),None=>{events_open=false;observe(&shared,Event::Disconnected {connection:shared.session.token()});}},
            _=shared.changed.notified()=>{},
            changed=impact_changes.changed(),if impact_open=>{if changed.is_err(){impact_open=false;}},
            _=async {sleep_until(next.unwrap()).await},if next.is_some()=>{},
            result=jobs.join_next_with_id(),if !jobs.is_empty()=>{if let Some(result)=result{job_completed(&shared,result,&mut deadline_job);}},
            command=commands.recv()=>match command {
                None=>break,
                Some(Command::Start(options,policy,admitted,slot))=>{
                    let refusal=reserve_start(&shared,&options,admitted);
                    if let Some(result)=refusal {reply(slot,result);}else{let shared=shared.clone();jobs.spawn(async move{let result=start_take(shared.clone(),options,policy).await;reply(slot,result);notify(&shared);});}
                },
                Some(Command::Clip(request,policy,cancel,admitted,ready,slot))=>{
                    let refusal=request.refusal().or_else(|| {
                        if cancel.is_cancelled() { Some(error("Record Clip cancelled before recording started")) }
                        else { reserve_start(&shared,&request.start_options(),admitted) }
                    });
                    if let Some(result)=refusal {let _=ready.send(());reply(slot,result);}else{
                        shared.inner.lock().unwrap().starting.as_mut().expect("reserved clip start").cancellation=Some(cancel.clone());
                        let shared=shared.clone();jobs.spawn(async move{run_clip(shared,request,policy,cancel,ready,slot).await;});
                    }
                },
                Some(Command::Stop(requested,policy,slot))=>{
                    if let Some(take)=current(&shared) {let shared=shared.clone();jobs.spawn(async move{let result=stop_take(shared.clone(),take,requested,policy).await;reply(slot,result);notify(&shared);});}
                    else {reply(slot,shared.inner.lock().unwrap().completed.clone().unwrap_or_else(no_take));}
                },
                Some(Command::Mark(name,policy,slot))=>{
                    if let Some(take)=current(&shared) {let shared=shared.clone();jobs.spawn(async move{let result=mark_take(shared.clone(),take,name,policy).await;reply(slot,result);notify(&shared);});}else{reply(slot,no_take());}
                },
                Some(Command::Status(slot))=>{
                    if let Some(take)=current(&shared) {let shared=shared.clone();jobs.spawn(async move{let result=status(shared.clone(),take).await;reply(slot,result);notify(&shared);});}else{reply(slot,no_take());}
                },
            },
        }
    }
    shared.closing.store(true, Ordering::Release);
    shared.clip_shutdown.cancel();
    commands.close();
    while let Some(command) = commands.recv().await {
        match command {
            Command::Start(_, _, _, reply_slot)
            | Command::Stop(_, _, reply_slot)
            | Command::Mark(_, _, reply_slot)
            | Command::Status(reply_slot) => reply(reply_slot, error("Take supervisor is closing")),
            Command::Clip(_, _, cancel, _, ready, reply_slot) => {
                cancel.cancel();
                let _ = ready.send(());
                reply(reply_slot, error("Take supervisor is closing"));
            }
        }
    }
    // Continue pumping continuity while joining every admitted worker. Only
    // after start/setup work settles can cleanup choose a stop for this take.
    while !jobs.is_empty() {
        tokio::select! {event=events.recv(),if events_open=>match event {Some(event)=>observe(&shared,event),None=>{events_open=false;observe(&shared,Event::Disconnected {connection:shared.session.token()});}},result=jobs.join_next_with_id()=>{if let Some(result)=result{job_completed(&shared,result,&mut deadline_job);}}}
    }
    let result = if let Some(take) = current(&shared) {
        let worker = shared.clone();
        let policy = internal_policy(&take.lock().unwrap().policy);
        jobs.spawn(async move {
            let _ = stop_take(worker, take, None, policy).await;
        });
        while !jobs.is_empty() {
            tokio::select! {event=events.recv(),if events_open=>match event {Some(event)=>observe(&shared,event),None=>{events_open=false;observe(&shared,Event::Disconnected {connection:shared.session.token()});}},result=jobs.join_next_with_id()=>{if let Some(result)=result{job_completed(&shared,result,&mut deadline_job);}}}
        }
        let completed = shared.inner.lock().unwrap().completed.clone();
        completed.unwrap_or_else(|| {
            uncertain(
                &shared,
                &current(&shared).unwrap(),
                "Supervisor cleanup could not establish a stopped owned recording".into(),
            )
        })
    } else {
        shared
            .inner
            .lock()
            .unwrap()
            .completed
            .clone()
            .unwrap_or_else(no_take)
    };
    shared.closed.store(true, Ordering::Release);
    // Cleanup is adapter-owned and independently bounded. Never turn a host
    // wait budget into permission to drop an unfinished observer join.
    let observers_joined = shared.io.join_observers().await.is_ok();
    summary.send_replace(read_snapshot(&shared));
    let store = shared
        .inner
        .lock()
        .unwrap()
        .store
        .take()
        .expect("sole writer returned after worker join");
    let stop_confirmed = result["structuredContent"]["stopConfirmed"] == true;
    Joined {
        store: Some(store),
        receipt: ShutdownReceipt {
            joined: true,
            stop_confirmed,
            result,
            observers_joined,
        },
    }
}

#[cfg(test)]
mod impact_deadline_tests {
    use super::*;
    #[test]
    fn a_due_new_take_keeps_its_deadline_when_an_older_deadline_worker_occupies_the_slot() {
        let now = Instant::now();
        let original = now - Duration::from_millis(1);
        let mut fired = false;
        assert!(!reserve_deadline_job(Some(original), &mut fired, true, now));
        assert!(
            !fired,
            "an occupied worker slot must not consume the new Take deadline"
        );
        assert!(reserve_deadline_job(Some(original), &mut fired, false, now));
        assert!(!reserve_deadline_job(
            Some(original),
            &mut fired,
            false,
            now
        ));
    }
    #[test]
    fn an_occupied_old_worker_suppresses_due_timer_selection_until_joined_without_resetting_time() {
        let now = Instant::now();
        let original = now - Duration::from_millis(1);
        assert_eq!(
            eligible_deadline(Some(original), false, true),
            None,
            "already-due timer must not spin while old worker is retained"
        );
        assert_eq!(
            eligible_deadline(Some(original), false, false),
            Some(original)
        );
        assert_eq!(eligible_deadline(Some(original), true, false), None);
    }
}

#[cfg(test)]
mod admitted_impact_tests {
    use super::*;
    use crate::{
        clock,
        session::{ObsSession, SessionOptions},
    };
    use futures_util::{SinkExt, StreamExt};
    struct NoEffects;
    impl TakeIo for NoEffects {
        fn preflight(&self, _: PreflightOptions) -> IoFuture<PreflightReport> {
            panic!("wrong registry reached preflight")
        }
        fn read(&self, _: ReadRequest, _: Duration) -> IoFuture<Result<Value, String>> {
            panic!("wrong registry reached read")
        }
        fn dispatch(
            &self,
            _: ConnectionToken,
            _: Effect,
            _: Duration,
            _: DispatchGuard,
        ) -> IoFuture<Result<Value, DispatchError>> {
            panic!("wrong registry dispatched")
        }
        fn monitor_setup(&self) -> IoFuture<Result<MonitorSetup, String>> {
            panic!("wrong registry reached monitor")
        }
        fn picture(&self, _: String) -> IoFuture<Result<Vec<u8>, String>> {
            panic!("wrong registry reached picture")
        }
        fn inspect(&self, _: String, _: bool) -> IoFuture<ClipCheck> {
            panic!("wrong registry reached inspection")
        }
        fn write_log(&self, _: String, _: Summary) -> IoFuture<Option<String>> {
            panic!("wrong registry reached log")
        }
        fn join_observers(&self) -> IoFuture<Result<(), String>> {
            Box::pin(async { Ok(()) })
        }
    }
    #[tokio::test]
    async fn supplied_ticket_from_other_registry_on_same_socket_is_refused_before_any_work() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut peer = tokio_tungstenite::accept_async(socket).await.unwrap();
            peer.send(tokio_tungstenite::tungstenite::Message::Text(
                json!({"op":0,"d":{"rpcVersion":1,"obsWebSocketVersion":"5.7.0"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            assert!(peer.next().await.is_some());
            peer.send(tokio_tungstenite::tungstenite::Message::Text(
                json!({"op":2,"d":{"negotiatedRpcVersion":1}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            while let Some(frame) = peer.next().await {
                assert!(
                    !matches!(frame, Ok(tokio_tungstenite::tungstenite::Message::Text(_))),
                    "unexpected RPC"
                );
                if matches!(
                    frame,
                    Ok(tokio_tungstenite::tungstenite::Message::Close(_)) | Err(_)
                ) {
                    let _ = peer.flush().await;
                    break;
                }
            }
        });
        let (session, _events) = ObsSession::connect(
            &url,
            None,
            SessionOptions {
                event_subscriptions: REQUIRED_SUBSCRIPTIONS,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let handle = session.handle();
        let correct = RecordImpactOwner::bind(&handle).unwrap();
        let other = RecordImpactOwner::bind(&handle).unwrap();
        let ticket = other.view().reserve_take().unwrap();
        assert_eq!(ticket.token(), handle.token());
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let base = std::env::var_os("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"));
        std::fs::create_dir_all(&base).unwrap();
        let path = base.join(format!(
            "wrong-registry-take-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        let (_sender, events) = mpsc::channel(1);
        let mut take = TakeSupervisor::spawn_with_record_impact(
            Loan {
                session: handle,
                store: OperationStore::new(&path, &url).unwrap(),
                control: Arc::new(Mutex::new(crate::control::ControlDomain::default())),
                clock: Arc::new(|| {
                    clock::from_epoch_duration(Duration::from_millis(1_793_448_000_000)).unwrap()
                }),
                identities: Box::new(|| panic!("wrong registry allocated operation identity")),
                io: Arc::new(NoEffects),
                events,
            },
            Limits::default(),
            correct.view(),
        )
        .unwrap();
        let result = take
            .handle()
            .start_with_impact_ticket(
                StartOptions::default(),
                Policy::default(),
                ticket,
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(result["isError"], true);
        assert!(result.to_string().contains("admission was invalidated"));
        let joined = take.shutdown().await.unwrap();
        assert!(joined.joined && joined.observers_joined);
        assert!(take.take_store().is_some());
        session.shutdown().await.unwrap();
        peer.await.unwrap();
        std::fs::remove_dir_all(path).unwrap();
    }
}
