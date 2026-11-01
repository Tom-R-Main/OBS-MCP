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
    session::{ALL_EVENTS, ConnectionToken, SessionHandle},
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
    task::{JoinHandle, JoinSet},
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
enum Command {
    Start(StartOptions, Policy, Reply),
    Stop(Option<f64>, Policy, Reply),
    Mark(String, Policy, Reply),
    Status(Reply),
}
#[derive(Default, Clone)]
struct Continuity {
    started: bool,
    stopped: bool,
    lost: Option<String>,
    path: Option<String>,
}
struct StartReservation {
    receipt: Option<OperationReceipt>,
    receipt_unavailable: bool,
    continuity: Arc<Mutex<Continuity>>,
}
struct Take {
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
fn read_snapshot(shared: &Shared) -> Value {
    let Some(take) = current(shared) else {
        return Value::Null;
    };
    let take = take.lock().unwrap();
    let continuity = take.continuity.lock().unwrap();
    json!({"takeId":take.id,"locked":take.options.lock,"maxDurationSeconds":take.options.max_duration_seconds,"ownership":if continuity.lost.is_some(){"unavailable"}else{"continuous"},"take":take.monitor.summary(now(shared))})
}
impl TakeSupervisor {
    pub fn spawn(loan: Loan, limits: Limits) -> Result<Self, SpawnFailure> {
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
        let task = tokio::spawn(actor(shared, commands, loan.events, stopping, summaries));
        Ok(Self {
            handle,
            shutdown,
            task: Some(task),
            joined: None,
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
        self.send(Command::Start(options, policy, reply), receiver)
            .await
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
        let lost = take.continuity.lock().unwrap().lost.clone();
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
    let result = start_reserved(shared.clone(), options, policy).await;
    shared.inner.lock().unwrap().starting = None;
    notify(&shared);
    result
}
fn reserve_start(shared: &Shared, options: &StartOptions) -> Option<Value> {
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
    inner.completed = None;
    inner.starting = Some(StartReservation {
        receipt: None,
        receipt_unavailable: false,
        continuity: Arc::new(Mutex::new(Continuity::default())),
    });
    None
}
async fn start_reserved(shared: Arc<Shared>, options: StartOptions, policy: Policy) -> Value {
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
        return error("Start Take failed: Preflight timed out");
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
            return error(format!(
                "Not recording; required preflight checks are not verified: {}",
                missing.join(", ")
            ));
        }
    }
    if !preflight.ready {
        return error(format!(
            "Not recording; preflight failed:\n{}",
            preflight
                .checks
                .iter()
                .filter(|check| check.status == Status::Fail)
                .map(|check| format!("- {}: {}", check.id, check.message))
                .collect::<Vec<_>>()
                .join("\n")
        ));
    }
    let id = take_id(&shared);
    let continuity = Arc::new(Mutex::new(Continuity::default()));
    let receipt = match prepare(&shared, OperationKind::RecordStart, &id)
        .and_then(|receipt| dispatching(&shared, &receipt))
    {
        Ok(receipt) => receipt,
        Err(_) => {
            return error(
                "Start Take failed: Operation receipt could not be persisted or inspected; inspect obs-observe before any further production change",
            );
        }
    };
    shared.inner.lock().unwrap().starting = Some(StartReservation {
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
        return result;
    }
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
        let continuous = continuity.lock().unwrap().lost.is_none();
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
    let setup = bounded(shared.limits.setup, shared.io.monitor_setup())
        .await
        .unwrap_or_else(|| Err("Take monitor setup timed out".into()));
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
    if setup.is_err() || interrupted {
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
        } else {
            "Take deadline elapsed during monitor initialization; inspect the stop result".into()
        };
        let mut content = vec![json!({"type":"text","text":prefix})];
        if interrupted && !setup_failed {
            content.extend(stopped["content"].as_array().cloned().unwrap_or_default());
        }
        result["content"] = json!(content);
        return result;
    }
    if let Some(name) = options.chapter.as_ref().filter(|name| !name.is_empty()) {
        let _ = mark_take(shared.clone(), take.clone(), name.clone(), policy.clone()).await;
    }
    let mut data = take.lock().unwrap();
    let claimed = shared.control.lock().unwrap().claim(
        &policy.caller,
        240,
        &format!("recording take {}", data.id),
        (shared.clock)().epoch_ms,
    );
    let claim_note=match claimed {
        Ok(ClaimOutcome::Claimed(_))=>{data.lease_holder=Some(policy.caller.clone());String::new()},
        Ok(ClaimOutcome::HeldBy(lease))=>format!(". {} already has control of OBS",lease.holder),
        _=>". Other clients sharing this server can still change OBS: this client did not identify itself (over HTTP, add ?client=<name> to the URL)".into(),
    };
    let summary = data.monitor.summary(now(&shared));
    let lost = data.continuity.lock().unwrap().lost.clone();
    let text=format!("Take {} is recording{}. Watching audio{}; stop it with obs-take-stop{}{}{}",data.id,if data.options.lock {" (profile and output changes locked)"} else {""},if summary.picture_checks=="on" {", frames, and picture"}else{" and frames"},data.options.max_duration_seconds.map_or_else(String::new,|seconds|format!(". Server stop deadline: {seconds}s (requires this process and continuous OBS connection)")),lost.as_ref().map_or_else(String::new,|reason|format!(". Ownership unavailable: {reason}; automatic stop is disabled")),claim_note);
    let mut structured = json!({"takeId":data.id,"receiptStatus":if data.receipt_unavailable {json!("unknown")}else{data.start_operation.as_ref().map(|receipt|receipt.to_value()["phase"].clone()).unwrap_or(Value::Null)},"ownership":if lost.is_some(){"unavailable"}else{"continuous"},"locked":data.options.lock,"pictureChecks":summary.picture_checks,"chapterNotes":data.chapter_notes});
    if let Some(receipt) = &data.start_operation {
        structured["operationId"] = json!(receipt.operation_id());
    }
    if let Some(seconds) = data.options.max_duration_seconds {
        structured["maxDurationSeconds"] = json!(seconds);
    }
    json!({"content":[{"type":"text","text":text}],"structuredContent":structured})
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
    if let Some(receipt) = &data.stop_operation
        && let Ok(receipt) = update(shared, receipt, OperationUpdate::new(Phase::Unknown))
    {
        data.stop_operation = Some(receipt);
    }
    let lost = data.continuity.lock().unwrap().lost.is_some();
    let mut content = json!({"takeId":data.id,"stopConfirmed":false,"ownership":if lost{"unavailable"}else{"continuous"},"reason":reason});
    if let Some(receipt) = &data.stop_operation {
        content["operationId"] = json!(receipt.operation_id());
    }
    json!({"content":[{"type":"text","text":format!("Take {}: {reason}. Recording stop is unconfirmed; check OBS. No automatic retry was made",data.id)}],"isError":true,"structuredContent":content})
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
    let Some(active) = record_status(&shared, shared.limits.rpc).await else {
        return uncertain(&shared, &take, "Recording status is unavailable".into());
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
    take.lock().unwrap().monitor.expect_stop();
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
    take.lock().unwrap().deadline = None;
    let summary = match monitor_stop(&shared, &take).await {
        Ok(summary) => summary,
        Err(reason) => return uncertain(&shared, &take, reason),
    };
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
    let (id, max, lost, summary) = {
        let data = take.lock().unwrap();
        let lost = data.continuity.lock().unwrap().lost.clone();
        (
            data.id.clone(),
            data.options.max_duration_seconds,
            lost,
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
fn schedule(shared: &Arc<Shared>, jobs: &mut JoinSet<()>) -> Option<Instant> {
    let take = current(shared)?;
    let mut data = take.lock().unwrap();
    let mut next = vec![];
    if let Some(deadline) = data.deadline.filter(|_| !data.deadline_fired) {
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
async fn deadline_stop(shared: Arc<Shared>, take: Arc<Mutex<Take>>) {
    let was_running = take.lock().unwrap().stop_running;
    let policy = internal_policy(&take.lock().unwrap().policy);
    stop_take(shared.clone(), take.clone(), None, policy.clone()).await;
    if was_running && current(&shared).is_some_and(|active| Arc::ptr_eq(&active, &take)) {
        stop_take(shared, take, None, policy).await;
    }
}
async fn actor(
    shared: Arc<Shared>,
    mut commands: mpsc::Receiver<Command>,
    mut events: mpsc::Receiver<Event>,
    mut stopping: watch::Receiver<bool>,
    summary: watch::Sender<Value>,
) -> Joined {
    let mut jobs = JoinSet::new();
    let mut events_open = true;
    loop {
        // Completed handles must not accumulate behind a continuously ready
        // event stream; live frontend work has32 admissions, with one worker
        // per monitor sample kind and a single deadline reconciliation job.
        while let Some(result) = jobs.try_join_next() {
            if result.is_err() {
                observe(
                    &shared,
                    Event::Disconnected {
                        connection: shared.session.token(),
                    },
                );
            }
            notify(&shared);
        }
        if *stopping.borrow() {
            break;
        }
        if let Some(take) = current(&shared) {
            let due = {
                let mut data = take.lock().unwrap();
                if data
                    .deadline
                    .is_some_and(|deadline| deadline <= Instant::now())
                    && !data.deadline_fired
                {
                    data.deadline_fired = true;
                    true
                } else {
                    false
                }
            };
            if due {
                jobs.spawn(deadline_stop(shared.clone(), take));
            }
        }
        let next = schedule(&shared, &mut jobs);
        summary.send_replace(read_snapshot(&shared));
        tokio::select! {
            _=stopping.changed()=>break,
            event=events.recv(),if events_open=>match event {Some(event)=>observe(&shared,event),None=>{events_open=false;observe(&shared,Event::Disconnected {connection:shared.session.token()});}},
            _=shared.changed.notified()=>{},
            _=async {sleep_until(next.unwrap()).await},if next.is_some()=>{},
            result=jobs.join_next(),if !jobs.is_empty()=>{if result.is_some_and(|result|result.is_err()) {observe(&shared,Event::Disconnected {connection:shared.session.token()});}notify(&shared);},
            command=commands.recv()=>match command {
                None=>break,
                Some(Command::Start(options,policy,slot))=>{
                    let refusal=reserve_start(&shared,&options);
                    if let Some(result)=refusal {reply(slot,result);}else{let shared=shared.clone();jobs.spawn(async move{let result=start_take(shared.clone(),options,policy).await;reply(slot,result);notify(&shared);});}
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
    commands.close();
    while let Some(command) = commands.recv().await {
        match command {
            Command::Start(_, _, reply_slot)
            | Command::Stop(_, _, reply_slot)
            | Command::Mark(_, _, reply_slot)
            | Command::Status(reply_slot) => reply(reply_slot, error("Take supervisor is closing")),
        }
    }
    // Continue pumping continuity while joining every admitted worker. Only
    // after start/setup work settles can cleanup choose a stop for this take.
    while !jobs.is_empty() {
        tokio::select! {event=events.recv(),if events_open=>match event {Some(event)=>observe(&shared,event),None=>{events_open=false;observe(&shared,Event::Disconnected {connection:shared.session.token()});}},_=jobs.join_next()=>notify(&shared)}
    }
    let result = if let Some(take) = current(&shared) {
        let worker = shared.clone();
        let policy = internal_policy(&take.lock().unwrap().policy);
        jobs.spawn(async move {
            let _ = stop_take(worker, take, None, policy).await;
        });
        while !jobs.is_empty() {
            tokio::select! {event=events.recv(),if events_open=>match event {Some(event)=>observe(&shared,event),None=>{events_open=false;observe(&shared,Event::Disconnected {connection:shared.session.token()});}},_=jobs.join_next()=>notify(&shared)}
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
