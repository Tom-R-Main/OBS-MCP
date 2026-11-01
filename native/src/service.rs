// SPDX-License-Identifier: GPL-2.0-only
//! Persistent composition of the physical transport, observation owner and sole
//! receipt writer. Frontend futures own replies, never production jobs. No MCP
//! routes are registered here. Dropping Service is emergency invalidation, not
//! evidence that OBS stopped; shutdown() is the joined lifecycle boundary.
use crate::{
    custody::{Clock, CustodyCoordinator, SharedControl, TrackedRequest},
    demo::{
        self, Demo, DemoAdmission, DemoContext, JournalFile, JournalStore, PrepareMetadata,
        Selection,
    },
    demo_transport::{BoundDemoAdapter, DemoBounds, DemoPolicy, DemoTransportContext},
    generic_requests,
    media_tools::{self, DirectoryFuture, RecordingDirectory},
    media_verification::Cancellation as MediaCancellation,
    observation::{ObservationError, ReadAdapter, ReadRequest, Target},
    operation_store::{OperationStore, OperationView},
    production_observation::{self, LocalState},
    property_list,
    receipt_broker::{ReceiptBroker, ScopeKind},
    scene_audit::{AuditOptions, ScreenshotLimit},
    scene_plan::PlanRequest,
    service_events::TakeEventFeed,
    service_read_tools::{self, ReadToolBounds, ReadToolContext, ReadToolRequest},
    service_read_workflows::{self, WorkflowBounds},
    session::{ObsSession, SessionHandle},
    session_observation::ObservationBridge,
    snapshot_tools,
    snapshots::{CaptureMetadata, SnapshotFile, SnapshotWriter},
    stage::{self, StageMachine},
    stage_transport::{BoundCall, CallerBinding, Cancellation, StageTransport, TransportContext},
    take_transport::{BoundTakeIo, Bounds, HostServices},
    takes::{self, Limits, Loan, Policy, StartOptions, TakeSupervisor},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
    time::Instant,
};

pub const REQUIRED_SUBSCRIPTIONS: u64 =
    crate::stage_transport::REQUIRED_STAGE_EVENTS | takes::REQUIRED_SUBSCRIPTIONS;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceError {
    Binding,
    Busy,
    Uncertain,
    StageToken,
    RetainedState,
    Unsupported,
    Closed,
    Capacity,
    Cleanup,
    Worker,
}
type Reply = oneshot::Sender<Result<Value, ServiceError>>;
type ReadReply = oneshot::Sender<Result<Value, ObservationError>>;
enum ReadCompletion {
    Observe(Reply, Value),
    Read(ReadReply, Result<Value, ObservationError>),
    Workflow(u64, Reply, Value),
}

/// Supplied owners are consumed together; no adapter may open another socket or
/// receipt writer. exact_endpoint is never retained in diagnostics or receipts.
pub struct Resources {
    pub session: ObsSession,
    pub bridge: ObservationBridge,
    pub store: OperationStore,
    pub control: SharedControl,
    pub clock: Clock,
    pub identities: Box<dyn FnMut() -> String + Send>,
}
pub struct TakeServices {
    pub host: Arc<dyn HostServices>,
    pub exact_endpoint: String,
    pub pending_settings: Arc<AtomicBool>,
    pub bounds: Bounds,
    pub limits: Limits,
}
/// Exact source connection projection. Debug intentionally omits the URL.
#[derive(Clone)]
pub struct ConnectionDescriptor {
    pub url: String,
    pub has_password: bool,
}
impl std::fmt::Debug for ConnectionDescriptor {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.debug_struct("ConnectionDescriptor")
            .field("has_password", &self.has_password)
            .finish_non_exhaustive()
    }
}
/// Single bootstrap owner transferred intact into Service. No Clone or shared
/// mutable file writer exists; readonly inspection uses separate narrow views.
pub struct PreparationOwners {
    root: PathBuf,
    snapshots: SnapshotFile,
    journals: JournalFile,
    admission: DemoAdmission,
}
impl PreparationOwners {
    pub fn new(root: &std::path::Path) -> Result<Self, ServiceError> {
        Ok(Self {
            root: root.to_owned(),
            snapshots: SnapshotFile::new(root, 8 * 1024 * 1024)
                .map_err(|_| ServiceError::Binding)?,
            journals: JournalFile::new(root, 1024 * 1024).map_err(|_| ServiceError::Binding)?,
            admission: DemoAdmission::default(),
        })
    }
    /// Before first connection (and again before adapter binding), historical
    /// uncertainty is refused using the core's exact provenance admission.
    fn blocks_handoff(&mut self, endpoint: &str) -> bool {
        let value = demo::recovery_summary(&mut self.journals, endpoint);
        value["status"] != "observed"
            || value["operations"]
                .as_array()
                .is_none_or(|items| !items.is_empty())
    }
    pub fn snapshot_list(&mut self, endpoint: &str) -> Value {
        snapshot_tools::list(&mut self.snapshots, endpoint)
    }
    pub fn local_recovery_refusal(&mut self, endpoint: &str, id: &str) -> Option<Value> {
        match self
            .journals
            .load(id)
            .and_then(|journal| demo::local_recovery_refusal(&journal, endpoint))
        {
            Ok(refusal) => refusal,
            Err(error) => Some(demo_failure(Some(id), error.tool_message())),
        }
    }
}
fn demo_failure(id: Option<&str>, message: &str) -> Value {
    let mut report = json!({"status":"failed","applied":false,"issues":[message],"recovery":if id.is_some(){"Inspect current state before retrying"}else{"Nothing changed"}});
    if let Some(id) = id {
        report["operationId"] = json!(id);
    }
    json!({"content":[{"type":"text","text":report.to_string()}],"structuredContent":report,"isError":true})
}
pub struct Options {
    pub operation_timeout: Duration,
    pub screenshot_max_bytes: usize,
    pub stage_screenshot_error: Option<ObservationError>,
    pub generic_readback_screenshot_limit: Option<Result<ScreenshotLimit, ObservationError>>,
    pub take: Option<TakeServices>,
    pub connection: Option<ConnectionDescriptor>,
    pub state_root: Option<PathBuf>,
    pub preparation: Option<PreparationOwners>,
    pub media: Option<Arc<media_tools::Config>>,
    pub read_tools_host: Option<Arc<crate::take_transport::LocalHost>>,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            operation_timeout: Duration::from_secs(30),
            screenshot_max_bytes: 4 * 1024 * 1024,
            stage_screenshot_error: None,
            generic_readback_screenshot_limit: None,
            take: None,
            connection: None,
            state_root: None,
            preparation: None,
            media: None,
            read_tools_host: None,
        }
    }
}
#[derive(Clone, Debug)]
pub struct ShutdownReceipt {
    pub joined: bool,
    pub observer_joined: bool,
    pub socket_joined: bool,
    pub take_joined: bool,
    pub workers_joined: bool,
    pub stop_confirmed: bool,
    pub journal_available: bool,
    pub handoff_allowed: bool,
}
struct Exit {
    receipt: ShutdownReceipt,
    store: Option<OperationStore>,
}
pub struct Service {
    handle: ServiceHandle,
    stop: watch::Sender<bool>,
    bridge: Weak<ObservationBridge>,
    task: Option<JoinHandle<Exit>>,
    exit: Option<Exit>,
}
#[derive(Clone)]
pub struct ServiceHandle {
    sender: mpsc::Sender<Command>,
    closed: Arc<AtomicBool>,
}
enum StageCall {
    Stage(Target),
    Take(String),
}
enum WorkflowCall {
    Plan(PlanRequest),
    Audit(AuditOptions, Result<ScreenshotLimit, ObservationError>),
}
enum PreparationCall {
    Prepare(Selection, bool),
    Recover(String, bool),
}
enum TakeCall {
    Start(StartOptions, Policy),
    Stop(Option<f64>, Policy),
    Mark(String, Policy),
    Status,
}
/// FIFO budgets cover retained encoded payloads and items, not process RSS.
const GENERIC_QUEUE_BYTES: usize = 16 * 1024 * 1024;
struct QueuedGeneric {
    call: generic_requests::Call,
    caller: String,
    cancel: Cancellation,
    at: Instant,
    reply: Reply,
    bytes: usize,
}
enum Command {
    Snapshot(snapshot_tools::Request, Cancellation, Reply),
    Property(property_list::Request, Cancellation, Reply),
    Stage(StageCall, CallerBinding, Cancellation, Reply),
    Tracked(TrackedRequest, Reply),
    Generic(generic_requests::Call, String, Cancellation, Instant, Reply),
    Preparation(PreparationCall, DemoPolicy, Cancellation, Reply),
    Take(TakeCall, Reply),
    Summary(Reply),
    TakeResource(Reply),
    Handoff(Reply),
    Observe(Option<String>, Reply),
    Read(ReadRequest, Value, Duration, ReadReply),
    Workflow(WorkflowCall, Cancellation, Reply),
    Media(media_tools::Request, MediaCancellation, Reply),
    ReadTool(
        ReadToolRequest,
        Result<ScreenshotLimit, ObservationError>,
        Cancellation,
        Reply,
    ),
}
struct TakeBorrow {
    owner: TakeSupervisor,
    // Feed remains alive until every take worker/observer has joined.
    _feed: TakeEventFeed,
    changed: watch::Receiver<Value>,
}
struct WriterReturn {
    store: Option<OperationStore>,
    machine: StageMachine,
    reply: Reply,
    result: Result<Value, ServiceError>,
    broken: bool,
}
enum Completion {
    Generic(Reply, Value),
    Preparation {
        store: OperationStore,
        owners: Box<PreparationOwners>,
        reply: Reply,
        result: Value,
    },
    Writer(WriterReturn),
    TakeInit {
        store: Option<OperationStore>,
        borrower: Option<Box<TakeBorrow>>,
        reply: Reply,
        result: Result<Value, ServiceError>,
        broken: bool,
    },
    TakeCall {
        reply: Reply,
        result: Result<Value, ServiceError>,
    },
    TakeRetired {
        store: Option<OperationStore>,
        completed: Option<Value>,
        joined: bool,
        stopped: bool,
    },
}

impl Service {
    pub fn spawn(
        resources: Resources,
        options: Options,
    ) -> Result<Self, (ServiceError, Box<Resources>)> {
        let handle = resources.session.handle();
        let fence = resources.bridge.fence();
        let evidence = *resources.bridge.evidence().borrow();
        if !matches!(
            &resources.store.mode,
            crate::operation_store::StoreMode::Direct(_)
        ) || handle.token() != fence.token()
            || handle.endpoint_hash() != fence.endpoint_hash()
            || handle.endpoint_hash() != resources.store.endpoint_hash()
            || options
                .preparation
                .as_ref()
                .is_some_and(|owners| owners.root != resources.store.state_root())
            || !fence.full_profile()
            || handle.event_subscriptions() & REQUIRED_SUBSCRIPTIONS != REQUIRED_SUBSCRIPTIONS
            || handle.invalidation().is_some()
            || options
                .state_root
                .as_ref()
                .is_some_and(|root| root != resources.store.state_root())
            || options.connection.as_ref().is_some_and(|connection| {
                format!("{:x}", Sha256::digest(connection.url.as_bytes())) != handle.endpoint_hash()
            })
            || !evidence.connected
            || evidence.cancelled
            || evidence.generation != handle.token().generation()
            || options.operation_timeout.is_zero()
            || options.operation_timeout > Duration::from_secs(600)
            || options.screenshot_max_bytes == 0
            || options.screenshot_max_bytes > 8 * 1024 * 1024
        {
            return Err((ServiceError::Binding, Box::new(resources)));
        }
        let Resources {
            session,
            bridge,
            store,
            control,
            clock,
            identities,
        } = resources;
        let bridge = Arc::new(bridge);
        let weak = Arc::downgrade(&bridge);
        let (sender, commands) = mpsc::channel(32);
        let (stop, stopping) = watch::channel(false);
        let closed = Arc::new(AtomicBool::new(false));
        let frontend = ServiceHandle {
            sender,
            closed: closed.clone(),
        };
        let task = tokio::spawn(run(
            session, bridge, store, control, clock, identities, options, commands, stopping, closed,
        ));
        Ok(Self {
            handle: frontend,
            stop,
            bridge: weak,
            task: Some(task),
            exit: None,
        })
    }
    pub fn handle(&self) -> ServiceHandle {
        self.handle.clone()
    }
    /// Retains the same actor join across a dropped/unpolled frontend await.
    pub async fn shutdown(&mut self) -> Result<ShutdownReceipt, ServiceError> {
        self.stop.send_replace(true);
        if self.exit.is_none() {
            let result = self.task.as_mut().ok_or(ServiceError::Closed)?.await;
            self.task.take();
            self.exit = Some(result.map_err(|_| ServiceError::Worker)?);
        }
        Ok(self.exit.as_ref().unwrap().receipt.clone())
    }
    /// Available only after joined cleanup. Never duplicates the writer.
    pub fn take_store(&mut self) -> Option<OperationStore> {
        self.exit
            .as_mut()
            .filter(|exit| exit.receipt.joined && exit.receipt.take_joined)
            .and_then(|exit| exit.store.take())
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        self.handle.closed.store(true, Ordering::Release);
        if let Some(bridge) = self.bridge.upgrade() {
            bridge.invalidate();
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
impl ServiceHandle {
    async fn call(&self, build: impl FnOnce(Reply) -> Command) -> Result<Value, ServiceError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ServiceError::Closed);
        }
        let (sender, receiver) = oneshot::channel();
        self.sender
            .try_send(build(sender))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => ServiceError::Capacity,
                mpsc::error::TrySendError::Closed(_) => ServiceError::Closed,
            })?;
        receiver.await.map_err(|_| ServiceError::Closed)?
    }
    pub async fn stage(
        &self,
        target: Target,
        caller: CallerBinding,
        cancel: Cancellation,
    ) -> Result<Value, ServiceError> {
        self.call(|reply| Command::Stage(StageCall::Stage(target), caller, cancel, reply))
            .await
    }
    pub async fn take_scene(
        &self,
        id: String,
        caller: CallerBinding,
        cancel: Cancellation,
    ) -> Result<Value, ServiceError> {
        self.call(|reply| Command::Stage(StageCall::Take(id), caller, cancel, reply))
            .await
    }
    /// Trusted internal custody API, not a raw MCP request admission surface.
    pub async fn tracked(&self, request: TrackedRequest) -> Result<Value, ServiceError> {
        self.call(|reply| Command::Tracked(request, reply)).await
    }
    pub async fn start_take(
        &self,
        options: StartOptions,
        policy: Policy,
    ) -> Result<Value, ServiceError> {
        self.call(|reply| Command::Take(TakeCall::Start(options, policy), reply))
            .await
    }
    pub async fn stop_take(
        &self,
        seconds: Option<f64>,
        policy: Policy,
    ) -> Result<Value, ServiceError> {
        self.call(|reply| Command::Take(TakeCall::Stop(seconds, policy), reply))
            .await
    }
    pub async fn mark_take(&self, name: String, policy: Policy) -> Result<Value, ServiceError> {
        self.call(|reply| Command::Take(TakeCall::Mark(name, policy), reply))
            .await
    }
    pub async fn plan_scene(
        &self,
        args: PlanRequest,
        cancel: Cancellation,
    ) -> Result<Value, ServiceError> {
        self.call(|reply| Command::Workflow(WorkflowCall::Plan(args), cancel, reply))
            .await
    }
    pub async fn audit_scene(
        &self,
        args: AuditOptions,
        limit: Result<ScreenshotLimit, ObservationError>,
        cancel: Cancellation,
    ) -> Result<Value, ServiceError> {
        self.call(|reply| Command::Workflow(WorkflowCall::Audit(args, limit), cancel, reply))
            .await
    }
    pub async fn snapshot(
        &self,
        request: snapshot_tools::Request,
        cancel: Cancellation,
    ) -> Result<Value, ServiceError> {
        self.call(|reply| Command::Snapshot(request, cancel, reply))
            .await
    }
    pub async fn property_list(
        &self,
        request: property_list::Request,
        cancel: Cancellation,
    ) -> Result<Value, ServiceError> {
        self.call(|reply| Command::Property(request, cancel, reply))
            .await
    }
    pub async fn read_tool(
        &self,
        request: ReadToolRequest,
        limit: Result<ScreenshotLimit, ObservationError>,
        cancel: Cancellation,
    ) -> Result<Value, ServiceError> {
        self.call(|reply| Command::ReadTool(request, limit, cancel, reply))
            .await
    }
    pub async fn generic(
        &self,
        call: generic_requests::Call,
        caller: String,
        cancel: Cancellation,
    ) -> Result<Value, ServiceError> {
        self.call(|reply| Command::Generic(call, caller, cancel, Instant::now(), reply))
            .await
    }
    pub async fn recording_media(
        &self,
        args: media_tools::Request,
        cancel: MediaCancellation,
    ) -> Result<Value, ServiceError> {
        self.call(|reply| Command::Media(args, cancel, reply)).await
    }
    pub async fn prepare_demo(
        &self,
        selection: Selection,
        apply: bool,
        policy: DemoPolicy,
        cancel: Cancellation,
    ) -> Result<Value, ServiceError> {
        self.call(|reply| {
            Command::Preparation(
                PreparationCall::Prepare(selection, apply),
                policy,
                cancel,
                reply,
            )
        })
        .await
    }
    pub async fn recover_demo(
        &self,
        id: String,
        apply: bool,
        policy: DemoPolicy,
        cancel: Cancellation,
    ) -> Result<Value, ServiceError> {
        self.call(|reply| {
            Command::Preparation(PreparationCall::Recover(id, apply), policy, cancel, reply)
        })
        .await
    }
    /// Current owned monitor only; external recording does not invent a take.
    pub async fn take_resource(&self) -> Result<Value, ServiceError> {
        self.call(Command::TakeResource).await
    }
    pub async fn take_status(&self) -> Result<Value, ServiceError> {
        self.call(|reply| Command::Take(TakeCall::Status, reply))
            .await
    }
    /// Closed, retained read jobs share the persistent physical session. A
    /// dropped frontend cannot abandon an actor request while a take runs.
    pub async fn read(
        &self,
        request: ReadRequest,
        data: Value,
        duration: Duration,
    ) -> Result<Value, ObservationError> {
        let unavailable =
            || ObservationError::new("evidence_unavailable", "Native service read is unavailable");
        if self.closed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        let (reply, result) = oneshot::channel();
        self.sender
            .try_send(Command::Read(request, data, duration, reply))
            .map_err(|_| unavailable())?;
        result.await.map_err(|_| unavailable())?
    }
    pub async fn observe(&self, canvas_uuid: Option<String>) -> Result<Value, ServiceError> {
        self.call(|reply| Command::Observe(canvas_uuid, reply))
            .await
    }
    pub async fn summary(&self) -> Result<Value, ServiceError> {
        self.call(Command::Summary).await
    }
    /// Successful handoff also retires and joins native owners. Refusals leave
    /// every token, loan and receipt intact; there is no automatic SDK replay.
    pub async fn handoff(&self) -> Result<Value, ServiceError> {
        self.call(Command::Handoff).await
    }
}
fn uncertainty(broker: &ReceiptBroker) -> bool {
    broker
        .admission()
        .map_or(true, |admission| admission.blocking)
}
async fn take_call(handle: takes::TakeHandle, call: TakeCall) -> Result<Value, ServiceError> {
    let result = match call {
        TakeCall::Start(options, policy) => handle.start(options, policy).await,
        TakeCall::Stop(seconds, policy) => handle.stop(seconds, policy).await,
        TakeCall::Mark(name, policy) => handle.mark(name, policy).await,
        TakeCall::Status => handle.status().await,
    };
    result.map_err(|_| ServiceError::Worker)
}

#[allow(clippy::too_many_arguments)]
async fn run(
    session: ObsSession,
    bridge: Arc<ObservationBridge>,
    initial: OperationStore,
    control: SharedControl,
    clock: Clock,
    identities: Box<dyn FnMut() -> String + Send>,
    mut options: Options,
    mut commands: mpsc::Receiver<Command>,
    mut stopping: watch::Receiver<bool>,
    closed: Arc<AtomicBool>,
) -> Exit {
    let physical = session.handle();
    // One client-local flag for this physical owner, including sessions with
    // no Take/Host adapter. Reidentify and individual reads never reset it.
    let pending_output_settings = options
        .take
        .as_ref()
        .map(|take| take.pending_settings.clone())
        .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
    let identities = Arc::new(Mutex::new(identities));
    let receipt_view = initial.read_view();
    let mut preparation = options.preparation.take();
    let preparation_enabled = preparation.is_some();
    let broker = ReceiptBroker::new(initial).expect("spawn validated a direct store");
    let mut machine = Some(StageMachine::default());
    let mut jobs = JoinSet::new();
    let mut waiting_generics: VecDeque<QueuedGeneric> = VecDeque::new();
    let mut waiting_bytes = 0usize;
    let mut observations: JoinSet<ReadCompletion> = JoinSet::new();
    let mut snapshot_jobs: JoinSet<(u64, Box<PreparationOwners>, Reply, Value)> = JoinSet::new();
    let mut media_jobs: JoinSet<(u64, Reply, Value)> = JoinSet::new();
    let mut media_cancel = HashMap::<u64, MediaCancellation>::new();
    let mut workflow_cancel = HashMap::<u64, Cancellation>::new();
    let mut next_job = 0u64;
    let local = Arc::new(Mutex::new(LocalMetadata { take: None }));
    let version = Arc::new(Mutex::new(None::<Option<Value>>));
    let mut borrower: Option<Box<TakeBorrow>> = None;
    let mut take_jobs = 0usize;
    let mut take_initializing = false;
    let mut completed = None;
    let mut broken = false;
    let mut closing = false;
    let mut handoff: Option<Reply> = None;
    let mut active_cancel: Option<Cancellation> = None;
    let mut workers_joined = true;
    let mut take_joined = true;
    let mut stop_confirmed = true;
    loop {
        if closing {
            for cancel in media_cancel.values() {
                cancel.cancel();
            }
            for cancel in workflow_cancel.values() {
                cancel.cancel();
            }
        }
        if let Some(take) = borrower.as_ref()
            && take_jobs == 0
            && (closing || !take.owner.has_take_or_start())
        {
            let mut take = borrower.take().unwrap();
            jobs.spawn(async move {
                let shutdown = take.owner.shutdown().await;
                let completed = take.owner.completed_result();
                let returned = take.owner.take_store();
                let joined = shutdown
                    .as_ref()
                    .is_ok_and(|receipt| receipt.joined && receipt.observers_joined);
                let stopped = shutdown
                    .as_ref()
                    .is_ok_and(|receipt| receipt.stop_confirmed);
                Completion::TakeRetired {
                    store: returned,
                    completed,
                    joined,
                    stopped,
                }
            });
        }
        if closing {
            while let Some(queued) = waiting_generics.pop_front() {
                waiting_bytes -= queued.bytes;
                queued.cancel.cancel();
                let _ = queued.reply.send(Ok(queued.call.cancelled_or_expired()));
            }
        }
        if closing
            && jobs.is_empty()
            && observations.is_empty()
            && media_jobs.is_empty()
            && snapshot_jobs.is_empty()
            && borrower.is_none()
        {
            break;
        }
        let generic_ready = !closing
            && !waiting_generics.is_empty()
            && !take_initializing
            && jobs.len() == take_jobs;
        tokio::select! {
            result=snapshot_jobs.join_next(),if !snapshot_jobs.is_empty()=>{
                match result {Some(Ok((id,owners,reply,result)))=>{workflow_cancel.remove(&id);preparation=Some(*owners);let _=reply.send(Ok(result));},_=>{workers_joined=false;broken=true;}}
            },
            result=media_jobs.join_next(),if !media_jobs.is_empty()=>{
                match result {Some(Ok((id,reply,result)))=>{media_cancel.remove(&id);let _=reply.send(Ok(result));},_=>{workers_joined=false;broken=true;}}
            },
            result=observations.join_next(),if !observations.is_empty()=>{
                match result {Some(Ok(ReadCompletion::Observe(reply,report)))=>{let _=reply.send(Ok(report));},Some(Ok(ReadCompletion::Read(reply,result)))=>{let _=reply.send(result);},Some(Ok(ReadCompletion::Workflow(id,reply,result)))=>{workflow_cancel.remove(&id);let _=reply.send(Ok(result));},_=>{workers_joined=false;broken=true;}}
            },
            value=jobs.join_next(),if !jobs.is_empty()=>{
                match value {
                    Some(Ok(Completion::Generic(reply,result)))=>{active_cancel=None;let _=reply.send(Ok(result));},
                    Some(Ok(Completion::Preparation {store:returned,owners,reply,result}))=>{drop(returned);preparation=Some(*owners);active_cancel=None;let _=reply.send(Ok(result));},
                    Some(Ok(Completion::Writer(returned)))=>{
                        drop(returned.store);machine=Some(returned.machine);broken|=returned.broken;active_cancel=None;
                        let _=returned.reply.send(returned.result);
                    },
                    Some(Ok(Completion::TakeInit {store:returned,borrower:take,reply,result,broken:failed}))=>{
                        drop(returned);borrower=take;take_initializing=false;broken|=failed;let _=reply.send(result);
                    },
                    Some(Ok(Completion::TakeCall {reply,result}))=>{take_jobs-=1;let _=reply.send(result);},
                    Some(Ok(Completion::TakeRetired {store:returned,completed:result,joined,stopped}))=>{
                        let missing=returned.is_none();drop(returned);completed=result.or(completed);local.lock().unwrap().take=None;take_joined&=joined;stop_confirmed&=stopped;broken|=!joined||missing;
                    },
                    _=>{broken=true;workers_joined=false;take_joined=false;stop_confirmed=false;active_cancel=None;take_jobs=take_jobs.saturating_sub(1);}
                }
            },
            _=stopping.changed(),if !closing=>{
                closing=true;closed.store(true,Ordering::Release);if let Some(cancel)=&active_cancel {cancel.cancel();}for cancel in media_cancel.values(){cancel.cancel();}for cancel in workflow_cancel.values(){cancel.cancel();}
            },
            live=async {if let Some(take)=borrower.as_mut() {take.changed.changed().await.is_ok()} else {std::future::pending::<bool>().await}},if !closing=>{
                if !live {broken=true;closing=true;closed.store(true,Ordering::Release);}
            },
            command=async {if generic_ready {Some(None)}else{commands.recv().await.map(Some)}},if !closing=>{
                let Some(incoming)=command else {closing=true;continue;};
                let command=if let Some(command)=incoming {command}else {let queued=waiting_generics.pop_front().unwrap();waiting_bytes-=queued.bytes;Command::Generic(queued.call,queued.caller,queued.cancel,queued.at,queued.reply)};
                match command {
                    Command::Snapshot(request,cancel,reply)=>{
                        let Some(id)=next_job.checked_add(1) else {let _=reply.send(Err(ServiceError::Capacity));continue;};
                        let Some(connection)=options.connection.clone() else {let _=reply.send(Err(ServiceError::Unsupported));continue;};
                        if broken {let _=reply.send(Err(ServiceError::Uncertain));continue;}
                        let Some(mut owners)=preparation.take() else {let _=reply.send(Ok(snapshot_tools::failure("Another restricted demo operation is running")));continue;};
                        next_job=id;workflow_cancel.insert(id,cancel.clone());
                        let known=version.lock().unwrap().clone().flatten();
                        let capabilities=known.as_ref().and_then(|value|value["availableRequests"].as_array()).map(|rows|Arc::new(rows.iter().filter_map(Value::as_str).map(str::to_owned).collect::<HashSet<_>>()));
                        let metadata_ids=identities.clone();let metadata_clock=clock.clone();let metadata_endpoint=connection.url.clone();
                        let metadata=move||{let id={let mut factory=metadata_ids.lock().unwrap();factory().chars().take(8).collect()};CaptureMetadata {id,obs_url:metadata_endpoint,taken_at:(metadata_clock)().utc}};
                        let context=snapshot_tools::Context {handle:physical.clone(),reader:bridge.reader(),fence:bridge.fence(),endpoint:connection.url.clone(),capabilities,cancellation:cancel.clone(),deadline:Instant::now()+options.operation_timeout};
                        snapshot_jobs.spawn(async move {
                            let endpoint=connection.url;
                            let deadline=context.deadline;
                            let captured=if request.list {None}else{Some(snapshot_tools::capture_with_metadata(context,request,metadata).await)};
                            // File publication is a retained, joined operation on the SAME
                            // owner. No receipt broker scope or take writer is borrowed.
                            let (owners,result)=tokio::task::spawn_blocking(move || {
                                let result=if cancel.is_cancelled() {snapshot_tools::failure("Snapshot request cancelled")}
                                    else if Instant::now()>=deadline {snapshot_tools::failure("Snapshot deadline elapsed")}
                                    else {match captured {None=>owners.snapshot_list(&endpoint),Some(Err(error))=>snapshot_tools::failure(error.tool_message()),Some(Ok(snapshot))=>match owners.snapshots.save(&snapshot) {Ok(())=>snapshot_tools::saved(&snapshot),Err(error)=>snapshot_tools::failure(error.tool_message())}}};
                                (owners,result)
                            }).await.expect("snapshot publication worker must join");
                            (id,Box::new(owners),reply,result)
                        });
                    },
                    Command::Property(request,cancel,reply)=>{
                        let Some(id)=next_job.checked_add(1) else {let _=reply.send(Err(ServiceError::Capacity));continue;};
                        if observations.len()>=32 {let _=reply.send(Err(ServiceError::Capacity));continue;}
                        let Some(connection)=options.connection.clone() else {let _=reply.send(Err(ServiceError::Unsupported));continue;};next_job=id;
                        let known=version.lock().unwrap().clone().flatten();
                        let capabilities=known.as_ref().and_then(|value|value["availableRequests"].as_array()).map(|rows|Arc::new(rows.iter().filter_map(Value::as_str).map(str::to_owned).collect::<HashSet<_>>()));
                        let context=property_list::Context {handle:physical.clone(),reader:bridge.reader(),fence:bridge.fence(),endpoint:connection.url,capabilities,cancellation:cancel.clone(),deadline:Instant::now()+options.operation_timeout};
                        workflow_cancel.insert(id,cancel);
                        observations.spawn(async move {let result=property_list::run(context,request).await.unwrap_or_else(|error|json!({"content":[{"type":"text","text":format!("Get Input Property List Items failed: {}",error.message)}],"isError":true}));ReadCompletion::Workflow(id,reply,result)});
                    },
                    Command::ReadTool(request,screenshot_limit,cancel,reply)=>{
                        let Some(id)=next_job.checked_add(1) else {let _=reply.send(Err(ServiceError::Capacity));continue;};
                        if observations.len()>=32 {let _=reply.send(Err(ServiceError::Capacity));continue;}
                        let Some(connection)=options.connection.clone() else {let _=reply.send(Err(ServiceError::Unsupported));continue;};next_job=id;
                        let known=version.lock().unwrap().clone().flatten();
                        let capabilities=known.as_ref().and_then(|value|value["availableRequests"].as_array()).map(|rows|Arc::new(rows.iter().filter_map(Value::as_str).map(str::to_owned).collect::<HashSet<_>>()));
                        let platform=known.as_ref().and_then(|value|value["platform"].as_str()).map(str::to_owned);
                        let pending=pending_output_settings.clone();
                        let context=ReadToolContext {handle:physical.clone(),reader:bridge.reader(),capabilities,platform,connection,clock:clock.clone(),cancellation:cancel.clone(),pending_output_settings:pending,writable:options.read_tools_host.clone(),screenshot_limit,bounds:ReadToolBounds::default()};
                        workflow_cancel.insert(id,cancel);
                        observations.spawn(async move {let result=service_read_tools::run(context,request).await.unwrap_or_else(|error|json!({"content":[{"type":"text","text":error.message}],"isError":true}));ReadCompletion::Workflow(id,reply,result)});
                    },
                    Command::Workflow(call,cancel,reply)=>{
                        let Some(id)=next_job.checked_add(1) else {let _=reply.send(Err(ServiceError::Capacity));continue;};
                        if observations.len()>=32 {let _=reply.send(Err(ServiceError::Capacity));continue;}next_job=id;
                        let supported=version.lock().unwrap().as_ref().and_then(|value|value.as_ref()).and_then(|value|value["availableRequests"].as_array()).map(|rows|Arc::new(rows.iter().filter_map(Value::as_str).map(str::to_owned).collect::<HashSet<_>>()));
                        workflow_cancel.insert(id,cancel.clone());let observer=bridge.clone();let clock=clock.clone();
                        observations.spawn(async move {
                            let bounds=WorkflowBounds::default();
                            let result=match call {WorkflowCall::Plan(args)=>service_read_workflows::plan_with_capabilities(observer,args,clock,cancel,bounds,supported).await,WorkflowCall::Audit(args,limit)=>service_read_workflows::audit_with_capabilities(observer,args,clock,cancel,bounds,limit,supported).await};
                            let result=result.unwrap_or_else(|error|json!({"content":[{"type":"text","text":format!("Native read workflow unavailable: {}",error.message)}],"isError":true}));
                            ReadCompletion::Workflow(id,reply,result)
                        });
                    },
                    Command::Media(request,cancel,reply)=>{
                        let Some(id)=next_job.checked_add(1) else {let _=reply.send(Err(ServiceError::Capacity));continue;};
                        if media_jobs.len()>=2 {let _=reply.send(Err(ServiceError::Capacity));continue;}
                        if broken {let _=reply.send(Err(ServiceError::Uncertain));continue;}
                        if take_initializing || borrower.as_ref().is_some_and(|take|take.owner.has_take_or_start()) {let _=reply.send(Err(ServiceError::Busy));continue;}
                        let Some(config)=options.media.clone() else {let _=reply.send(Err(ServiceError::Unsupported));continue;};next_job=id;
                        media_cancel.insert(id,cancel.clone());let provider=Arc::new(ServiceRecordingDirectory {reader:bridge.reader()});
                        media_jobs.spawn(async move {
                            let result=match media_tools::Operation::new((*config).clone(),provider,request,cancel) {Ok(mut operation)=>operation.finish().await,Err(error)=>json!({"content":[{"type":"text","text":format!("Recording media unavailable: {error}")}],"isError":true})};
                            (id,reply,result)
                        });
                    },
                    Command::Read(request,data,duration,reply)=>{
                        if observations.len()>=32 || duration.is_zero() || duration>options.operation_timeout {let _=reply.send(Err(ObservationError::new("evidence_unavailable","Native service read admission unavailable")));continue;}
                        let observer=bridge.clone();let cached=version.clone();
                        observations.spawn(async move {
                            let known=cached.lock().unwrap().clone().flatten();
                            let supported=known.as_ref().and_then(|value|value["availableRequests"].as_array()).map(|rows|rows.iter().filter_map(Value::as_str).any(|name|name==request.as_str()));
                            let result=if supported==Some(false) {Err(ObservationError::new("capability_unavailable",format!("OBS WebSocket does not advertise support for request '{}'",request.as_str())))} else {observer.reader().read(request,data,Instant::now()+duration).await};
                            if request==ReadRequest::Version {let mut cache=cached.lock().unwrap();if cache.is_none(){*cache=Some(result.as_ref().ok().cloned());}}
                            ReadCompletion::Read(reply,result)
                        });
                    },
                    Command::Observe(canvas,reply)=>{
                        let Some(connection)=options.connection.clone() else {let _=reply.send(Err(ServiceError::Unsupported));continue;};
                        if observations.len()>=32 {let _=reply.send(Err(ServiceError::Capacity));continue;}
                        let observer=bridge.clone();let handle=physical.clone();let local=local.clone();let control=control.clone();let clock=clock.clone();let version=version.clone();let root=Some(receipt_view.state_root().to_owned());let receipts=receipt_view.clone();
                        observations.spawn(async move {let report=observe_job(observer,handle,local,control,clock,version,connection,root,receipts,canvas).await;ReadCompletion::Observe(reply,report)});
                    },
                    Command::TakeResource(reply)=>{
                        let result=if let Some(take)=borrower.as_ref(){take.owner.handle().resource_summary().map_err(|_|ServiceError::Closed)}else{Ok(json!({"running":false}))};
                        let _=reply.send(result);
                    },
                    Command::Summary(reply)=>{
                        let mut result=receipt_view.summary();result["jobsPending"]=json!(!jobs.is_empty()||!snapshot_jobs.is_empty()||!waiting_generics.is_empty());result["queuedCommands"]=json!(waiting_generics.len());result["queuedArgumentBytes"]=json!(waiting_bytes);result["outputSettingsPending"]=json!(pending_output_settings.load(Ordering::Acquire));let _=reply.send(Ok(result));
                    },
                    Command::Handoff(reply)=>{
                        let refusal=if broken {Some(ServiceError::Uncertain)} else if borrower.is_some()||!jobs.is_empty()||!observations.is_empty()||!media_jobs.is_empty()||!snapshot_jobs.is_empty()||!waiting_generics.is_empty() {Some(ServiceError::Busy)} else if machine.as_ref().is_some_and(|machine|machine.token_count()!=0) {Some(ServiceError::StageToken)} else if uncertainty(&broker) {Some(ServiceError::Uncertain)} else if broker.admission().map_or(true,|state|state.generic_v2_present) || completed.is_some() || control.lock().unwrap().active_lease((clock)().epoch_ms).is_some() || options.connection.as_ref().is_some_and(|connection|preparation.as_mut().is_some_and(|owners|owners.blocks_handoff(&connection.url))) || options.connection.as_ref().is_some_and(|connection|{let summary=demo::inspect_recovery_summary(receipt_view.state_root(),1024*1024,&connection.url);summary["status"]!="observed" || summary["operations"].as_array().is_none_or(|items|!items.is_empty())}) {Some(ServiceError::RetainedState)} else {None};
                        if let Some(error)=refusal {let _=reply.send(Err(error));} else {handoff=Some(reply);closing=true;closed.store(true,Ordering::Release);}
                    },
                    Command::Take(call,reply)=>{
                        if matches!(call,TakeCall::Start(..)) && !media_jobs.is_empty() {let _=reply.send(Err(ServiceError::Busy));continue;}
                        if let Some(take)=borrower.as_ref() {
                            if take_jobs >= 32 {let _=reply.send(Err(ServiceError::Capacity));continue;}
                            let handle=take.owner.handle();take_jobs+=1;jobs.spawn(async move {Completion::TakeCall {reply,result:take_call(handle,call).await}});continue;
                        }
                        if matches!(call,TakeCall::Status) {let _=reply.send(Ok(Value::Null));continue;}
                        if matches!(call,TakeCall::Stop(..)) && let Some(result)=completed.as_ref() {let _=reply.send(Ok(result.clone()));continue;}
                        let refusal=if broken {Some(ServiceError::Uncertain)} else if !jobs.is_empty() {Some(ServiceError::Busy)} else if uncertainty(&broker) {Some(ServiceError::Uncertain)} else if !matches!(call,TakeCall::Start(..))||options.take.is_none() {Some(ServiceError::Unsupported)} else {None};
                        if let Some(error)=refusal {let _=reply.send(Err(error));continue;}
                        take_initializing=true;
                        let configuration=options.take.as_ref().unwrap();
                        let host=configuration.host.clone();let endpoint=configuration.exact_endpoint.clone();let pending=pending_output_settings.clone();let bounds=configuration.bounds.clone();let limits=configuration.limits.clone();
                        let writer=match broker.scope(ScopeKind::Take){Ok(port)=>port,Err(_)=>{take_initializing=false;let _=reply.send(Err(ServiceError::Uncertain));continue;}};let observer=bridge.clone();let physical=physical.clone();let control=control.clone();let clock=clock.clone();
                        // Only UUID production is delegated; adapter gets no journal writer.
                        let ids=identities.clone();let local_state=local.clone();
                        jobs.spawn(async move {
                            let mut feed=match observer.attach_take_feed() {Ok(feed)=>feed,Err(_)=>return Completion::TakeInit {store:Some(writer),borrower:None,reply,result:Err(ServiceError::Binding),broken:false}};
                            let io=match BoundTakeIo::bind(physical.clone(),Arc::new(feed.fence()),host,&endpoint,pending,bounds).await {Ok(io)=>io,Err(_)=>return Completion::TakeInit {store:Some(writer),borrower:None,reply,result:Err(ServiceError::Binding),broken:false}};
                            let loan=Loan {session:physical,store:writer,control,clock,identities:Box::new(move ||(ids.lock().unwrap())()),io:Arc::new(io),events:feed.take_receiver().unwrap()};
                            let owner=match TakeSupervisor::spawn(loan,limits) {Ok(owner)=>owner,Err(failure)=>return Completion::TakeInit {store:Some(failure.loan.store),borrower:None,reply,result:Err(ServiceError::Binding),broken:false}};
                            let handle=owner.handle();let changed=handle.summaries();local_state.lock().unwrap().take=Some(handle.clone());let result=take_call(handle,call).await;
                            Completion::TakeInit {store:None,borrower:Some(Box::new(TakeBorrow {owner,_feed:feed,changed})),reply,result,broken:false}
                        });
                    },
                    Command::Stage(call,caller,cancel,reply)=>{
                        if matches!(call,StageCall::Stage(_)) && let Some(error)=options.stage_screenshot_error.as_ref(){let _=reply.send(Ok(json!({"content":[{"type":"text","text":error.message}],"isError":true})));continue;}

                        let refusal=if broken {Some(ServiceError::Uncertain)} else if take_initializing||jobs.len()>take_jobs {Some(ServiceError::Busy)} else if uncertainty(&broker) {Some(ServiceError::Uncertain)} else {None};
                        if let Some(error)=refusal {let _=reply.send(Err(error));continue;}
                        let writer=match broker.scope(ScopeKind::Stage){Ok(port)=>port,Err(_)=>{let _=reply.send(Err(ServiceError::Uncertain));continue;}};let state=machine.take().unwrap();let observer=bridge.clone();let physical=physical.clone();let control=control.clone();let clock=clock.clone();let duration=options.operation_timeout;let screenshot=options.screenshot_max_bytes;
                        let stage_id=(identities.lock().unwrap())();let operation_id=(identities.lock().unwrap())();active_cancel=Some(cancel.clone());
                        jobs.spawn(stage_job(writer,state,observer,physical,control,clock,call,caller,cancel,stage_id,operation_id,duration,screenshot,reply));
                    },
                    Command::Preparation(call,policy,cancel,reply)=>{
                        if preparation_enabled && preparation.is_none() && !broken {
                            let report=json!({"status":"conflict","applied":false,"issues":["Another restricted demo operation is running"]});
                            let _=reply.send(Ok(json!({"content":[{"type":"text","text":report.to_string()}],"structuredContent":report,"isError":true})));continue;
                        }
                        let refusal=if broken {Some(ServiceError::Uncertain)}else if borrower.is_some()||!jobs.is_empty(){Some(ServiceError::Busy)}else if preparation.is_none()||options.connection.is_none(){Some(ServiceError::Unsupported)}else if matches!(call,PreparationCall::Prepare(..))&&uncertainty(&broker){Some(ServiceError::Uncertain)}else{None};
                        if let Some(error)=refusal {let _=reply.send(Err(error));continue;}
                        let writer=match broker.scope(ScopeKind::Stage){Ok(port)=>port,Err(_)=>{let _=reply.send(Err(ServiceError::Uncertain));continue;}};let owners=preparation.take().unwrap();let observer=bridge.clone();let physical=physical.clone();let control=control.clone();let clock=clock.clone();let endpoint=options.connection.as_ref().unwrap().url.clone();let duration=options.operation_timeout;
                        let operation_id=(identities.lock().unwrap())();let snapshot_id=(identities.lock().unwrap())().chars().take(8).collect();active_cancel=Some(cancel.clone());
                        jobs.spawn(async move {
                            let mut owners=owners;
                            let result=if let PreparationCall::Recover(id,_)=&call {owners.local_recovery_refusal(&endpoint,id)}else{None};
                            let result=if let Some(result)=result {result}else {
                                let deadline=Instant::now()+duration;
                                let context=DemoTransportContext {handle:physical.clone(),reader:observer.reader(),fence:observer.fence(),control,clock:clock.clone(),cancellation:cancel};
                                match BoundDemoAdapter::bind(context,&endpoint,policy,deadline,DemoBounds::default()).await {
                                    Err(error)=>demo_failure(match &call {PreparationCall::Recover(id,_)=>Some(id.as_str()),_=>None},error.message()),
                                    Ok(mut adapter)=>{
                                        let context=DemoContext {endpoint:endpoint.clone(),host:adapter.host(),generation:physical.token().generation(),deadline,admission:owners.admission.clone()};
                                        let mut demo=Demo::new(&mut adapter,&mut owners.snapshots,&mut owners.journals,context);
                                        match call {PreparationCall::Prepare(selection,apply)=>demo.prepare(selection,apply,PrepareMetadata {operation_id,snapshot:CaptureMetadata {id:snapshot_id,obs_url:endpoint,taken_at:(clock)().utc}}).await,PreparationCall::Recover(id,apply)=>demo.recover(&id,apply).await}
                                    }
                                }
                            };
                            Completion::Preparation {store:writer,owners:Box::new(owners),reply,result}
                        });
                    },
                    Command::Generic(call,caller,cancel,admitted_at,reply)=>{
                        let supported=version.lock().unwrap().as_ref().and_then(|value|value.as_ref()).and_then(|value|value["availableRequests"].as_array()).map(|rows|rows.iter().any(|value|value.as_str()==Some(call.kind().request_type())));
                        if supported==Some(false){
                            // Frontend policy can change before this command runs.
                            // Match Source's policy-before-callback order for this
                            // zero-send capability refusal. Supported requests retain
                            // execute()'s admission and actual-dispatch checks.
                            let decision=match generic_requests::policy(&call,&caller,&control,&clock) {Ok(decision)=>decision,Err(())=>{let _=reply.send(Err(ServiceError::Binding));continue;}};
                            if let Some(result)=decision.tool_result(){let _=reply.send(Ok(result));continue;}
                            let _=reply.send(Ok(call.unsupported()));continue;
                        }

                        if broken{let _=reply.send(Err(ServiceError::Uncertain));continue;}
                        if cancel.is_cancelled()||admitted_at.elapsed()>=options.operation_timeout.min(Duration::from_secs(10)){let _=reply.send(Ok(call.cancelled_or_expired()));continue;}
                        if take_initializing||jobs.len()>take_jobs {
                            let bytes=call.retained_bytes().and_then(|bytes|bytes.checked_add(caller.len()));
                            if waiting_generics.len()>=32||bytes.is_none_or(|bytes|bytes>GENERIC_QUEUE_BYTES.saturating_sub(waiting_bytes)){let _=reply.send(Err(ServiceError::Capacity));}else{let bytes=bytes.unwrap();waiting_bytes+=bytes;waiting_generics.push_back(QueuedGeneric{call,caller,cancel,at:admitted_at,reply,bytes});}continue;
                        }
                        if uncertainty(&broker){let _=reply.send(Err(ServiceError::Uncertain));continue;}
                        let writer=match broker.scope(ScopeKind::Generic(call.kind())){Ok(port)=>port,Err(_)=>{let _=reply.send(Err(ServiceError::Uncertain));continue;}};
                        let physical=physical.clone();let control=control.clone();let clock=clock.clone();let id=(identities.lock().unwrap())();let duration=options.operation_timeout.min(Duration::from_secs(10));active_cancel=Some(cancel.clone());
                        let readback=if call.requires_readback(){options.generic_readback_screenshot_limit.as_ref().map(|limit|{
                            let cached=version.lock().unwrap();let known=cached.as_ref().and_then(|value|value.as_ref());
                            let capabilities=known.and_then(|value|value["availableRequests"].as_array()).map(|rows|Arc::new(rows.iter().filter_map(Value::as_str).map(str::to_owned).collect::<HashSet<_>>()));
                            generic_requests::ReadbackServices{capabilities,screenshot_limit:limit.clone()}
                        })}else{None};
                        let services=generic_requests::GenericServices{pending_output_settings:Some(pending_output_settings.clone()),readback};
                        jobs.spawn(async move {Completion::Generic(reply,generic_requests::execute_with_services(writer,call,physical,control,clock,caller,cancel,id,duration,admitted_at,services).await)});
                    },
                    Command::Tracked(request,reply)=>{
                        if request.request_type=="StartRecord" && !media_jobs.is_empty() {let _=reply.send(Err(ServiceError::Busy));continue;}
                        if request.timeout.is_zero() || request.timeout > options.operation_timeout {let _=reply.send(Err(ServiceError::Binding));continue;}
                        let refusal=if broken {Some(ServiceError::Uncertain)} else if borrower.is_some()||!jobs.is_empty() {Some(ServiceError::Busy)} else if uncertainty(&broker) {Some(ServiceError::Uncertain)} else {None};
                        if let Some(error)=refusal {let _=reply.send(Err(error));continue;}
                        let writer=match broker.scope(match request.kind {crate::operation_store::OperationKind::RecordStart|crate::operation_store::OperationKind::RecordStop=>ScopeKind::CriticalRecord,_=>ScopeKind::Stage}){Ok(port)=>port,Err(_)=>{let _=reply.send(Err(ServiceError::Uncertain));continue;}};let state=machine.take().unwrap();let physical=physical.clone();let control=control.clone();let clock=clock.clone();
                        jobs.spawn(async move {
                            let mut coordinator=match CustodyCoordinator::new(writer,physical,control,clock) {Ok(owner)=>owner,Err(_)=>return Completion::Writer(WriterReturn {store:None,machine:state,reply,result:Err(ServiceError::Binding),broken:true})};
                            let result=match coordinator.begin(request) {Ok(())=>match coordinator.finish().await {Ok(outcome)=>Ok(custody_result(outcome)),Err(_)=>Err(ServiceError::Uncertain)},Err(_)=>Err(ServiceError::Binding)};
                            // A transfer refusal returns the owner, never an abandoned
                            // pending observer. Drain that same observer before reclaiming.
                            let writer=loop {
                                match coordinator.into_store() {
                                    Ok(writer)=>break writer,
                                    Err(owner)=>{coordinator = *owner;let _=coordinator.finish().await;}
                                }
                            };
                            Completion::Writer(WriterReturn {store:Some(writer),machine:state,reply,result,broken:false})
                        });
                    },
                }
            },
        }
    }
    closed.store(true, Ordering::Release);
    commands.close();
    // No private job Arc may escape this actor. try_unwrap makes that ownership
    // precondition observable instead of pretending a dropped observer was joined.
    bridge.invalidate();
    let observer_joined = match Arc::try_unwrap(bridge) {
        Ok(bridge) => bridge.shutdown().await.is_ok(),
        Err(_) => false,
    };
    let socket_joined = session.shutdown().await.is_ok();
    let store = broker.into_store().ok();
    let available = store.is_some() && !broken;
    let allowed = handoff.is_some()
        && available
        && observer_joined
        && socket_joined
        && take_joined
        && workers_joined;
    if let Some(reply) = handoff {
        let _ = reply.send(if allowed {
            Ok(json!({"handoffAllowed":true,"nativeOwnersJoined":true}))
        } else {
            Err(ServiceError::Cleanup)
        });
    }
    Exit {
        receipt: ShutdownReceipt {
            joined: observer_joined && socket_joined && take_joined && workers_joined,
            observer_joined,
            socket_joined,
            take_joined,
            workers_joined,
            stop_confirmed,
            journal_available: available,
            handoff_allowed: allowed,
        },
        store: store.filter(|_| available),
    }
}
fn custody_result(outcome: crate::custody::Outcome) -> Value {
    use crate::custody::Outcome;
    let (status, receipt) = match outcome {
        Outcome::Accepted { receipt, .. } => ("accepted", receipt),
        Outcome::NotDispatched { receipt, .. } => ("not_dispatched", receipt),
        Outcome::Rejected { receipt, .. } => ("rejected", receipt),
        Outcome::Unknown { receipt, .. } => ("unknown", receipt),
    };
    json!({"status":status,"receipt":receipt})
}
#[allow(clippy::too_many_arguments)]
async fn stage_job(
    mut store: OperationStore,
    mut machine: StageMachine,
    bridge: Arc<ObservationBridge>,
    physical: SessionHandle,
    control: SharedControl,
    clock: Clock,
    call: StageCall,
    caller: CallerBinding,
    cancel: Cancellation,
    stage_id: String,
    operation_id: String,
    duration: Duration,
    screenshot: usize,
    reply: Reply,
) -> Completion {
    let binding = match &call {
        StageCall::Stage(target) => BoundCall::stage(target.clone()),
        StageCall::Take(id) => BoundCall::take(id),
    };
    let mut broken = false;
    let result = match binding {
        Err(_) => Err(ServiceError::Binding),
        Ok(binding) => match StageTransport::bind(
            TransportContext {
                handle: physical,
                bridge: &bridge,
                control,
                clock: clock.clone(),
                cancellation: cancel,
            },
            binding,
            caller,
        ) {
            Err(_) => Err(ServiceError::Binding),
            Ok(mut transport) => {
                let mut context = stage::Context {
                    reader: &mut transport.reader,
                    mutator: &mut transport.mutator,
                    store: &mut store,
                    clock,
                    deadline: Instant::now() + duration,
                    screenshot_max_bytes: screenshot,
                };
                let result = match call {
                    StageCall::Stage(target) => {
                        machine
                            .stage(&mut context, target, &stage_id, &operation_id)
                            .await
                    }
                    StageCall::Take(id) => machine.take(&mut context, &id, &operation_id).await,
                };
                let value = match result {
                    Ok(success) => success.tool_result(),
                    Err(failure) => failure.tool_result(),
                };
                if transport.shutdown().await.is_err() {
                    broken = true;
                    Err(ServiceError::Cleanup)
                } else {
                    Ok(value)
                }
            }
        },
    };
    Completion::Writer(WriterReturn {
        store: Some(store),
        machine,
        reply,
        result,
        broken,
    })
}

struct LocalMetadata {
    take: Option<takes::TakeHandle>,
}
struct ProductionLocal {
    metadata: Arc<Mutex<LocalMetadata>>,
    control: SharedControl,
    clock: Clock,
    handle: SessionHandle,
    connection: ConnectionDescriptor,
    version: Option<Value>,
    root: Option<PathBuf>,
    receipts: OperationView,
}
impl LocalState for ProductionLocal {
    fn snapshot(&self) -> Value {
        let receipts = self.receipts.summary();
        let take = self.metadata.lock().unwrap().take.clone();
        let active = take.map_or(Value::Null, |take| {
            take.production_metadata()
                .unwrap_or_else(|_| json!({"status":"unknown","reason":"take_supervisor_closed"}))
        });
        let now = (self.clock)().epoch_ms;
        let lease = self.control.lock().unwrap().active_lease(now).cloned();
        let recovery = self
            .root
            .as_ref()
            .map(|root| {
                crate::demo::inspect_recovery_summary(root, 1024 * 1024, &self.connection.url)
            })
            .unwrap_or_else(
                || json!({"status":"unknown","reason":"Recovery state directory is unavailable"}),
            );
        json!({"control":lease,"activeTake":active,"recovery":recovery,"operationReceipts":receipts})
    }
    fn connection(&self) -> Value {
        let live = self.handle.invalidation().is_none();
        let count = self
            .version
            .as_ref()
            .and_then(|version| version["availableRequests"].as_array())
            .map(|requests| {
                requests
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<HashSet<_>>()
                    .len()
            });
        json!({"generation":self.handle.token().generation(),"connected":live,"identified":live,"url":self.connection.url,"hasPassword":self.connection.has_password,"availableRequestCount":count,"versionInfo":self.version})
    }
}
struct CatalogReader {
    inner: crate::session_observation::SessionReader,
    supported: Option<HashSet<String>>,
}
impl ReadAdapter for CatalogReader {
    fn supports(&self, request: ReadRequest) -> Option<bool> {
        self.supported
            .as_ref()
            .map(|supported| supported.contains(request.as_str()))
            .or_else(|| self.inner.supports(request))
    }
    fn read<'a>(
        &'a mut self,
        request: ReadRequest,
        data: Value,
        deadline: Instant,
    ) -> crate::observation::ReadFuture<'a> {
        self.inner.read(request, data, deadline)
    }
}
#[allow(clippy::too_many_arguments)]
async fn observe_job(
    bridge: Arc<ObservationBridge>,
    handle: SessionHandle,
    metadata: Arc<Mutex<LocalMetadata>>,
    control: SharedControl,
    clock: Clock,
    cached: Arc<Mutex<Option<Option<Value>>>>,
    connection: ConnectionDescriptor,
    root: Option<PathBuf>,
    receipts: OperationView,
    canvas: Option<String>,
) -> Value {
    let started = clock();
    let deadline = Instant::now() + crate::observation::MAX_OBSERVATION;
    let existing = cached.lock().unwrap().clone();
    let version = if let Some(version) = existing {
        version
    } else {
        // Source connect refreshes capabilities before constructing Observation.
        // Metadata failure leaves unknown capabilities; it never replays an effect.
        let version = handle
            .request(
                handle.token(),
                "GetVersion",
                Value::Null,
                Duration::from_secs(2),
            )
            .await
            .ok();
        *cached.lock().unwrap() = Some(version.clone());
        version
    };
    let local = ProductionLocal {
        metadata,
        control,
        clock: clock.clone(),
        handle: handle.clone(),
        connection,
        version: version.clone(),
        root,
        receipts,
    };
    let synchronized = bridge.synchronize(deadline).await;
    let (state, connect_error) = match synchronized {
        Ok(state) => (state, None),
        Err(error) => (bridge.evidence(), Some(error)),
    };
    let supported = version
        .as_ref()
        .and_then(|version| version["availableRequests"].as_array())
        .map(|requests| {
            requests
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        });
    let mut reader = CatalogReader {
        inner: bridge.reader(),
        supported,
    };
    let mut report = production_observation::observe(
        &mut reader,
        production_observation::Context {
            state,
            generation: handle.token().generation(),
            local: &local,
            clock,
            deadline,
            connect_error,
            started: Some(started),
        },
        canvas.as_deref(),
    )
    .await;
    // A retiring failed take owner must remain explicit rather than turning
    // unavailable local custody into a clear/no-take assertion.
    if report["status"] != "unknown" && report["activeTake"]["status"] == "unknown" {
        report["status"] = json!("incomplete");
        report["missingEvidence"].as_array_mut().unwrap().push(json!({"component":"activeTake","reason":"evidence_unavailable","nextCheck":"Inspect recording ownership before another take"}));
    }
    report
}

/// Fixed reads on the Service's original bridge/session. This provider owns no
/// socket/task/writer. Operation retains its returned future through cleanup.
/// Qualification conservatively refuses media while any recording is active;
/// source-file identities and post-decode directory checks remain independent.
struct ServiceRecordingDirectory {
    reader: crate::session_observation::SessionReader,
}
impl RecordingDirectory for ServiceRecordingDirectory {
    fn current_directory(&self) -> DirectoryFuture {
        let mut reader = self.reader.clone();
        Box::pin(async move {
            let deadline = Instant::now() + Duration::from_secs(2);
            let status = reader
                .read(ReadRequest::RecordStatus, Value::Null, deadline)
                .await
                .map_err(|_| "Recording status is unavailable")?;
            if status["outputActive"] != false {
                return Err("Media inspection requires inactive recording".into());
            }
            let result = reader
                .read(ReadRequest::RecordDirectory, Value::Null, deadline)
                .await
                .map_err(|_| "Recording directory is unavailable")?;
            result["recordDirectory"]
                .as_str()
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
                .ok_or_else(|| "Recording directory is unavailable".into())
        })
    }
}
