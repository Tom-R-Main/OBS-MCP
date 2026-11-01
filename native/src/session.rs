// SPDX-License-Identifier: GPL-2.0-only
//! A policy-free OBS transport shared by native read-only observations and
//! future governed effects. Domain effects must use checked requests/batches
//! to revalidate lease/take/confirmation authority at actual dispatch.
//!
//! Capacity limits cover admitted request/event items and encoded wire bytes,
//! not caller-owned inputs/results, allocator capacity, or total process RSS.
use crate::{
    js_json::{Limits, NumericNormalizationError, Prepared},
    obs::{MAX_BYTES, Socket, open_socket},
    ordered_transform::{
        ProjectionOwner, RawReadback, SceneItemSelectors, SceneItemTransformReadback,
        TransformReadbackError,
    },
};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fmt,
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, timeout, timeout_at},
};
use tokio_tungstenite::tungstenite::Message;

const QUEUED: u8 = 0;
const DISPATCHING: u8 = 1;
const ABANDONED: u8 = 2;
const RESERVED_STOP_BYTES: usize = 512;
static NEXT_CONNECTION: AtomicU64 = AtomicU64::new(0);
// Qualified parser/domain payloads are at most 128 deep. A serial batch adds
// four wrappers (d, requests, item, requestData). Deeper direct-library Values
// refuse before queue/guard admission rather than recursing without a bound.
const MAX_WIRE_JSON_DEPTH: usize = 128 + 4;
/// Matches EventSubscription.All in src/client.ts and docs/protocol.json.
pub const ALL_EVENTS: u64 = (1 << 12) - 1;
/// Pinned optional high-volume subscriptions are bits16..19.
pub const SUPPORTED_EVENT_SUBSCRIPTIONS: u64 = ALL_EVENTS | (0xf << 16);

fn advance(counter: &AtomicU64) -> Option<NonZeroU64> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(1)
        })
        .ok()
        .and_then(|previous| NonZeroU64::new(previous + 1))
}

/// Unique within this process. Exhaustion permanently prevents new tokens;
/// there is no reset, wrap, or public integer constructor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ConnectionToken(NonZeroU64);

impl ConnectionToken {
    /// Actual process connection identity for durable receipt metadata. Reading
    /// it does not grant a way to construct or revive a connection token.
    pub fn generation(self) -> u64 {
        self.0.get()
    }

    fn allocate() -> Result<Self, ConnectError> {
        advance(&NEXT_CONNECTION)
            .map(Self)
            .ok_or(ConnectError::IdentityExhausted)
    }
}

#[derive(Clone, Debug)]
pub struct SessionOptions {
    pub request_items: usize,
    pub request_bytes: usize,
    pub event_items: usize,
    pub event_bytes: usize,
    pub event_subscriptions: u64,
    pub connect_timeout: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            request_items: 32,
            request_bytes: MAX_BYTES,
            event_items: 256,
            event_bytes: MAX_BYTES,
            event_subscriptions: ALL_EVENTS,
            connect_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_millis(250),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectError {
    InvalidOptions,
    UnsupportedTransport,
    IdentityExhausted,
    Deadline,
    Handshake(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Invalidation {
    Shutdown,
    OwnerDropped,
    ActorStopped,
    Disconnected,
    MalformedMessage,
    EventOverflow,
    EventConsumerDropped,
    CancelledAfterDispatch,
    DeadlineAfterDispatch,
    SocketWriteFailed,
    ResponseIdentityMismatch,
    IdentityExhausted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BeforeDispatch {
    StaleConnection,
    Closed(Invalidation),
    InvalidRequest,
    Capacity,
    ByteCapacity,
    Deadline,
    Cancelled,
    IdentityExhausted,
    GuardRefused(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestError {
    BeforeDispatch(BeforeDispatch),
    Rejected {
        code: u64,
    },
    RejectedDetailed {
        code: u64,
        comment: RejectionComment,
    },
    Unknown(Invalidation),
}

/// Opaque peer diagnostic, exposed only through explicit tool-result projection.
/// Debug/Display never reveal settings that a peer may include in its comment.
#[derive(Clone, PartialEq, Eq)]
pub struct RejectionComment(Arc<str>);
impl fmt::Debug for RejectionComment {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("RejectionComment")
            .field("bytes", &self.0.len())
            .finish_non_exhaustive()
    }
}
impl RequestError {
    pub fn rejection_code(&self) -> Option<u64> {
        match self {
            Self::Rejected { code } | Self::RejectedDetailed { code, .. } => Some(*code),
            _ => None,
        }
    }
    /// Matches the predecessor OBSRequestError tool message. Never use this for
    /// stderr, Debug, durable receipts or other unsolicited diagnostic logging.
    pub fn obs_failure_message(&self, request_type: &str) -> Option<String> {
        let code = self.rejection_code()?;
        let suffix = match self {
            Self::RejectedDetailed { comment, .. } => format!(": {}", comment.0),
            _ => String::new(),
        };
        Some(format!(
            "OBS request {request_type} failed with code {code}{suffix}"
        ))
    }
}

/// Parallel execution is intentionally unrepresentable: pinned OBS5.7 can
/// deadlock its worker pool and mislabel results in parallel batches.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum Execution {
    #[default]
    SerialRealtime = 0,
    SerialFrame = 1,
}
#[derive(Clone)]
pub struct BatchRequest {
    pub request_type: String,
    /// None omits requestData; Some(null) preserves explicit null like TS.
    pub request_data: Option<Value>,
}
impl fmt::Debug for BatchRequest {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("BatchRequest")
            .field("request_type", &self.request_type)
            .field("has_request_data", &self.request_data.is_some())
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Debug, Default)]
pub struct BatchOptions {
    pub execution: Execution,
    pub halt_on_failure: bool,
    pub timeout: Option<Duration>,
}
impl BatchOptions {
    /// Same default arithmetic as client.ts:10s plus both Sleep fields, with
    /// frames converted at30fps regardless of selected execution mode.
    /// Invalid/nonpositive/unrepresentable defaults refuse before dispatch
    /// rather than adopting Node's overflowing-timer fallback to1ms.
    pub fn effective_timeout(&self, requests: &[BatchRequest]) -> Result<Duration, BeforeDispatch> {
        if let Some(duration) = self.timeout {
            return if duration.is_zero() {
                Err(BeforeDispatch::Deadline)
            } else {
                Ok(duration)
            };
        }
        let mut sleep_millis = 0.0;
        for request in requests {
            if request.request_type == "Sleep"
                && let Some(data) = request
                    .request_data
                    .as_ref()
                    .filter(|data| data.is_object())
            {
                if let Some(value) = data.get("sleepMillis").and_then(Value::as_f64) {
                    sleep_millis += value;
                }
                if let Some(value) = data.get("sleepFrames").and_then(Value::as_f64) {
                    sleep_millis += (value / 30.0) * 1000.0;
                }
            }
        }
        let millis = 10_000.0 + sleep_millis;
        if !millis.is_finite() || millis <= 0.0 || millis > f64::from(i32::MAX) {
            return Err(BeforeDispatch::Deadline);
        }
        Duration::try_from_secs_f64(millis / 1000.0).map_err(|_| BeforeDispatch::Deadline)
    }
}
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchResult {
    pub request_type: String,
    pub ok: bool,
    pub code: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub response_data: Value,
}
impl fmt::Debug for BatchResult {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("BatchResult")
            .field("request_type", &self.request_type)
            .field("ok", &self.ok)
            .field("code", &self.code)
            .field("has_comment", &self.comment.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShutdownError {
    ActorFailed,
    Deadline,
}

macro_rules! debug_error {
    ($($ty:ty),+ $(,)?) => { $(impl fmt::Display for $ty {
        fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result { write!(out, "{self:?}") }
    }
    impl std::error::Error for $ty {})+ };
}
debug_error!(ConnectError, RequestError, ShutdownError);

struct RequestLease {
    _item: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}
struct Reply {
    result: Result<ReplyData, RequestError>,
    _lease: RequestLease,
}
type Guard = Box<dyn FnOnce() -> Result<(), String> + Send>;
enum ReplyData {
    Single(Value),
    Batch(Vec<BatchResult>),
    Transform(Result<RawReadback, crate::ordered_transform::ProjectionError>),
}
enum CommandKind {
    Single(String),
    ReservedControl(StopControl),
    Transform,
    Batch {
        request_types: Vec<String>,
        halt_on_failure: bool,
    },
}

#[derive(Clone, Copy)]
enum StopControl {
    StopRecord,
    RecordStatus,
}
impl StopControl {
    fn request_type(self) -> &'static str {
        match self {
            Self::StopRecord => "StopRecord",
            Self::RecordStatus => "GetRecordStatus",
        }
    }
}
enum SingleRequest<'a> {
    Ordinary(&'a str),
    Transform,
    Reserved(StopControl),
}
impl SingleRequest<'_> {
    fn request_type(&self) -> &str {
        match self {
            Self::Ordinary(name) => name,
            Self::Transform => "GetSceneItemTransform",
            Self::Reserved(control) => control.request_type(),
        }
    }
}

struct CommandQueues {
    normal: mpsc::Receiver<Command>,
    stop: mpsc::Receiver<Command>,
}

struct Command {
    expected: ConnectionToken,
    kind: CommandKind,
    request_id: String,
    wire: String,
    deadline: Instant,
    state: Arc<AtomicU8>,
    guard: Option<Guard>,
    reply: oneshot::Sender<Reply>,
    lease: RequestLease,
}
struct Admission {
    expected: ConnectionToken,
    kind: CommandKind,
    request_id: String,
    deadline: Instant,
    item: OwnedSemaphorePermit,
}

struct Cancellation(Arc<AtomicU8>);
impl Drop for Cancellation {
    fn drop(&mut self) {
        let _ = self
            .0
            .compare_exchange(QUEUED, ABANDONED, Ordering::AcqRel, Ordering::Acquire);
    }
}

/// A cloneable admission handle; it owns no socket or independent reader task.
#[derive(Clone)]
pub struct SessionHandle {
    token: ConnectionToken,
    endpoint_hash: Arc<str>,
    event_subscriptions: u64,
    sender: mpsc::Sender<Command>,
    stop_sender: mpsc::Sender<Command>,
    stop_items: Arc<Semaphore>,
    stop_bytes: Arc<Semaphore>,
    items: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    max_bytes: usize,
    projections: Arc<ProjectionOwner>,
    sequence: Arc<AtomicU64>,
    event_cursor: Arc<AtomicU64>,
    terminal: watch::Receiver<Option<Invalidation>>,
}

/// Owns the actor lifetime. Call `shutdown` to establish task/socket exit.
/// Dropping the owner invalidates handles and aborts its task as a fallback;
/// that fallback is not an awaited shutdown receipt.
pub struct ObsSession {
    handle: SessionHandle,
    stop: watch::Sender<bool>,
    terminal: watch::Sender<Option<Invalidation>>,
    task: Option<JoinHandle<()>>,
    shutdown_timeout: Duration,
}

/// Its permits remain charged until the event itself is dropped, even after
/// receiving it from the queue. Retaining events can therefore close a session.
pub struct ObsEvent {
    pub connection: ConnectionToken,
    /// Connection-scoped delivery order. Pair with `connection`; sequence alone
    /// is a watermark, never proof that current OBS state matches this event.
    pub sequence: u64,
    pub event_type: String,
    pub intent: u64,
    pub data: Option<Value>,
    pub encoded_bytes: usize,
    _item: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

impl fmt::Debug for ObsEvent {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("ObsEvent")
            .field("connection", &self.connection)
            .field("sequence", &self.sequence)
            .field("event_type", &self.event_type)
            .field("intent", &self.intent)
            .field("encoded_bytes", &self.encoded_bytes)
            .field("has_data", &self.data.is_some())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for SessionHandle {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("SessionHandle")
            .field("token", &self.token)
            .field("endpoint_hash", &self.endpoint_hash)
            .field("event_cursor", &self.event_cursor())
            .field("invalidation", &self.invalidation())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for ObsSession {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("ObsSession")
            .field("handle", &self.handle)
            .finish_non_exhaustive()
    }
}

pub struct EventReceiver {
    token: ConnectionToken,
    receiver: mpsc::Receiver<ObsEvent>,
    terminal: watch::Receiver<Option<Invalidation>>,
}

impl EventReceiver {
    /// Intrinsic source identity for binding an event bridge to its handle.
    /// Equal endpoint hashes do not make receivers from different connections
    /// interchangeable, even if their numerical event cursors happen to match.
    pub fn connection_token(&self) -> ConnectionToken {
        self.token
    }

    /// Terminal invalidation wins over queued events. Previously delivered
    /// events remain historical facts, never proof that the connection is live.
    pub async fn next(&mut self) -> Result<ObsEvent, Invalidation> {
        loop {
            if let Some(reason) = self.terminal.borrow().clone() {
                self.receiver.close();
                while self.receiver.try_recv().is_ok() {}
                return Err(reason);
            }
            tokio::select! {
                biased;
                changed = self.terminal.changed() => {
                    if changed.is_err() { return Err(Invalidation::ActorStopped); }
                },
                event = self.receiver.recv() => {
                    if let Some(reason) = self.terminal.borrow().clone() { return Err(reason); }
                    return event.ok_or(Invalidation::ActorStopped);
                }
            }
        }
    }

    pub fn invalidation(&self) -> Option<Invalidation> {
        self.terminal.borrow().clone()
    }
}

impl ObsSession {
    pub async fn connect(
        url: &str,
        password: Option<&str>,
        options: SessionOptions,
    ) -> Result<(Self, EventReceiver), ConnectError> {
        if [
            options.request_items,
            options.request_bytes,
            options.event_items,
            options.event_bytes,
        ]
        .into_iter()
        .any(|limit| limit == 0 || limit > Semaphore::MAX_PERMITS)
            || options.request_bytes > u32::MAX as usize
            || options.event_bytes > u32::MAX as usize
            || options.event_subscriptions & !SUPPORTED_EVENT_SUBSCRIPTIONS != 0
            || options.connect_timeout.is_zero()
            || options.shutdown_timeout.is_zero()
            || Instant::now()
                .checked_add(
                    options
                        .shutdown_timeout
                        .saturating_add(Duration::from_secs(1)),
                )
                .is_none()
        {
            return Err(ConnectError::InvalidOptions);
        }
        // No implicit downgrade or unqualified replacement TLS implementation.
        if !url.starts_with("ws://") {
            return Err(ConnectError::UnsupportedTransport);
        }
        let token = ConnectionToken::allocate()?;
        let deadline = Instant::now()
            .checked_add(options.connect_timeout)
            .ok_or(ConnectError::InvalidOptions)?;
        let socket = timeout_at(
            deadline,
            open_socket(url, password, options.event_subscriptions, deadline),
        )
        .await
        .map_err(|_| ConnectError::Deadline)?
        .map_err(ConnectError::Handshake)?;
        let (sender, commands) = mpsc::channel(options.request_items);
        let (stop_sender, stop_commands) = mpsc::channel(1);
        let (events, receiver) = mpsc::channel(options.event_items);
        let (terminal, terminal_receiver) = watch::channel(None);
        let (stop, stopping) = watch::channel(false);
        let event_cursor = Arc::new(AtomicU64::new(0));
        let handle = SessionHandle {
            token,
            endpoint_hash: format!("{:x}", Sha256::digest(url.as_bytes())).into(),
            event_subscriptions: options.event_subscriptions,
            sender,
            stop_sender,
            stop_items: Arc::new(Semaphore::new(1)),
            stop_bytes: Arc::new(Semaphore::new(RESERVED_STOP_BYTES)),
            items: Arc::new(Semaphore::new(options.request_items)),
            bytes: Arc::new(Semaphore::new(options.request_bytes)),
            max_bytes: options.request_bytes.min(MAX_BYTES),
            projections: Arc::new(ProjectionOwner::new(options.request_items)),
            sequence: Arc::new(AtomicU64::new(0)),
            event_cursor: event_cursor.clone(),
            terminal: terminal_receiver.clone(),
        };
        let publisher = Publisher {
            token,
            sender: events,
            items: Arc::new(Semaphore::new(options.event_items)),
            bytes: Arc::new(Semaphore::new(options.event_bytes)),
            cursor: event_cursor,
        };
        let task = tokio::spawn(actor(
            socket,
            token,
            CommandQueues {
                normal: commands,
                stop: stop_commands,
            },
            publisher,
            stopping,
            ActorLifetime {
                terminal: terminal.clone(),
                projections: handle.projections.clone(),
            },
            options.shutdown_timeout,
        ));
        let owner = Self {
            handle,
            stop,
            terminal,
            task: Some(task),
            shutdown_timeout: options.shutdown_timeout,
        };
        Ok((
            owner,
            EventReceiver {
                token,
                receiver,
                terminal: terminal_receiver,
            },
        ))
    }

    pub fn handle(&self) -> SessionHandle {
        self.handle.clone()
    }
    pub fn token(&self) -> ConnectionToken {
        self.handle.token
    }

    /// Stops this transport and joins its actor. It does not stop recording,
    /// streaming, or any other OBS effect that may already have been dispatched.
    pub async fn shutdown(mut self) -> Result<(), ShutdownError> {
        self.stop.send_replace(true);
        let mut task = self.task.take().expect("owned actor");
        let budget = self.shutdown_timeout.saturating_add(Duration::from_secs(1));
        match timeout(budget, &mut task).await {
            Ok(result) => result.map_err(|_| ShutdownError::ActorFailed),
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err(ShutdownError::Deadline)
            }
        }
    }
}

impl Drop for ObsSession {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            let unset = self.terminal.borrow().is_none();
            if unset {
                self.terminal.send_replace(Some(Invalidation::OwnerDropped));
            }
            self.stop.send_replace(true);
            self.handle.projections.cancel();
            task.abort();
        }
    }
}

fn request_deadline(duration: Duration) -> Result<Instant, RequestError> {
    let fail = || RequestError::BeforeDispatch(BeforeDispatch::Deadline);
    let deadline = Instant::now().checked_add(duration).ok_or_else(fail)?;
    if duration.is_zero() {
        Err(fail())
    } else {
        Ok(deadline)
    }
}

impl SessionHandle {
    /// Subscription mask authenticated by this connection's Identify exchange.
    /// A consumer's event filter cannot establish that the peer was subscribed.
    pub fn event_subscriptions(&self) -> u64 {
        self.event_subscriptions
    }

    /// Highest event sequence reserved by this socket owner, including events
    /// that a consumer has not processed. Pair with `token()` and wait for the
    /// consumer to cross this watermark or invalidate. It is not delivery or
    /// current-state proof; overflow may invalidate before publication completes.
    pub fn event_cursor(&self) -> u64 {
        self.event_cursor.load(Ordering::Acquire)
    }

    /// Fingerprint of the exact connect URL; the handle retains no raw URL or
    /// password. Matches OperationStore's endpoint binding without normalization.
    pub fn endpoint_hash(&self) -> &str {
        &self.endpoint_hash
    }

    pub fn token(&self) -> ConnectionToken {
        self.token
    }
    pub fn invalidation(&self) -> Option<Invalidation> {
        self.terminal.borrow().clone()
    }

    /// Transport-only request. Domain effects must use `request_checked` with
    /// their final authority check; transport token validation is not policy.
    pub async fn request(
        &self,
        expected: ConnectionToken,
        request_type: &str,
        data: Value,
        timeout: Duration,
    ) -> Result<Value, RequestError> {
        self.request_checked(expected, request_type, data, timeout, || Ok(()))
            .await
    }

    /// The synchronous guard runs inside the socket owner after queue wait and
    /// token/deadline checks, immediately before send. It must be bounded, have
    /// no side effects, and release all policy locks before returning.
    pub async fn request_checked<F>(
        &self,
        expected: ConnectionToken,
        request_type: &str,
        data: Value,
        duration: Duration,
        guard: F,
    ) -> Result<Value, RequestError>
    where
        F: FnOnce() -> Result<(), String> + Send + 'static,
    {
        let deadline = request_deadline(duration)?;
        match self
            .single_until(
                expected,
                SingleRequest::Ordinary(request_type),
                data,
                deadline,
                guard,
            )
            .await?
        {
            ReplyData::Single(value) => Ok(value),
            _ => Err(RequestError::Unknown(
                Invalidation::ResponseIdentityMismatch,
            )),
        }
    }

    /// Fixed read-only projection; no arbitrary request name or subtree path.
    /// Canonicalization is retained outside the physical socket actor. Its text
    /// lease covers transport-owned text only, not caller/tool/frontend copies.
    pub async fn scene_item_transform_checked<F>(
        &self,
        expected: ConnectionToken,
        selectors: SceneItemSelectors,
        duration: Duration,
        guard: F,
    ) -> Result<SceneItemTransformReadback, TransformReadbackError>
    where
        F: FnOnce() -> Result<(), String> + Send + 'static,
    {
        let deadline = request_deadline(duration).map_err(TransformReadbackError::Request)?;
        // Pinned Source Zod4 int().nonnegative() and exported schema admit
        // only safe nonnegative integers, even though the host type is u64.
        if selectors.scene_item_id > 9_007_199_254_740_991 {
            return Err(TransformReadbackError::Request(
                RequestError::BeforeDispatch(BeforeDispatch::InvalidRequest),
            ));
        }
        let data = serde_json::to_value(selectors).expect("closed transform selectors");
        let result = self
            .single_until(expected, SingleRequest::Transform, data, deadline, guard)
            .await
            .map_err(TransformReadbackError::Request)?;
        let ReplyData::Transform(raw) = result else {
            return Err(TransformReadbackError::Request(RequestError::Unknown(
                Invalidation::ResponseIdentityMismatch,
            )));
        };
        self.projections
            .project(raw.map_err(TransformReadbackError::Projection)?)
            .await
            .map_err(TransformReadbackError::Projection)
    }

    /// A closed, separately bounded StopRecord lane. Domain callers must retain
    /// their original journal reservation and final dispatch guard. This cannot
    /// preempt a frame already flushing through TCP and grants no confirmation.
    pub async fn stop_record_checked<F>(
        &self,
        expected: ConnectionToken,
        duration: Duration,
        guard: F,
    ) -> Result<Value, RequestError>
    where
        F: FnOnce() -> Result<(), String> + Send + 'static,
    {
        self.reserved_control_checked(expected, StopControl::StopRecord, duration, guard)
            .await
    }
    /// Exact freshness read for Stop workflow pre/post checks only. Other
    /// GetRecordStatus reads retain ordinary admission and serialization.
    pub async fn stop_record_status_checked<F>(
        &self,
        expected: ConnectionToken,
        duration: Duration,
        guard: F,
    ) -> Result<Value, RequestError>
    where
        F: FnOnce() -> Result<(), String> + Send + 'static,
    {
        self.reserved_control_checked(expected, StopControl::RecordStatus, duration, guard)
            .await
    }
    async fn reserved_control_checked<F>(
        &self,
        expected: ConnectionToken,
        control: StopControl,
        duration: Duration,
        guard: F,
    ) -> Result<Value, RequestError>
    where
        F: FnOnce() -> Result<(), String> + Send + 'static,
    {
        let deadline = request_deadline(duration)?;
        match self
            .single_until(
                expected,
                SingleRequest::Reserved(control),
                Value::Null,
                deadline,
                guard,
            )
            .await?
        {
            ReplyData::Single(value) => Ok(value),
            _ => Err(RequestError::Unknown(
                Invalidation::ResponseIdentityMismatch,
            )),
        }
    }

    async fn single_until<F>(
        &self,
        expected: ConnectionToken,
        request: SingleRequest<'_>,
        data: Value,
        deadline: Instant,
        guard: F,
    ) -> Result<ReplyData, RequestError>
    where
        F: FnOnce() -> Result<(), String> + Send + 'static,
    {
        let fail = RequestError::BeforeDispatch;
        let request_type = request.request_type();
        let reserved = matches!(request, SingleRequest::Reserved(_));
        if expected != self.token {
            return Err(fail(BeforeDispatch::StaleConnection));
        }
        if let Some(reason) = self.invalidation() {
            return Err(fail(BeforeDispatch::Closed(reason)));
        }
        if request_type.is_empty()
            || request_type.len() > 128
            || !request_type
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric())
            || !(data.is_null() || data.is_object())
        {
            return Err(fail(BeforeDispatch::InvalidRequest));
        }
        let item = if reserved {
            &self.stop_items
        } else {
            &self.items
        }
        .clone()
        .try_acquire_owned()
        .map_err(|_| fail(BeforeDispatch::Capacity))?;
        let sequence =
            advance(&self.sequence).ok_or_else(|| fail(BeforeDispatch::IdentityExhausted))?;
        let request_id = format!("{}:{sequence}", self.token.0);
        let mut frame = json!({"op":6,"d":{"requestId":request_id,"requestType":request_type}});
        if !data.is_null() {
            frame["d"]["requestData"] = data;
        }
        let admission = Admission {
            expected,
            kind: match request {
                SingleRequest::Transform => CommandKind::Transform,
                SingleRequest::Reserved(control) => CommandKind::ReservedControl(control),
                SingleRequest::Ordinary(name) => CommandKind::Single(name.to_owned()),
            },
            request_id,
            deadline,
            item,
        };
        self.enqueue(admission, frame, guard, reserved).await
    }

    /// Transport-only serial batch. Domain effects must use batch_checked.
    /// Like TS, an empty batch produces no frame or authority/guard check.
    pub async fn batch(
        &self,
        expected: ConnectionToken,
        requests: Vec<BatchRequest>,
        options: BatchOptions,
    ) -> Result<Vec<BatchResult>, RequestError> {
        self.batch_checked(expected, requests, options, || Ok(()))
            .await
    }
    /// Same final dispatch guard contract as request_checked. There is one
    /// guard for the entire batch; this does not make multiple OBS writes an
    /// atomic transaction or prevent an external controller changing OBS.
    pub async fn batch_checked<F>(
        &self,
        expected: ConnectionToken,
        requests: Vec<BatchRequest>,
        options: BatchOptions,
        guard: F,
    ) -> Result<Vec<BatchResult>, RequestError>
    where
        F: FnOnce() -> Result<(), String> + Send + 'static,
    {
        let admitted_at = Instant::now();
        if requests.is_empty() {
            return Ok(vec![]);
        }
        let fail = RequestError::BeforeDispatch;
        let duration = options.effective_timeout(&requests).map_err(fail)?;
        let deadline = admitted_at
            .checked_add(duration)
            .ok_or_else(|| fail(BeforeDispatch::Deadline))?;
        if expected != self.token {
            return Err(fail(BeforeDispatch::StaleConnection));
        }
        if let Some(reason) = self.invalidation() {
            return Err(fail(BeforeDispatch::Closed(reason)));
        }
        if requests.iter().any(|request| {
            request.request_type.is_empty()
                || request.request_type.len() > 128
                || !request
                    .request_type
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric())
        }) {
            return Err(fail(BeforeDispatch::InvalidRequest));
        }
        // Reject an impossible inventory before allocating internal metadata.
        // This is a lower bound from the smallest legal item, not a new
        // arbitrary per-batch request-count limit.
        const MIN_ITEM: usize = r#"{"requestId":"0","requestType":"A"}"#.len();
        if requests
            .len()
            .checked_mul(MIN_ITEM)
            .is_none_or(|bytes| bytes > self.max_bytes)
        {
            return Err(fail(BeforeDispatch::ByteCapacity));
        }
        let item = self
            .items
            .clone()
            .try_acquire_owned()
            .map_err(|_| fail(BeforeDispatch::Capacity))?;
        let sequence =
            advance(&self.sequence).ok_or_else(|| fail(BeforeDispatch::IdentityExhausted))?;
        let request_id = format!("{}:{sequence}", self.token.0);
        let mut request_types = Vec::with_capacity(requests.len());
        let requests = requests
            .into_iter()
            .enumerate()
            .map(|(index, request)| {
                request_types.push(request.request_type.clone());
                let mut item =
                    json!({"requestType":request.request_type,"requestId":index.to_string()});
                if let Some(data) = request.request_data {
                    item["requestData"] = data;
                }
                item
            })
            .collect::<Vec<_>>();
        let frame = json!({"op":8,"d":{"requestId":request_id,"haltOnFailure":options.halt_on_failure,"executionType":options.execution as u8,"requests":requests}});
        let admission = Admission {
            expected,
            kind: CommandKind::Batch {
                request_types,
                halt_on_failure: options.halt_on_failure,
            },
            request_id,
            deadline,
            item,
        };
        match self.enqueue(admission, frame, guard, false).await? {
            ReplyData::Batch(results) => Ok(results),
            _ => Err(RequestError::Unknown(
                Invalidation::ResponseIdentityMismatch,
            )),
        }
    }

    async fn enqueue<F>(
        &self,
        admission: Admission,
        frame: Value,
        guard: F,
        reserved: bool,
    ) -> Result<ReplyData, RequestError>
    where
        F: FnOnce() -> Result<(), String> + Send + 'static,
    {
        let fail = RequestError::BeforeDispatch;
        let Admission {
            expected,
            kind,
            request_id,
            deadline,
            item,
        } = admission;
        // Validate/count the immutable frame with the same Source-number
        // encoder used for publication. Decimal expansion is part of the exact
        // charge. Prepared allocates no complete wire buffer or opaque Value
        // clone, and into_buffer below reuses its checked length without a
        // second count. The first full frame allocation follows byte admission.
        let prepared = Prepared::new(
            &frame,
            Limits {
                max_depth: MAX_WIRE_JSON_DEPTH,
                max_encoded_bytes: if reserved {
                    RESERVED_STOP_BYTES
                } else {
                    self.max_bytes
                },
            },
        )
        .map_err(|error| {
            fail(match error {
                NumericNormalizationError::EncodedBytes => BeforeDispatch::ByteCapacity,
                NumericNormalizationError::Depth | NumericNormalizationError::NumberUnavailable => {
                    BeforeDispatch::InvalidRequest
                }
            })
        })?;
        let bytes = if reserved {
            &self.stop_bytes
        } else {
            &self.bytes
        }
        .clone()
        .try_acquire_many_owned(prepared.len() as u32)
        .map_err(|_| fail(BeforeDispatch::ByteCapacity))?;
        let buffer = prepared
            .into_buffer()
            .map_err(|_| fail(BeforeDispatch::ByteCapacity))?;
        drop(frame);
        let wire = String::from_utf8(buffer).expect("Source-number JSON encoder emits UTF-8");
        let state = Arc::new(AtomicU8::new(QUEUED));
        let _cancel = Cancellation(state.clone());
        let (reply, receive) = oneshot::channel();
        let command = Command {
            expected,
            kind,
            request_id,
            wire,
            deadline,
            state: state.clone(),
            guard: Some(Box::new(guard)),
            reply,
            lease: RequestLease {
                _item: item,
                _bytes: bytes,
            },
        };
        if Instant::now() >= deadline {
            return Err(fail(BeforeDispatch::Deadline));
        }
        let sender = if reserved {
            &self.stop_sender
        } else {
            &self.sender
        };
        sender.try_send(command).map_err(|error| {
            fail(match error {
                mpsc::error::TrySendError::Full(_) => BeforeDispatch::Capacity,
                mpsc::error::TrySendError::Closed(_) => BeforeDispatch::Closed(
                    self.invalidation().unwrap_or(Invalidation::ActorStopped),
                ),
            })
        })?;
        match timeout_at(deadline, receive).await {
            Ok(Ok(reply)) => reply.result,
            Ok(Err(_)) => {
                let reason = self.invalidation().unwrap_or(Invalidation::ActorStopped);
                if state.load(Ordering::Acquire) == DISPATCHING {
                    Err(RequestError::Unknown(reason))
                } else {
                    Err(fail(BeforeDispatch::Closed(reason)))
                }
            }
            Err(_) => {
                if state
                    .compare_exchange(QUEUED, ABANDONED, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    Err(fail(BeforeDispatch::Deadline))
                } else {
                    Err(RequestError::Unknown(Invalidation::DeadlineAfterDispatch))
                }
            }
        }
    }
}

struct Publisher {
    token: ConnectionToken,
    sender: mpsc::Sender<ObsEvent>,
    items: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    cursor: Arc<AtomicU64>,
}
impl Publisher {
    fn publish(&self, mut data: Value, encoded_bytes: usize) -> Result<(), Invalidation> {
        let event_type = data["eventType"]
            .as_str()
            .filter(|name| !name.is_empty())
            .ok_or(Invalidation::MalformedMessage)?
            .to_owned();
        let intent = data["eventIntent"]
            .as_u64()
            .ok_or(Invalidation::MalformedMessage)?;
        if intent & !SUPPORTED_EVENT_SUBSCRIPTIONS != 0 {
            return Err(Invalidation::MalformedMessage);
        }
        // Reserve before publication so an RPC reply cannot overtake the
        // watermark of an earlier event merely because its consumer is lagging.
        // If publication fails, terminal invalidation prevents false delivery.
        let sequence = advance(&self.cursor)
            .ok_or(Invalidation::IdentityExhausted)?
            .get();
        let item = self
            .items
            .clone()
            .try_acquire_owned()
            .map_err(|_| Invalidation::EventOverflow)?;
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(encoded_bytes as u32)
            .map_err(|_| Invalidation::EventOverflow)?;
        let event = ObsEvent {
            connection: self.token,
            sequence,
            event_type,
            intent,
            data: data
                .as_object_mut()
                .expect("validated event object")
                .remove("eventData"),
            encoded_bytes,
            _item: item,
            _bytes: bytes,
        };
        self.sender.try_send(event).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => Invalidation::EventOverflow,
            mpsc::error::TrySendError::Closed(_) => Invalidation::EventConsumerDropped,
        })
    }
}

enum Incoming {
    Response {
        batch: bool,
        data: Value,
        raw: tokio_tungstenite::tungstenite::Utf8Bytes,
    },
    Other,
}
fn incoming(
    message: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
    publisher: &Publisher,
) -> Result<Incoming, Invalidation> {
    match message {
        Some(Ok(Message::Text(text))) => {
            let mut frame: Value =
                serde_json::from_str(&text).map_err(|_| Invalidation::MalformedMessage)?;
            if !frame.is_object() || !frame["d"].is_object() {
                return Err(Invalidation::MalformedMessage);
            }
            match frame["op"].as_u64() {
                Some(5) => {
                    publisher.publish(frame["d"].take(), text.len())?;
                    Ok(Incoming::Other)
                }
                Some(7) => Ok(Incoming::Response {
                    batch: false,
                    data: frame["d"].take(),
                    raw: text,
                }),
                Some(9) => Ok(Incoming::Response {
                    batch: true,
                    data: frame["d"].take(),
                    raw: text,
                }),
                Some(0 | 2) => Err(Invalidation::ResponseIdentityMismatch),
                _ => Err(Invalidation::MalformedMessage),
            }
        }
        // tungstenite queues Pong during read and flushes it on the next read.
        Some(Ok(Message::Ping(_) | Message::Pong(_))) => Ok(Incoming::Other),
        Some(Ok(Message::Close(_))) | None | Some(Err(_)) => Err(Invalidation::Disconnected),
        _ => Err(Invalidation::MalformedMessage),
    }
}

fn response(
    mut data: Value,
    command: &Command,
    batch: bool,
    raw: &str,
    projections: &ProjectionOwner,
) -> Result<Option<Result<ReplyData, RequestError>>, Invalidation> {
    let request_type = match &command.kind {
        CommandKind::Single(request_type) if !batch => request_type.as_str(),
        CommandKind::Transform if !batch => "GetSceneItemTransform",
        CommandKind::ReservedControl(control) if !batch => control.request_type(),
        CommandKind::Batch {
            request_types,
            halt_on_failure,
        } if batch => {
            return batch_response(data, &command.request_id, request_types, *halt_on_failure)
                .map(|results| Some(Ok(ReplyData::Batch(results))));
        }
        _ => return Err(Invalidation::ResponseIdentityMismatch),
    };
    if data["requestId"].as_str() != Some(command.request_id.as_str()) {
        return Ok(None);
    }
    if data["requestType"].as_str() != Some(request_type) {
        return Err(Invalidation::ResponseIdentityMismatch);
    }
    let status = &data["requestStatus"];
    let code = status["code"]
        .as_u64()
        .ok_or(Invalidation::MalformedMessage)?;
    let success = status["result"]
        .as_bool()
        .ok_or(Invalidation::MalformedMessage)?;
    // Keep comments owned and bounded for explicit tool errors; Debug/Display
    // and durable custody continue to contain classification/code only.
    let comment = status
        .get("comment")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty());
    if !success && comment.is_some_and(|value| value.len() > 64 * 1024) {
        return Err(Invalidation::MalformedMessage);
    }
    let rejection = if let Some(comment) = comment {
        RequestError::RejectedDetailed {
            code,
            comment: RejectionComment(Arc::from(comment)),
        }
    } else {
        RequestError::Rejected { code }
    };
    Ok(Some(if success {
        let value = data
            .as_object_mut()
            .expect("validated response object")
            .remove("responseData")
            .filter(|value| !value.is_null())
            .unwrap_or_else(|| json!({}));
        if matches!(command.kind, CommandKind::Transform) {
            Ok(ReplyData::Transform(projections.capture(
                raw,
                value,
                command.deadline,
            )))
        } else {
            Ok(ReplyData::Single(value))
        }
    } else {
        Err(rejection)
    }))
}

fn batch_response(
    mut data: Value,
    request_id: &str,
    request_types: &[String],
    halt_on_failure: bool,
) -> Result<Vec<BatchResult>, Invalidation> {
    if data["requestId"].as_str() != Some(request_id) {
        return Err(Invalidation::ResponseIdentityMismatch);
    }
    let results = data
        .as_object_mut()
        .and_then(|data| data.remove("results"))
        .and_then(|results| match results {
            Value::Array(results) => Some(results),
            _ => None,
        })
        .ok_or(Invalidation::MalformedMessage)?;
    // Deliberate safety divergence from TS's permissive positional/coerced-ID
    // sorting: every result must identify exactly one admitted request, with
    // an exact canonical index and matching request type. Missing/duplicate/
    // foreign IDs and unexplained partial results invalidate the whole session.
    if results.is_empty()
        || results.len() > request_types.len()
        || (!halt_on_failure && results.len() != request_types.len())
    {
        return Err(Invalidation::ResponseIdentityMismatch);
    }
    let mut ordered = (0..results.len()).map(|_| None).collect::<Vec<_>>();
    for mut result in results {
        if !result.is_object() || !result["requestStatus"].is_object() {
            return Err(Invalidation::MalformedMessage);
        }
        let index = result["requestId"]
            .as_str()
            .and_then(|id| {
                id.parse::<usize>()
                    .ok()
                    .filter(|index| index.to_string() == id)
            })
            .filter(|index| *index < ordered.len())
            .ok_or(Invalidation::ResponseIdentityMismatch)?;
        let request_type = result["requestType"]
            .as_str()
            .ok_or(Invalidation::MalformedMessage)?;
        if request_type != request_types[index] || ordered[index].is_some() {
            return Err(Invalidation::ResponseIdentityMismatch);
        }
        let status = &result["requestStatus"];
        let ok = status["result"]
            .as_bool()
            .ok_or(Invalidation::MalformedMessage)?;
        // Official OBS status codes are unsigned integers. Unlike the source
        // typeof-number admission, negative/fractional/out-of-range codes are
        // malformed. Equivalent JSON spelling604.0 remains the integer604.
        let code = status["code"]
            .as_u64()
            .or_else(|| {
                status["code"]
                    .as_f64()
                    .filter(|n| {
                        n.is_finite()
                            && *n >= 0.0
                            && *n < 18_446_744_073_709_551_616.0
                            && n.fract() == 0.0
                    })
                    .map(|n| n as u64)
            })
            .ok_or(Invalidation::MalformedMessage)?;
        let comment = status["comment"].as_str().map(str::to_owned);
        let request_type = request_type.to_owned();
        let response_data = result
            .as_object_mut()
            .unwrap()
            .remove("responseData")
            .filter(|data| !data.is_null())
            .unwrap_or_else(|| json!({}));
        ordered[index] = Some(BatchResult {
            request_type,
            ok,
            code,
            comment,
            response_data,
        });
    }
    let results = ordered
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or(Invalidation::ResponseIdentityMismatch)?;
    if halt_on_failure {
        let first_failure = results.iter().position(|result| !result.ok);
        if first_failure.is_some_and(|index| index + 1 != results.len())
            || (first_failure.is_none() && results.len() != request_types.len())
        {
            return Err(Invalidation::ResponseIdentityMismatch);
        }
    }
    Ok(results)
}

type Sink = SplitSink<Socket, Message>;
type Stream = SplitStream<Socket>;

fn prepare_dispatch(
    command: &mut Command,
    token: ConnectionToken,
    companion: Option<&Command>,
) -> Option<BeforeDispatch> {
    if command.expected != token {
        return Some(BeforeDispatch::StaleConnection);
    }
    if Instant::now() >= command.deadline {
        return Some(BeforeDispatch::Deadline);
    }
    if command.reply.is_closed() || command.state.load(Ordering::Acquire) != QUEUED {
        return Some(BeforeDispatch::Cancelled);
    }
    if let Err(reason) = command.guard.take().expect("queued dispatch guard")() {
        return Some(BeforeDispatch::GuardRefused(reason));
    }
    if Instant::now() >= command.deadline {
        return Some(BeforeDispatch::Deadline);
    }
    if let Some(other) = companion {
        if Instant::now() >= other.deadline {
            return Some(BeforeDispatch::Closed(Invalidation::DeadlineAfterDispatch));
        }
        if other.reply.is_closed() {
            return Some(BeforeDispatch::Closed(Invalidation::CancelledAfterDispatch));
        }
    }
    if command
        .state
        .compare_exchange(QUEUED, DISPATCHING, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
        || command.reply.is_closed()
    {
        return Some(BeforeDispatch::Cancelled);
    }
    None
}
fn unknown(command: &mut Option<Command>, reason: &Invalidation) {
    if let Some(command) = command.take() {
        finish(command, Err(RequestError::Unknown(reason.clone())));
    }
}
async fn request_closed(command: &mut Option<Command>) {
    if let Some(command) = command {
        command.reply.closed().await;
    } else {
        std::future::pending::<()>().await;
    }
}
fn command_deadline(command: &Option<Command>) -> Instant {
    command
        .as_ref()
        .map_or_else(Instant::now, |command| command.deadline)
}

/// Route only the two actually admitted IDs. Matching identity/type/status rules
/// remain response()'s rules; unknown IDs retain the ordinary conservative policy.
fn route_response(
    incoming: Incoming,
    primary: &mut Option<Command>,
    other: &mut Option<Command>,
    defer_primary: bool,
    projections: &ProjectionOwner,
) -> Result<Option<Result<ReplyData, RequestError>>, Invalidation> {
    let Incoming::Response { batch, data, raw } = incoming else {
        return Ok(None);
    };
    let id = data["requestId"].as_str();
    let use_primary = match (primary.as_ref(), other.as_ref()) {
        (Some(first), Some(second)) => {
            if id == Some(first.request_id.as_str()) {
                true
            } else if id == Some(second.request_id.as_str()) {
                false
            } else {
                !matches!(first.kind, CommandKind::ReservedControl(_))
            }
        }
        (Some(_), None) => true,
        (None, Some(_)) => false,
        (None, None) => return Ok(None),
    };
    let target = if use_primary { primary } else { other };
    let command = target.as_ref().expect("selected active command");
    if let Some(result) = response(data, command, batch, &raw, projections)? {
        if use_primary && defer_primary {
            return Ok(Some(result));
        }
        finish(target.take().expect("active reply owner"), result);
    }
    Ok(None)
}

#[derive(Default)]
struct DeferredReply(Option<Result<ReplyData, RequestError>>);
impl DeferredReply {
    fn observe(&mut self, result: Result<ReplyData, RequestError>) {
        if self.0.is_none() {
            self.0 = Some(result);
        }
    }
}

/// Flush exactly one complete frame; a second frame never interleaves its bytes.
/// Already-sent other work may settle while this frame is flushing.
async fn send_active(
    sending: &mut Option<Command>,
    other: &mut Option<Command>,
    sink: &mut Sink,
    stream: &mut Stream,
    publisher: &Publisher,
    stopping: &mut watch::Receiver<bool>,
    projections: &ProjectionOwner,
) -> Result<Option<Result<ReplyData, RequestError>>, Invalidation> {
    let wire = std::mem::take(&mut sending.as_mut().expect("active sender").wire);
    let send = sink.send(Message::Text(wire.into()));
    tokio::pin!(send);
    let mut early = DeferredReply::default();
    loop {
        let send_deadline = command_deadline(sending);
        let other_deadline = command_deadline(other);
        let has_other = other.is_some();
        tokio::select! {biased;
            _=stopping.changed()=>return Err(Invalidation::Shutdown),
            _=tokio::time::sleep_until(send_deadline)=>return Err(Invalidation::DeadlineAfterDispatch),
            _=request_closed(sending)=>return Err(Invalidation::CancelledAfterDispatch),
            _=tokio::time::sleep_until(other_deadline),if has_other=>return Err(Invalidation::DeadlineAfterDispatch),
            _=request_closed(other),if has_other=>return Err(Invalidation::CancelledAfterDispatch),
            _=publisher.sender.closed()=>return Err(Invalidation::EventConsumerDropped),
            sent=&mut send=>{sent.map_err(|_|Invalidation::SocketWriteFailed)?;return Ok(early.0);},
            next=stream.next()=>{if let Some(result)=route_response(incoming(next,publisher)?,sending,other,true,projections)?{early.observe(result);}},
        }
    }
}

/// One ordinary command plus at most one closed reserved StopRecord. The
/// conservative policy still retires the connection if either dispatched
/// command is cancelled/expires: unresolved peers become Unknown, never replay.
async fn drive(
    command: Command,
    stop_commands: &mut mpsc::Receiver<Command>,
    sink: &mut Sink,
    stream: &mut Stream,
    publisher: &Publisher,
    stopping: &mut watch::Receiver<bool>,
    projections: &ProjectionOwner,
) -> Option<Invalidation> {
    let token = command.expected;
    let is_stop = matches!(command.kind, CommandKind::ReservedControl(_));
    let mut first = Some(command);
    let mut empty = None;
    match send_active(
        &mut first,
        &mut empty,
        sink,
        stream,
        publisher,
        stopping,
        projections,
    )
    .await
    {
        Ok(Some(result)) => finish(first.take().unwrap(), result),
        Ok(None) => {}
        Err(reason) => {
            unknown(&mut first, &reason);
            return Some(reason);
        }
    }
    let (mut normal, mut stop) = if is_stop {
        (None, first)
    } else {
        (first, None)
    };
    loop {
        if normal.is_none() && stop.is_none() {
            return None;
        }
        let normal_deadline = command_deadline(&normal);
        let stop_deadline = command_deadline(&stop);
        let has_normal = normal.is_some();
        let has_stop = stop.is_some();
        let failure = tokio::select! {biased;
            _=stopping.changed()=>Some(Invalidation::Shutdown),
            _=tokio::time::sleep_until(normal_deadline),if has_normal=>Some(Invalidation::DeadlineAfterDispatch),
            _=request_closed(&mut normal),if has_normal=>Some(Invalidation::CancelledAfterDispatch),
            _=tokio::time::sleep_until(stop_deadline),if has_stop=>Some(Invalidation::DeadlineAfterDispatch),
            _=request_closed(&mut stop),if has_stop=>Some(Invalidation::CancelledAfterDispatch),
            _=publisher.sender.closed()=>Some(Invalidation::EventConsumerDropped),
            queued=stop_commands.recv(),if !has_stop&&has_normal=>{
                let Some(mut queued)=queued else{unknown(&mut normal,&Invalidation::Shutdown);return Some(Invalidation::Shutdown);};
                if let Some(refusal)=prepare_dispatch(&mut queued,token,normal.as_ref()){finish(queued,Err(RequestError::BeforeDispatch(refusal)));continue;}
                stop=Some(queued);
                match send_active(&mut stop,&mut normal,sink,stream,publisher,stopping,projections).await {
                    Ok(Some(result))=>{finish(stop.take().unwrap(),result);None},
                    Ok(None)=>None,
                    Err(reason)=>Some(reason),
                }
            },
            next=stream.next()=>match incoming(next,publisher){
                Ok(incoming)=>route_response(incoming,&mut normal,&mut stop,false,projections).err(),
                Err(reason)=>Some(reason),
            },
        };
        if let Some(reason) = failure {
            unknown(&mut normal, &reason);
            unknown(&mut stop, &reason);
            return Some(reason);
        }
    }
}
async fn next_command(
    normal: &mut mpsc::Receiver<Command>,
    stop: &mut mpsc::Receiver<Command>,
) -> Option<Command> {
    tokio::select! {biased;command=stop.recv()=>command,command=normal.recv()=>command}
}

fn finish(command: Command, result: Result<ReplyData, RequestError>) {
    let _ = command.reply.send(Reply {
        result,
        _lease: command.lease,
    });
}

struct ActorLifetime {
    terminal: watch::Sender<Option<Invalidation>>,
    projections: Arc<ProjectionOwner>,
}
impl Drop for ActorLifetime {
    fn drop(&mut self) {
        self.projections.cancel();
        let unset = self.terminal.borrow().is_none();
        if unset {
            self.terminal.send_replace(Some(Invalidation::ActorStopped));
        }
    }
}

async fn actor(
    socket: Socket,
    token: ConnectionToken,
    queues: CommandQueues,
    publisher: Publisher,
    mut stopping: watch::Receiver<bool>,
    lifetime: ActorLifetime,
    shutdown_timeout: Duration,
) {
    let CommandQueues {
        normal: mut commands,
        stop: mut stop_commands,
    } = queues;
    let _lifetime = lifetime;
    let terminal = _lifetime.terminal.clone();
    let projections = _lifetime.projections.clone();
    let (mut sink, mut stream) = socket.split();
    let reason = loop {
        if *stopping.borrow() {
            break Invalidation::Shutdown;
        }
        tokio::select! {
            biased;
            _ = stopping.changed() => break Invalidation::Shutdown,
            _ = publisher.sender.closed() => break Invalidation::EventConsumerDropped,
            next = stream.next() => match incoming(next, &publisher) {
                Ok(Incoming::Response {batch:true,..}) => break Invalidation::ResponseIdentityMismatch,
                Ok(_) => {}, Err(reason) => break reason,
            },
            command = next_command(&mut commands,&mut stop_commands) => {
                let Some(mut command) = command else { break Invalidation::Shutdown; };
                if let Some(refusal)=prepare_dispatch(&mut command,token,None){finish(command,Err(RequestError::BeforeDispatch(refusal)));continue;}
                if let Some(reason)=drive(command,&mut stop_commands,&mut sink,&mut stream,&publisher,&mut stopping,&projections).await {break reason;}
            }
        }
    };
    terminal.send_replace(Some(reason.clone()));
    projections.cancel();
    commands.close();
    stop_commands.close();
    while let Some(command) = stop_commands.recv().await {
        finish(
            command,
            Err(RequestError::BeforeDispatch(BeforeDispatch::Closed(
                reason.clone(),
            ))),
        );
    }
    while let Some(command) = commands.recv().await {
        finish(
            command,
            Err(RequestError::BeforeDispatch(BeforeDispatch::Closed(
                reason.clone(),
            ))),
        );
    }
    // Bounded close handshake followed by dropping both halves, including when
    // the peer refuses Close. Joining this task establishes actual socket drop.
    let _ = timeout(shutdown_timeout, sink.close()).await;
    drop(stream);
    drop(sink);
    // Close the physical socket first, then join every admitted formatter.
    // Shutdown seals admission under its short mutex; none spans this await.
    projections.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_exhaust_without_wrap_or_reuse() {
        let counter = AtomicU64::new(u64::MAX - 1);
        assert_eq!(advance(&counter).unwrap().get(), u64::MAX);
        assert_eq!(advance(&counter), None);
        assert_eq!(advance(&counter), None);
        assert_eq!(counter.load(Ordering::Acquire), u64::MAX);
    }

    #[test]
    fn exhausted_event_cursor_refuses_publication_without_reset_or_permit_loss() {
        let (sender, mut receiver) = mpsc::channel(1);
        let cursor = Arc::new(AtomicU64::new(u64::MAX));
        let publisher = Publisher {
            token: ConnectionToken::allocate().unwrap(),
            sender,
            items: Arc::new(Semaphore::new(1)),
            bytes: Arc::new(Semaphore::new(1024)),
            cursor: cursor.clone(),
        };
        for _ in 0..2 {
            assert_eq!(
                publisher.publish(
                    json!({"eventType":"RecordStateChanged","eventIntent":64}),
                    80
                ),
                Err(Invalidation::IdentityExhausted)
            );
            assert_eq!(cursor.load(Ordering::Acquire), u64::MAX);
            assert_eq!(publisher.items.available_permits(), 1);
            assert_eq!(publisher.bytes.available_permits(), 1024);
            assert!(matches!(
                receiver.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
        }
    }
    // Test-only worker gate is absent from production builds. These controls
    // prove socket/Stop progress and actual joining, rather than timing a race.
    async fn projection_fixture() -> (
        String,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let (close, closed) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
            socket
                .send(Message::Text(
                    json!({"op":0,"d":{"rpcVersion":1,"obsWebSocketVersion":"5.7.0"}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            let _ = socket.next().await.unwrap().unwrap();
            socket
                .send(Message::Text(
                    json!({"op":2,"d":{"negotiatedRpcVersion":1}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            let mut closed = closed;
            loop {
                tokio::select! {
                    _=&mut closed=>{let _=socket.close(None).await;break;},
                    next=socket.next()=>match next {
                        Some(Ok(Message::Text(text)))=>{let frame:Value=serde_json::from_str(&text).unwrap();let data=if frame["d"]["requestType"]=="GetSceneItemTransform"{json!({"sceneItemTransform":{"z":3,"a":1}})}else{assert_eq!(frame["d"]["requestType"],"StopRecord");json!({"outputPath":"fixture"})};socket.send(Message::Text(json!({"op":7,"d":{"requestId":frame["d"]["requestId"],"requestType":frame["d"]["requestType"],"requestStatus":{"result":true,"code":100},"responseData":data}}).to_string().into())).await.unwrap();},
                        Some(Ok(Message::Close(_)))|None|Some(Err(_))=>{let _=socket.flush().await;break;},
                        _=>{},
                    }
                }
            }
        });
        (url, close, peer)
    }
    fn projection_selectors() -> SceneItemSelectors {
        SceneItemSelectors {
            canvas_uuid: None,
            scene_name: Some("private-scene".into()),
            scene_uuid: None,
            scene_item_id: 2,
        }
    }
    async fn projection_started(gate: &crate::ordered_transform::TestGate) {
        timeout(Duration::from_secs(2), async {
            while !gate.started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn held_projection_does_not_block_stop_or_socket_close_and_shutdown_joins() {
        let (url, _close, peer) = projection_fixture().await;
        let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let handle = owner.handle();
        let gate = handle.projections.hold_next();
        let mut read = Box::pin(handle.scene_item_transform_checked(
            owner.token(),
            projection_selectors(),
            Duration::from_secs(5),
            || Ok(()),
        ));
        tokio::select! { result=&mut read=>panic!("held formatter unexpectedly completed: {result:?}"), _=projection_started(&gate)=>{} }
        let stop = handle.request(
            owner.token(),
            "StopRecord",
            json!({}),
            Duration::from_secs(2),
        );
        timeout(Duration::from_secs(2), stop)
            .await
            .unwrap()
            .unwrap();
        drop(read);
        let mut shutdown = Box::pin(owner.shutdown());
        assert!(futures_util::poll!(shutdown.as_mut()).is_pending());
        timeout(Duration::from_secs(2), peer)
            .await
            .unwrap()
            .unwrap();
        assert!(
            futures_util::poll!(shutdown.as_mut()).is_pending(),
            "shutdown discarded held formatter"
        );
        gate.release();
        timeout(Duration::from_secs(2), shutdown)
            .await
            .unwrap()
            .unwrap();
    }
    #[tokio::test]
    async fn peer_eof_with_dropped_projection_frontend_keeps_formatter_owned_until_join() {
        let (url, close, peer) = projection_fixture().await;
        let (owner, _events) = ObsSession::connect(&url, None, SessionOptions::default())
            .await
            .unwrap();
        let handle = owner.handle();
        let gate = handle.projections.hold_next();
        let mut read = Box::pin(handle.scene_item_transform_checked(
            owner.token(),
            projection_selectors(),
            Duration::from_secs(5),
            || Ok(()),
        ));
        tokio::select! { result=&mut read=>panic!("held formatter unexpectedly completed: {result:?}"), _=projection_started(&gate)=>{} }
        drop(read);
        close.send(()).unwrap();
        timeout(Duration::from_secs(2), peer)
            .await
            .unwrap()
            .unwrap();
        let mut shutdown = Box::pin(owner.shutdown());
        assert!(futures_util::poll!(shutdown.as_mut()).is_pending());
        gate.release();
        timeout(Duration::from_secs(2), shutdown)
            .await
            .unwrap()
            .unwrap();
    }
    #[tokio::test]
    async fn deferred_first_valid_ack_cannot_be_overwritten_and_wrong_types_still_fail() {
        // Deterministic deferred-response state boundary. Actual TCP flush
        // backpressure is deliberately not manufactured or claimed here.
        let token = ConnectionToken::allocate().unwrap();
        let items = Arc::new(Semaphore::new(1));
        let bytes = Arc::new(Semaphore::new(1));
        let (send, receive) = oneshot::channel();
        let command = Command {
            expected: token,
            kind: CommandKind::ReservedControl(StopControl::StopRecord),
            request_id: "controlled:1".into(),
            wire: String::new(),
            deadline: Instant::now() + Duration::from_secs(2),
            state: Arc::new(AtomicU8::new(DISPATCHING)),
            guard: None,
            reply: send,
            lease: RequestLease {
                _item: items.clone().try_acquire_owned().unwrap(),
                _bytes: bytes.clone().try_acquire_owned().unwrap(),
            },
        };
        let (sender, _events) = mpsc::channel(1);
        let publisher = Publisher {
            token,
            sender,
            items: Arc::new(Semaphore::new(1)),
            bytes: Arc::new(Semaphore::new(1024)),
            cursor: Arc::new(AtomicU64::new(0)),
        };
        let projections = ProjectionOwner::new(1);
        let mut first = Some(command);
        let mut other = None;
        let mut deferred = DeferredReply::default();
        let packet = |kind: &str, success: bool, value: &str| {
            Message::Text(json!({"op":7,"d":{"requestId":"controlled:1","requestType":kind,"requestStatus":{"result":success,"code":if success{100}else{602}},"responseData":{"accepted":value}}}).to_string().into())
        };
        for (success, value) in [(true, "first"), (false, "second")] {
            let frame =
                incoming(Some(Ok(packet("StopRecord", success, value))), &publisher).unwrap();
            if let Some(result) =
                route_response(frame, &mut first, &mut other, true, &projections).unwrap()
            {
                deferred.observe(result);
            }
        }
        let bad = incoming(
            Some(Ok(packet("GetRecordStatus", true, "spoof"))),
            &publisher,
        )
        .unwrap();
        assert!(matches!(
            route_response(bad, &mut first, &mut other, true, &projections),
            Err(Invalidation::ResponseIdentityMismatch)
        ));
        finish(first.take().unwrap(), deferred.0.unwrap());
        let reply = receive.await.unwrap();
        match &reply.result {
            Ok(ReplyData::Single(value)) => assert_eq!(value["accepted"], "first"),
            _ => panic!("known first ACK was replaced"),
        };
        assert_eq!(items.available_permits(), 0);
        drop(reply);
        assert_eq!(items.available_permits(), 1);
        assert_eq!(bytes.available_permits(), 1);
        projections.shutdown().await;
    }
}
