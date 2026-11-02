// SPDX-License-Identifier: GPL-2.0-only
//! Same-physical-session transport for restricted demo preparation/recovery.
//! The service owns this adapter and its admitted future until settled. It
//! creates no socket, observer task, filesystem writer, or retry authority.
use crate::{
    control::{CallerContext, Disposition, GateRequest, caller_name},
    custody::{Clock, SharedControl},
    demo::{
        Delivery, DemoAdapter, DemoMutation, DispatchFence, EvidenceStamp, FitKey, HostClass,
        MutationFailure, MutationFuture,
    },
    observation::{ObservationError, ReadAdapter, ReadFuture, ReadRequest},
    session::{
        ALL_EVENTS, BatchOptions, BatchRequest, BeforeDispatch, RequestError, SessionHandle,
    },
    session_observation::{ObservationFence, SessionReader},
    snapshots::{BatchReadResult, ReadCall, SnapshotError, SnapshotFuture, SnapshotReader},
    stage_transport::Cancellation,
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::HashSet, fmt, io, net::IpAddr, sync::Arc};
use tokio::time::Instant;

pub const REQUIRED_DEMO_EVENTS: u64 = ALL_EVENTS | (1 << 19);

#[derive(Clone, Copy, Debug)]
pub enum DemoAction {
    Prepare,
    Recover,
}
impl DemoAction {
    fn tool_name(self) -> &'static str {
        match self {
            Self::Prepare => "obs-prepare-demo",
            Self::Recover => "obs-recover-demo",
        }
    }
}
/// Resolved trusted registration/caller identity, never arbitrary request names.
pub struct DemoPolicy {
    action: DemoAction,
    caller: String,
}
impl DemoPolicy {
    pub fn resolve(action: DemoAction, context: CallerContext<'_>) -> Self {
        Self {
            action,
            caller: caller_name(context),
        }
    }
    fn evaluate(&self, control: &SharedControl, clock: &Clock) -> Result<(), String> {
        let mut domain = control
            .lock()
            .map_err(|_| "Demo control policy unavailable")?;
        let decision = domain.evaluate(
            GateRequest {
                tool_name: self.action.tool_name(),
                arguments: &json!({"apply":true}),
                read_only_hint: false,
                caller: &self.caller,
                confirmation: Default::default(),
            },
            clock().epoch_ms,
        );
        match decision {
            Disposition::Allow => Ok(()),
            Disposition::Refused { message, .. } => Err(message),
            _ => Err("Demo control policy refused dispatch".into()),
        }
    }
}
impl fmt::Debug for DemoPolicy {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("DemoPolicy")
            .field("action", &self.action)
            .finish_non_exhaustive()
    }
}

pub struct DemoTransportContext {
    pub handle: SessionHandle,
    pub reader: SessionReader,
    pub fence: ObservationFence,
    pub control: SharedControl,
    pub clock: Clock,
    pub cancellation: Cancellation,
}
#[derive(Clone, Copy, Debug)]
pub struct DemoBounds {
    pub max_batch: usize,
    pub max_evidence_bytes: usize,
}
impl Default for DemoBounds {
    fn default() -> Self {
        Self {
            max_batch: 256,
            max_evidence_bytes: 4 * 1024 * 1024,
        }
    }
}
/// Metadata-only diagnostics; explicit tool bodies receive `.message()`.
pub struct BindingError {
    reason: &'static str,
    message: String,
}
impl BindingError {
    fn new(reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            reason,
            message: message.into(),
        }
    }
    pub fn reason(&self) -> &'static str {
        self.reason
    }
    pub fn message(&self) -> &str {
        &self.message
    }
}
impl fmt::Debug for BindingError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("BindingError")
            .field("reason", &self.reason)
            .field("message_bytes", &self.message.len())
            .finish()
    }
}
impl fmt::Display for BindingError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(self.reason)
    }
}
impl std::error::Error for BindingError {}

pub struct BoundDemoAdapter {
    context: DemoTransportContext,
    policy: Arc<DemoPolicy>,
    requests: HashSet<String>,
    host: HostClass,
    platform: String,
    deadline: Instant,
    bounds: DemoBounds,
}
impl fmt::Debug for BoundDemoAdapter {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("BoundDemoAdapter")
            .field("connection", &self.context.handle.token())
            .field("bounds", &self.bounds)
            .finish_non_exhaustive()
    }
}
fn observed(error: RequestError, name: &str) -> ObservationError {
    match error.rejection_code() {
        Some(code) => ObservationError::rejected(
            code,
            "evidence_unavailable",
            error.obs_failure_message(name).expect("typed rejection"),
        ),
        None => ObservationError::new(
            "demo_invalidated",
            "Demo transport evidence unavailable; inspect OBS before retrying",
        ),
    }
}
fn invalidated() -> ObservationError {
    ObservationError::new(
        "demo_invalidated",
        "Demo connection or recipe evidence changed; inspect OBS before retrying",
    )
}
fn expired() -> ObservationError {
    ObservationError::new(
        "demo_deadline",
        "Restricted demo deadline elapsed; inspect state before retrying",
    )
}
fn remaining(deadline: Instant) -> Result<std::time::Duration, ObservationError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|duration| !duration.is_zero())
        .ok_or_else(expired)
}
fn valid_mutation(mutation: &DemoMutation) -> bool {
    match mutation {
        DemoMutation::Mute { input_name, .. } => !input_name.is_empty(),
        DemoMutation::Tracks { input_name, tracks } => {
            !input_name.is_empty()
                && tracks.len() == 6
                && (1..=6).all(|track| {
                    tracks
                        .get(&track.to_string())
                        .is_some_and(Value::is_boolean)
                })
        }
        DemoMutation::Fit {
            scene_name,
            item_id,
            key,
            value,
        } => {
            !scene_name.is_empty()
                && item_id.is_finite()
                && item_id.fract() == 0.0
                && if *key == FitKey::BoundsType {
                    value
                        .as_str()
                        .is_some_and(|value| value.starts_with("OBS_BOUNDS_"))
                } else {
                    value.as_f64().is_some_and(f64::is_finite)
                }
        }
    }
}
struct Count {
    left: usize,
}
impl io::Write for Count {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.left = self
            .left
            .checked_sub(data.len())
            .ok_or_else(|| io::Error::other("demo byte limit"))?;
        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn bounded(value: &impl Serialize, limit: usize) -> Result<(), ObservationError> {
    serde_json::to_writer(Count { left: limit }, value)
        .map_err(|_| ObservationError::new("demo_limit", "Demo evidence exceeds byte limit"))
}
impl BoundDemoAdapter {
    pub async fn bind(
        mut context: DemoTransportContext,
        endpoint: &str,
        policy: DemoPolicy,
        deadline: Instant,
        bounds: DemoBounds,
    ) -> Result<Self, BindingError> {
        let fail = || {
            BindingError::new(
                "demo_binding",
                "Demo transport does not match its physical observation session",
            )
        };
        if bounds.max_batch == 0
            || bounds.max_batch > 1024
            || bounds.max_evidence_bytes == 0
            || bounds.max_evidence_bytes > 16 * 1024 * 1024
        {
            return Err(BindingError::new(
                "demo_limit",
                "Demo transport bounds unavailable",
            ));
        }
        let token = context.handle.token();
        if !context.fence.full_profile()
            || context.fence.token() != token
            || context.reader.token() != token
            || context.fence.endpoint_hash() != context.handle.endpoint_hash()
            || context.reader.endpoint_hash() != context.handle.endpoint_hash()
            || format!("{:x}", Sha256::digest(endpoint.as_bytes()))
                != context.handle.endpoint_hash()
            || context.handle.event_subscriptions() & REQUIRED_DEMO_EVENTS != REQUIRED_DEMO_EVENTS
            || context.handle.invalidation().is_some()
            || context.cancellation.is_cancelled()
        {
            return Err(fail());
        }
        let uri: tokio_tungstenite::tungstenite::http::Uri =
            endpoint.parse().map_err(|_| fail())?;
        if uri.scheme_str() != Some("ws")
            || uri
                .authority()
                .is_none_or(|authority| authority.as_str().contains('@'))
        {
            return Err(fail());
        }
        let hostname = uri
            .host()
            .ok_or_else(fail)?
            .trim_matches(['[', ']'])
            .to_ascii_lowercase();
        let host = if hostname == "localhost"
            || hostname.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
        {
            HostClass::Loopback
        } else {
            HostClass::Other
        };
        // Closed demo recipe only supports source's exact hostname forms; no
        // broad IP loopback aliases or resolver-based locality inference.
        let host = if matches!(host, HostClass::Loopback)
            && !matches!(hostname.as_str(), "localhost" | "127.0.0.1" | "::1")
        {
            HostClass::Other
        } else {
            host
        };
        context
            .reader
            .synchronize(deadline)
            .await
            .map_err(|error| BindingError::new(error.reason, error.message))?;
        let version = context
            .handle
            .request(
                token,
                "GetVersion",
                Value::Null,
                remaining(deadline)
                    .map_err(|error| BindingError::new(error.reason, error.message))?,
            )
            .await;
        context
            .reader
            .synchronize(deadline)
            .await
            .map_err(|error| BindingError::new(error.reason, error.message))?;
        let version = version.map_err(|error| {
            let error = observed(error, "GetVersion");
            BindingError::new(error.reason, error.message)
        })?;
        bounded(&version, bounds.max_evidence_bytes)
            .map_err(|error| BindingError::new(error.reason, error.message))?;
        let list = version
            .get("availableRequests")
            .and_then(Value::as_array)
            .filter(|list| !list.is_empty() && list.len() <= 2048)
            .ok_or_else(fail)?;
        let mut requests = HashSet::new();
        for request in list {
            let name = request
                .as_str()
                .filter(|name| {
                    !name.is_empty()
                        && name.len() <= 128
                        && name.bytes().all(|byte| byte.is_ascii_alphanumeric())
                })
                .ok_or_else(fail)?;
            requests.insert(name.to_owned());
        }
        let platform = version
            .get("platform")
            .and_then(Value::as_str)
            .filter(|platform| matches!(*platform, "macos" | "linux"))
            .ok_or_else(|| {
                BindingError::new(
                    "demo_platform",
                    "Restricted demo requires actual OBS macOS or Linux platform metadata",
                )
            })?
            .to_owned();
        let this = Self {
            context,
            policy: Arc::new(policy),
            requests,
            host,
            platform,
            deadline,
            bounds,
        };
        this.check()
            .map_err(|error| BindingError::new(error.reason, error.message))?;
        Ok(this)
    }
    pub fn host(&self) -> HostClass {
        self.host
    }
    pub fn platform(&self) -> &str {
        &self.platform
    }
    fn check(&self) -> Result<(), ObservationError> {
        remaining(self.deadline)?;
        let state = self.context.reader.evidence();
        if self.context.cancellation.is_cancelled()
            || self.context.handle.invalidation().is_some()
            || !state.connected
            || state.cancelled
            || state.generation != self.context.handle.token().generation()
        {
            Err(invalidated())
        } else {
            Ok(())
        }
    }
    fn supports_name(&self, name: &str) -> Result<(), ObservationError> {
        if self.requests.contains(name) {
            Ok(())
        } else {
            Err(ObservationError::new(
                "capability_unavailable",
                format!("OBS WebSocket does not advertise support for request '{name}'"),
            ))
        }
    }
}
impl ReadAdapter for BoundDemoAdapter {
    fn supports(&self, request: ReadRequest) -> Option<bool> {
        Some(self.requests.contains(request.as_str()))
    }
    fn read<'a>(
        &'a mut self,
        request: ReadRequest,
        data: Value,
        deadline: Instant,
    ) -> ReadFuture<'a> {
        Box::pin(async move {
            self.check()?;
            self.supports_name(request.as_str())?;
            bounded(&data, self.bounds.max_evidence_bytes)?;
            let deadline = deadline.min(self.deadline);
            self.context.reader.synchronize(deadline).await?;
            let result = self.context.reader.read(request, data, deadline).await;
            self.context.reader.synchronize(deadline).await?;
            self.check()?;
            let value = result?;
            bounded(&value, self.bounds.max_evidence_bytes)?;
            Ok(value)
        })
    }
}
impl SnapshotReader for BoundDemoAdapter {
    fn batch<'a>(&'a mut self, calls: Vec<ReadCall>) -> SnapshotFuture<'a, Vec<BatchReadResult>> {
        Box::pin(async move {
            self.check()?;
            if calls.len() > self.bounds.max_batch {
                return Err(SnapshotError::Read("Demo batch exceeds item limit".into()));
            }
            if calls.is_empty() {
                return Ok(vec![]);
            }
            for call in &calls {
                self.supports_name(call.request.as_str())?;
                bounded(&call.data, self.bounds.max_evidence_bytes)?;
            }
            self.context.reader.synchronize(self.deadline).await?;
            let token = self.context.handle.token();
            let cancellation = self.context.cancellation.clone();
            let deadline = self.deadline;
            let result = self
                .context
                .handle
                .batch_checked(
                    token,
                    calls
                        .into_iter()
                        .map(|call| BatchRequest {
                            request_type: call.request.as_str().into(),
                            request_data: Some(call.data),
                        })
                        .collect(),
                    BatchOptions {
                        timeout: Some(remaining(deadline)?),
                        ..Default::default()
                    },
                    move || {
                        if cancellation.is_cancelled() || Instant::now() >= deadline {
                            Err("Demo batch permission expired".into())
                        } else {
                            Ok(())
                        }
                    },
                )
                .await;
            self.context.reader.synchronize(deadline).await?;
            self.check()?;
            let rows = result.map_err(|error| observed(error, "RequestBatch"))?;
            bounded(&rows, self.bounds.max_evidence_bytes)?;
            Ok(rows
                .into_iter()
                .map(|row| BatchReadResult {
                    ok: row.ok,
                    code: Some(row.code),
                    comment: row.comment,
                    response_data: row.response_data,
                })
                .collect())
        })
    }
}
impl DemoAdapter for BoundDemoAdapter {
    fn evidence(&self) -> EvidenceStamp {
        let state = self.context.reader.evidence();
        EvidenceStamp {
            generation: state.generation,
            revision: state.revision,
            connected: state.connected && self.context.handle.invalidation().is_none(),
            cancelled: state.cancelled
                || self.context.cancellation.is_cancelled()
                || Instant::now() >= self.deadline,
        }
    }
    fn mutate<'a>(
        &'a mut self,
        mutation: DemoMutation,
        expected: DispatchFence,
        deadline: Instant,
    ) -> MutationFuture<'a> {
        Box::pin(async move {
            let not_sent = |message: String| MutationFailure {
                delivery: Delivery::NotDispatched,
                message,
            };
            self.check().map_err(|error| not_sent(error.message))?;
            if !valid_mutation(&mutation) {
                return Err(not_sent("Restricted demo mutation is incomplete".into()));
            }
            if !matches!(self.host, HostClass::Loopback) {
                return Err(not_sent("V1 requires local OBS on loopback".into()));
            }
            let body = mutation.request();
            let name = body["requestType"].as_str().expect("closed mutation");
            let data = body["requestData"].clone();
            self.supports_name(name)
                .map_err(|error| not_sent(error.message))?;
            bounded(&data, self.bounds.max_evidence_bytes)
                .map_err(|error| not_sent(error.message))?;
            let deadline = deadline.min(self.deadline);
            self.context
                .reader
                .synchronize(deadline)
                .await
                .map_err(|error| not_sent(error.message))?;
            let duration = remaining(deadline).map_err(|error| not_sent(error.message))?;
            let (fence, cancellation, control, clock, policy) = (
                self.context.fence.clone(),
                self.context.cancellation.clone(),
                self.context.control.clone(),
                self.context.clock.clone(),
                self.policy.clone(),
            );
            let result = self
                .context
                .handle
                .request_checked(
                    self.context.handle.token(),
                    name,
                    data,
                    duration,
                    move || {
                        if cancellation.is_cancelled() || Instant::now() >= deadline {
                            return Err("Demo dispatch permission expired".into());
                        }
                        policy.evaluate(&control, &clock)?;
                        if Instant::now() >= deadline
                            || !fence.ready_at_revision(expected.generation, expected.revision)
                        {
                            return Err("Demo final evidence fence changed".into());
                        }
                        Ok(())
                    },
                )
                .await;
            let result = match result {
                Err(RequestError::BeforeDispatch(BeforeDispatch::GuardRefused(message))) => {
                    return Err(not_sent(message));
                }
                Err(RequestError::BeforeDispatch(_)) => {
                    return Err(not_sent(
                        "Demo request was not dispatched; inspect state before retrying".into(),
                    ));
                }
                other => other,
            };
            self.context
                .reader
                .synchronize(deadline)
                .await
                .map_err(|error| MutationFailure {
                    delivery: Delivery::Unknown,
                    message: error.message,
                })?;
            self.check().map_err(|error| MutationFailure {
                delivery: Delivery::Unknown,
                message: error.message,
            })?;
            result.map(|_| ()).map_err(|error| {
                let delivery = if error.rejection_code().is_some() {
                    Delivery::Rejected
                } else {
                    Delivery::Unknown
                };
                MutationFailure {
                    delivery,
                    message: observed(error, name).message,
                }
            })
        })
    }
}
