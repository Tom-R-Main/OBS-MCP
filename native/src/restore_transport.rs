// SPDX-License-Identifier: GPL-2.0-only
//! Closed retained manual-Restore batch custody. No public route, socket,
//! observer task, recording-impact ticket, file authority or Stop right exists
//! here. The composing Service must retain this owner and finish its one pending
//! operation before graceful shutdown. Cancelling a finish await leaves that
//! exact observer owned; dropping the owner is emergency invalidation, not join.
use crate::{
    control::{Disposition, GateRequest},
    custody::{Clock, SharedControl},
    operation_store::{OperationStore, RestoreCounts, RestorePhase, RestoreReceipt, StoreError},
    session::{
        BatchOptions, BatchRequest, BatchResult, BeforeDispatch, ConnectionToken, Execution,
        RequestError, SessionHandle,
    },
    snapshots::Mutation,
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

/// Only Source's fixed Restore callback identity and validated arguments enter
/// policy evaluation. No caller-supplied tool name or raw request descriptor.
pub struct Policy {
    pub caller: String,
    pub snapshot_id: Option<String>,
}
impl Policy {
    fn evaluate(&self, control: &SharedControl, clock: &Clock) -> Result<Disposition, ()> {
        let arguments = match &self.snapshot_id {
            Some(id) => json!({"snapshotId":id,"dryRun":false}),
            None => json!({"dryRun":false}),
        };
        Ok(control.lock().map_err(|_| ())?.evaluate(
            GateRequest {
                tool_name: "obs-restore",
                arguments: &arguments,
                read_only_hint: false,
                caller: &self.caller,
                confirmation: Default::default(),
            },
            clock().epoch_ms,
        ))
    }
}
/// An optional cached capability list remains tied to its exact physical owner.
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
/// A nonempty closed plan, never an external JSON list. Mutations cannot encode
/// lifecycle, profile, raw, vendor or output-control requests.
/// ```compile_fail
/// use obs_mcp_native::snapshots::Mutation;
/// let raw = serde_json::json!({"requestType":"StopRecord"});
/// let forbidden: Vec<Mutation> = vec![raw];
/// ```
pub struct Request {
    pub operation_id: String,
    pub mutations: Vec<Mutation>,
    pub policy: Policy,
    pub capabilities: Option<Capabilities>,
    pub cancellation: Cancellation,
    pub admitted_at: Instant,
    pub timeout: Duration,
}
#[derive(Debug)]
pub enum BeginError {
    Busy,
    Binding,
    Empty,
    InvalidMutation,
    Deadline,
    Cancelled,
    Unsupported(&'static str),
    Policy(Disposition),
    PolicyUnavailable,
    Receipt(StoreError),
    Uncertain,
}
#[derive(Debug)]
pub enum FinishError {
    NoPending,
}
/// A complete result vector includes known individual failures. `transport` is
/// independent of receipt-publication failure and never claims field state.
pub struct Outcome {
    pub transport: Result<Vec<BatchResult>, RequestError>,
    pub receipt: RestoreReceipt,
    pub receipt_error: Option<StoreError>,
    pub policy_refusal: Option<Disposition>,
}
impl fmt::Debug for Outcome {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("RestoreBatchOutcome")
            .field("phase", &self.receipt.phase())
            .field("receipt_error", &self.receipt_error)
            .finish_non_exhaustive()
    }
}
type Observer = Pin<Box<dyn Future<Output = Result<Vec<BatchResult>, RequestError>> + Send>>;
struct Pending {
    receipt: RestoreReceipt,
    observer: Observer,
    refusal: Arc<Mutex<Option<Disposition>>>,
}
pub struct RestoreBatchOwner {
    store: OperationStore,
    session: SessionHandle,
    control: SharedControl,
    clock: Clock,
    pending: Option<Pending>,
}
impl fmt::Debug for RestoreBatchOwner {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("RestoreBatchOwner")
            .field("pending", &self.pending.is_some())
            .finish_non_exhaustive()
    }
}
fn method(mutation: &Mutation) -> &'static str {
    match mutation {
        Mutation::InputSettings { .. } => "SetInputSettings",
        Mutation::InputMute { .. } => "SetInputMute",
        Mutation::InputVolume { .. } => "SetInputVolume",
        Mutation::InputAudioTracks { .. } => "SetInputAudioTracks",
        Mutation::ItemTransform { .. } => "SetSceneItemTransform",
        Mutation::ItemEnabled { .. } => "SetSceneItemEnabled",
        Mutation::ItemLocked { .. } => "SetSceneItemLocked",
        Mutation::ItemIndex { .. } => "SetSceneItemIndex",
    }
}
fn valid(mutation: &Mutation) -> bool {
    let item = |v: f64| v.is_finite();
    match mutation {
        Mutation::InputVolume { volume, .. } => volume.is_finite(),
        Mutation::ItemTransform { item_id, .. }
        | Mutation::ItemEnabled { item_id, .. }
        | Mutation::ItemLocked { item_id, .. }
        | Mutation::ItemIndex { item_id, .. } => item(*item_id),
        _ => true,
    }
}
impl RestoreBatchOwner {
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
                .is_none_or(|f| f.is_restore_batch() != Ok(true))
        {
            return Err(BeginError::Binding);
        }
        Ok(Self {
            store,
            session,
            control,
            clock,
            pending: None,
        })
    }
    pub fn has_pending(&self) -> bool {
        self.pending.is_some()
    }
    /// Intent and Dispatching publication precede physical queue admission.
    /// Dispatching means admission might follow; it never asserts a send.
    pub fn begin(&mut self, request: Request) -> Result<(), BeginError> {
        if self.pending.is_some() {
            return Err(BeginError::Busy);
        }
        let fence = self.store.dispatch_fence().ok_or(BeginError::Binding)?;
        if fence.is_restore_batch() != Ok(true) || self.session.invalidation().is_some() {
            return Err(BeginError::Binding);
        }
        if fence.admission().map_or(true, |a| a.blocking) {
            return Err(BeginError::Uncertain);
        }
        let deadline = request
            .admitted_at
            .checked_add(request.timeout)
            .filter(|_| !request.timeout.is_zero() && request.admitted_at <= Instant::now())
            .ok_or(BeginError::Deadline)?;
        if Instant::now() >= deadline {
            return Err(BeginError::Deadline);
        }
        match request.policy.evaluate(&self.control, &self.clock) {
            Ok(Disposition::Allow) => {}
            Ok(no) => return Err(BeginError::Policy(no)),
            Err(()) => return Err(BeginError::PolicyUnavailable),
        }
        if request.cancellation.is_cancelled() {
            return Err(BeginError::Cancelled);
        }
        if request.mutations.is_empty() {
            return Err(BeginError::Empty);
        }
        if request.mutations.iter().any(|m| !valid(m)) {
            return Err(BeginError::InvalidMutation);
        }
        if let Some(capabilities) = &request.capabilities {
            if capabilities.connection != self.session.token() {
                return Err(BeginError::Binding);
            }
            for mutation in &request.mutations {
                let name = method(mutation);
                if !capabilities.requests.contains(name) {
                    return Err(BeginError::Unsupported(name));
                }
            }
        }
        let count =
            u64::try_from(request.mutations.len()).map_err(|_| BeginError::InvalidMutation)?;
        let prepared = self
            .store
            .prepare_restore(
                &request.operation_id,
                self.session.token().generation(),
                count,
                &(self.clock)().utc,
            )
            .map_err(|e| {
                self.store.read_view().mark_uncertain();
                BeginError::Receipt(e)
            })?;
        let receipt = self
            .store
            .update_restore(
                &prepared,
                RestorePhase::Dispatching,
                None,
                &(self.clock)().utc,
            )
            .map_err(|e| {
                self.store.read_view().mark_uncertain();
                BeginError::Receipt(e)
            })?;
        let session = self.session.clone();
        let expected = session.token();
        let control = self.control.clone();
        let clock = self.clock.clone();
        let refusal = Arc::new(Mutex::new(None));
        let observed = refusal.clone();
        let observer = Box::pin(async move {
            let requests = request
                .mutations
                .into_iter()
                .map(|mutation| {
                    let mut body = mutation.request();
                    let data = body
                        .as_object_mut()
                        .expect("closed mutation object")
                        .remove("requestData")
                        .expect("closed mutation data");
                    BatchRequest {
                        request_type: method(&mutation).into(),
                        request_data: Some(data),
                    }
                })
                .collect();
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .filter(|d| !d.is_zero())
                .ok_or(RequestError::BeforeDispatch(BeforeDispatch::Deadline))?;
            session
                .batch_checked(
                    expected,
                    requests,
                    BatchOptions {
                        execution: Execution::SerialRealtime,
                        halt_on_failure: false,
                        timeout: Some(remaining),
                    },
                    move || {
                        match request.policy.evaluate(&control, &clock) {
                            Ok(Disposition::Allow) => {}
                            Ok(no) => {
                                if let Ok(mut saved) = observed.lock() {
                                    *saved = Some(no);
                                }
                                return Err("Control policy changed before Restore dispatch".into());
                            }
                            Err(()) => return Err("Restore control policy unavailable".into()),
                        }
                        if request.cancellation.is_cancelled() || Instant::now() >= deadline {
                            return Err(
                                "Restore cancelled or original deadline expired before dispatch"
                                    .into(),
                            );
                        }
                        if !fence.permits(&request.operation_id).unwrap_or(false) {
                            return Err("Restore receipt custody changed before dispatch".into());
                        }
                        Ok(())
                    },
                )
                .await
        });
        self.pending = Some(Pending {
            receipt,
            observer,
            refusal,
        });
        Ok(())
    }
    /// A cancelled await retains the same pinned observer and durable intent.
    /// After its terminal response, publication has no await/cancellation gap.
    pub async fn finish(&mut self) -> Result<Outcome, FinishError> {
        let pending = self.pending.as_mut().ok_or(FinishError::NoPending)?;
        let transport = pending.observer.as_mut().await;
        let pending = self.pending.take().expect("owned observer settled");
        let (phase, counts) = match &transport {
            Ok(rows) => {
                let successful = rows.iter().filter(|r| r.ok).count() as u64;
                (
                    RestorePhase::Acknowledged,
                    Some(RestoreCounts {
                        successful,
                        failed: rows.len() as u64 - successful,
                    }),
                )
            }
            Err(RequestError::BeforeDispatch(_)) => (RestorePhase::NotDispatched, None),
            Err(_) => (RestorePhase::Unknown, None),
        };
        let (receipt, receipt_error) =
            match self
                .store
                .update_restore(&pending.receipt, phase, counts, &(self.clock)().utc)
            {
                Ok(receipt) => (receipt, None),
                Err(error) => {
                    self.store.read_view().mark_uncertain();
                    (pending.receipt, Some(error))
                }
            };
        let policy_refusal = pending
            .refusal
            .lock()
            .ok()
            .and_then(|mut value| value.take());
        Ok(Outcome {
            transport,
            receipt,
            receipt_error,
            policy_refusal,
        })
    }
    /// The composing actor may reclaim only after finishing its pending effect.
    pub fn into_store(self) -> Result<OperationStore, Box<Self>> {
        if self.pending.is_some() {
            Err(Box::new(self))
        } else {
            Ok(self.store)
        }
    }
}
