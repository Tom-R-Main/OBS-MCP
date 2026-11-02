// SPDX-License-Identifier: GPL-2.0-only
//! Trusted internal single-RPC custody. This is not a raw MCP mutation API.
//!
//! The workflow owner must first validate schemas, preflight/stage state, tool
//! registration and recording ownership. A reply establishes
//! acceptance only. This coordinator does not infer observed or rendered results.
//! One coordinator owns the sole receipt writer and one pending observer future.
//! The workflow owner must reconcile prior uncertainty before choosing another
//! production change; a new operation ID is not evidence that replay is safe.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use serde_json::Value;
use tokio::time::Instant;

use crate::{
    control::{ConfirmationContext, ControlDomain, Disposition, GateRequest},
    operation_store::{
        OperationKind, OperationReceipt, OperationStore, OperationUpdate, Phase, PrepareDetails,
        StoreError,
    },
    session::{BeforeDispatch, ConnectionToken, Invalidation, RequestError, SessionHandle},
};

/// The trusted host supplies a consistent epoch/UTC pair. The store validates
/// UTC before any queue admission and on every later publication. No clock or
/// environment is read implicitly by this module.
#[derive(Clone, Debug)]
pub struct ClockSnapshot {
    pub epoch_ms: u64,
    pub utc: String,
}

pub type Clock = Arc<dyn Fn() -> ClockSnapshot + Send + Sync>;
pub type SharedControl = Arc<Mutex<ControlDomain>>;

#[derive(Default)]
pub struct Confirmation {
    pub enabled: bool,
    pub can_elicit: bool,
    pub input_responses: Option<Value>,
}

pub struct PolicyCall {
    pub tool_name: String,
    pub arguments: Value,
    /// The trusted registration annotation, never client-supplied metadata.
    pub read_only_hint: bool,
    pub caller: String,
    pub confirmation: Confirmation,
}

impl PolicyCall {
    fn evaluate(&self, control: &mut ControlDomain, now_ms: u64) -> Disposition {
        control.evaluate(
            GateRequest {
                tool_name: &self.tool_name,
                arguments: &self.arguments,
                read_only_hint: self.read_only_hint,
                caller: &self.caller,
                confirmation: ConfirmationContext {
                    enabled: self.confirmation.enabled,
                    can_elicit: self.confirmation.can_elicit,
                    input_responses: self.confirmation.input_responses.as_ref(),
                },
            },
            now_ms,
        )
    }
}

#[derive(Default)]
pub struct Details {
    pub scene_collection: Option<String>,
    pub scene_uuid: Option<String>,
    pub stage_id: Option<String>,
    pub take_id: Option<String>,
}

/// All fields come from a validated workflow, not arbitrary external JSON.
/// The owner binds kind/details to the same observed subject and
/// binds this RPC to the public tool's already-validated semantics.
pub struct TrackedRequest {
    pub operation_id: String,
    pub kind: OperationKind,
    pub expected: ConnectionToken,
    pub policy: PolicyCall,
    pub details: Details,
    pub request_type: String,
    pub request_data: Value,
    /// Dispatch/RPC budget includes initial policy, intent publication, caller
    /// delay and queue wait. Final custody publication still completes after a
    /// terminal RPC outcome; synchronous filesystem work cannot be preempted.
    pub timeout: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntentStep {
    Prepared,
    Dispatching,
}

#[derive(Debug)]
pub enum BeginError {
    Busy,
    InvalidDeadline,
    StaleConnection,
    PolicyUnavailable,
    Policy(Disposition),
    /// Nothing was queued. Publication may be visible for DurabilityUncertain;
    /// inspect this operation ID rather than preparing it again.
    Receipt {
        operation_id: String,
        step: IntentStep,
        error: StoreError,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransportDisposition {
    Accepted,
    NotDispatched(BeforeDispatch),
    Rejected { code: u64 },
    Unknown(Invalidation),
}

#[derive(Debug)]
pub enum FinishError {
    NoPending,
    /// Do not report success or retry. The visible file may differ from the
    /// last confirmed durable receipt if publication itself was uncertain.
    CustodyUncertain {
        last_confirmed_receipt: Box<OperationReceipt>,
        transport: TransportDisposition,
        error: StoreError,
    },
}

#[derive(Debug)]
pub enum Outcome {
    Accepted {
        receipt: OperationReceipt,
        response: Value,
    },
    NotDispatched {
        receipt: OperationReceipt,
        reason: BeforeDispatch,
        policy: Option<Disposition>,
    },
    Rejected {
        receipt: OperationReceipt,
        code: u64,
    },
    Unknown {
        receipt: OperationReceipt,
        reason: Invalidation,
    },
}

type Observer = Pin<Box<dyn Future<Output = Result<Value, RequestError>> + Send>>;
struct Pending {
    receipt: OperationReceipt,
    observer: Observer,
    policy_refusal: Arc<Mutex<Option<Disposition>>>,
}

pub struct CustodyCoordinator {
    store: OperationStore,
    session: SessionHandle,
    control: SharedControl,
    clock: Clock,
    pending: Option<Pending>,
}

impl CustodyCoordinator {
    /// Reject a receipt writer bound to a different endpoint before any intent.
    pub fn new(
        store: OperationStore,
        session: SessionHandle,
        control: SharedControl,
        clock: Clock,
    ) -> Result<Self, StoreError> {
        if store.endpoint_hash() != session.endpoint_hash() {
            return Err(StoreError::EndpointMismatch);
        }
        Ok(Self {
            store,
            session,
            control,
            clock,
            pending: None,
        })
    }

    /// Transfer the sole writer only after its owned observer has settled.
    /// Refusal returns the entire coordinator, retaining pending work.
    pub fn into_store(self) -> Result<OperationStore, Box<Self>> {
        if self.pending.is_some() {
            Err(Box::new(self))
        } else {
            Ok(self.store)
        }
    }

    pub fn has_pending(&self) -> bool {
        self.pending.is_some()
    }

    pub fn pending_receipt(&self) -> Option<&OperationReceipt> {
        self.pending.as_ref().map(|pending| &pending.receipt)
    }

    pub fn read(&self, operation_id: &str) -> Result<OperationReceipt, StoreError> {
        self.store.read(operation_id)
    }

    pub fn summary(&self) -> Value {
        self.store.summary()
    }

    /// Initial refusal creates no receipt. Both intent states are durable before
    /// the request can enter the session queue. A second begin cannot detach or
    /// replace the first observer. There is no retry path in this API.
    pub fn begin(&mut self, request: TrackedRequest) -> Result<(), BeginError> {
        if self.pending.is_some() {
            return Err(BeginError::Busy);
        }
        let deadline = Instant::now()
            .checked_add(request.timeout)
            .filter(|_| !request.timeout.is_zero())
            .ok_or(BeginError::InvalidDeadline)?;
        let snapshot = (self.clock)();
        {
            let mut control = self
                .control
                .lock()
                .map_err(|_| BeginError::PolicyUnavailable)?;
            let disposition = request.policy.evaluate(&mut control, snapshot.epoch_ms);
            if disposition != Disposition::Allow {
                return Err(BeginError::Policy(disposition));
            }
        }
        if request.expected != self.session.token() {
            return Err(BeginError::StaleConnection);
        }
        let prepared = self
            .store
            .prepare(
                &request.operation_id,
                request.kind,
                self.session.token().generation(),
                &snapshot.utc,
                PrepareDetails {
                    scene_collection: request.details.scene_collection.as_deref(),
                    scene_uuid: request.details.scene_uuid.as_deref(),
                    stage_id: request.details.stage_id.as_deref(),
                    take_id: request.details.take_id.as_deref(),
                },
            )
            .map_err(|error| BeginError::Receipt {
                operation_id: request.operation_id.clone(),
                step: IntentStep::Prepared,
                error,
            })?;
        let mut intent = OperationUpdate::new(Phase::Dispatching);
        intent.request_dispatched = Some(None);
        let receipt = self
            .store
            .update(&prepared, intent, &snapshot.utc)
            .map_err(|error| BeginError::Receipt {
                operation_id: request.operation_id,
                step: IntentStep::Dispatching,
                error,
            })?;
        let receipt_fence = self.store.dispatch_fence();
        let own_receipt = receipt.operation_id().to_owned();
        let session = self.session.clone();
        let control = self.control.clone();
        let clock = self.clock.clone();
        let policy_refusal = Arc::new(Mutex::new(None));
        let observed_refusal = policy_refusal.clone();
        let observer = Box::pin(async move {
            // A delayed first poll consumes the original whole-operation budget.
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .filter(|remaining| !remaining.is_zero())
                .ok_or(RequestError::BeforeDispatch(BeforeDispatch::Deadline))?;
            session.request_checked(request.expected, &request.request_type, request.request_data, remaining, move || {
                if Instant::now() >= deadline {
                    return Err("Custody operation deadline expired before dispatch".to_owned());
                }
                let snapshot = clock();
                let disposition = {
                    let mut control = control.lock().map_err(|_| "Control policy is unavailable".to_owned())?;
                    request.policy.evaluate(&mut control, snapshot.epoch_ms)
                };
                if disposition == Disposition::Allow {
                    if receipt_fence.as_ref().is_some_and(|fence|!fence.permits(&own_receipt).unwrap_or(false)){return Err("Receipt custody changed before dispatch".into());}
                    return Ok(());
                }
                if let Ok(mut saved) = observed_refusal.lock() {
                    *saved = Some(disposition);
                }
                Err("Control policy changed before dispatch; inspect the returned disposition".to_owned())
            }).await
        });
        self.pending = Some(Pending {
            receipt,
            observer,
            policy_refusal,
        });
        Ok(())
    }

    /// Cancelling this await leaves the same observer owned here. Polling again
    /// resumes it; it does not admit another request. Dropping the coordinator
    /// drops the observer: a queued request is abandoned, an in-flight actor is
    /// invalidated, and the last durable dispatching receipt remains uncertain.
    pub async fn finish(&mut self) -> Result<Outcome, FinishError> {
        let pending = self.pending.as_mut().ok_or(FinishError::NoPending)?;
        let response = pending.observer.as_mut().await;
        // No await follows: cancellation cannot interrupt the durable result update.
        let pending = self
            .pending
            .take()
            .expect("observer was owned throughout await");
        let (transport, mut update) = match &response {
            Ok(_) => (
                TransportDisposition::Accepted,
                OperationUpdate::new(Phase::Accepted),
            ),
            Err(RequestError::BeforeDispatch(reason)) => (
                TransportDisposition::NotDispatched(reason.clone()),
                OperationUpdate::new(Phase::NotDispatched),
            ),
            Err(RequestError::Rejected { code } | RequestError::RejectedDetailed { code, .. }) => (
                TransportDisposition::Rejected { code: *code },
                OperationUpdate::new(Phase::Unknown),
            ),
            Err(RequestError::Unknown(reason)) => (
                TransportDisposition::Unknown(reason.clone()),
                OperationUpdate::new(Phase::Unknown),
            ),
        };
        match transport {
            TransportDisposition::Accepted => {
                update.request_dispatched = Some(Some(true));
                update.request_accepted = Some(Some(true));
            }
            TransportDisposition::NotDispatched(_) => {
                update.request_dispatched = Some(Some(false));
                update.request_accepted = Some(None);
            }
            TransportDisposition::Rejected { .. } => {
                update.request_dispatched = Some(Some(true));
                update.request_accepted = Some(Some(false));
            }
            // A send attempt is not proof the frame was delivered in full.
            TransportDisposition::Unknown(_) => {
                update.request_dispatched = Some(None);
                update.request_accepted = Some(None);
            }
        }
        let snapshot = (self.clock)();
        let receipt = self
            .store
            .update(&pending.receipt, update, &snapshot.utc)
            .map_err(|error| FinishError::CustodyUncertain {
                last_confirmed_receipt: Box::new(pending.receipt),
                transport: transport.clone(),
                error,
            })?;
        Ok(match response {
            Ok(response) => Outcome::Accepted { receipt, response },
            Err(RequestError::BeforeDispatch(reason)) => Outcome::NotDispatched {
                receipt,
                reason,
                policy: pending
                    .policy_refusal
                    .lock()
                    .ok()
                    .and_then(|mut saved| saved.take()),
            },
            Err(RequestError::Rejected { code } | RequestError::RejectedDetailed { code, .. }) => {
                Outcome::Rejected { receipt, code }
            }
            Err(RequestError::Unknown(reason)) => Outcome::Unknown { receipt, reason },
        })
    }
}
