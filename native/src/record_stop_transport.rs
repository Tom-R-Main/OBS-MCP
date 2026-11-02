// SPDX-License-Identifier: GPL-2.0-only
//! Retained library execution for the deferred manual StopRecord callback.
//!
//! No tool, Service queue, frontend or observer is created. The eventual owner
//! must retain and join this exact execute future after cancellation/EOF and
//! retain the shared RecordImpactOwner through settlement. Dropping the future
//! is not joined qualification: its pending impact conservatively revokes and
//! durable Dispatching/orphan uncertainty remains a barrier; no replay occurs.
use crate::{
    control::{ConfirmationContext, Disposition, GateRequest},
    custody::{Clock, SharedControl},
    operation_store::{GenericKind, GenericPhase, GenericReceipt, OperationStore, StoreError},
    output_stop_result,
    record_impact::{ImpactError, ManualImpactAttempt, ManualRecordImpactPort},
    session::{BeforeDispatch, ConnectionToken, Invalidation, RequestError, SessionHandle},
    stage_transport::Cancellation,
    takes::Policy,
};
use serde_json::{Value, json};
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;

#[derive(Clone)]
pub struct BoundManualRecordStop {
    session: SessionHandle,
    impact: ManualRecordImpactPort,
}
impl fmt::Debug for BoundManualRecordStop {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("BoundManualRecordStop { .. }")
    }
}
/// Input storage/caller identity bounds belong to the composing owner. The
/// actual closed transport frame shares the public one-item/512-byte pool.
/// Cached support belongs to this exact physical Service generation. An absent
/// array/cache remains unknown, matching Source; it is not forced unsupported.
#[derive(Clone, Copy)]
pub struct StopRecordCapability {
    connection: ConnectionToken,
    supported: bool,
}
impl StopRecordCapability {
    pub fn from_version(session: &SessionHandle, version: &Value) -> Option<Self> {
        let requests = version.get("availableRequests")?.as_array()?;
        Some(Self {
            connection: session.token(),
            supported: requests.iter().any(|v| v.as_str() == Some("StopRecord")),
        })
    }
}
pub struct Request {
    pub operation_id: String,
    pub capabilities: Option<StopRecordCapability>,
    pub policy: Policy,
    pub control: SharedControl,
    pub clock: Clock,
    pub cancellation: Cancellation,
    pub admitted_at: Instant,
    pub timeout: Duration,
}
/// Transport knowledge is independent of callback and custody-publication
/// failure. Some(Ok(())) is always a known ACK, including formatting failures.
pub struct Outcome {
    pub result: Value,
    pub transport: Option<Result<(), RequestError>>,
    pub receipt: Option<GenericReceipt>,
    pub receipt_error: Option<StoreError>,
    pub impact_error: Option<ImpactError>,
    pub impact_started: bool,
}
impl fmt::Debug for Outcome {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("ManualRecordStopOutcome")
            .field("transport", &self.transport)
            .field(
                "receipt_phase",
                &self.receipt.as_ref().map(GenericReceipt::phase),
            )
            .field("receipt_error", &self.receipt_error)
            .field("impact_error", &self.impact_error)
            .field("impact_started", &self.impact_started)
            .finish_non_exhaustive()
    }
}
fn no_attempt(message: &str) -> Outcome {
    Outcome {
        result: output_stop_result::record_error(message),
        transport: None,
        receipt: None,
        receipt_error: None,
        impact_error: None,
        impact_started: false,
    }
}
fn policy(policy: &Policy, control: &SharedControl, clock: &Clock) -> Result<Disposition, ()> {
    Ok(control.lock().map_err(|_| ())?.evaluate(
        GateRequest {
            tool_name: "obs-stop-record",
            arguments: &json!({}),
            read_only_hint: false,
            caller: &policy.caller,
            confirmation: ConfirmationContext {
                enabled: policy.confirmation_enabled,
                can_elicit: policy.can_elicit,
                input_responses: policy.input_responses.as_ref(),
            },
        },
        clock().epoch_ms,
    ))
}
pub(crate) fn settle_impact(
    attempt: ManualImpactAttempt,
    response: &Result<Value, RequestError>,
) -> Result<(), ImpactError> {
    match response {
        Ok(_) => attempt.accepted(),
        Err(RequestError::BeforeDispatch(_)) => attempt.not_dispatched(),
        Err(RequestError::Rejected { .. } | RequestError::RejectedDetailed { .. }) => {
            attempt.rejected_unqualified()
        }
        Err(RequestError::Unknown(_)) => attempt.unknown(),
    }
}
impl BoundManualRecordStop {
    /// Bind a mandatory exact physical impact port before any receipt intent.
    /// This validates liveness, never allocates an attempt or recording right.
    pub fn bind(
        session: SessionHandle,
        impact: ManualRecordImpactPort,
    ) -> Result<Self, ImpactError> {
        impact.check_connection(session.token())?;
        Ok(Self { session, impact })
    }
    /// The caller must retain/join this future through actual Session terminal
    /// disposition. Cancellation is checked before send; once sent, the same
    /// operation settles under its original deadline with no replay/select-drop.
    pub async fn execute(self, mut store: OperationStore, request: Request) -> Outcome {
        let Request {
            operation_id,
            capabilities,
            policy: call_policy,
            control,
            clock,
            cancellation,
            admitted_at,
            timeout,
        } = request;
        if let Err(reason) = self.impact.check_connection(self.session.token()) {
            let mut out = no_attempt(
                "Recording-impact binding is unavailable; no OBS request was dispatched",
            );
            out.impact_error = Some(reason);
            return out;
        }
        let Some(deadline) = admitted_at
            .checked_add(timeout)
            .filter(|_| !timeout.is_zero())
        else {
            return no_attempt("Invalid original operation deadline");
        };
        let Some(fence) = store.dispatch_fence() else {
            return no_attempt("Manual StopRecord requires its dedicated broker scope");
        };
        if store.endpoint_hash() != self.session.endpoint_hash()
            || fence.is_manual_record_stop() != Ok(true)
        {
            return no_attempt("Manual StopRecord receipt binding is unavailable");
        }
        if fence.admission().map_or(true, |a| a.blocking) {
            return no_attempt("Unresolved receipt custody prevents dispatch");
        }
        let call_policy = Arc::new(call_policy);
        match policy(&call_policy, &control, &clock) {
            Ok(Disposition::Allow) => {}
            Ok(refusal) => {
                let mut out = no_attempt("Control policy refused dispatch");
                if let Some(result) = refusal.tool_result() {
                    out.result = result;
                }
                return out;
            }
            Err(()) => return no_attempt("Control policy is unavailable"),
        }
        if let Some(capability) = capabilities {
            if capability.connection != self.session.token() {
                return no_attempt("Cached StopRecord capability belongs to another connection");
            }
            if !capability.supported {
                return no_attempt(
                    "OBS WebSocket does not advertise support for request 'StopRecord'",
                );
            }
        }
        if cancellation.is_cancelled() || Instant::now() >= deadline {
            return no_attempt("Operation cancelled or expired before dispatch");
        }
        let prepared = match store.prepare_generic(
            &operation_id,
            GenericKind::ManualRecordStop,
            self.session.token().generation(),
            &clock().utc,
        ) {
            Ok(receipt) => receipt,
            Err(reason) => {
                let mut out = no_attempt(
                    "Operation receipt could not be persisted; no OBS request was dispatched",
                );
                out.receipt_error = Some(reason);
                return out;
            }
        };
        let receipt = match store.update_generic(&prepared, GenericPhase::Dispatching, &clock().utc)
        {
            Ok(receipt) => receipt,
            Err(reason) => {
                let mut out = no_attempt(
                    "Operation intent could not be persisted; no OBS request was dispatched",
                );
                out.receipt = Some(prepared);
                out.receipt_error = Some(reason);
                return out;
            }
        };
        let pending = Arc::new(Mutex::new(None));
        let response = if Instant::now() < deadline {
            let capture = pending.clone();
            let impact = self.impact.clone();
            let expected = self.session.token();
            let checked_policy = call_policy;
            let checked_clock = clock.clone();
            let own = operation_id;
            let guard = move || {
                match policy(&checked_policy, &control, &checked_clock) {
                    Ok(Disposition::Allow) => {}
                    _ => return Err("Control policy refused dispatch".into()),
                }
                if cancellation.is_cancelled() || Instant::now() >= deadline {
                    return Err("Operation cancelled or expired before dispatch".into());
                }
                if !fence.permits(&own).unwrap_or(false) {
                    return Err("Unresolved receipt custody prevents dispatch".into());
                }
                let mut slot = capture
                    .lock()
                    .map_err(|_| "Recording-impact slot is unavailable")?;
                if slot.is_some() {
                    return Err("Recording-impact slot is already occupied".into());
                }
                // Final synchronous step inside the sole physical writer's
                // guard. No await or external callback follows this begin.
                *slot = Some(
                    impact
                        .begin_at_send(expected)
                        .map_err(|_| "Recording-impact admission is unavailable")?,
                );
                Ok(())
            };
            self.session
                .manual_record_stop_checked_until(expected, deadline, guard)
                .await
        } else {
            Err(RequestError::BeforeDispatch(BeforeDispatch::Deadline))
        };
        let (impact_started, impact_error) = match pending.lock() {
            Ok(mut slot) => match slot.take() {
                Some(attempt) => (true, settle_impact(attempt, &response).err()),
                None => (false, None),
            },
            Err(_) => (true, Some(ImpactError::Poisoned)),
        };
        // Record impact knowledge precedes receipt publication, so failure to
        // publish Accepted cannot restore an old Take's automatic permission.
        let phase = match &response {
            Ok(_) => GenericPhase::Accepted,
            Err(RequestError::BeforeDispatch(_)) => GenericPhase::NotDispatched,
            Err(RequestError::Rejected { .. } | RequestError::RejectedDetailed { .. }) => {
                GenericPhase::Rejected
            }
            Err(RequestError::Unknown(_)) => GenericPhase::Unknown,
        };
        let transport = Some(response.as_ref().map(|_| ()).map_err(Clone::clone));
        let terminal = store.update_generic(&receipt, phase, &clock().utc);
        let (receipt, receipt_error) = match terminal {
            Ok(receipt) => (receipt, None),
            Err(reason) => (receipt, Some(reason)),
        };
        let result = if receipt_error.is_some() {
            output_stop_result::record_error(if response.is_ok() {
                "StopRecord was acknowledged, but its receipt outcome could not be persisted; inspect obs-observe before another change"
            } else {
                "Operation receipt outcome could not be persisted; inspect obs-observe before another change"
            })
        } else {
            match response {
                Ok(data) => output_stop_result::record_ack(&data),
                Err(reason) => {
                    let message = reason
                        .obs_failure_message("StopRecord")
                        .or_else(|| self.session.authorized_disconnect_message(&reason))
                        .unwrap_or_else(|| match reason {
                            RequestError::BeforeDispatch(BeforeDispatch::GuardRefused(message)) => message,
                            RequestError::BeforeDispatch(_) => "StopRecord was not dispatched".into(),
                            RequestError::Unknown(Invalidation::DeadlineAfterDispatch) => format!(
                                "Request StopRecord timed out after {}ms", timeout.as_millis()
                            ),
                            _ => "StopRecord outcome is unknown; inspect obs-observe before another change".into(),
                        });
                    output_stop_result::record_error(&message)
                }
            }
        };
        Outcome {
            result,
            transport,
            receipt: Some(receipt),
            receipt_error,
            impact_error,
            impact_started,
        }
    }
}
