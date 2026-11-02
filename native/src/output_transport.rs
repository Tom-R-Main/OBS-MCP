// SPDX-License-Identifier: GPL-2.0-only
//! Retained invocation owner for three explicit, open-ended output starts.
//! The caller owns this complete future. No future Stop promise, task, socket,
//! broker or journal writer is created; the supplied scoped port stays owned
//! through the actual RPC, confirmation, result construction and cleanup.
use crate::{
    control::Disposition,
    custody::{Clock, SharedControl},
    generic_requests::{self, Call},
    operation_store::{GenericPhase, GenericReceipt, OperationStore},
    output_events::{OutputEvents, OutputKind},
    output_start::OutputFuture,
    output_start_v2::{self, OutputIo, RpcFailure, RpcFate, Timing},
    session::{BeforeDispatch, ConnectionToken, Invalidation, RequestError, SessionHandle},
    stage_transport::Cancellation,
};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;

/// Same-physical-owner dependencies. Capabilities must come from the actual
/// Service's cached GetVersion; None means unknown, never an invented empty set.
pub struct Context {
    pub session: SessionHandle,
    pub events: OutputEvents,
    pub control: SharedControl,
    pub clock: Clock,
    pub capabilities: Option<Arc<HashSet<String>>>,
}
struct ReceiptState {
    store: OperationStore,
    receipt: Option<GenericReceipt>,
    known_ack: bool,
}
struct BoundIo {
    context: Context,
    call: Arc<Call>,
    kind: OutputKind,
    token: ConnectionToken,
    caller: String,
    cancellation: Cancellation,
    operation_id: String,
    original_deadline: Instant,
    state: Mutex<ReceiptState>,
    refusal: Arc<Mutex<Option<Disposition>>>,
}
fn error(message: impl Into<String>) -> Value {
    json!({"content":[{"type":"text","text":message.into()}],"isError":true})
}
fn failure(kind: OutputKind, message: impl AsRef<str>) -> Value {
    error(format!(
        "{} failed to start: {}",
        output_start_v2::definition(kind).label,
        message.as_ref()
    ))
}
// Keep the transport fact separate from custody availability. This projection
// step is synchronous; the returned guard never crosses another OBS await.
fn failed_receipt<'a>(
    state: &'a Mutex<ReceiptState>,
    transport: &RequestError,
) -> Result<
    (
        std::sync::MutexGuard<'a, ReceiptState>,
        GenericPhase,
        RpcFate,
    ),
    RpcFailure,
> {
    let (phase, fate) = match transport {
        RequestError::BeforeDispatch(_) => (GenericPhase::NotDispatched, RpcFate::NotDispatched),
        error if error.rejection_code().is_some() => (
            GenericPhase::Rejected,
            RpcFate::Rejected {
                code: error.rejection_code().unwrap(),
            },
        ),
        _ => (GenericPhase::Unknown, RpcFate::Unknown),
    };
    let state = state.lock().map_err(|poison| {
        // A precise transport fact survives loss of custody availability. The
        // same namespace is nevertheless unavailable even while its loan is
        // still retained: it cannot be excluded as healthy in-flight work.
        poison.into_inner().store.read_view().mark_uncertain();
        RpcFailure::new(fate, "Operation receipt owner unavailable")
    })?;
    Ok((state, phase, fate))
}

impl BoundIo {
    fn supported(&self, request: &str) -> bool {
        self.context
            .capabilities
            .as_ref()
            .is_none_or(|known| known.contains(request))
    }
    fn policy(&self) -> Result<(), String> {
        let decision = generic_requests::policy(
            &self.call,
            &self.caller,
            &self.context.control,
            &self.context.clock,
        )
        .map_err(|()| "Control policy unavailable".to_owned())?;
        if decision != Disposition::Allow {
            if let Ok(mut refusal) = self.refusal.lock() {
                *refusal = Some(decision);
            }
            return Err("Control policy changed before dispatch".into());
        }
        Ok(())
    }
    fn check(&self, kind: OutputKind, deadline: Instant) -> Result<(), String> {
        if kind != self.kind || kind == OutputKind::Record {
            return Err("Native output kind binding is unavailable".into());
        }
        self.policy()?;
        if self.cancellation.is_cancelled()
            || Instant::now() >= deadline
            || Instant::now() >= self.original_deadline
        {
            return Err("Operation cancelled or expired before dispatch".into());
        }
        Ok(())
    }
    fn acknowledged(&self) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "Operation receipt owner unavailable".to_owned())?;
        if !state.known_ack {
            return Err("Start acknowledgement is unavailable".into());
        }
        let receipt = state
            .receipt
            .as_ref()
            .ok_or("Operation receipt is unavailable")?
            .clone();
        let accepted=state.store.update_generic(&receipt,GenericPhase::Accepted,&(self.context.clock)().utc)
            .map_err(|_|"Operation receipt outcome is uncertain; inspect obs-observe before any further production change".to_owned())?;
        state.receipt = Some(accepted);
        Ok(())
    }
    fn message(&self, request: &str, error: &RequestError, rpc_duration: Duration) -> String {
        error
            .obs_failure_message(request)
            .or_else(|| self.context.session.authorized_disconnect_message(error))
            .unwrap_or_else(|| match error {
                RequestError::Unknown(Invalidation::DeadlineAfterDispatch) => format!(
                    "Request {request} timed out after {}ms",
                    rpc_duration.as_millis()
                ),
                RequestError::BeforeDispatch(BeforeDispatch::GuardRefused(reason)) => {
                    reason.clone()
                }
                RequestError::BeforeDispatch(_) => {
                    "OBS request was not sent: native request admission is unavailable".into()
                }
                _ => "OBS request outcome is uncertain; inspect obs-observe before another change"
                    .into(),
            })
    }
}
impl OutputIo for Arc<BoundIo> {
    fn start(
        &self,
        kind: OutputKind,
        rpc_deadline: Instant,
    ) -> OutputFuture<Result<(), RpcFailure>> {
        let owner = self.clone();
        Box::pin(async move {
            owner
                .check(kind, rpc_deadline)
                .map_err(|message| RpcFailure::new(RpcFate::NotDispatched, message))?;
            let request = output_start_v2::definition(kind).request_type;
            if !owner.supported(request) {
                return Err(RpcFailure::new(
                    RpcFate::NotDispatched,
                    format!("OBS WebSocket does not advertise support for request '{request}'"),
                ));
            }
            let fence = {
                let mut state = owner.state.lock().map_err(|_| {
                    RpcFailure::new(
                        RpcFate::NotDispatched,
                        "Operation receipt owner unavailable",
                    )
                })?;
                if state.receipt.is_some() {
                    return Err(RpcFailure::new(
                        RpcFate::NotDispatched,
                        "Start invocation has already been admitted; no replay is authorized",
                    ));
                }
                if state
                    .store
                    .read_view()
                    .admission()
                    .map_or(true, |value| value.blocking)
                {
                    return Err(RpcFailure::new(
                        RpcFate::NotDispatched,
                        "Unresolved receipt custody prevents dispatch",
                    ));
                }
                let receipt=state.store.prepare_generic(&owner.operation_id,owner.call.kind(),owner.token.generation(),&(owner.context.clock)().utc)
                    .map_err(|_|RpcFailure::new(RpcFate::NotDispatched,"Operation receipt could not be persisted or inspected; inspect obs-observe before any further production change"))?;
                let receipt=state.store.update_generic(&receipt,GenericPhase::Dispatching,&(owner.context.clock)().utc)
                    .map_err(|_|RpcFailure::new(RpcFate::NotDispatched,"Operation receipt could not be persisted or inspected; inspect obs-observe before any further production change"))?;
                state.receipt = Some(receipt);
                state.store.dispatch_fence().ok_or_else(|| {
                    RpcFailure::new(
                        RpcFate::NotDispatched,
                        "Scoped receipt custody is unavailable",
                    )
                })?
            };
            let Some(remaining) = rpc_deadline.checked_duration_since(Instant::now()) else {
                let mut state = owner.state.lock().map_err(|_| {
                    RpcFailure::new(
                        RpcFate::NotDispatched,
                        "Operation receipt owner unavailable",
                    )
                })?;
                let receipt = state.receipt.as_ref().unwrap().clone();
                state
                    .store
                    .update_generic(
                        &receipt,
                        GenericPhase::NotDispatched,
                        &(owner.context.clock)().utc,
                    )
                    .map_err(|_| {
                        RpcFailure::new(
                            RpcFate::NotDispatched,
                            "Operation receipt outcome is uncertain",
                        )
                    })?;
                return Err(RpcFailure::new(
                    RpcFate::NotDispatched,
                    "Operation deadline expired before dispatch",
                ));
            };
            let (call, caller, control, clock, cancel, own, refusal, original) = (
                owner.call.clone(),
                owner.caller.clone(),
                owner.context.control.clone(),
                owner.context.clock.clone(),
                owner.cancellation.clone(),
                owner.operation_id.clone(),
                owner.refusal.clone(),
                owner.original_deadline,
            );
            let guard = move || {
                let decision = generic_requests::policy(&call, &caller, &control, &clock)
                    .map_err(|()| "Control policy unavailable".to_owned())?;
                if decision != Disposition::Allow {
                    if let Ok(mut target) = refusal.lock() {
                        *target = Some(decision);
                    }
                    return Err("Control policy changed before dispatch".into());
                }
                if cancel.is_cancelled()
                    || Instant::now() >= rpc_deadline
                    || Instant::now() >= original
                {
                    return Err("Operation cancelled or expired before dispatch".into());
                }
                if !fence.permits(&own).unwrap_or(false) {
                    return Err("Unresolved receipt custody prevents dispatch".into());
                }
                Ok(())
            };
            let result = owner
                .context
                .session
                .request_checked(owner.token, request, Value::Null, remaining, guard)
                .await;
            if result.is_ok() {
                // The successful transport return is already an ACK fact, even
                // if a poisoned receipt mutex prevents its later publication.
                if let Ok(mut state) = owner.state.lock() {
                    state.known_ack = true;
                }
                return Ok(());
            }
            let transport = result.as_ref().unwrap_err();
            let (mut state, phase, fate) = failed_receipt(&owner.state, transport)?;
            let receipt = state.receipt.as_ref().unwrap().clone();
            if state
                .store
                .update_generic(&receipt, phase, &(owner.context.clock)().utc)
                .is_err()
            {
                return Err(RpcFailure::new(
                    fate,
                    "Operation receipt outcome is uncertain; inspect obs-observe before any further production change",
                ));
            }
            Err(RpcFailure::new(
                fate,
                owner.message(request, transport, remaining),
            ))
        })
    }
    fn status(
        &self,
        kind: OutputKind,
        rpc_deadline: Instant,
    ) -> OutputFuture<Result<Value, RpcFailure>> {
        let owner = self.clone();
        Box::pin(async move {
            if kind != owner.kind || kind == OutputKind::Record {
                return Err(RpcFailure::new(
                    RpcFate::NotDispatched,
                    "Native output kind binding is unavailable",
                ));
            }
            let request = output_start_v2::definition(kind).status_request;
            if !owner.supported(request) {
                return Err(RpcFailure::new(
                    RpcFate::NotDispatched,
                    format!("OBS WebSocket does not advertise support for request '{request}'"),
                ));
            }
            let remaining = rpc_deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| {
                    RpcFailure::new(
                        RpcFate::NotDispatched,
                        "Operation deadline expired before status",
                    )
                })?;
            let (cancel, original) = (owner.cancellation.clone(), owner.original_deadline);
            owner
                .context
                .session
                .request_checked(owner.token, request, Value::Null, remaining, move || {
                    if cancel.is_cancelled()
                        || Instant::now() >= rpc_deadline
                        || Instant::now() >= original
                    {
                        Err("Operation cancelled or expired before status".into())
                    } else {
                        Ok(())
                    }
                })
                .await
                .map_err(|error| {
                    let fate = match &error {
                        RequestError::BeforeDispatch(_) => RpcFate::NotDispatched,
                        error if error.rejection_code().is_some() => RpcFate::Rejected {
                            code: error.rejection_code().unwrap(),
                        },
                        _ => RpcFate::Unknown,
                    };
                    RpcFailure::new(fate, owner.message(request, &error, remaining))
                })
        })
    }
}
/// Retain this complete future through settlement; dropping it is not a joined
/// receipt. Service uses its existing owned JoinSet, not a frontend future.
#[allow(clippy::too_many_arguments)]
pub async fn execute(
    store: OperationStore,
    call: Call,
    context: Context,
    caller: String,
    cancellation: Cancellation,
    operation_id: String,
    original_deadline: Instant,
) -> Value {
    let Some(kind) = call.confirmed_output_kind() else {
        return error(
            "Native confirmed-output request binding is unavailable; no OBS request was dispatched",
        );
    };
    if context.events.connection_token() != context.session.token()
        || store.endpoint_hash() != context.session.endpoint_hash()
        || store.dispatch_fence().is_none()
    {
        return failure(
            kind,
            "Native output owner binding is unavailable; no OBS request was dispatched",
        );
    }
    let io = Arc::new(BoundIo {
        token: context.session.token(),
        context,
        call: Arc::new(call),
        kind,
        caller,
        cancellation,
        operation_id,
        original_deadline,
        state: Mutex::new(ReceiptState {
            store,
            receipt: None,
            known_ack: false,
        }),
        refusal: Arc::new(Mutex::new(None)),
    });
    if let Err(message) = io.check(kind, original_deadline) {
        if let Some(result) = io
            .refusal
            .lock()
            .ok()
            .and_then(|mut value| value.take())
            .and_then(|value| value.tool_result())
        {
            return result;
        }
        return failure(kind, message);
    }
    let request = output_start_v2::definition(kind).request_type;
    if !io.supported(request) {
        return failure(
            kind,
            format!("OBS WebSocket does not advertise support for request '{request}'"),
        );
    }
    let execution = output_start_v2::execute(
        &io,
        &io.context.events,
        kind,
        Timing::source(kind, original_deadline),
        || io.acknowledged(),
    )
    .await;
    let result = io
        .refusal
        .lock()
        .ok()
        .and_then(|mut value| value.take())
        .and_then(|value| value.tool_result())
        .unwrap_or(execution.result);
    // Same global Source success wrapper; the helper's individual body remains unchanged.
    if result.get("isError").and_then(Value::as_bool) != Some(true) {
        let mut result = result;
        if let Some(message) = result["content"]
            .as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row["text"].as_str())
            .map(str::to_owned)
        {
            result["structuredContent"] = json!({"message":message});
        }
        result
    } else {
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        operation_store::GenericKind,
        receipt_broker::{ReceiptBroker, ScopeKind},
        session::SafetyStopRequest,
    };
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };
    const ID: &str = "00000000-0000-4000-8000-000000000001";
    const UTC: &str = "2026-11-01T12:00:00.000Z";
    struct Dir(PathBuf);
    impl Dir {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let parent = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target");
            // Cargo may use an external target directory in isolated checks.
            fs::create_dir_all(&parent).unwrap();
            let root = parent.join(format!(
                "start-fate-poison-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&root).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            }
            Self(root)
        }
    }
    impl Drop for Dir {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
    fn seed(dir: &Dir) -> (ReceiptBroker, Mutex<ReceiptState>) {
        let broker =
            ReceiptBroker::new(OperationStore::new(&dir.0, "ws://127.0.0.1:4455").unwrap())
                .unwrap();
        let mut port = broker
            .scope(ScopeKind::Generic(GenericKind::StreamStart))
            .unwrap();
        let prepared = port
            .prepare_generic(ID, GenericKind::StreamStart, 1, UTC)
            .unwrap();
        let dispatching = port
            .update_generic(&prepared, GenericPhase::Dispatching, UTC)
            .unwrap();
        (
            broker,
            Mutex::new(ReceiptState {
                store: port,
                receipt: Some(dispatching),
                known_ack: false,
            }),
        )
    }
    fn poisoned_projection(transport: RequestError, expected: RpcFate) {
        let dir = Dir::new();
        let (broker, state) = seed(&dir);
        let fence = state.lock().unwrap().store.dispatch_fence().unwrap();
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = state.lock().unwrap();
            panic!("controlled custody mutex poison");
        }));
        assert!(poisoned.is_err());
        assert!(state.is_poisoned());
        let error = match failed_receipt(&state, &transport) {
            Err(error) => error,
            Ok(_) => panic!("poisoned custody unexpectedly available"),
        };
        assert_eq!(
            error.fate(),
            expected,
            "transport knowledge must survive actual custody poison"
        );
        assert!(
            broker.admission().is_err(),
            "poison is uncertainty, not permission from a still-live loan"
        );
        assert!(!fence.permits(ID).unwrap_or(false));
        assert!(
            broker
                .scope(ScopeKind::SafetyStop(SafetyStopRequest::Stream))
                .is_err()
        );
        let bytes = fs::read(dir.0.join("operation-receipts").join(format!("{ID}.json"))).unwrap();
        let durable: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(durable["phase"], "dispatching");
        assert_eq!(durable["postconditionObserved"], false);
        drop(state);
        drop(fence);
        assert!(
            broker
                .scope(ScopeKind::Generic(GenericKind::StreamStart))
                .is_err()
        );
        // Reopen sees the unchanged durable Dispatching blocker; no synthetic
        // terminal publication or restart/replay authority was manufactured.
        drop(broker);
        let reopened = OperationStore::new(&dir.0, "ws://127.0.0.1:4455").unwrap();
        assert!(reopened.read_view().admission().unwrap().blocking);
    }
    #[test]
    fn poison_preserves_before_dispatch_fact_and_blocks_live_replay() {
        poisoned_projection(
            RequestError::BeforeDispatch(BeforeDispatch::Cancelled),
            RpcFate::NotDispatched,
        );
    }
    #[test]
    fn poison_preserves_known_rejection_fact_and_blocks_live_replay() {
        poisoned_projection(
            RequestError::Rejected { code: 500 },
            RpcFate::Rejected { code: 500 },
        );
    }
    #[test]
    fn healthy_custody_projects_all_closed_failure_fates_without_publishing() {
        for (transport, expected_phase, expected_fate) in [
            (
                RequestError::BeforeDispatch(BeforeDispatch::Cancelled),
                GenericPhase::NotDispatched,
                RpcFate::NotDispatched,
            ),
            (
                RequestError::Rejected { code: 500 },
                GenericPhase::Rejected,
                RpcFate::Rejected { code: 500 },
            ),
            (
                RequestError::Unknown(Invalidation::DeadlineAfterDispatch),
                GenericPhase::Unknown,
                RpcFate::Unknown,
            ),
        ] {
            let dir = Dir::new();
            let (_broker, state) = seed(&dir);
            let (guard, phase, fate) = failed_receipt(&state, &transport)
                .unwrap_or_else(|_| panic!("healthy custody unavailable"));
            assert_eq!(phase, expected_phase);
            assert_eq!(fate, expected_fate);
            drop(guard);
        }
    }
}
