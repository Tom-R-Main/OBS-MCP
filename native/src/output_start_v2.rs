// SPDX-License-Identifier: GPL-2.0-only
//! Additive closed output confirmation over the sole observer's V2 provider.
//! The caller retains this entire future and its RPC/receipt owner. No task,
//! connection, store or public tool is created here. Legacy Take is unchanged.
use crate::{
    output_events::{OutputEventError, OutputEvents, OutputKind, Terminal},
    output_start::{self, Definition, OutputFuture},
};
use serde_json::{Map, Value};
use std::{fmt, time::Duration};
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcFate {
    NotDispatched,
    Rejected { code: u64 },
    Unknown,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportFate {
    NotDispatched,
    Rejected { code: u64 },
    Unknown,
    Accepted,
}
impl From<RpcFate> for TransportFate {
    fn from(fate: RpcFate) -> Self {
        match fate {
            RpcFate::NotDispatched => Self::NotDispatched,
            RpcFate::Rejected { code } => Self::Rejected { code },
            RpcFate::Unknown => Self::Unknown,
        }
    }
}
/// Authorized tool-result text, deliberately absent from Debug/Display.
pub struct RpcFailure {
    fate: RpcFate,
    message: String,
}
impl RpcFailure {
    pub fn new(fate: RpcFate, message: impl Into<String>) -> Self {
        Self {
            fate,
            message: message.into(),
        }
    }
    pub fn fate(&self) -> RpcFate {
        self.fate
    }
}
impl fmt::Debug for RpcFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcFailure")
            .field("fate", &self.fate)
            .field("message_bytes", &self.message.len())
            .finish_non_exhaustive()
    }
}
/// These owned futures must enforce the supplied absolute deadline and retain
/// admitted work through actual settlement. This helper never drops an RPC in
/// an outer timeout. A concrete production Session adapter is not supplied here.
pub trait OutputIo: Send + Sync {
    fn start(
        &self,
        kind: OutputKind,
        rpc_deadline: Instant,
    ) -> OutputFuture<Result<(), RpcFailure>>;
    fn status(
        &self,
        kind: OutputKind,
        rpc_deadline: Instant,
    ) -> OutputFuture<Result<Value, RpcFailure>>;
}
#[derive(Clone, Copy, Debug)]
pub struct Timing {
    pub operation_deadline: Instant,
    /// Explicit library/test override. Source constructor retains exact5/15s.
    pub confirmation_timeout: Duration,
    pub rpc_timeout: Duration,
}
impl Timing {
    pub fn source(kind: OutputKind, operation_deadline: Instant) -> Self {
        Self {
            operation_deadline,
            confirmation_timeout: definition(kind).timeout,
            rpc_timeout: Duration::from_secs(10),
        }
    }
}
pub fn definition(kind: OutputKind) -> Definition {
    match kind {
        OutputKind::Record => output_start::RECORD_OUTPUT,
        OutputKind::Stream => output_start::STREAM_OUTPUT,
        OutputKind::VirtualCam => output_start::VIRTUAL_CAM_OUTPUT,
        OutputKind::ReplayBuffer => output_start::REPLAY_BUFFER_OUTPUT,
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unconfirmed {
    Stopped,
    ConfirmationExpired,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Detail {
    InvalidTiming,
    OperationDeadline,
    Registration(OutputEventError),
    RequestFailed(RpcFate),
    AckHookFailed,
    ObservationUnavailable(OutputEventError),
    StartedEvent,
    ActiveStatus,
    Unconfirmed {
        reason: Unconfirmed,
        status_failure: Option<RpcFate>,
    },
}
/// `result` is the whole helper tool body; fate is independent of its isError.
/// Returning/copying/serializing this Value is outside the provider path budget.
pub struct Execution {
    pub fate: TransportFate,
    pub detail: Detail,
    pub result: Value,
}
impl fmt::Debug for Execution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutputStartExecution")
            .field("fate", &self.fate)
            .field("detail", &self.detail)
            .field(
                "is_error",
                &self.result.get("isError").and_then(Value::as_bool),
            )
            .finish_non_exhaustive()
    }
}
fn body(text: String, is_error: bool) -> Value {
    let mut block = Map::new();
    block.insert("type".into(), Value::String("text".into()));
    block.insert("text".into(), Value::String(text));
    let mut result = Map::new();
    result.insert("content".into(), Value::Array(vec![Value::Object(block)]));
    if is_error {
        result.insert("isError".into(), Value::Bool(true));
    }
    Value::Object(result)
}
fn execution(fate: TransportFate, detail: Detail, text: String, is_error: bool) -> Execution {
    Execution {
        fate,
        detail,
        result: body(text, is_error),
    }
}
fn rpc_deadline(timing: Timing) -> Option<Instant> {
    let now = Instant::now();
    if now >= timing.operation_deadline {
        return None;
    }
    Some(
        now.checked_add(timing.rpc_timeout)
            .map_or(timing.operation_deadline, |deadline| {
                deadline.min(timing.operation_deadline)
            }),
    )
}
fn unavailable(def: Definition, error: OutputEventError) -> Execution {
    execution(
        TransportFate::Accepted,
        Detail::ObservationUnavailable(error),
        format!(
            "OBS accepted {}, but native output confirmation is unavailable ({error}). Inspect current output state before any further production change; do not automatically retry the start.",
            def.request_type
        ),
        true,
    )
}
/// Register before Start; onAccepted runs exactly once after a known transport
/// ACK. Confirmation eligibility never extends when ACK or projection is late.
/// The original operation deadline bounds prefix settlement and future RPCs.
pub async fn execute(
    io: &impl OutputIo,
    events: &OutputEvents,
    kind: OutputKind,
    timing: Timing,
    on_accepted: impl FnOnce() -> Result<(), String>,
) -> Execution {
    let def = definition(kind);
    let now = Instant::now();
    if now >= timing.operation_deadline {
        return execution(
            TransportFate::NotDispatched,
            Detail::OperationDeadline,
            format!(
                "{} failed to start: Operation deadline expired before dispatch",
                def.label
            ),
            true,
        );
    }
    let Some(confirmation_deadline) = now
        .checked_add(timing.confirmation_timeout)
        .filter(|_| !timing.confirmation_timeout.is_zero() && !timing.rpc_timeout.is_zero())
    else {
        return execution(
            TransportFate::NotDispatched,
            Detail::InvalidTiming,
            format!(
                "{} failed to start: Invalid native output timing",
                def.label
            ),
            true,
        );
    };
    let subscription = match events.subscribe(kind, confirmation_deadline) {
        Ok(subscription) => subscription,
        Err(error) => {
            return execution(
                TransportFate::NotDispatched,
                Detail::Registration(error),
                format!(
                    "{} failed to start: Native output observation registration unavailable ({error})",
                    def.label
                ),
                true,
            );
        }
    };
    let Some(deadline) = rpc_deadline(timing) else {
        return execution(
            TransportFate::NotDispatched,
            Detail::OperationDeadline,
            format!(
                "{} failed to start: Operation deadline expired before dispatch",
                def.label
            ),
            true,
        );
    };
    if let Err(error) = io.start(kind, deadline).await {
        return execution(
            error.fate.into(),
            Detail::RequestFailed(error.fate),
            format!("{} failed to start: {}", def.label, error.message),
            true,
        );
    }
    if let Err(message) = on_accepted() {
        return execution(
            TransportFate::Accepted,
            Detail::AckHookFailed,
            format!("{} failed to start: {message}", def.label),
            true,
        );
    }
    let reason = match subscription.receive(timing.operation_deadline).await {
        Ok(event) if event.terminal() == Terminal::Started => {
            // Keep the non-Clone event and its selected-path permit through completed
            // result construction. No full Value or Outcome String clone is introduced.
            let text = event
                .output_path()
                .filter(|path| !path.is_empty())
                .map_or_else(
                    || format!("{} started", def.label),
                    |path| format!("{} started, writing to: {path}", def.label),
                );
            let result = body(text, false);
            drop(event);
            return Execution {
                fate: TransportFate::Accepted,
                detail: Detail::StartedEvent,
                result,
            };
        }
        Ok(_) => Unconfirmed::Stopped,
        Err(OutputEventError::ConfirmationExpired) => Unconfirmed::ConfirmationExpired,
        Err(error) => return unavailable(def, error),
    };
    let Some(deadline) = rpc_deadline(timing) else {
        return execution(
            TransportFate::Accepted,
            Detail::OperationDeadline,
            format!(
                "OBS accepted {}, but the native operation deadline ended before fallback status could be inspected. Inspect current output state; do not automatically retry the start.",
                def.request_type
            ),
            true,
        );
    };
    let status = io.status(kind, deadline).await;
    if status.as_ref().is_ok_and(|status| {
        status.is_object() && status.get("outputActive") == Some(&Value::Bool(true))
    }) {
        return execution(
            TransportFate::Accepted,
            Detail::ActiveStatus,
            format!("{} started", def.label),
            false,
        );
    }
    let status_failure = status.as_ref().err().map(RpcFailure::fate);
    let why = match reason {
        Unconfirmed::Stopped => "OBS stopped the output immediately after starting it".into(),
        Unconfirmed::ConfirmationExpired => {
            let seconds = crate::observation::js_string(Some(&serde_json::json!(
                timing.confirmation_timeout.as_secs_f64()
            )));
            format!("the output did not become active within {seconds}s")
        }
    };
    execution(
        TransportFate::Accepted,
        Detail::Unconfirmed {
            reason,
            status_failure,
        },
        format!(
            "OBS accepted {}, but {why}. OBS may be showing an error dialog that blocks further output requests until it is dismissed. If output settings were changed with obs-set-profile-parameter, restart OBS so it rebuilds its outputs.",
            def.request_type
        ),
        true,
    )
}
