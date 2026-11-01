// SPDX-License-Identifier: GPL-2.0-only
//! Retained-future output confirmation. This helper spawns no tasks and owns
//! no connection: its supervisor must keep the future through dispatch and
//! cleanup. A subscription is established before the start request.
use serde_json::{Value, json};
use std::{future::Future, pin::Pin, time::Duration};
use tokio::time::{Instant, timeout_at};
pub type OutputFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
#[derive(Clone, Copy, Debug)]
pub struct Definition {
    pub label: &'static str,
    pub request_type: &'static str,
    pub status_request: &'static str,
    pub state_event: &'static str,
    pub timeout: Duration,
}
pub const RECORD_OUTPUT: Definition = Definition {
    label: "Recording",
    request_type: "StartRecord",
    status_request: "GetRecordStatus",
    state_event: "RecordStateChanged",
    timeout: Duration::from_secs(5),
};
pub const STREAM_OUTPUT: Definition = Definition {
    label: "Streaming",
    request_type: "StartStream",
    status_request: "GetStreamStatus",
    state_event: "StreamStateChanged",
    timeout: Duration::from_secs(15),
};
pub const VIRTUAL_CAM_OUTPUT: Definition = Definition {
    label: "Virtual camera",
    request_type: "StartVirtualCam",
    status_request: "GetVirtualCamStatus",
    state_event: "VirtualcamStateChanged",
    timeout: Duration::from_secs(5),
};
pub const REPLAY_BUFFER_OUTPUT: Definition = Definition {
    label: "Replay buffer",
    request_type: "StartReplayBuffer",
    status_request: "GetReplayBufferStatus",
    state_event: "ReplayBufferStateChanged",
    timeout: Duration::from_secs(5),
};
pub struct ObservedEvent {
    pub at: Instant,
    pub data: Value,
}
pub trait Subscription: Send {
    /// The provider retains the first matching state event, including events
    /// arriving while request().await is pending. Drop removes this observer.
    fn next(&mut self) -> Pin<Box<dyn Future<Output = Option<ObservedEvent>> + Send + '_>>;
}
pub trait OutputIo: Send + Sync {
    fn subscribe(&self, event: &'static str) -> Box<dyn Subscription>;
    fn request(&self, request: &'static str) -> OutputFuture<Result<Value, String>>;
}
fn error(text: String) -> Value {
    json!({"content":[{"type":"text","text":text}],"isError":true})
}
enum Outcome {
    Started(Option<String>),
    Stopped,
    Timeout,
}
pub async fn start_output_and_confirm(
    io: &impl OutputIo,
    definition: Definition,
    on_accepted: impl FnOnce() -> Result<(), String>,
) -> Value {
    let deadline = Instant::now() + definition.timeout;
    let mut subscription = io.subscribe(definition.state_event);
    // The subscription, not an unowned spawned task, retains events during
    // the request await. The source confirmation timeout does not cancel the
    // start request itself; the transport independently owns that RPC deadline.
    if let Err(message) = io
        .request(definition.request_type)
        .await
        .and_then(|_| on_accepted())
    {
        return error(format!("{} failed to start: {message}", definition.label));
    }
    let outcome = loop {
        // Poll a queued event before the expired timer: an event observed
        // before the deadline remains valid even if the ACK arrived later.
        let next = if let Ok(Some(event)) = timeout_at(deadline, subscription.next()).await {
            Some(event)
        } else {
            None
        };
        let Some(event) = next else {
            break Outcome::Timeout;
        };
        if event.at > deadline {
            break Outcome::Timeout;
        }
        match event.data.get("outputState").and_then(Value::as_str) {
            Some("OBS_WEBSOCKET_OUTPUT_STARTED") => {
                break Outcome::Started(
                    event
                        .data
                        .get("outputPath")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                );
            }
            Some("OBS_WEBSOCKET_OUTPUT_STOPPED") => break Outcome::Stopped,
            _ => {}
        }
    };
    if let Outcome::Started(path) = outcome {
        return json!({"content":[{"type":"text","text":path.filter(|path|!path.is_empty()).map_or_else(||format!("{} started",definition.label),|path|format!("{} started, writing to: {path}",definition.label))}]});
    }
    let active = io
        .request(definition.status_request)
        .await
        .ok()
        .is_some_and(|status| {
            status.is_object() && status.get("outputActive") == Some(&Value::Bool(true))
        });
    if active {
        return json!({"content":[{"type":"text","text":format!("{} started",definition.label)}]});
    }
    let reason = match outcome {
        Outcome::Stopped => "OBS stopped the output immediately after starting it".into(),
        _ => format!(
            "the output did not become active within {}s",
            definition.timeout.as_secs_f64()
        ),
    };
    error(format!(
        "OBS accepted {}, but {reason}. OBS may be showing an error dialog that blocks further output requests until it is dismissed. If output settings were changed with obs-set-profile-parameter, restart OBS so it rebuilds its outputs.",
        definition.request_type
    ))
}
