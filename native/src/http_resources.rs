// SPDX-License-Identifier: MIT
//! HTTP stream namespaces around the one notification router/publisher.
use super::{
    http_sink::{self, Sender},
    native_host::FrontendId,
};
use crate::protocol::OwnedReplyContext;
use obs_mcp_native::resource_events::{Batch, CancelPort, DeliverySink, Failure, Owner};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};
const MAX_WIRE_ID_BYTES: usize = 256 * 1024;
struct Listener {
    wire: Value,
    bytes: usize,
    sink: Arc<Sender>,
}
struct State {
    listeners: HashMap<String, Listener>,
    wire_bytes: usize,
    closed: bool,
}
pub(super) struct Listen<'a> {
    pub(super) frontend: FrontendId,
    pub(super) reply: &'a OwnedReplyContext,
    pub(super) filter: &'a Value,
    pub(super) valid: bool,
}
pub(super) struct Resources {
    state: Mutex<State>,
    cancel: CancelPort,
}
#[derive(Clone)]
pub(super) struct Lease {
    owner: Weak<Resources>,
    key: String,
}
impl Lease {
    pub(super) fn cancel(self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.cancel(&self.key, http_sink::Failure::Closed);
        }
    }
}
impl Resources {
    pub(super) fn bind(owner: &Owner) -> Result<Arc<Self>, Failure> {
        let routes = Arc::new(Self {
            state: Mutex::new(State {
                listeners: HashMap::new(),
                wire_bytes: 0,
                closed: false,
            }),
            cancel: owner.cancel_port(),
        });
        let sink: Arc<dyn DeliverySink> = routes.clone();
        owner.bind_delivery(&sink)?;
        Ok(routes)
    }
    pub(super) fn install(
        self: &Arc<Self>,
        owner: &Owner,
        listen: Listen<'_>,
        sink: Arc<Sender>,
        cleanup: &Arc<Mutex<Option<Lease>>>,
    ) -> Result<(), Value> {
        let Listen {
            frontend,
            reply,
            filter,
            valid,
        } = listen;
        let internal = frontend.listener_key();
        let key = internal.as_str().expect("internal listener key").to_owned();
        let Some(wire) = reply.subscription_id(1024) else {
            return Err(reply.error(-32603, "Native subscription identity capacity exceeded"));
        };
        let bytes = serde_json::to_vec(&wire)
            .map_err(|_| reply.error(-32603, "Native subscription identity unavailable"))?
            .len();
        let mut state = self
            .state
            .lock()
            .map_err(|_| reply.error(-32603, "Native resource routing unavailable"))?;
        if state.closed || !sink.available() {
            return Err(reply.error(-32603, "Native resource routing unavailable"));
        }
        // The global Owner checks its1024 subscription cap before filter shape.
        let plan = owner
            .plan_listen(&internal, filter, valid)
            .map_err(|reason| {
                reply.error(
                    if reason == Failure::InvalidFilter {
                        -32602
                    } else {
                        -32603
                    },
                    reason.message(),
                )
            })?;
        if bytes > 1024
            || state
                .wire_bytes
                .checked_add(bytes)
                .is_none_or(|n| n > MAX_WIRE_ID_BYTES)
        {
            return Err(reply.error(-32603, "Native subscription identity capacity exceeded"));
        }
        let ack = obs_mcp_native::http_protocol::Response::sse(
            200,
            &reply.subscription_ack(plan.honored().clone()),
        );
        let reserved = sink.reserve(ack).map_err(|_| {
            reply.error(-32603, "Native subscription publication capacity exceeded")
        })?;
        let mut lease = cleanup
            .lock()
            .map_err(|_| reply.error(-32603, "Native stream cleanup unavailable"))?;
        owner
            .commit_listen(plan)
            .map_err(|reason| reply.error(-32603, reason.message()))?;
        state.wire_bytes += bytes;
        state.listeners.insert(
            key.clone(),
            Listener {
                wire,
                bytes,
                sink: sink.clone(),
            },
        );
        // A producer may snapshot the new listener now, but cannot enter this
        // registry until the ACK is committed. ACK always precedes its events.
        *lease = Some(Lease {
            owner: Arc::downgrade(self),
            key: key.clone(),
        });
        if reserved.commit().is_err() {
            if let Some(old) = state.listeners.remove(&key) {
                state.wire_bytes -= old.bytes;
            }
            let _ = self.cancel.cancel(&internal);
            sink.fail(http_sink::Failure::Closed);
            return Err(reply.error(-32603, "Native stream closed before ACK publication"));
        }
        Ok(())
    }
    fn cancel(&self, key: &str, reason: http_sink::Failure) {
        if let Ok(mut state) = self.state.lock()
            && let Some(listener) = state.listeners.remove(key)
        {
            state.wire_bytes -= listener.bytes;
            let _ = self.cancel.cancel(&Value::String(key.into()));
            listener.sink.fail(reason);
        }
    }
    pub(super) fn heartbeat(&self) {
        if let Ok(mut state) = self.state.lock() {
            let mut failed = Vec::new();
            for (key, listener) in &state.listeners {
                let response = obs_mcp_native::http_protocol::Response {
                    status: 200,
                    content_type: Some("text/event-stream"),
                    authenticate_bearer: false,
                    body: b": keepalive\n\n".to_vec(),
                };
                if let Err(reason) = listener.sink.send(response) {
                    failed.push((key.clone(), reason));
                }
            }
            for (key, reason) in failed {
                if let Some(listener) = state.listeners.remove(&key) {
                    state.wire_bytes -= listener.bytes;
                    let _ = self.cancel.cancel(&Value::String(key));
                    listener.sink.fail(reason);
                }
            }
        }
    }
    pub(super) fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            for (key, listener) in state.listeners.drain() {
                let _ = self.cancel.cancel(&Value::String(key));
                listener.sink.fail(http_sink::Failure::Closed);
            }
            state.wire_bytes = 0;
        }
    }
    #[cfg(test)]
    pub(super) fn count(&self) -> usize {
        self.state.lock().unwrap().listeners.len()
    }
}
impl DeliverySink for Resources {
    fn deliver(&self, batch: Batch) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.closed {
            return;
        }
        for delivery in &batch.deliveries {
            for audience in &delivery.audiences {
                let Some(key) = audience.as_ref().and_then(Value::as_str) else {
                    continue;
                };
                let failure = state.listeners.get(key).and_then(|listener| {
                    listener.sink.event(&delivery.change, &listener.wire).err()
                });
                if let Some(reason) = failure
                    && let Some(listener) = state.listeners.remove(key)
                {
                    state.wire_bytes -= listener.bytes;
                    let _ = self.cancel.cancel(&Value::String(key.into()));
                    listener.sink.fail(reason);
                }
            }
        }
        // Batch's existing shared8MiB ownership is released only after encoded
        // packet credits have been acquired, never while borrowed fields escape.
    }
}
impl Drop for Resources {
    fn drop(&mut self) {
        self.close();
    }
}
