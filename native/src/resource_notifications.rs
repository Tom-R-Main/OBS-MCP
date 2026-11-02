// SPDX-License-Identifier: GPL-2.0-only
//! Stdout binding for the typed resource owner. No OBS/Service operations.
use crate::{
    protocol::OwnedReplyContext,
    stdio_output::{Cycle, Output},
};
use obs_mcp_native::resource_events::{Batch, Change, Failure, Owner};
use serde::Serialize;
use serde_json::{Value, json};
use std::io;

pub(crate) fn listen(
    owner: &Owner,
    reply: &OwnedReplyContext,
    filter: &Value,
    valid: bool,
    output: &Output,
) -> io::Result<Option<Value>> {
    let Some(id) = reply.subscription_id(1024) else {
        return Ok(Some(
            reply.error(-32603, "Native subscription identity capacity exceeded"),
        ));
    };
    let plan = match owner.plan_listen(&id, filter, valid) {
        Ok(plan) => plan,
        Err(reason) => {
            return Ok(Some(reply.error(
                if reason == Failure::InvalidFilter {
                    -32602
                } else {
                    -32603
                },
                reason.message(),
            )));
        }
    };
    let ack = reply.subscription_ack(plan.honored().clone());
    let reserved = output.reserve_publication(&[Cycle {
        values: std::slice::from_ref(&ack),
        repetitions: 1,
    }])?;
    owner.commit_listen(plan).map_err(io::Error::other)?;
    drop(reserved.commit());
    Ok(None)
}
/// Source's legacy callback changes its Set synchronously, but its reply may
/// still be suppressed by a buffered cancellation. Return the retained reply.
pub(crate) fn uri(
    owner: &Owner,
    reply: &OwnedReplyContext,
    uri: Result<&str, &str>,
    add: bool,
    output: &Output,
) -> io::Result<crate::stdio_output::Reservation> {
    let (plan, frame) = match uri {
        Err(message) => (None, reply.error(-32603, message)),
        Ok(uri) => match owner.plan_uri(uri, add) {
            Ok(plan) => (Some(plan), reply.complete(json!({}), false)),
            Err(reason) => (None, reply.error(-32000, reason.message())),
        },
    };
    let reserved = output.reserve_publication(&[Cycle {
        values: std::slice::from_ref(&frame),
        repetitions: 1,
    }])?;
    if let Some(plan) = plan {
        owner.commit_uri(plan).map_err(io::Error::other)?;
    }
    Ok(reserved)
}

#[derive(Serialize)]
struct Frame<'a> {
    jsonrpc: &'static str,
    method: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<Params<'a>>,
}
#[derive(Serialize)]
struct Params<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    uri: Option<&'a str>,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    meta: Option<Meta<'a>>,
}
#[derive(Serialize)]
struct Meta<'a> {
    #[serde(rename = "io.modelcontextprotocol/subscriptionId")]
    id: &'a Value,
}
fn frame<'a>(change: &'a Change, id: Option<&'a Value>) -> Frame<'a> {
    let (method, uri) = match change {
        Change::Updated(uri) => ("notifications/resources/updated", Some(uri.as_str())),
        Change::ListChanged => ("notifications/resources/list_changed", None),
    };
    Frame {
        jsonrpc: "2.0",
        method,
        params: (uri.is_some() || id.is_some()).then_some(Params {
            uri,
            meta: id.map(|id| Meta { id }),
        }),
    }
}
pub(crate) fn write_event(
    writer: impl io::Write,
    change: &Change,
    id: Option<&Value>,
) -> io::Result<()> {
    serde_json::to_writer(writer, &frame(change, id)).map_err(io::Error::other)
}
/// Each Source callback gets one finite publication. Bound template count and
/// exact encoded bytes using borrowed fields BEFORE cloning a parsed frame set.
pub(crate) fn publish(batch: Batch, output: &Output) -> io::Result<()> {
    for delivery in &batch.deliveries {
        struct Count(usize);
        impl io::Write for Count {
            fn write(&mut self, b: &[u8]) -> io::Result<usize> {
                self.0 = self
                    .0
                    .checked_add(b.len())
                    .filter(|n| *n <= 256 * 1024)
                    .ok_or_else(|| {
                        io::Error::other("Native resource notification template capacity exceeded")
                    })?;
                Ok(b.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        if delivery.audiences.len() > 1024 {
            return Err(io::Error::other(
                "Native resource notification recipient capacity exceeded",
            ));
        }
        let mut count = Count(0);
        for id in &delivery.audiences {
            serde_json::to_writer(&mut count, &frame(&delivery.change, id.as_ref()))?;
            io::Write::write_all(&mut count, b"\n")?;
        }
        let frames = delivery
            .audiences
            .iter()
            .map(|id| {
                serde_json::to_value(frame(&delivery.change, id.as_ref())).map_err(io::Error::other)
            })
            .collect::<io::Result<Vec<_>>>()?;
        let reserved = output.reserve_publication(&[Cycle {
            values: &frames,
            repetitions: 1,
        }])?;
        drop(reserved.commit());
    }
    Ok(())
}

/// Drain a finite snapshot before an immediate RPC response. Already admitted
/// notifications stay ahead of cancellation/discovery fences on the same pipe.
pub(crate) fn flush(owner: &mut Owner, output: &Output) -> io::Result<()> {
    for batch in owner.drain_ready() {
        publish(batch, output)?;
    }
    if let Some(reason) = owner.terminal_failure() {
        return Err(io::Error::other(reason));
    }
    Ok(())
}
